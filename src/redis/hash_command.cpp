#include "hash_command.h"

#include <charconv>
#include <limits>

#include "keylane/resp.h"

namespace keylane {

namespace {

storage::StorageEngine* g_storage = nullptr;

CommandReply BuiltReply(std::string_view encoded) {
  CommandReply reply;
  reply.encoded_ = encoded;
  return reply;
}

std::string_view AppendStorageError(ReplyBuilder& builder,
                                    const absl::Status& status) {
  if (status.message().starts_with("WRONGTYPE ")) {
    return builder.AppendError(status.message());
  }
  return builder.AppendError("ERR " + std::string(status.message()));
}

template <typename Integer>
bool ParseInteger(std::string_view text, Integer* output) {
  const auto parsed =
      std::from_chars(text.data(), text.data() + text.size(), *output);
  return parsed.ec == std::errc{} && parsed.ptr == text.data() + text.size();
}

bool EqualsIgnoreCase(std::string_view left, std::string_view lower) {
  if (left.size() != lower.size()) return false;
  for (std::size_t i = 0; i < left.size(); ++i) {
    unsigned char current = static_cast<unsigned char>(left[i]);
    if (current >= 'A' && current <= 'Z') current += 'a' - 'A';
    if (current != static_cast<unsigned char>(lower[i])) return false;
  }
  return true;
}

Task<CommandReply> ExecuteHashCommandImpl(const CommandRequest& request,
                                          const storage::Digest* digest,
                                          storage::TxShardWrites* tx,
                                          ReplyBuilder& reply_builder) {
  const auto& args = request.args_;
  if (request.kind_ == CommandKind::kHIncrBy ||
      request.kind_ == CommandKind::kHIncrByFloat) {
    MarkReplicationCommandHandled(request);
  }
  storage::HashOperation operation;
  switch (request.kind_) {
    case CommandKind::kHSet:
    case CommandKind::kHMSet:
      if ((args.size() - 2) % 2 != 0) {
        co_return BuiltReply(
            reply_builder.AppendError("ERR wrong number of arguments for '" +
                                      args.front() + "' command"));
      }
      operation.kind_ = storage::HashOperationKind::kSet;
      for (std::size_t i = 2; i < args.size(); i += 2) {
        operation.fields_.push_back(args[i]);
        operation.values_.push_back(args[i + 1]);
      }
      break;
    case CommandKind::kHSetNx:
      operation.kind_ = storage::HashOperationKind::kSetIfAbsent;
      operation.fields_.push_back(args[2]);
      operation.values_.push_back(args[3]);
      break;
    case CommandKind::kHGet:
      operation.kind_ = storage::HashOperationKind::kGet;
      operation.fields_.push_back(args[2]);
      break;
    case CommandKind::kHMGet:
      operation.kind_ = storage::HashOperationKind::kGetMany;
      for (std::size_t i = 2; i < args.size(); ++i) {
        operation.fields_.push_back(args[i]);
      }
      break;
    case CommandKind::kHDel:
      operation.kind_ = storage::HashOperationKind::kDelete;
      for (std::size_t i = 2; i < args.size(); ++i) {
        operation.fields_.push_back(args[i]);
      }
      break;
    case CommandKind::kHLen:
      operation.kind_ = storage::HashOperationKind::kLength;
      break;
    case CommandKind::kHExists:
      operation.kind_ = storage::HashOperationKind::kExists;
      operation.fields_.push_back(args[2]);
      break;
    case CommandKind::kHGetAll:
      operation.kind_ = storage::HashOperationKind::kGetAll;
      break;
    case CommandKind::kHKeys:
      operation.kind_ = storage::HashOperationKind::kKeys;
      break;
    case CommandKind::kHVals:
      operation.kind_ = storage::HashOperationKind::kValues;
      break;
    case CommandKind::kHStrlen:
      operation.kind_ = storage::HashOperationKind::kStringLength;
      operation.fields_.push_back(args[2]);
      break;
    case CommandKind::kHIncrBy:
      operation.kind_ = storage::HashOperationKind::kIncrementInteger;
      operation.fields_.push_back(args[2]);
      operation.values_.push_back(args[3]);
      break;
    case CommandKind::kHIncrByFloat:
      operation.kind_ = storage::HashOperationKind::kIncrementFloat;
      operation.fields_.push_back(args[2]);
      operation.values_.push_back(args[3]);
      break;
    case CommandKind::kHRandField:
      operation.kind_ = storage::HashOperationKind::kRandomFields;
      if (args.size() >= 3) {
        if (!ParseInteger(args[2], &operation.count_)) {
          co_return BuiltReply(reply_builder.AppendError(
              "ERR value is not an integer or out of range"));
        }
        operation.count_provided_ = true;
        if (operation.count_ == std::numeric_limits<std::int64_t>::min()) {
          co_return BuiltReply(
              reply_builder.AppendError("ERR value is out of range"));
        }
      }
      if (args.size() == 4) {
        if (!EqualsIgnoreCase(args[3], "withvalues")) {
          co_return BuiltReply(reply_builder.AppendError("ERR syntax error"));
        }
        const std::uint64_t magnitude = static_cast<std::uint64_t>(
            operation.count_ < 0 ? -operation.count_ : operation.count_);
        if (magnitude > static_cast<std::uint64_t>(
                            std::numeric_limits<std::int64_t>::max()) /
                            2) {
          co_return BuiltReply(
              reply_builder.AppendError("ERR value is out of range"));
        }
        operation.with_values_ = true;
      }
      break;
    case CommandKind::kHScan: {
      operation.kind_ = storage::HashOperationKind::kScan;
      if (!ParseInteger(args[2], &operation.cursor_)) {
        co_return BuiltReply(reply_builder.AppendError("ERR invalid cursor"));
      }
      for (std::size_t i = 3; i < args.size();) {
        if (EqualsIgnoreCase(args[i], "match") && i + 1 < args.size()) {
          operation.match_ = args[i + 1];
          i += 2;
        } else if (EqualsIgnoreCase(args[i], "count") && i + 1 < args.size()) {
          if (!ParseInteger(args[i + 1], &operation.scan_count_) ||
              operation.scan_count_ == 0) {
            co_return BuiltReply(reply_builder.AppendError("ERR syntax error"));
          }
          i += 2;
        } else {
          co_return BuiltReply(reply_builder.AppendError("ERR syntax error"));
        }
      }
      break;
    }
    default:
      co_return BuiltReply(
          reply_builder.AppendError("ERR unsupported Hash command path"));
  }

  // Both locked and ordinary dispatch assign this placeholder. A non-empty
  // diagnostic here only allocates a StatusRep that is discarded immediately.
  absl::StatusOr<storage::HashResult> result;
  auto replication = tx == nullptr ? PrepareReplicationCommand(request)
                                   : std::nullopt;
  if (digest == nullptr) {
    result = co_await g_storage->ExecuteHash(
        request.db_id_, args[1], operation,
        replication ? &*replication : nullptr);
  } else {
    result = co_await g_storage->ExecuteHashLocked(request.db_id_, args[1],
                                                   *digest, operation, tx,
                                                   replication ? &*replication
                                                               : nullptr);
  }
  if (!result.ok()) {
    co_return BuiltReply(AppendStorageError(reply_builder, result.status()));
  }
  if (request.kind_ == CommandKind::kHIncrBy) {
    CaptureReplicationCommand(
        request, {"HSET", args[1], args[2],
                  std::to_string(result->signed_integer_)});
  } else if (request.kind_ == CommandKind::kHIncrByFloat) {
    CaptureReplicationCommand(request,
                              {"HSET", args[1], args[2], result->scalar_});
  }
  switch (request.kind_) {
    case CommandKind::kHSet:
    case CommandKind::kHSetNx:
    case CommandKind::kHDel:
      co_return BuiltReply(reply_builder.AppendInteger(result->integer_));
    case CommandKind::kHMSet:
      co_return BuiltReply(reply_builder.AppendSimpleString("OK"));
    case CommandKind::kHLen:
      co_return BuiltReply(reply_builder.AppendInteger(result->length_));
    case CommandKind::kHExists:
    case CommandKind::kHStrlen:
      co_return BuiltReply(reply_builder.AppendInteger(result->integer_));
    case CommandKind::kHIncrBy:
      co_return BuiltReply(
          reply_builder.AppendInteger(result->signed_integer_));
    case CommandKind::kHIncrByFloat:
      // RESP3 has a native double; RESP2 keeps Redis' bulk-string shape.
      co_return BuiltReply(reply_builder.AppendDoubleText(result->scalar_));
    case CommandKind::kHGet:
      co_return BuiltReply(
          result->values_.empty() || !result->values_.front().has_value()
              ? reply_builder.AppendNull()
              : reply_builder.AppendBulkString(*result->values_.front()));
    case CommandKind::kHMGet:
    case CommandKind::kHKeys:
    case CommandKind::kHVals:
      reply_builder.AppendArrayHeader(result->values_.size());
      for (const auto& value : result->values_) {
        if (value.has_value()) {
          reply_builder.AppendBulkString(*value);
        } else {
          reply_builder.AppendNull();
        }
      }
      co_return BuiltReply(reply_builder.View());
    case CommandKind::kHGetAll:
      reply_builder.AppendMapHeader(result->values_.size() / 2);
      for (const auto& value : result->values_) {
        if (value.has_value()) {
          reply_builder.AppendBulkString(*value);
        } else {
          reply_builder.AppendNull();
        }
      }
      co_return BuiltReply(reply_builder.View());
    case CommandKind::kHRandField:
      if (!operation.count_provided_) {
        co_return BuiltReply(
            result->values_.empty() || !result->values_.front().has_value()
                ? reply_builder.AppendNull()
                : reply_builder.AppendBulkString(*result->values_.front()));
      }
      if (operation.with_values_ &&
          reply_builder.version() == RespVersion::k3) {
        reply_builder.AppendArrayHeader(result->values_.size() / 2);
        for (std::size_t i = 0; i < result->values_.size(); i += 2) {
          reply_builder.AppendArrayHeader(2);
          reply_builder.AppendBulkString(*result->values_[i]);
          reply_builder.AppendBulkString(*result->values_[i + 1]);
        }
      } else {
        reply_builder.AppendArrayHeader(result->values_.size());
        for (const auto& value : result->values_) {
          reply_builder.AppendBulkString(*value);
        }
      }
      co_return BuiltReply(reply_builder.View());
    case CommandKind::kHScan:
      reply_builder.AppendArrayHeader(2);
      reply_builder.AppendBulkString(std::to_string(result->cursor_));
      reply_builder.AppendArrayHeader(result->values_.size());
      for (const auto& value : result->values_) {
        reply_builder.AppendBulkString(*value);
      }
      co_return BuiltReply(reply_builder.View());
    default:
      break;
  }
  co_return BuiltReply(reply_builder.AppendError("ERR unreachable Hash reply"));
}

}  // namespace

void InitHashCommandStorage(storage::StorageEngine* engine) {
  g_storage = engine;
}

Task<CommandReply> ExecuteHashCommand(const CommandRequest& request,
                                      ReplyBuilder& reply_builder) {
  return ExecuteHashCommandImpl(request, nullptr, nullptr, reply_builder);
}

Task<CommandReply> ExecuteHashCommandLocked(const CommandRequest& request,
                                            const storage::Digest& digest,
                                            storage::TxShardWrites* tx,
                                            ReplyBuilder& reply_builder) {
  return ExecuteHashCommandImpl(request, &digest, tx, reply_builder);
}

}  // namespace keylane
