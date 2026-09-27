/*
 * Copyright (C) 2026 EloqData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "list_command.h"

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "bycorf/runtime/cross_core.h"
#include "bycorf/runtime/worker.h"
#include "cluster_gate.h"
#include "lavik/command_table.h"
#include "lavik/redis_parse.h"
#include "lavik/resp.h"
#include "lavik/storage/engine.h"
#include "lavik/tx/transaction.h"
#include "lavik/tx/tx_shard.h"

namespace lavik {
using namespace bycorf;

namespace {

storage::StorageEngine* g_storage = nullptr;

CommandReply BuiltReply(std::string_view encoded) {
  CommandReply reply;
  reply.encoded_ = encoded;
  return reply;
}

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

bool ParseInt64(std::string_view text, std::int64_t* value) {
  if (value == nullptr || text.empty()) return false;
  const char* begin = text.data();
  const char* end = begin + text.size();
  const auto [parsed_end, error] = std::from_chars(begin, end, *value);
  return error == std::errc{} && parsed_end == end;
}

bool ParseNonNegative(std::string_view text, std::uint64_t* value) {
  std::int64_t parsed = 0;
  if (!ParseInt64(text, &parsed) || parsed < 0) return false;
  *value = static_cast<std::uint64_t>(parsed);
  return true;
}

bool ParseRespCount(std::string_view encoded, std::size_t* offset, char prefix,
                    std::uint64_t* value) {
  if (*offset >= encoded.size() || encoded[*offset] != prefix) return false;
  const std::size_t begin = ++*offset;
  const std::size_t end = encoded.find("\r\n", begin);
  if (end == std::string_view::npos || end == begin) return false;
  const auto parsed =
      std::from_chars(encoded.data() + begin, encoded.data() + end, *value);
  if (parsed.ec != std::errc{} || parsed.ptr != encoded.data() + end) {
    return false;
  }
  *offset = end + 2;
  return true;
}

bool ParseRespBulk(std::string_view encoded, std::size_t* offset,
                   std::string_view* value) {
  std::uint64_t bytes = 0;
  if (!ParseRespCount(encoded, offset, '$', &bytes) ||
      bytes > encoded.size() - *offset ||
      encoded.size() - *offset - static_cast<std::size_t>(bytes) < 2) {
    return false;
  }
  *value = encoded.substr(*offset, static_cast<std::size_t>(bytes));
  *offset += static_cast<std::size_t>(bytes);
  if (encoded.substr(*offset, 2) != "\r\n") return false;
  *offset += 2;
  return true;
}

std::string_view AppendStorageError(ReplyBuilder& reply_builder,
                                    const absl::Status& status) {
  if (status.code() == absl::StatusCode::kResourceExhausted &&
      status.message().starts_with("OOM ")) {
    return reply_builder.AppendError(status.message());
  }
  return status.message().starts_with("WRONGTYPE ")
             ? reply_builder.AppendError(status.message())
             : reply_builder.AppendError("ERR ", status.message());
}

void AppendBulkArray(ReplyBuilder& builder,
                     const std::vector<std::string>& values) {
  builder.AppendArrayHeader(values.size());
  for (const std::string& value : values) builder.AppendBulkString(value);
}

Task<absl::Status> ListLockHoldCallback(void*, const tx::ShardSlice&) {
  co_return absl::OkStatus();
}

absl::StatusOr<bool> ParseListLeft(std::string_view value) {
  if (CmpCaseInsensitive(value, "left")) return true;
  if (CmpCaseInsensitive(value, "right")) return false;
  return absl::Status(absl::StatusCode::kInvalidArgument, "syntax error");
}

std::vector<std::string> EncodeListMoveEffects(
    std::uint8_t db_id, std::string_view source, std::string_view destination,
    bool source_left, bool destination_left, std::string_view value) {
  std::vector<CapturedReplicationCommand> effects;
  effects.reserve(2);
  effects.push_back(CapturedReplicationCommand{
      db_id, {source_left ? "LPOP" : "RPOP", std::string(source)}});
  effects.push_back(CapturedReplicationCommand{
      db_id,
      {destination_left ? "LPUSH" : "RPUSH", std::string(destination),
       std::string(value)}});
  return EncodeReplicationCommandEffects(std::move(effects));
}

std::vector<std::string> EncodeListPopEffect(std::string_view key, bool left,
                                             std::size_t count) {
  return {left ? "LPOP" : "RPOP", std::string(key), std::to_string(count)};
}

struct SingleShardListOutcome {
  explicit SingleShardListOutcome(absl::Status status, std::string key = {},
                                  std::vector<std::string> values = {})
      : status_(std::move(status)),
        key_(std::move(key)),
        values_(std::move(values)) {}

  absl::Status status_;
  std::string key_;
  std::vector<std::string> values_;
};

Task<SingleShardListOutcome> ExecuteSingleShardListMulti(
    std::uint8_t db_id, std::vector<std::string> keys, bool move,
    bool source_left, bool destination_left, bool pop_left,
    std::uint64_t pop_count, ReplicationTransactionGuard* replication,
    storage::MutationPrecondition mutation_precondition) {
  std::vector<tx::KeyRef> locks;
  locks.reserve(keys.size());
  for (const std::string& key : keys) {
    const tx::LockFp fingerprint =
        tx::FingerprintOf(storage::ComputeDigest(key));
    bool duplicate = false;
    for (const tx::KeyRef& lock : locks) {
      if (lock.db_ == db_id && lock.fp_ == fingerprint) {
        duplicate = true;
        break;
      }
    }
    if (!duplicate) {
      locks.push_back(tx::KeyRef{fingerprint, tx::LockMode::kExclusive, db_id});
    }
  }
  auto guard = co_await tx::CurrentTxShard().AcquireKeys(locks);
  if (replication != nullptr) replication->EnterCurrentShard();

  if (!move) {
    for (const std::string& key : keys) {
      storage::ListOperation pop;
      pop.kind_ = pop_left ? storage::ListOperationKind::kPopLeft
                           : storage::ListOperationKind::kPopRight;
      pop.count_ = pop_count;
      pop.count_provided_ = true;
      auto result = co_await g_storage->ExecuteListLocked(
          db_id, key, storage::ComputeDigest(key), pop, nullptr, nullptr,
          &mutation_precondition);
      if (!result.ok()) {
        co_return SingleShardListOutcome(result.status());
      }
      if (!result->values_.empty()) {
        co_return SingleShardListOutcome(absl::OkStatus(), key,
                                         std::move(result->values_));
      }
    }
    co_return SingleShardListOutcome(absl::OkStatus());
  }

  const std::string& source = keys[0];
  const std::string& destination = keys[1];
  if (source == destination) {
    const std::uint64_t txid = storage::StorageEngine::AllocateWriteTxid();
    storage::TxShardWrites writes;
    g_storage->InitializeTxWrites(txid, std::span(&writes, 1),
                                  mutation_precondition);
    writes.collect_undo_ = true;
    storage::ListOperation operation;
    operation.kind_ = storage::ListOperationKind::kMoveWithin;
    operation.first_ = source_left ? 1 : 0;
    operation.second_ = destination_left ? 1 : 0;
    auto result = co_await g_storage->ExecuteListLocked(
        db_id, source, storage::ComputeDigest(source), operation, &writes);
    if (!result.ok()) {
      co_return SingleShardListOutcome(result.status());
    }
    if (result->values_.empty()) {
      co_return SingleShardListOutcome(absl::OkStatus());
    }
    std::vector<storage::TxShardWrites*> write_refs{&writes};
    absl::Status committed =
        co_await g_storage->CommitTxWrites(txid, std::move(write_refs));
    if (!committed.ok()) {
      (void)co_await g_storage->RollbackTxLocal(txid);
      co_return SingleShardListOutcome(std::move(committed));
    }
    (void)co_await g_storage->DiscardTxUndoLocal(txid);
    if (replication != nullptr) {
      replication->SetCommandArgs(
          EncodeListMoveEffects(db_id, source, destination, source_left,
                                destination_left, result->values_.front()));
      replication->SetFinalExpirations(
          std::span<const storage::TxShardWrites>(&writes, 1));
    }
    co_return SingleShardListOutcome(absl::OkStatus(), {},
                                     std::move(result->values_));
  }

  const std::uint64_t txid = storage::StorageEngine::AllocateWriteTxid();
  storage::TxShardWrites writes;
  g_storage->InitializeTxWrites(txid, std::span(&writes, 1),
                                mutation_precondition);
  writes.collect_undo_ = true;
  storage::ListOperation pop;
  pop.kind_ = source_left ? storage::ListOperationKind::kPopLeft
                          : storage::ListOperationKind::kPopRight;
  pop.count_ = 1;
  auto popped = co_await g_storage->ExecuteListLocked(
      db_id, source, storage::ComputeDigest(source), pop, &writes);
  if (!popped.ok()) {
    co_return SingleShardListOutcome(popped.status());
  }
  if (popped->values_.empty()) {
    co_return SingleShardListOutcome(absl::OkStatus());
  }

  storage::ListOperation push;
  push.kind_ = destination_left ? storage::ListOperationKind::kPushLeft
                                : storage::ListOperationKind::kPushRight;
  push.values_.push_back(popped->values_.front());
  auto pushed = co_await g_storage->ExecuteListLocked(
      db_id, destination, storage::ComputeDigest(destination), push, &writes);
  if (!pushed.ok()) {
    (void)co_await g_storage->RollbackTxLocal(txid);
    co_return SingleShardListOutcome(pushed.status());
  }
  // The source and destination records form one logical Redis move.
  writes.dataset_changes_ = 1;
  std::vector<storage::TxShardWrites*> write_refs{&writes};
  absl::Status committed =
      co_await g_storage->CommitTxWrites(txid, std::move(write_refs));
  if (!committed.ok()) {
    (void)co_await g_storage->RollbackTxLocal(txid);
    co_return SingleShardListOutcome(std::move(committed));
  }
  (void)co_await g_storage->DiscardTxUndoLocal(txid);
  if (replication != nullptr) {
    replication->SetCommandArgs(
        EncodeListMoveEffects(db_id, source, destination, source_left,
                              destination_left, popped->values_.front()));
    replication->SetFinalExpirations(
        std::span<const storage::TxShardWrites>(&writes, 1));
  }
  co_return SingleShardListOutcome(absl::OkStatus(), {},
                                   std::move(popped->values_));
}

}  // namespace

void InitListCommandStorage(storage::StorageEngine* engine) {
  g_storage = engine;
}

Task<CommandReply> ExecuteSingleListCommandImpl(const CommandRequest& request,
                                                const storage::Digest* digest,
                                                storage::TxShardWrites* tx,
                                                ReplyBuilder& reply_builder) {
  const auto& args = request.args_;
  storage::ListOperation op;
  switch (request.kind_) {
    case CommandKind::kLPush:
    case CommandKind::kLPushX:
    case CommandKind::kRPush:
    case CommandKind::kRPushX: {
      op.kind_ = request.kind_ == CommandKind::kLPush
                     ? storage::ListOperationKind::kPushLeft
                 : request.kind_ == CommandKind::kLPushX
                     ? storage::ListOperationKind::kPushLeftIfExists
                 : request.kind_ == CommandKind::kRPush
                     ? storage::ListOperationKind::kPushRight
                     : storage::ListOperationKind::kPushRightIfExists;
      op.values_.reserve(args.size() - 2);
      for (std::size_t i = 2; i < args.size(); ++i) {
        op.values_.push_back(args[i]);
      }
      break;
    }
    case CommandKind::kLPop:
    case CommandKind::kRPop:
      op.kind_ = request.kind_ == CommandKind::kLPop
                     ? storage::ListOperationKind::kPopLeft
                     : storage::ListOperationKind::kPopRight;
      op.count_provided_ = args.size() == 3;
      op.count_ = 1;
      if (op.count_provided_ && !ParseNonNegative(args[2], &op.count_)) {
        co_return BuiltReply(reply_builder.AppendError(
            "ERR value is out of range, must be positive"));
      }
      break;
    case CommandKind::kLLen:
      op.kind_ = storage::ListOperationKind::kLength;
      break;
    case CommandKind::kLIndex:
      op.kind_ = storage::ListOperationKind::kIndex;
      if (!ParseInt64(args[2], &op.first_)) {
        co_return BuiltReply(reply_builder.AppendError(
            "ERR value is not an integer or out of range"));
      }
      break;
    case CommandKind::kLRange:
    case CommandKind::kLTrim:
      op.kind_ = request.kind_ == CommandKind::kLRange
                     ? storage::ListOperationKind::kRange
                     : storage::ListOperationKind::kTrim;
      if (!ParseInt64(args[2], &op.first_) ||
          !ParseInt64(args[3], &op.second_)) {
        co_return BuiltReply(reply_builder.AppendError(
            "ERR value is not an integer or out of range"));
      }
      break;
    case CommandKind::kLSet:
      op.kind_ = storage::ListOperationKind::kSet;
      if (!ParseInt64(args[2], &op.first_)) {
        co_return BuiltReply(reply_builder.AppendError(
            "ERR value is not an integer or out of range"));
      }
      op.value_ = args[3];
      break;
    case CommandKind::kLInsert:
      if (CmpCaseInsensitive(args[2], "before")) {
        op.kind_ = storage::ListOperationKind::kInsertBefore;
      } else if (CmpCaseInsensitive(args[2], "after")) {
        op.kind_ = storage::ListOperationKind::kInsertAfter;
      } else {
        co_return BuiltReply(reply_builder.AppendError("ERR syntax error"));
      }
      op.pivot_ = args[3];
      op.value_ = args[4];
      break;
    case CommandKind::kLRem:
      op.kind_ = storage::ListOperationKind::kRemove;
      if (!ParseInt64(args[2], &op.first_)) {
        co_return BuiltReply(reply_builder.AppendError(
            "ERR value is not an integer or out of range"));
      }
      op.value_ = args[3];
      break;
    case CommandKind::kLPos: {
      op.kind_ = storage::ListOperationKind::kPosition;
      op.value_ = args[2];
      for (std::size_t i = 3; i < args.size();) {
        if (CmpCaseInsensitive(args[i], "rank")) {
          if (++i >= args.size() || !ParseInt64(args[i], &op.rank_)) {
            co_return BuiltReply(reply_builder.AppendError(
                "ERR value is not an integer or out of range"));
          }
          if (op.rank_ == std::numeric_limits<std::int64_t>::min()) {
            co_return BuiltReply(
                reply_builder.AppendError("ERR value is out of range"));
          }
          if (op.rank_ == 0) {
            co_return BuiltReply(reply_builder.AppendError(
                "ERR RANK can't be zero: use 1 to start from the first "
                "match, 2 from the second ... or use negative to start from "
                "the end of the list"));
          }
          ++i;
        } else if (CmpCaseInsensitive(args[i], "count")) {
          op.count_provided_ = true;
          if (++i >= args.size() || !ParseNonNegative(args[i], &op.count_)) {
            co_return BuiltReply(reply_builder.AppendError(
                "ERR value is out of range, must be positive"));
          }
          ++i;
        } else if (CmpCaseInsensitive(args[i], "maxlen")) {
          op.max_length_provided_ = true;
          if (++i >= args.size() ||
              !ParseNonNegative(args[i], &op.max_length_)) {
            co_return BuiltReply(reply_builder.AppendError(
                "ERR value is out of range, must be positive"));
          }
          ++i;
        } else {
          co_return BuiltReply(reply_builder.AppendError("ERR syntax error"));
        }
      }
      break;
    }
    default:
      co_return BuiltReply(
          reply_builder.AppendError("ERR unsupported List command path"));
  }

  // This handler accepts only single-key commands. LMPOP/BLPOP and LMOVE use
  // separate paths, some without durable receipts, so tx == nullptr alone
  // cannot authorize releasing store state while they prepare a mutation.
  op.prepare_unlocked_ = true;
  absl::StatusOr<storage::ListResult> result;
  auto replication =
      tx == nullptr ? PrepareReplicationCommand(request) : std::nullopt;
  if (digest == nullptr) {
    const storage::MutationPrecondition mutation_precondition =
        ClusterMutationPrecondition(request);
    result = co_await g_storage->ExecuteList(
        request.db_id_, args[1], op, replication ? &*replication : nullptr,
        &mutation_precondition);
  } else {
    result = co_await g_storage->ExecuteListLocked(
        request.db_id_, args[1], *digest, op, tx,
        replication ? &*replication : nullptr);
  }
  if (!result.ok()) {
    co_return BuiltReply(AppendStorageError(reply_builder, result.status()));
  }
  switch (request.kind_) {
    case CommandKind::kLPush:
    case CommandKind::kLPushX:
    case CommandKind::kRPush:
    case CommandKind::kRPushX:
    case CommandKind::kLPop:
    case CommandKind::kRPop:
    case CommandKind::kLSet:
    case CommandKind::kLInsert:
    case CommandKind::kLRem:
    case CommandKind::kLTrim:
      if (result->length_ != 0) {
        NotifyListBlockingKey(request, args[1]);
      }
      break;
    default:
      break;
  }
  switch (request.kind_) {
    case CommandKind::kLPush:
    case CommandKind::kLPushX:
    case CommandKind::kRPush:
    case CommandKind::kRPushX:
    case CommandKind::kLLen:
      co_return BuiltReply(
          reply_builder.AppendInteger(static_cast<long long>(result->length_)));
    case CommandKind::kLPop:
    case CommandKind::kRPop:
      if (op.count_provided_) {
        if (!result->key_exists_) {
          co_return BuiltReply(reply_builder.AppendNullArray());
        }
        AppendBulkArray(reply_builder, result->values_);
        co_return BuiltReply(reply_builder.View());
      }
      co_return BuiltReply(
          result->values_.empty()
              ? reply_builder.AppendNull()
              : reply_builder.AppendBulkString(result->values_.front()));
    case CommandKind::kLIndex:
      co_return BuiltReply(
          result->values_.empty()
              ? reply_builder.AppendNull()
              : reply_builder.AppendBulkString(result->values_.front()));
    case CommandKind::kLRange:
      AppendBulkArray(reply_builder, result->values_);
      co_return BuiltReply(reply_builder.View());
    case CommandKind::kLSet:
    case CommandKind::kLTrim:
      co_return BuiltReply(reply_builder.AppendSimpleString("OK"));
    case CommandKind::kLInsert:
      co_return BuiltReply(reply_builder.AppendInteger(
          result->integer_ == -1 ? -1
                                 : static_cast<long long>(result->length_)));
    case CommandKind::kLRem:
      co_return BuiltReply(reply_builder.AppendInteger(result->integer_));
    case CommandKind::kLPos:
      if (op.count_provided_) {
        reply_builder.AppendArrayHeader(result->positions_.size());
        for (std::int64_t position : result->positions_) {
          reply_builder.AppendInteger(position);
        }
        co_return BuiltReply(reply_builder.View());
      }
      co_return BuiltReply(
          result->positions_.empty()
              ? reply_builder.AppendNull()
              : reply_builder.AppendInteger(result->positions_.front()));
    default:
      break;
  }
  co_return BuiltReply(reply_builder.AppendError("ERR unreachable List reply"));
}

Task<CommandReply> ExecuteSingleListCommand(const CommandRequest& request,
                                            ReplyBuilder& reply_builder) {
  return ExecuteSingleListCommandImpl(request, nullptr, nullptr, reply_builder);
}

Task<CommandReply> ExecuteSingleListCommandLocked(const CommandRequest& request,
                                                  const storage::Digest& digest,
                                                  storage::TxShardWrites* tx,
                                                  ReplyBuilder& reply_builder) {
  return ExecuteSingleListCommandImpl(request, &digest, tx, reply_builder);
}
Task<CommandReply> ExecuteListMultiKey(const CommandRequest& request,
                                       ReplyBuilder& reply_builder,
                                       bool* unavailable) {
  if (unavailable != nullptr) *unavailable = false;
  const auto& args = request.args_;
  const storage::MutationPrecondition mutation_precondition =
      ClusterMutationPrecondition(request);
  const bool move = request.kind_ == CommandKind::kLMove ||
                    request.kind_ == CommandKind::kRPopLPush;
  std::vector<std::size_t> key_args;
  bool source_left = false;
  bool destination_left = true;
  bool pop_left = true;
  std::uint64_t pop_count = 1;

  if (move) {
    key_args = {1, 2};
    if (request.kind_ == CommandKind::kLMove) {
      auto source = ParseListLeft(args[3]);
      auto destination = ParseListLeft(args[4]);
      if (!source.ok() || !destination.ok()) {
        co_return BuiltReply(reply_builder.AppendError("ERR syntax error"));
      }
      source_left = *source;
      destination_left = *destination;
    }
  } else {
    std::int64_t numkeys = 0;
    if (args.size() < 4 || !ParseInt64(args[1], &numkeys) || numkeys <= 0) {
      co_return BuiltReply(
          reply_builder.AppendError("ERR numkeys should be greater than 0"));
    }
    const std::size_t count = static_cast<std::size_t>(numkeys);
    if (count > args.size() - 3) {
      co_return BuiltReply(reply_builder.AppendError("ERR syntax error"));
    }
    for (std::size_t i = 0; i < count; ++i) key_args.push_back(2 + i);
    const std::size_t direction_arg = 2 + count;
    auto direction = ParseListLeft(args[direction_arg]);
    if (!direction.ok()) {
      co_return BuiltReply(reply_builder.AppendError("ERR syntax error"));
    }
    pop_left = *direction;
    std::size_t next = direction_arg + 1;
    if (next < args.size()) {
      if (!CmpCaseInsensitive(args[next], "count") || next + 2 != args.size()) {
        co_return BuiltReply(reply_builder.AppendError("ERR syntax error"));
      }
      std::int64_t parsed_count = 0;
      if (!ParseInt64(args[next + 1], &parsed_count) || parsed_count <= 0) {
        co_return BuiltReply(
            reply_builder.AppendError("ERR count should be greater than 0"));
      }
      pop_count = static_cast<std::uint64_t>(parsed_count);
    }
  }

  const unsigned first_owner = ShardForKey(args[key_args.front()]);
  bool single_shard = true;
  std::vector<std::string> keys;
  keys.reserve(key_args.size());
  for (std::size_t arg : key_args) {
    single_shard &= ShardForKey(args[arg]) == first_owner;
    keys.push_back(args[arg]);
  }
  struct SnapshotAttemptGuard {
    bool snapshot_active_ = false;
    bool order_active_ = false;
    ~SnapshotAttemptGuard() {
      if (snapshot_active_) EndSnapshotTransaction();
      if (order_active_) EndReplicationTransactionOrder();
    }
  } snapshot_attempt;
  if (!request.replication_origin_ && request.spec_ != nullptr &&
      (request.spec_->flags_ & kCmdMayBlock) != 0 &&
      g_storage->ReplicationLogActive()) {
    while (!TryBeginReplicationTransactionOrder()) {
      absl::Status waited = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) {
        co_return BuiltReply(
            reply_builder.AppendError("ERR ", waited.message()));
      }
    }
    snapshot_attempt.order_active_ = true;
    while (!TryBeginSnapshotTransaction()) {
      absl::Status waited = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) {
        co_return BuiltReply(
            reply_builder.AppendError("ERR ", waited.message()));
      }
    }
    snapshot_attempt.snapshot_active_ = true;
  }
  if (single_shard) {
    ReplicationTransactionGuard replication(request,
                                            std::vector<unsigned>{first_owner});
    if (!replication.status().ok()) {
      co_return BuiltReply(
          AppendStorageError(reply_builder, replication.status()));
    }
    SingleShardListOutcome outcome = co_await bycorf::SubmitTaskTo(
        first_owner,
        [&request, db_id = request.db_id_, keys = std::move(keys), move,
         source_left, destination_left, pop_left, pop_count,
         mutation_precondition,
         replication = &replication]() mutable -> Task<SingleShardListOutcome> {
          // Choke point 2 for the transaction-free single-shard path: re-check
          // the cluster admission on the owner right before mutating. Cluster
          // admission guarantees one slot, hence this path.
          const absl::Status authority =
              RecheckClusterRequestAuthority(request);
          if (!authority.ok()) {
            co_return SingleShardListOutcome(authority);
          }
          co_return co_await ExecuteSingleShardListMulti(
              db_id, std::move(keys), move, source_left, destination_left,
              pop_left, pop_count, replication, mutation_precondition);
        });
    if (!outcome.status_.ok()) {
      if (IsClusterAuthorityChanged(outcome.status_)) {
        // The re-check fired before the hop mutated anything.
        co_return ClusterAuthorityChangedReply(
            request.ClusterSlots(), request.connection_tls_, reply_builder);
      }
      co_return BuiltReply(AppendStorageError(reply_builder, outcome.status_));
    }
    if (outcome.values_.empty()) {
      if (unavailable != nullptr) *unavailable = true;
      co_return BuiltReply(move ? reply_builder.AppendNull()
                                : reply_builder.AppendNullArray());
    }
    if (!move) {
      replication.SetCommandArgs(
          EncodeListPopEffect(outcome.key_, pop_left, outcome.values_.size()));
    }
    replication.Commit();
    for (std::size_t arg : key_args) {
      NotifyListBlockingKey(request, args[arg]);
    }
    if (move) {
      co_return BuiltReply(
          reply_builder.AppendBulkString(outcome.values_.front()));
    }
    reply_builder.AppendArrayHeader(2);
    reply_builder.AppendBulkString(outcome.key_);
    AppendBulkArray(reply_builder, outcome.values_);
    co_return BuiltReply(reply_builder.View());
  }

  tx::Transaction txn;
  for (std::size_t arg : key_args) {
    txn.AddKey(ShardForKey(args[arg]), request.db_id_,
               storage::ComputeDigest(args[arg]),
               static_cast<std::uint32_t>(arg), tx::LockMode::kExclusive);
  }
  txn.Seal();
  ClusterShardValidatorContext cluster_validator;
  InstallClusterShardValidator(txn, request, cluster_validator);
  ReplicationTransactionGuard replication(request, &txn);
  if (!replication.status().ok()) {
    co_return BuiltReply(
        AppendStorageError(reply_builder, replication.status()));
  }
  absl::Status status = co_await txn.Schedule();
  if (!status.ok()) {
    co_return BuiltReply(reply_builder.AppendError("ERR ", status.message()));
  }
  status = co_await txn.Execute(&ListLockHoldCallback, nullptr, false);
  if (!status.ok()) {
    if (cluster_validator.tripped_.load(std::memory_order_relaxed)) {
      // The hold hop retains its locks on failure; drop them before
      // answering. Nothing mutated: ListLockHoldCallback never ran.
      (void)co_await txn.Release();
      co_return ClusterValidatorFailureReply(
          txn, cluster_validator, request.connection_tls_, reply_builder);
    }
    co_return BuiltReply(reply_builder.AppendError("ERR ", status.message()));
  }

  auto release = [&]() -> Task<absl::Status> {
    // The settle hop must not be fenced off retroactively: it releases the
    // holds under which earlier hops already mutated (or not).
    txn.SetShardValidator(nullptr, nullptr);
    co_return co_await txn.Execute(&ListLockHoldCallback, nullptr, true);
  };

  if (!move) {
    for (std::size_t arg : key_args) {
      storage::ListOperation op;
      op.kind_ = pop_left ? storage::ListOperationKind::kPopLeft
                          : storage::ListOperationKind::kPopRight;
      op.count_ = pop_count;
      op.count_provided_ = true;
      const storage::Digest digest = storage::ComputeDigest(args[arg]);
      auto popped = co_await bycorf::SubmitTaskTo(
          ShardForKey(args[arg]),
          [db = request.db_id_, key = std::string(args[arg]), digest, op,
           mutation_precondition]() mutable
              -> Task<absl::StatusOr<storage::ListResult>> {
            co_return co_await g_storage->ExecuteListLocked(
                db, key, digest, op, nullptr, nullptr, &mutation_precondition);
          });
      if (!popped.ok()) {
        (void)co_await release();
        co_return BuiltReply(
            AppendStorageError(reply_builder, popped.status()));
      }
      if (!popped->values_.empty()) {
        status = co_await release();
        if (!status.ok()) {
          co_return BuiltReply(
              reply_builder.AppendError("ERR ", status.message()));
        }
        replication.SetCommandArgs(
            EncodeListPopEffect(args[arg], pop_left, popped->values_.size()));
        replication.Commit();
        reply_builder.AppendArrayHeader(2);
        reply_builder.AppendBulkString(args[arg]);
        AppendBulkArray(reply_builder, popped->values_);
        for (std::size_t key_arg : key_args) {
          NotifyListBlockingKey(request, args[key_arg]);
        }
        co_return BuiltReply(reply_builder.View());
      }
    }
    (void)co_await release();
    if (unavailable != nullptr) *unavailable = true;
    co_return BuiltReply(reply_builder.AppendNullArray());
  }

  const std::string_view source_key = args[1];
  const std::string_view destination_key = args[2];
  if (source_key == destination_key) {
    storage::ListOperation op;
    op.kind_ = storage::ListOperationKind::kMoveWithin;
    op.first_ = source_left ? 1 : 0;
    op.second_ = destination_left ? 1 : 0;
    const storage::Digest digest = storage::ComputeDigest(source_key);
    auto moved = co_await bycorf::SubmitTaskTo(
        ShardForKey(source_key),
        [db = request.db_id_, key = std::string(source_key), digest, op,
         mutation_precondition]() mutable
            -> Task<absl::StatusOr<storage::ListResult>> {
          co_return co_await g_storage->ExecuteListLocked(
              db, key, digest, op, nullptr, nullptr, &mutation_precondition);
        });
    (void)co_await release();
    if (!moved.ok()) {
      co_return BuiltReply(AppendStorageError(reply_builder, moved.status()));
    }
    if (moved->values_.empty() && unavailable != nullptr) *unavailable = true;
    if (!moved->values_.empty()) replication.Commit();
    co_return BuiltReply(
        moved->values_.empty()
            ? reply_builder.AppendNull()
            : reply_builder.AppendBulkString(moved->values_.front()));
  }

  const std::uint64_t txid = storage::StorageEngine::AllocateWriteTxid();
  std::vector<storage::TxShardWrites> writes(g_storage->worker_count());
  g_storage->InitializeTxWrites(txid, writes, mutation_precondition);
  for (auto& write : writes) {
    write.collect_undo_ = true;
  }
  storage::ListOperation pop;
  pop.kind_ = source_left ? storage::ListOperationKind::kPopLeft
                          : storage::ListOperationKind::kPopRight;
  pop.count_ = 1;
  const storage::Digest source_digest = storage::ComputeDigest(source_key);
  const unsigned source_owner = ShardForKey(source_key);
  auto popped = co_await bycorf::SubmitTaskTo(
      source_owner, [db = request.db_id_, key = std::string(source_key),
                     source_digest, pop, write = &writes[source_owner]] {
        return g_storage->ExecuteListLocked(db, key, source_digest, pop, write);
      });
  if (!popped.ok() || popped->values_.empty()) {
    (void)co_await release();
    if (!popped.ok()) {
      co_return BuiltReply(AppendStorageError(reply_builder, popped.status()));
    }
    if (unavailable != nullptr) *unavailable = true;
    co_return BuiltReply(reply_builder.AppendNull());
  }

  storage::ListOperation push;
  push.kind_ = destination_left ? storage::ListOperationKind::kPushLeft
                                : storage::ListOperationKind::kPushRight;
  push.values_.push_back(popped->values_.front());
  const storage::Digest destination_digest =
      storage::ComputeDigest(destination_key);
  const unsigned destination_owner = ShardForKey(destination_key);
  auto pushed = co_await bycorf::SubmitTaskTo(
      destination_owner,
      [db = request.db_id_, key = std::string(destination_key),
       destination_digest, push, write = &writes[destination_owner]] {
        return g_storage->ExecuteListLocked(db, key, destination_digest, push,
                                            write);
      });
  if (!pushed.ok()) {
    for (const unsigned owner : {source_owner, destination_owner}) {
      (void)co_await bycorf::SubmitTaskTo(
          owner, [txid] { return g_storage->RollbackTxLocal(txid); });
    }
    (void)co_await release();
    co_return BuiltReply(AppendStorageError(reply_builder, pushed.status()));
  }

  std::vector<storage::TxShardWrites*> write_ptrs;
  // One LMOVE/RPOPLPUSH is one logical dataset change even though its atomic
  // storage transaction writes both the source and destination records.
  for (auto& write : writes) write.dataset_changes_ = 0;
  writes[source_owner].dataset_changes_ = 1;
  for (auto& write : writes) write_ptrs.push_back(&write);
  status = co_await g_storage->CommitTxWrites(txid, std::move(write_ptrs));
  if (!status.ok()) {
    for (const unsigned owner : {source_owner, destination_owner}) {
      (void)co_await bycorf::SubmitTaskTo(
          owner, [txid] { return g_storage->RollbackTxLocal(txid); });
    }
  } else {
    for (const unsigned owner : {source_owner, destination_owner}) {
      (void)co_await bycorf::SubmitTaskTo(
          owner, [txid] { return g_storage->DiscardTxUndoLocal(txid); });
    }
  }
  absl::Status released = co_await release();
  if (!status.ok()) {
    co_return BuiltReply(AppendStorageError(reply_builder, status));
  }
  if (!released.ok()) {
    co_return BuiltReply(reply_builder.AppendError("ERR ", released.message()));
  }
  replication.SetCommandArgs(EncodeListMoveEffects(
      request.db_id_, source_key, destination_key, source_left,
      destination_left, popped->values_.front()));
  replication.SetFinalExpirations(writes);
  replication.Commit();
  NotifyListBlockingKey(request, source_key);
  NotifyListBlockingKey(request, destination_key);
  co_return BuiltReply(reply_builder.AppendBulkString(popped->values_.front()));
}

Task<CommandReply> ExecuteBlockingListCommand(const CommandRequest& request,
                                              ReplyBuilder& reply_builder,
                                              std::uint64_t client_id) {
  const auto& args = request.args_;
  const std::size_t timeout_arg = request.kind_ == CommandKind::kBLMPop   ? 1
                                  : request.kind_ == CommandKind::kBLMove ? 5
                                  : request.kind_ == CommandKind::kBRPopLPush
                                      ? 3
                                      : args.size() - 1;
  double timeout_seconds = 0;
  if (!ParseRedisDouble(args[timeout_arg], &timeout_seconds)) {
    long double extended_timeout = 0;
    if (ParseRedisLongDouble(args[timeout_arg], &extended_timeout) &&
        extended_timeout * 1000.0L >
            static_cast<long double>(
                std::numeric_limits<std::int64_t>::max())) {
      co_return BuiltReply(
          reply_builder.AppendError("ERR timeout is out of range"));
    }
    co_return BuiltReply(reply_builder.AppendError(
        "ERR timeout is not a float or out of range"));
  }
  if (timeout_seconds < 0) {
    co_return BuiltReply(reply_builder.AppendError("ERR timeout is negative"));
  }

  CommandRequest nonblocking = request;
  if (request.kind_ == CommandKind::kBLPop ||
      request.kind_ == CommandKind::kBRPop) {
    nonblocking.kind_ = CommandKind::kLMPop;
    nonblocking.args_.clear();
    nonblocking.args_.push_back("LMPOP");
    nonblocking.args_.push_back(std::to_string(args.size() - 2));
    for (std::size_t i = 1; i + 1 < args.size(); ++i) {
      nonblocking.args_.push_back(args[i]);
    }
    nonblocking.args_.push_back(request.kind_ == CommandKind::kBLPop ? "LEFT"
                                                                     : "RIGHT");
  } else if (request.kind_ == CommandKind::kBLMPop) {
    nonblocking.kind_ = CommandKind::kLMPop;
    nonblocking.args_.clear();
    nonblocking.args_.push_back("LMPOP");
    nonblocking.args_.insert(nonblocking.args_.end(), args.begin() + 2,
                             args.end());
  } else if (request.kind_ == CommandKind::kBLMove) {
    nonblocking.kind_ = CommandKind::kLMove;
    nonblocking.args_.assign(args.begin(), args.begin() + 5);
    nonblocking.args_[0] = "LMOVE";
  } else {
    nonblocking.kind_ = CommandKind::kRPopLPush;
    nonblocking.args_.assign(args.begin(), args.begin() + 3);
    nonblocking.args_[0] = "RPOPLPUSH";
  }

  auto wait_deadline = BlockingDeadlineFromSeconds(timeout_seconds);
  if (!wait_deadline.ok()) {
    co_return BuiltReply(
        reply_builder.AppendError("ERR ", wait_deadline.status().message()));
  }

  auto finish_attempt = [&](const CommandReply& attempt) {
    if (attempt.encoded_.starts_with("-") ||
        (request.kind_ != CommandKind::kBLPop &&
         request.kind_ != CommandKind::kBRPop)) {
      return BuiltReply(reply_builder.AppendRaw(attempt.encoded_));
    }
    // LMPOP returns [key, [value]]. Decode by RESP lengths so arbitrary
    // binary key/value bytes cannot be mistaken for framing delimiters.
    const std::string_view encoded = attempt.encoded_;
    std::size_t offset = 0;
    std::uint64_t outer_count = 0;
    std::uint64_t inner_count = 0;
    std::string_view selected_key;
    std::string_view selected_value;
    if (!ParseRespCount(encoded, &offset, '*', &outer_count) ||
        outer_count != 2 || !ParseRespBulk(encoded, &offset, &selected_key) ||
        !ParseRespCount(encoded, &offset, '*', &inner_count) ||
        inner_count != 1 || !ParseRespBulk(encoded, &offset, &selected_value) ||
        offset != encoded.size()) {
      return BuiltReply(
          reply_builder.AppendError("ERR invalid internal blocking pop reply"));
    }
    reply_builder.AppendArrayHeader(2);
    reply_builder.AppendBulkString(selected_key);
    reply_builder.AppendBulkString(selected_value);
    return BuiltReply(reply_builder.View());
  };
  auto timeout_reply = [&] {
    return BuiltReply((request.kind_ == CommandKind::kBLMove ||
                       request.kind_ == CommandKind::kBRPopLPush)
                          ? reply_builder.AppendNull()
                          : reply_builder.AppendNullArray());
  };

  std::vector<BlockingWaitSpec> specs;
  auto register_key = [&](std::string_view key) {
    for (const BlockingWaitSpec& spec : specs) {
      if (spec.key_ == key) return;
    }
    specs.push_back(BlockingWaitSpec{
        .key_ = std::string(key),
        .lane_ = {},
        .value_type_ = BlockingValueType::kList,
        .policy_ = BlockingQueuePolicy::kFifo,
        .stream_after_ = std::nullopt,
    });
  };
  if (nonblocking.kind_ == CommandKind::kLMPop) {
    std::int64_t key_count = 0;
    if (!ParseRedisInt64(nonblocking.args_[1], &key_count) || key_count <= 0) {
      co_return BuiltReply(reply_builder.AppendError("ERR syntax error"));
    }
    // One argument is the command, one is numkeys, and at least one trailing
    // argument is the pop direction. Validate before reserve/indexing: BLMPOP
    // reaches this code before the nonblocking LMPOP parser runs.
    if (nonblocking.args_.size() < 3 ||
        static_cast<std::uint64_t>(key_count) > nonblocking.args_.size() - 3) {
      co_return BuiltReply(reply_builder.AppendError("ERR syntax error"));
    }
    specs.reserve(static_cast<std::size_t>(key_count));
    for (std::size_t i = 0; i < static_cast<std::size_t>(key_count); ++i) {
      register_key(nonblocking.args_[2 + i]);
    }
  } else {
    // BLMOVE/BRPOPLPUSH block only on their source. Registering the
    // destination would put this waiter ahead of legitimate BLPOP waiters
    // even though a destination push cannot make the move executable.
    register_key(nonblocking.args_[1]);
  }

  auto attempt =
      [&](BlockingWakeCascade* cascade) -> Task<BlockingAttemptResult> {
    // ExecuteBlockingWaitLoop may have re-armed the original request after an
    // authority publication. The rewritten non-blocking form must carry that
    // exact protected proof into its transaction validators.
    nonblocking.cluster_authority_admission_ =
        request.cluster_authority_admission_;
    nonblocking.blocking_wake_cascade_ = cascade;
    ReplyBuilder attempt_builder(nonblocking.resp_version_);
    bool unavailable = false;
    CommandReply result = co_await ExecuteListMultiKey(
        nonblocking, attempt_builder, &unavailable);
    // Clang 18 cannot lower a temporary non-trivial aggregate through this
    // coroutine promise. A named frame object preserves the same move
    // semantics and makes the reply lifetime explicit.
    if (unavailable) {
      BlockingAttemptResult retry;
      co_return retry;
    }
    BlockingAttemptResult completed;
    completed.state_ = BlockingAttemptState::kComplete;
    completed.reply_ = finish_attempt(result);
    co_return completed;
  };
  auto status_reply = [&](const absl::Status& status) {
    if (absl::IsAborted(status)) {
      return BuiltReply(
          reply_builder.AppendError("TRYAGAIN ", status.message()));
    }
    return BuiltReply(reply_builder.AppendError("ERR ", status.message()));
  };
  auto unblock_error_reply = [&] {
    return BuiltReply(reply_builder.AppendError(
        "UNBLOCKED client unblocked via CLIENT UNBLOCK"));
  };
  co_return co_await ExecuteBlockingWaitLoop(
      request, reply_builder, client_id, std::move(specs), *wait_deadline,
      "blocking List wait cancelled", std::move(attempt), timeout_reply,
      unblock_error_reply, status_reply,
      request.kind_ == CommandKind::kBLMove ||
          request.kind_ == CommandKind::kBRPopLPush);
}

}  // namespace lavik
