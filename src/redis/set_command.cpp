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

#include "set_command.h"

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <limits>
#include <new>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "bycorf/runtime/cross_core.h"
#include "bycorf/runtime/worker.h"
#include "cluster_gate.h"
#include "lavik/command_table.h"
#include "lavik/memory.h"
#include "lavik/resp.h"
#include "lavik/storage/engine.h"
#include "lavik/tx/transaction.h"

namespace lavik {
using namespace bycorf;

namespace {

storage::StorageEngine* g_storage = nullptr;

CommandReply BuiltReply(std::string_view encoded) {
  CommandReply reply;
  reply.encoded_ = encoded;
  return reply;
}

std::string_view AppendStorageError(ReplyBuilder& builder,
                                    const absl::Status& status) {
  if (status.code() == absl::StatusCode::kResourceExhausted &&
      status.message().starts_with("OOM ")) {
    return builder.AppendError(status.message());
  }
  if (status.message().starts_with("WRONGTYPE ")) {
    return builder.AppendError(status.message());
  }
  return builder.AppendError("ERR " + std::string(status.message()));
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

template <typename Integer>
bool ParseInteger(std::string_view text, Integer* output) {
  const auto parsed =
      std::from_chars(text.data(), text.data() + text.size(), *output);
  return parsed.ec == std::errc{} && parsed.ptr == text.data() + text.size();
}

void AppendBulkArray(ReplyBuilder& builder,
                     const std::vector<std::optional<std::string>>& values) {
  builder.AppendArrayHeader(values.size());
  for (const auto& value : values) {
    builder.AppendBulkString(*value);
  }
}

Task<CommandReply> ExecuteSetCommandImpl(const CommandRequest& request,
                                         const storage::Digest* digest,
                                         storage::TxShardWrites* tx,
                                         ReplyBuilder& reply_builder) {
  const auto& args = request.args_;
  if (request.kind_ == CommandKind::kSPop) {
    MarkReplicationCommandHandled(request);
  }
  storage::HashOperation operation;
  switch (request.kind_) {
    case CommandKind::kSAdd:
      operation.kind_ = storage::HashOperationKind::kSet;
      for (std::size_t i = 2; i < args.size(); ++i) {
        operation.fields_.push_back(args[i]);
        operation.values_.push_back({});
      }
      break;
    case CommandKind::kSCard:
      operation.kind_ = storage::HashOperationKind::kLength;
      break;
    case CommandKind::kSIsMember:
      operation.kind_ = storage::HashOperationKind::kExists;
      operation.fields_.push_back(args[2]);
      break;
    case CommandKind::kSMembers:
      operation.kind_ = storage::HashOperationKind::kKeys;
      break;
    case CommandKind::kSMIsMember:
      operation.kind_ = storage::HashOperationKind::kGetMany;
      for (std::size_t i = 2; i < args.size(); ++i) {
        operation.fields_.push_back(args[i]);
      }
      break;
    case CommandKind::kSRem:
      operation.kind_ = storage::HashOperationKind::kDelete;
      for (std::size_t i = 2; i < args.size(); ++i) {
        operation.fields_.push_back(args[i]);
      }
      break;
    case CommandKind::kSPop:
      operation.kind_ = storage::HashOperationKind::kPopRandom;
      if (args.size() == 3) {
        if (!ParseInteger(args[2], &operation.count_) || operation.count_ < 0) {
          co_return BuiltReply(reply_builder.AppendError(
              "ERR value is out of range, must be positive"));
        }
        operation.count_provided_ = true;
      }
      break;
    case CommandKind::kSRandMember:
      operation.kind_ = storage::HashOperationKind::kRandomFields;
      if (args.size() == 3) {
        if (!ParseInteger(args[2], &operation.count_)) {
          co_return BuiltReply(reply_builder.AppendError(
              "ERR value is not an integer or out of range"));
        }
        if (operation.count_ == std::numeric_limits<std::int64_t>::min()) {
          co_return BuiltReply(
              reply_builder.AppendError("ERR value is out of range"));
        }
        operation.count_provided_ = true;
      }
      break;
    case CommandKind::kSScan:
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
    default:
      co_return BuiltReply(
          reply_builder.AppendError("ERR unsupported Set command path"));
  }

  // Both locked and ordinary dispatch assign this placeholder. A non-empty
  // diagnostic here only allocates a StatusRep that is discarded immediately.
  absl::StatusOr<storage::HashResult> result;
  auto replication =
      tx == nullptr ? PrepareReplicationCommand(request) : std::nullopt;
  if (digest == nullptr) {
    const storage::MutationPrecondition mutation_precondition =
        ClusterMutationPrecondition(request);
    result = co_await g_storage->ExecuteSet(
        request.db_id_, args[1], operation,
        replication ? &*replication : nullptr, &mutation_precondition);
  } else {
    result = co_await g_storage->ExecuteSetLocked(
        request.db_id_, args[1], *digest, operation, tx,
        replication ? &*replication : nullptr);
  }
  if (!result.ok()) {
    co_return BuiltReply(AppendStorageError(reply_builder, result.status()));
  }
  if (request.kind_ == CommandKind::kSPop && !result->values_.empty()) {
    std::vector<std::string> canonical{"SREM", args[1]};
    canonical.reserve(result->values_.size() + 2);
    for (const auto& member : result->values_) {
      if (member.has_value()) canonical.push_back(*member);
    }
    CaptureReplicationCommand(request, std::move(canonical));
  }

  switch (request.kind_) {
    case CommandKind::kSAdd:
    case CommandKind::kSRem:
    case CommandKind::kSIsMember:
      co_return BuiltReply(reply_builder.AppendInteger(result->integer_));
    case CommandKind::kSCard:
      co_return BuiltReply(reply_builder.AppendInteger(result->length_));
    case CommandKind::kSMembers:
      reply_builder.AppendSetHeader(result->values_.size());
      for (const auto& value : result->values_) {
        reply_builder.AppendBulkString(*value);
      }
      co_return BuiltReply(reply_builder.View());
    case CommandKind::kSMIsMember:
      reply_builder.AppendArrayHeader(result->values_.size());
      for (const auto& value : result->values_) {
        reply_builder.AppendInteger(value.has_value() ? 1 : 0);
      }
      co_return BuiltReply(reply_builder.View());
    case CommandKind::kSPop:
      if (!operation.count_provided_) {
        co_return BuiltReply(
            result->values_.empty()
                ? reply_builder.AppendNull()
                : reply_builder.AppendBulkString(*result->values_.front()));
      }
      reply_builder.AppendSetHeader(result->values_.size());
      for (const auto& value : result->values_) {
        reply_builder.AppendBulkString(*value);
      }
      co_return BuiltReply(reply_builder.View());
    case CommandKind::kSRandMember:
      if (!operation.count_provided_) {
        co_return BuiltReply(
            result->values_.empty()
                ? reply_builder.AppendNull()
                : reply_builder.AppendBulkString(*result->values_.front()));
      }
      AppendBulkArray(reply_builder, result->values_);
      co_return BuiltReply(reply_builder.View());
    case CommandKind::kSScan:
      reply_builder.AppendArrayHeader(2);
      reply_builder.AppendBulkString(std::to_string(result->cursor_));
      AppendBulkArray(reply_builder, result->values_);
      co_return BuiltReply(reply_builder.View());
    default:
      break;
  }
  co_return BuiltReply(reply_builder.AppendError("ERR unreachable Set reply"));
}

enum class SetAggregate : std::uint8_t { kDifference, kIntersection, kUnion };

struct SetMultiContext {
  // Destroy charges after the strings and operation views. Inputs are produced
  // on different owners, while aggregation and reply destruction may run on
  // the coordinator; a worker-local reservation cannot represent that handoff.
  std::vector<RetainedMemoryCharge> input_charges_;
  std::vector<RetainedMemoryCharge> input_vector_charges_;
  RetainedMemoryCharge aggregate_charge_;
  RetainedMemoryCharge effect_charge_;
  const CommandRequest* request_ = nullptr;
  std::vector<std::vector<std::string>> members_by_arg_;
  std::vector<std::string> output_;
  std::vector<storage::TxShardWrites> tx_writes_;
  std::string_view move_member_;
  storage::HashOperation add_;
  storage::HashOperation remove_;
  ReplicationTransactionGuard* replication_ = nullptr;
  unsigned coordinator_ = 0;
  bool effects_prepared_ = false;
  bool source_exists_ = false;
  bool source_contains_ = false;
  bool validate_move_destination_ = false;
  bool changed_ = false;
  bool rollback_ = false;
  bool single_shard_ = false;
  std::size_t last_source_arg_ = 0;
  SetAggregate aggregate_ = SetAggregate::kUnion;
};

absl::Status SetMultiOom() {
  return absl::ResourceExhaustedError(
      "OOM Set multi-key working set exceeds maxmemory");
}

bool AddWorkingBytes(std::size_t* bytes, std::size_t count,
                     std::size_t width = 1) {
  if (count > (std::numeric_limits<std::size_t>::max() - *bytes) / width) {
    return false;
  }
  *bytes += count * width;
  return true;
}

void ReleaseSetWorkingSet(SetMultiContext* context) noexcept {
  // All write callbacks have returned before rollback. The undo journal owns
  // its predecessor and receipts independently; none of these borrowed views
  // or aggregate copies are needed to compensate a failed command.
  context->add_ = {};
  context->remove_ = {};
  std::vector<std::string>().swap(context->output_);
  for (auto& input : context->members_by_arg_) {
    std::vector<std::string>().swap(input);
  }
  for (auto& charge : context->input_charges_) charge.Reset();
  for (auto& charge : context->input_vector_charges_) charge.Reset();
  context->aggregate_charge_.Reset();
  context->effect_charge_.Reset();
}

storage::TxShardWrites* LocalWrites(SetMultiContext& context) {
  return context.tx_writes_.empty()
             ? nullptr
             : &context.tx_writes_[bycorf::ThisWorker().id_];
}

absl::flat_hash_set<std::string> AggregateMembers(
    const SetMultiContext& context) {
  const auto& request = *context.request_;
  const std::size_t first_source =
      request.kind_ == CommandKind::kSInterCard ? 2
      : request.kind_ == CommandKind::kSDiffStore ||
              request.kind_ == CommandKind::kSInterStore ||
              request.kind_ == CommandKind::kSUnionStore
          ? 2
          : 1;
  absl::flat_hash_set<std::string> result;
  if (first_source >= request.args_.size()) return result;
  for (const std::string& member : context.members_by_arg_[first_source]) {
    result.insert(member);
  }
  if (context.aggregate_ == SetAggregate::kUnion) {
    for (std::size_t i = first_source + 1; i <= context.last_source_arg_; ++i) {
      for (const std::string& member : context.members_by_arg_[i]) {
        result.insert(member);
      }
    }
    return result;
  }
  if (context.aggregate_ == SetAggregate::kDifference) {
    for (std::size_t i = first_source + 1; i <= context.last_source_arg_; ++i) {
      for (const std::string& member : context.members_by_arg_[i]) {
        result.erase(member);
      }
    }
    return result;
  }
  for (std::size_t i = first_source + 1; i <= context.last_source_arg_; ++i) {
    absl::flat_hash_set<std::string_view> current;
    current.reserve(context.members_by_arg_[i].size());
    for (const std::string& member : context.members_by_arg_[i]) {
      current.insert(member);
    }
    for (auto it = result.begin(); it != result.end();) {
      if (!current.contains(*it)) {
        auto remove = it++;
        result.erase(remove);
      } else {
        ++it;
      }
    }
  }
  return result;
}

absl::Status ComputeAggregate(SetMultiContext* context) {
  // This legacy algorithm still materializes all inputs. Reserve the peak of
  // owning hash-table copies, rehash/intersection scratch, output copies and
  // STORE's borrowed field/value vectors before any of those allocations.
  std::size_t bytes = 4096;
  for (const auto& input : context->members_by_arg_) {
    if (!AddWorkingBytes(&bytes, input.size(), 256)) return SetMultiOom();
    for (const auto& member : input) {
      if (!AddWorkingBytes(&bytes, member.capacity() + 1, 3))
        return SetMultiOom();
    }
  }
  auto admission = TryReserveMemory(bytes);
  if (!admission) return SetMultiOom();
  context->aggregate_charge_.Adopt(&*admission, bytes);
  try {
    absl::flat_hash_set<std::string> members = AggregateMembers(*context);
    context->output_.assign(std::make_move_iterator(members.begin()),
                            std::make_move_iterator(members.end()));
    std::sort(context->output_.begin(), context->output_.end());
    context->add_.kind_ = storage::HashOperationKind::kSet;
    context->add_.fields_.reserve(context->output_.size());
    context->add_.values_.resize(context->output_.size());
    for (const auto& member : context->output_) {
      context->add_.fields_.push_back(member);
    }
    return absl::OkStatus();
  } catch (const std::bad_alloc&) {
    return SetMultiOom();
  } catch (const std::length_error&) {
    return SetMultiOom();
  }
}

std::vector<std::string> EncodeSetMoveEffects(const CommandRequest& request) {
  std::vector<CapturedReplicationCommand> effects;
  effects.reserve(2);
  effects.push_back(CapturedReplicationCommand{
      request.db_id_, {"SREM", request.args_[1], request.args_[3]}});
  effects.push_back(CapturedReplicationCommand{
      request.db_id_, {"SADD", request.args_[2], request.args_[3]}});
  return EncodeReplicationCommandEffects(std::move(effects));
}

std::vector<std::string> EncodeSetReplacement(
    const CommandRequest& request, const std::vector<std::string>& members) {
  std::vector<CapturedReplicationCommand> effects;
  effects.reserve(members.empty() ? 1 : 2);
  effects.push_back(
      CapturedReplicationCommand{request.db_id_, {"DEL", request.args_[1]}});
  if (!members.empty()) {
    std::vector<std::string> add{"SADD", request.args_[1]};
    add.insert(add.end(), members.begin(), members.end());
    effects.push_back(
        CapturedReplicationCommand{request.db_id_, std::move(add)});
  }
  return EncodeReplicationCommandEffects(std::move(effects));
}

Task<absl::Status> PrepareSetEffects(SetMultiContext* context) {
  if (context->effects_prepared_) co_return absl::OkStatus();
  const auto& request = *context->request_;
  const bool move = request.kind_ == CommandKind::kSMove;
  std::size_t bytes = 4096;
  for (const auto& argument : request.args_) {
    if (!AddWorkingBytes(&bytes, argument.size() + 1, 8))
      co_return SetMultiOom();
  }
  for (const auto& member : context->output_) {
    if (!AddWorkingBytes(&bytes, member.size() + 1, 4) ||
        !AddWorkingBytes(&bytes, 1, 256))
      co_return SetMultiOom();
  }
  auto admission = TryReserveMemory(bytes);
  if (!admission) co_return SetMultiOom();
  context->effect_charge_.Adopt(&*admission, bytes);
  try {
    auto effects = move ? EncodeSetMoveEffects(request)
                        : EncodeSetReplacement(request, context->output_);
    // The guard owns coordinator-local replication state. Prepare its durable
    // command image before DEL/SREM, even for a single owner callback running
    // elsewhere; an allocation failure must still be an untouched transaction.
    absl::Status status;
    if (ThisWorker().id_ == context->coordinator_) {
      status = context->replication_->TrySetCommandArgs(std::move(effects));
    } else {
      status = co_await SubmitTo(
          context->coordinator_, [guard = context->replication_,
                                  effects = std::move(effects)]() mutable {
            return guard->TrySetCommandArgs(std::move(effects));
          });
    }
    if (!status.ok()) co_return status;
    context->effects_prepared_ = true;
    co_return absl::OkStatus();
  } catch (const std::bad_alloc&) {
    co_return SetMultiOom();
  } catch (const std::length_error&) {
    co_return SetMultiOom();
  }
}

Task<absl::Status> ReplaceDestination(SetMultiContext* context) {
  try {
    const auto& request = *context->request_;
    const std::string& destination = request.args_[1];
    const storage::Digest digest = storage::ComputeDigest(destination);
    auto deleted = co_await g_storage->DeleteLocked(
        request.db_id_, destination, digest, LocalWrites(*context));
    if (!deleted.ok()) co_return deleted.status();
    if (context->output_.empty()) {
      context->changed_ = *deleted;
      co_return absl::OkStatus();
    }
    auto added = co_await g_storage->ExecuteSetLocked(
        request.db_id_, destination, digest, context->add_,
        LocalWrites(*context));
    if (!added.ok()) co_return added.status();
    context->changed_ = true;
    co_return absl::OkStatus();
  } catch (const std::bad_alloc&) {
    // The caller's normal failure path rolls back a staged DEL too.
    co_return SetMultiOom();
  } catch (const std::length_error&) {
    co_return SetMultiOom();
  }
}

Task<absl::Status> SetReadShardCallback(void* opaque,
                                        const tx::ShardSlice& slice) {
  try {
    auto* context = static_cast<SetMultiContext*>(opaque);
    const auto& request = *context->request_;
    const bool store = request.kind_ == CommandKind::kSDiffStore ||
                       request.kind_ == CommandKind::kSInterStore ||
                       request.kind_ == CommandKind::kSUnionStore;
    const bool move = request.kind_ == CommandKind::kSMove;
    for (const tx::TxKey& key : slice.keys_) {
      if (store && key.arg_index_ == 1) continue;
      if (move) {
        if (context->validate_move_destination_) {
          if (key.arg_index_ != 2) continue;
          storage::HashOperation length;
          length.kind_ = storage::HashOperationKind::kLength;
          auto result = co_await g_storage->ExecuteSetLocked(
              request.db_id_, request.args_[2], key.digest_, length);
          if (!result.ok()) co_return result.status();
          continue;
        }
        if (key.arg_index_ != 1) continue;
        storage::HashOperation contains;
        contains.kind_ = storage::HashOperationKind::kGet;
        contains.fields_.push_back(context->move_member_);
        auto result = co_await g_storage->ExecuteSetLocked(
            request.db_id_, request.args_[1], key.digest_, contains);
        if (!result.ok()) co_return result.status();
        context->source_exists_ = result->key_exists_;
        context->source_contains_ =
            !result->values_.empty() && result->values_.front().has_value();
        continue;
      }
      storage::HashOperation read;
      read.kind_ = storage::HashOperationKind::kKeys;
      auto result = co_await g_storage->ExecuteSetLocked(
          request.db_id_, request.args_[key.arg_index_], key.digest_, read);
      if (!result.ok()) co_return result.status();
      std::size_t bytes = 0;
      if (!AddWorkingBytes(&bytes, result->values_.size(),
                           sizeof(std::string))) {
        co_return SetMultiOom();
      }
      // Full kKeys results may come from the legacy compact path, which has no
      // exported charge. Adopt their already-built strings on the producing
      // owner before allocating the new vector or yielding to another owner.
      if (result->retained_charge_.bytes() == 0) {
        for (const auto& member : result->values_) {
          if (member && !AddWorkingBytes(&bytes, member->capacity() + 1)) {
            co_return SetMultiOom();
          }
        }
      }
      auto admission = TryReserveMemory(bytes);
      if (!admission) co_return SetMultiOom();
      context->input_vector_charges_[key.arg_index_].Adopt(&*admission, bytes);
      context->input_charges_[key.arg_index_] =
          std::move(result->retained_charge_);
      auto& destination = context->members_by_arg_[key.arg_index_];
      destination.reserve(result->values_.size());
      for (auto& member : result->values_) {
        destination.push_back(std::move(*member));
      }
    }
    if (context->single_shard_ && store) {
      auto prepared = ComputeAggregate(context);
      if (prepared.ok()) prepared = co_await PrepareSetEffects(context);
      if (!prepared.ok()) co_return prepared;
      absl::Status replaced = co_await ReplaceDestination(context);
      if (!replaced.ok()) {
        ReleaseSetWorkingSet(context);
        if (!context->tx_writes_.empty()) {
          auto restored = co_await g_storage->RollbackTxLocal(
              context->tx_writes_.front().txid_);
          if (!restored.ok()) co_return restored;
        }
        co_return replaced;
      }
      if (!context->tx_writes_.empty()) {
        co_return co_await g_storage->DiscardTxUndoLocal(
            context->tx_writes_.front().txid_);
      }
    }
    co_return absl::OkStatus();
  } catch (const std::bad_alloc&) {
    co_return SetMultiOom();
  } catch (const std::length_error&) {
    co_return SetMultiOom();
  }
}

Task<absl::Status> SetWriteShardCallback(void* opaque,
                                         const tx::ShardSlice& slice) {
  try {
    auto* context = static_cast<SetMultiContext*>(opaque);
    const auto& request = *context->request_;
    if (request.kind_ != CommandKind::kSMove) {
      for (const tx::TxKey& key : slice.keys_) {
        if (key.arg_index_ == 1) co_return co_await ReplaceDestination(context);
      }
      co_return absl::OkStatus();
    }
    for (const tx::TxKey& key : slice.keys_) {
      if (key.arg_index_ != 1 && key.arg_index_ != 2) continue;
      const auto& operation =
          key.arg_index_ == 1 ? context->remove_ : context->add_;
      auto result = co_await g_storage->ExecuteSetLocked(
          request.db_id_, request.args_[key.arg_index_], key.digest_, operation,
          LocalWrites(*context));
      if (!result.ok()) co_return result.status();
      context->changed_ = true;
    }
    co_return absl::OkStatus();
  } catch (const std::bad_alloc&) {
    co_return SetMultiOom();
  } catch (const std::length_error&) {
    co_return SetMultiOom();
  }
}

Task<absl::Status> SetFinishShardCallback(void* opaque, const tx::ShardSlice&) {
  auto* context = static_cast<SetMultiContext*>(opaque);
  if (context->tx_writes_.empty()) co_return absl::OkStatus();
  const std::uint64_t txid = context->tx_writes_.front().txid_;
  if (context->rollback_) {
    co_return co_await g_storage->RollbackTxLocal(txid);
  }
  co_return co_await g_storage->DiscardTxUndoLocal(txid);
}

Task<absl::Status> RunSetTxCommit(std::uint64_t txid,
                                  std::vector<storage::TxShardWrites> writes) {
  struct CommitDone {
    ~CommitDone() { g_storage->NoteTxCommitFinished(); }
  } commit_done;
  std::vector<storage::TxShardWrites*> shards;
  for (auto& shard : writes) {
    if (!shard.fences_.empty() || !shard.retirements_.empty()) {
      shards.push_back(&shard);
    }
  }
  if (shards.empty()) co_return absl::OkStatus();
  co_return co_await g_storage->CommitTxWrites(txid, std::move(shards));
}

}  // namespace

void InitSetCommandStorage(storage::StorageEngine* engine) {
  g_storage = engine;
}

Task<CommandReply> ExecuteSetCommand(const CommandRequest& request,
                                     ReplyBuilder& reply_builder) {
  return ExecuteSetCommandImpl(request, nullptr, nullptr, reply_builder);
}

Task<CommandReply> ExecuteSetCommandLocked(const CommandRequest& request,
                                           const storage::Digest& digest,
                                           storage::TxShardWrites* tx,
                                           ReplyBuilder& reply_builder) {
  return ExecuteSetCommandImpl(request, &digest, tx, reply_builder);
}

Task<CommandReply> ExecuteSetMultiKey(const CommandRequest& request,
                                      ReplyBuilder& reply_builder) {
  const auto& args = request.args_;
  auto key_view = DetermineKeys(*request.spec_, args);
  if (!key_view.ok()) {
    co_return BuiltReply(reply_builder.AppendError(
        absl::StrCat("ERR ", key_view.status().message())));
  }

  std::uint64_t cardinality_limit = 0;
  if (request.kind_ == CommandKind::kSInterCard) {
    const std::size_t after_keys = key_view->last_ + 1;
    for (std::size_t option = after_keys; option < args.size(); option += 2) {
      std::int64_t parsed_limit = 0;
      if (option + 1 >= args.size() ||
          !EqualsIgnoreCase(args[option], "limit")) {
        co_return BuiltReply(reply_builder.AppendError("ERR syntax error"));
      }
      if (!ParseInteger(args[option + 1], &parsed_limit) || parsed_limit < 0) {
        co_return BuiltReply(
            reply_builder.AppendError("ERR LIMIT can't be negative"));
      }
      cardinality_limit = static_cast<std::uint64_t>(parsed_limit);
    }
  }

  SetMultiContext context;
  context.request_ = &request;
  context.input_charges_.resize(args.size());
  context.input_vector_charges_.resize(args.size());
  context.members_by_arg_.resize(args.size());
  context.last_source_arg_ = key_view->last_;
  const bool store = request.kind_ == CommandKind::kSDiffStore ||
                     request.kind_ == CommandKind::kSInterStore ||
                     request.kind_ == CommandKind::kSUnionStore;
  const bool move = request.kind_ == CommandKind::kSMove;
  const bool write = store || move;
  if (request.kind_ == CommandKind::kSDiff ||
      request.kind_ == CommandKind::kSDiffStore) {
    context.aggregate_ = SetAggregate::kDifference;
  } else if (request.kind_ == CommandKind::kSInter ||
             request.kind_ == CommandKind::kSInterCard ||
             request.kind_ == CommandKind::kSInterStore) {
    context.aggregate_ = SetAggregate::kIntersection;
  }
  if (move) {
    context.move_member_ = args[3];
    // Both owners borrow these vectors. No operation-vector allocation is
    // allowed after the source removal, including the second owner hop.
    context.remove_.kind_ = storage::HashOperationKind::kDelete;
    context.remove_.fields_.push_back(context.move_member_);
    context.add_.kind_ = storage::HashOperationKind::kSet;
    context.add_.fields_.push_back(context.move_member_);
    context.add_.values_.push_back({});
  }

  tx::Transaction transaction;
  for (std::size_t i = key_view->first_; i <= key_view->last_;
       i += key_view->step_) {
    const tx::LockMode lock_mode = move || (store && i == 1)
                                       ? tx::LockMode::kExclusive
                                       : tx::LockMode::kShared;
    transaction.AddKey(g_storage->OwnerForKey(args[i]), request.db_id_,
                       storage::ComputeDigest(args[i]),
                       static_cast<std::uint32_t>(i), lock_mode);
  }
  transaction.Seal();
  // Cluster owner-side re-check: the admission was captured at dispatch time;
  // a newer Meta projection or lease transition may have fenced it since.
  // Single-shard executions (the only shape cluster admission allows) mutate
  // inside the one callback hop, so the pre-callback validator is airtight
  // there; multi-shard flows validate per hop and clear the hook before the
  // settle hop below.
  ClusterShardValidatorContext cluster_validator;
  if (write) {
    InstallClusterShardValidator(transaction, request, cluster_validator);
  }
  ReplicationTransactionGuard replication(request, &transaction);
  context.replication_ = &replication;
  context.coordinator_ = ThisWorker().id_;
  if (!replication.status().ok()) {
    co_return BuiltReply(
        AppendStorageError(reply_builder, replication.status()));
  }
  context.single_shard_ = transaction.single_shard();

  std::uint64_t txid = 0;
  if (write) {
    txid = storage::StorageEngine::AllocateWriteTxid();
    context.tx_writes_.resize(g_storage->worker_count());
    g_storage->InitializeTxWrites(txid, context.tx_writes_,
                                  ClusterMutationPrecondition(request));
    for (auto& shard : context.tx_writes_) {
      shard.collect_undo_ = true;
    }
  }

  absl::Status status = co_await transaction.Schedule();
  if (!status.ok()) {
    co_return BuiltReply(
        reply_builder.AppendError(absl::StrCat("ERR ", status.message())));
  }

  if (context.single_shard_ && move) {
    // A single-shard Transaction cannot hold locks over multiple hops, so
    // the source check, destination type check, and both writes share one
    // callback in this special case.
    auto single_move = [](void* opaque,
                          const tx::ShardSlice& slice) -> Task<absl::Status> {
      auto* ctx = static_cast<SetMultiContext*>(opaque);
      absl::Status read = co_await SetReadShardCallback(opaque, slice);
      if (!read.ok() || ctx->request_->args_[1] == ctx->request_->args_[2] ||
          !ctx->source_exists_) {
        co_return read;
      }
      ctx->validate_move_destination_ = true;
      read = co_await SetReadShardCallback(opaque, slice);
      if (!read.ok() || !ctx->source_contains_) co_return read;
      read = co_await PrepareSetEffects(ctx);
      if (!read.ok()) co_return read;
      absl::Status written = co_await SetWriteShardCallback(opaque, slice);
      if (!written.ok()) {
        ReleaseSetWorkingSet(ctx);
        auto restored =
            co_await g_storage->RollbackTxLocal(ctx->tx_writes_.front().txid_);
        if (!restored.ok()) co_return restored;
        co_return written;
      }
      co_return co_await g_storage->DiscardTxUndoLocal(
          ctx->tx_writes_.front().txid_);
    };
    status = co_await transaction.Execute(single_move, &context, true);
  } else {
    const bool release_after_read = !write || context.single_shard_;
    status = co_await transaction.Execute(&SetReadShardCallback, &context,
                                          release_after_read);
  }
  if (!status.ok()) {
    if (cluster_validator.tripped_.load(std::memory_order_relaxed)) {
      // A multi-shard read hop retains its holds on failure; drop them before
      // answering. Single-shard hops already released in-band (the failing
      // hop itself was armed with release=true).
      if (write && !context.single_shard_ && !transaction.releasing()) {
        (void)co_await transaction.Release();
      }
      co_return ClusterValidatorFailureReply(transaction, cluster_validator,
                                             request.connection_tls_,
                                             reply_builder);
    }
    if (write && !context.single_shard_) {
      (void)co_await transaction.Release();
    }
    co_return BuiltReply(AppendStorageError(reply_builder, status));
  }

  if (move) {
    if (args[1] == args[2]) {
      if (!context.single_shard_) (void)co_await transaction.Release();
      co_return BuiltReply(
          reply_builder.AppendInteger(context.source_contains_ ? 1 : 0));
    }
    if (!context.source_exists_) {
      if (!context.single_shard_) (void)co_await transaction.Release();
      co_return BuiltReply(reply_builder.AppendInteger(0));
    }
    if (!context.single_shard_) {
      context.validate_move_destination_ = true;
      status =
          co_await transaction.Execute(&SetReadShardCallback, &context, false);
      if (!status.ok()) {
        if (cluster_validator.tripped_.load(std::memory_order_relaxed)) {
          (void)co_await transaction.Release();
          co_return ClusterValidatorFailureReply(transaction, cluster_validator,
                                                 request.connection_tls_,
                                                 reply_builder);
        }
        (void)co_await transaction.Release();
        co_return BuiltReply(AppendStorageError(reply_builder, status));
      }
      if (!context.source_contains_) {
        (void)co_await transaction.Release();
        co_return BuiltReply(reply_builder.AppendInteger(0));
      }
      status = co_await PrepareSetEffects(&context);
      if (!status.ok()) {
        (void)co_await transaction.Release();
        co_return BuiltReply(AppendStorageError(reply_builder, status));
      }
      status =
          co_await transaction.Execute(&SetWriteShardCallback, &context, false);
      context.rollback_ = !status.ok();
      if (context.rollback_) ReleaseSetWorkingSet(&context);
      // The finish hop settles (or rolls back) what the write hop did; it must
      // not be fenced off by an authority change the write hop already beat.
      transaction.SetShardValidator(nullptr, nullptr);
      absl::Status finished =
          co_await transaction.Execute(&SetFinishShardCallback, &context, true);
      if (!finished.ok()) status = finished;
    }
    if (context.single_shard_ && !context.source_contains_) {
      co_return BuiltReply(reply_builder.AppendInteger(0));
    }
  } else if (store) {
    if (!context.single_shard_) {
      status = ComputeAggregate(&context);
      if (status.ok()) status = co_await PrepareSetEffects(&context);
      if (!status.ok()) {
        (void)co_await transaction.Release();
        co_return BuiltReply(AppendStorageError(reply_builder, status));
      }
      status =
          co_await transaction.Execute(&SetWriteShardCallback, &context, false);
      context.rollback_ = !status.ok();
      if (context.rollback_) ReleaseSetWorkingSet(&context);
      transaction.SetShardValidator(nullptr, nullptr);
      absl::Status finished =
          co_await transaction.Execute(&SetFinishShardCallback, &context, true);
      if (!finished.ok()) status = finished;
    }
  } else {
    status = ComputeAggregate(&context);
  }
  if (!status.ok()) {
    if (cluster_validator.tripped_.load(std::memory_order_relaxed)) {
      co_return ClusterValidatorFailureReply(transaction, cluster_validator,
                                             request.connection_tls_,
                                             reply_builder);
    }
    co_return BuiltReply(AppendStorageError(reply_builder, status));
  }

  if (write) {
    replication.SetFinalExpirations(context.tx_writes_);
    replication.Commit();
    g_storage->NoteTxCommitStarted();
    SpawnOnCurrentWorker(RunSetTxCommit(txid, std::move(context.tx_writes_)));
  }

  if (store) {
    co_return BuiltReply(reply_builder.AppendInteger(context.output_.size()));
  }
  if (move) {
    co_return BuiltReply(reply_builder.AppendInteger(1));
  }
  if (request.kind_ == CommandKind::kSInterCard) {
    const std::uint64_t cardinality = context.output_.size();
    co_return BuiltReply(reply_builder.AppendInteger(
        cardinality_limit == 0 ? cardinality
                               : std::min(cardinality, cardinality_limit)));
  }
  try {
    reply_builder.AppendSetHeader(context.output_.size());
    for (const std::string& member : context.output_) {
      reply_builder.AppendBulkString(member);
    }
    co_return BuiltReply(reply_builder.View());
  } catch (const std::bad_alloc&) {
    co_return BuiltReply(AppendStorageError(reply_builder, SetMultiOom()));
  } catch (const std::length_error&) {
    co_return BuiltReply(AppendStorageError(reply_builder, SetMultiOom()));
  }
}

}  // namespace lavik
