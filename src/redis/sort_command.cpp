#include "sort_command.h"

#include <algorithm>
#include <charconv>
#include <cstring>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "blocking_wait.h"
#include "keylane/redis_parse.h"
#include "keylane/resp.h"
#include "keylane/storage/engine.h"
#include "keylane/storage/format.h"
#include "keylane/tx/transaction.h"
#include "zset_command.h"

namespace keylane {
using namespace celer;

namespace {

storage::StorageEngine* g_storage = nullptr;

struct SortOptions {
  bool descending_ = false;
  bool alpha_ = false;
  bool dont_sort_ = false;
  std::int64_t limit_start_ = 0;
  std::int64_t limit_count_ = -1;
  std::optional<std::string_view> by_;
  std::vector<std::string_view> gets_;
  std::optional<std::size_t> store_arg_;
};

struct LockedKey {
  std::string name_;
  storage::Digest digest_;
  unsigned owner_ = 0;
  tx::LockMode mode_ = tx::LockMode::kShared;
};

struct SortSource {
  storage::ValueType type_ = storage::ValueType::kList;
  std::vector<std::string> elements_;
};

struct PatternReference {
  std::string key_;
  std::optional<std::string> field_;
};

struct SortItem {
  std::string value_;
  std::optional<std::string> comparison_;
  double score_ = 0;
};

struct SortProduct {
  std::vector<std::optional<std::string>> reply_values_;
  std::vector<std::string> stored_values_;
};

using PatternCache =
    std::map<std::pair<std::string, std::optional<std::string>>,
             std::optional<std::string>>;

CommandReply Built(std::string_view encoded) {
  CommandReply reply;
  reply.encoded_ = encoded;
  return reply;
}

bool EqualCi(std::string_view left, std::string_view right) {
  return RedisEqualsIgnoreCase(left, right);
}

absl::StatusOr<SortOptions> ParseSortOptions(const CommandRequest& request) {
  SortOptions options;
  const bool read_only = request.kind_ == CommandKind::kSortRo;
  const auto& args = request.args_;
  for (std::size_t i = 2; i < args.size(); ++i) {
    if (EqualCi(args[i], "asc")) {
      options.descending_ = false;
    } else if (EqualCi(args[i], "desc")) {
      options.descending_ = true;
    } else if (EqualCi(args[i], "alpha")) {
      options.alpha_ = true;
    } else if (EqualCi(args[i], "limit") && i + 2 < args.size()) {
      if (!ParseRedisInt64(args[i + 1], &options.limit_start_) ||
          !ParseRedisInt64(args[i + 2], &options.limit_count_)) {
        return absl::InvalidArgumentError(
            "value is not an integer or out of range");
      }
      i += 2;
    } else if (EqualCi(args[i], "by") && i + 1 < args.size()) {
      options.by_ = args[++i];
      options.dont_sort_ = options.by_->find('*') == std::string_view::npos;
    } else if (EqualCi(args[i], "get") && i + 1 < args.size()) {
      options.gets_.push_back(args[++i]);
    } else if (!read_only && EqualCi(args[i], "store") && i + 1 < args.size()) {
      options.store_arg_ = ++i;
    } else {
      return absl::InvalidArgumentError("syntax error");
    }
  }
  return options;
}

std::string EncodeSortError(const absl::Status& status) {
  if (status.code() == absl::StatusCode::kResourceExhausted &&
      status.message().starts_with("OOM ")) {
    return EncodeError(status.message());
  }
  if (status.message().starts_with("WRONGTYPE ") ||
      status.message().starts_with("ERR ")) {
    return EncodeError(status.message());
  }
  return EncodeError(absl::StrCat("ERR ", status.message()));
}

std::string_view AppendSortError(ReplyBuilder& builder,
                                 const absl::Status& status) {
  if (status.code() == absl::StatusCode::kResourceExhausted &&
      status.message().starts_with("OOM ")) {
    return builder.AppendError(status.message());
  }
  if (status.message().starts_with("WRONGTYPE ") ||
      status.message().starts_with("ERR ")) {
    return builder.AppendError(status.message());
  }
  return builder.AppendError("ERR ", status.message());
}

const LockedKey* FindLockedKey(std::span<const LockedKey> keys,
                               std::string_view name) {
  for (const LockedKey& key : keys) {
    if (key.name_ == name) return &key;
  }
  return nullptr;
}

void AddLockedKey(std::vector<LockedKey>* keys, std::string_view name,
                  tx::LockMode mode) {
  for (LockedKey& key : *keys) {
    if (key.name_ == name) {
      if (mode == tx::LockMode::kExclusive) key.mode_ = mode;
      return;
    }
  }
  keys->push_back(LockedKey{.name_ = std::string(name),
                            .digest_ = storage::ComputeDigest(name),
                            .owner_ = g_storage->OwnerForKey(name),
                            .mode_ = mode});
}

Task<absl::Status> HoldSortLocks(void*, const tx::ShardSlice&) {
  co_return absl::OkStatus();
}

Task<absl::StatusOr<SortSource>> ReadSortSourceLocked(std::uint8_t db_id,
                                                      const LockedKey& key) {
  auto read = [&]() -> Task<absl::StatusOr<SortSource>> {
    const storage::ExpirationInfo info =
        co_await g_storage->GetExpirationLocked(db_id, key.name_, key.digest_);
    SortSource source;
    if (!info.exists_) co_return source;
    source.type_ = info.value_type_;
    if (info.value_type_ == storage::ValueType::kList) {
      storage::ListOperation operation;
      operation.kind_ = storage::ListOperationKind::kRange;
      operation.first_ = 0;
      operation.second_ = -1;
      auto result = co_await g_storage->ExecuteListLocked(
          db_id, key.name_, key.digest_, operation);
      if (!result.ok()) co_return result.status();
      source.elements_ = std::move(result->values_);
      co_return source;
    }
    if (info.value_type_ == storage::ValueType::kSet) {
      storage::HashOperation operation;
      operation.kind_ = storage::HashOperationKind::kKeys;
      auto result = co_await g_storage->ExecuteSetLocked(
          db_id, key.name_, key.digest_, operation);
      if (!result.ok()) co_return result.status();
      source.elements_.reserve(result->values_.size());
      for (auto& member : result->values_) {
        if (!member.has_value()) {
          co_return absl::InternalError("Set member is unexpectedly missing");
        }
        source.elements_.push_back(std::move(*member));
      }
      co_return source;
    }
    if (info.value_type_ == storage::ValueType::kSortedSet) {
      auto members =
          co_await ZSetMembersSnapshotLocked(db_id, key.name_, key.digest_);
      if (!members.ok()) co_return members.status();
      source.elements_ = std::move(*members);
      co_return source;
    }
    co_return absl::FailedPreconditionError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  };
  if (key.owner_ == ThisWorker().id_) co_return co_await read();
  co_return co_await SubmitTaskTo(key.owner_, read);
}

std::optional<PatternReference> ResolvePattern(std::string_view pattern,
                                               std::string_view substitute) {
  const std::size_t star = pattern.find('*');
  if (star == std::string_view::npos) return std::nullopt;
  const std::size_t arrow = pattern.find("->", star + 1);
  const bool hash_field =
      arrow != std::string_view::npos && arrow + 2 < pattern.size();
  const std::size_t key_suffix_end = hash_field ? arrow : pattern.size();
  PatternReference reference;
  reference.key_.reserve(star + substitute.size() + key_suffix_end - star - 1);
  reference.key_.append(pattern.substr(0, star));
  reference.key_.append(substitute);
  reference.key_.append(pattern.substr(star + 1, key_suffix_end - star - 1));
  if (hash_field) reference.field_.emplace(pattern.substr(arrow + 2));
  return reference;
}

std::vector<std::string> CollectPatternKeys(
    const SortOptions& options, std::span<const std::string> elements) {
  std::vector<std::string> keys;
  absl::flat_hash_set<std::string> seen;
  auto collect = [&](std::string_view pattern) {
    for (const std::string& element : elements) {
      auto reference = ResolvePattern(pattern, element);
      if (!reference.has_value()) continue;
      if (seen.insert(reference->key_).second) {
        keys.push_back(std::move(reference->key_));
      }
    }
  };
  if (options.by_.has_value() && !options.dont_sort_) collect(*options.by_);
  for (std::string_view pattern : options.gets_) {
    if (pattern != "#") collect(pattern);
  }
  return keys;
}

Task<absl::StatusOr<std::optional<std::string>>> ReadPatternReferenceLocked(
    std::uint8_t db_id, const PatternReference& reference,
    std::span<const LockedKey> keys) {
  const LockedKey* key = FindLockedKey(keys, reference.key_);
  if (key == nullptr) {
    co_return absl::FailedPreconditionError(
        "SORT BY/GET pattern key is not locked in MULTI");
  }
  auto read = [&]() -> Task<absl::StatusOr<std::optional<std::string>>> {
    if (reference.field_.has_value()) {
      storage::HashOperation operation;
      operation.kind_ = storage::HashOperationKind::kGet;
      operation.fields_.push_back(*reference.field_);
      auto result = co_await g_storage->ExecuteHashLocked(
          db_id, key->name_, key->digest_, operation);
      if (!result.ok()) {
        if (result.status().message().starts_with("WRONGTYPE ")) {
          co_return std::optional<std::string>{};
        }
        co_return result.status();
      }
      if (result->values_.empty() || !result->values_.front().has_value()) {
        co_return std::optional<std::string>{};
      }
      co_return std::optional<std::string>{std::move(*result->values_.front())};
    }
    auto raw =
        co_await g_storage->ReadRawValueLocked(db_id, key->name_, key->digest_);
    if (!raw.ok()) {
      if (raw.status().code() == absl::StatusCode::kNotFound) {
        co_return std::optional<std::string>{};
      }
      co_return raw.status();
    }
    if (raw->value_type_ != storage::ValueType::kString) {
      co_return std::optional<std::string>{};
    }
    co_return std::optional<std::string>{std::move(raw->encoded_)};
  };
  if (key->owner_ == ThisWorker().id_) co_return co_await read();
  co_return co_await SubmitTaskTo(key->owner_, read);
}

Task<absl::StatusOr<std::optional<std::string>>> LookupPatternValue(
    std::uint8_t db_id, std::string_view pattern, std::string_view substitute,
    std::span<const LockedKey> keys, PatternCache* cache) {
  auto reference = ResolvePattern(pattern, substitute);
  if (!reference.has_value()) co_return std::optional<std::string>{};
  auto cache_key = std::make_pair(reference->key_, reference->field_);
  auto cached = cache->find(cache_key);
  if (cached != cache->end()) {
    co_return cached->second;
  }
  auto value = co_await ReadPatternReferenceLocked(db_id, *reference, keys);
  if (!value.ok()) co_return value.status();
  cache->emplace(std::move(cache_key), *value);
  co_return *value;
}

Task<absl::StatusOr<SortProduct>> BuildSortProduct(
    const CommandRequest& request, const SortOptions& options,
    SortSource source, std::span<const LockedKey> keys) {
  PatternCache cache;
  std::vector<SortItem> items;
  items.reserve(source.elements_.size());
  for (std::string& element : source.elements_) {
    SortItem item{
        .value_ = std::move(element), .comparison_ = std::nullopt, .score_ = 0};
    if (!options.dont_sort_) {
      if (options.by_.has_value()) {
        auto comparison = co_await LookupPatternValue(
            request.db_id_, *options.by_, item.value_, keys, &cache);
        if (!comparison.ok()) co_return comparison.status();
        item.comparison_ = std::move(*comparison);
      } else {
        item.comparison_ = item.value_;
      }
      if (!options.alpha_) {
        if (item.comparison_.has_value() &&
            !ParseRedisDouble(*item.comparison_, &item.score_, true)) {
          co_return absl::InvalidArgumentError(
              "One or more scores can't be converted into double");
        }
      }
    }
    items.push_back(std::move(item));
  }

  bool dont_sort = options.dont_sort_;
  bool alpha = options.alpha_;
  if (dont_sort && source.type_ == storage::ValueType::kSet &&
      options.store_arg_.has_value()) {
    dont_sort = false;
    alpha = true;
    for (SortItem& item : items) item.comparison_ = item.value_;
  }
  if (!dont_sort) {
    auto ascending = [&](const SortItem& left, const SortItem& right) {
      int comparison = 0;
      if (alpha) {
        if (!left.comparison_.has_value() || !right.comparison_.has_value()) {
          if (left.comparison_.has_value() == right.comparison_.has_value()) {
            comparison = 0;
          } else {
            comparison = left.comparison_.has_value() ? 1 : -1;
          }
        } else {
          comparison = options.store_arg_.has_value()
                           ? left.comparison_->compare(*right.comparison_)
                           : std::strcoll(left.comparison_->c_str(),
                                          right.comparison_->c_str());
        }
      } else if (left.score_ < right.score_) {
        comparison = -1;
      } else if (left.score_ > right.score_) {
        comparison = 1;
      } else {
        comparison = left.value_.compare(right.value_);
      }
      return options.descending_ ? comparison > 0 : comparison < 0;
    };
    std::sort(items.begin(), items.end(), ascending);
  } else if (options.descending_) {
    std::reverse(items.begin(), items.end());
  }

  const std::size_t size = items.size();
  const std::uint64_t nonnegative_start =
      options.limit_start_ < 0
          ? 0
          : static_cast<std::uint64_t>(options.limit_start_);
  const std::size_t begin = static_cast<std::size_t>(
      std::min<std::uint64_t>(nonnegative_start, size));
  std::size_t end = size;
  if (options.limit_count_ >= 0) {
    const std::uint64_t count =
        static_cast<std::uint64_t>(options.limit_count_);
    end = begin + static_cast<std::size_t>(
                      std::min<std::uint64_t>(count, size - begin));
  }

  SortProduct product;
  const std::size_t per_item = options.gets_.empty() ? 1 : options.gets_.size();
  product.reply_values_.reserve((end - begin) * per_item);
  for (std::size_t i = begin; i < end; ++i) {
    if (options.gets_.empty()) {
      product.reply_values_.emplace_back(items[i].value_);
      continue;
    }
    for (std::string_view pattern : options.gets_) {
      if (pattern == "#") {
        product.reply_values_.emplace_back(items[i].value_);
        continue;
      }
      auto value = co_await LookupPatternValue(request.db_id_, pattern,
                                               items[i].value_, keys, &cache);
      if (!value.ok()) co_return value.status();
      product.reply_values_.push_back(std::move(*value));
    }
  }
  if (options.store_arg_.has_value()) {
    product.stored_values_.reserve(product.reply_values_.size());
    for (const auto& value : product.reply_values_) {
      product.stored_values_.push_back(value.value_or(""));
    }
  }
  co_return product;
}

Task<absl::StatusOr<bool>> ReplaceDestinationLocked(
    const CommandRequest& request, const LockedKey& destination,
    std::span<const std::string> values, storage::TxShardWrites* writes) {
  auto replace = [&]() -> Task<absl::StatusOr<bool>> {
    auto deleted = co_await g_storage->DeleteLocked(
        request.db_id_, destination.name_, destination.digest_, writes);
    if (!deleted.ok()) co_return deleted.status();
    if (values.empty()) co_return *deleted;
    storage::ListOperation push;
    push.kind_ = storage::ListOperationKind::kPushRight;
    push.values_.reserve(values.size());
    for (const std::string& value : values) push.values_.push_back(value);
    auto pushed = co_await g_storage->ExecuteListLocked(
        request.db_id_, destination.name_, destination.digest_, push, writes);
    if (!pushed.ok()) co_return pushed.status();
    co_return true;
  };
  if (destination.owner_ == ThisWorker().id_) co_return co_await replace();
  co_return co_await SubmitTaskTo(destination.owner_, replace);
}

std::vector<std::string> SortReplicationEffects(
    const CommandRequest& request, std::string_view destination,
    std::span<const std::string> values) {
  std::vector<CapturedReplicationCommand> effects;
  effects.push_back(CapturedReplicationCommand{
      request.db_id_, {"DEL", std::string(destination)}});
  if (!values.empty()) {
    std::vector<std::string> push{"RPUSH", std::string(destination)};
    push.insert(push.end(), values.begin(), values.end());
    effects.push_back(
        CapturedReplicationCommand{request.db_id_, std::move(push)});
  }
  return EncodeReplicationCommandEffects(std::move(effects));
}

void AppendSortArray(ReplyBuilder& builder,
                     std::span<const std::optional<std::string>> values) {
  builder.AppendArrayHeader(values.size());
  for (const auto& value : values) {
    if (value.has_value())
      builder.AppendBulkString(*value);
    else
      builder.AppendNull();
  }
}

Task<absl::Status> ReleaseSortTransaction(tx::Transaction* transaction) {
  return transaction->Execute(&HoldSortLocks, nullptr, true);
}

}  // namespace

void InitSortCommandStorage(storage::StorageEngine* engine) {
  g_storage = engine;
}

Task<CommandReply> ExecuteSortCommand(const CommandRequest& request,
                                      ReplyBuilder& reply_builder) {
  auto options = ParseSortOptions(request);
  if (!options.ok()) {
    co_return Built(AppendSortError(reply_builder, options.status()));
  }

  std::vector<LockedKey> keys;
  AddLockedKey(&keys, request.args_[1], tx::LockMode::kShared);
  if (options->store_arg_.has_value()) {
    AddLockedKey(&keys, request.args_[*options->store_arg_],
                 tx::LockMode::kExclusive);
  }

  for (;;) {
    tx::Transaction transaction;
    for (std::size_t i = 0; i < keys.size(); ++i) {
      transaction.AddKey(keys[i].owner_, request.db_id_, keys[i].digest_, i,
                         keys[i].mode_);
    }
    transaction.Seal();
    std::unique_ptr<ReplicationTransactionGuard> replication;
    if (options->store_arg_.has_value()) {
      replication =
          std::make_unique<ReplicationTransactionGuard>(request, &transaction);
      if (!replication->status().ok()) {
        co_return Built(AppendSortError(reply_builder, replication->status()));
      }
    }
    absl::Status status = co_await transaction.Schedule();
    if (!status.ok()) {
      co_return Built(AppendSortError(reply_builder, status));
    }
    status = co_await transaction.Execute(&HoldSortLocks, nullptr, false);
    if (!status.ok()) {
      co_return Built(AppendSortError(reply_builder, status));
    }

    const LockedKey* source_key = FindLockedKey(keys, request.args_[1]);
    auto source = co_await ReadSortSourceLocked(request.db_id_, *source_key);
    if (!source.ok()) {
      (void)co_await ReleaseSortTransaction(&transaction);
      co_return Built(AppendSortError(reply_builder, source.status()));
    }
    bool expanded = false;
    for (const std::string& pattern_key :
         CollectPatternKeys(*options, source->elements_)) {
      if (FindLockedKey(keys, pattern_key) == nullptr) {
        AddLockedKey(&keys, pattern_key, tx::LockMode::kShared);
        expanded = true;
      }
    }
    if (expanded) {
      status = co_await ReleaseSortTransaction(&transaction);
      if (!status.ok()) {
        co_return Built(AppendSortError(reply_builder, status));
      }
      continue;
    }

    auto product =
        co_await BuildSortProduct(request, *options, std::move(*source), keys);
    if (!product.ok()) {
      (void)co_await ReleaseSortTransaction(&transaction);
      co_return Built(AppendSortError(reply_builder, product.status()));
    }
    if (!options->store_arg_.has_value()) {
      status = co_await ReleaseSortTransaction(&transaction);
      if (!status.ok()) {
        co_return Built(AppendSortError(reply_builder, status));
      }
      AppendSortArray(reply_builder, product->reply_values_);
      co_return Built(reply_builder.View());
    }

    const std::string& destination_name = request.args_[*options->store_arg_];
    const LockedKey* destination = FindLockedKey(keys, destination_name);
    const std::uint64_t txid = storage::StorageEngine::AllocateWriteTxid();
    std::vector<storage::TxShardWrites> writes(g_storage->worker_count());
    g_storage->InitializeTxWrites(txid, writes);
    writes[destination->owner_].collect_undo_ = true;
    auto replaced = co_await ReplaceDestinationLocked(
        request, *destination, product->stored_values_,
        &writes[destination->owner_]);
    if (!replaced.ok()) {
      (void)co_await SubmitTaskTo(destination->owner_, [txid] {
        return g_storage->RollbackTxLocal(txid);
      });
      (void)co_await ReleaseSortTransaction(&transaction);
      co_return Built(AppendSortError(reply_builder, replaced.status()));
    }

    if (*replaced) {
      std::vector<storage::TxShardWrites*> changed;
      if (!writes[destination->owner_].fences_.empty() ||
          !writes[destination->owner_].retirements_.empty()) {
        changed.push_back(&writes[destination->owner_]);
      }
      status = co_await g_storage->CommitTxWrites(txid, std::move(changed));
      if (!status.ok()) {
        (void)co_await SubmitTaskTo(destination->owner_, [txid] {
          return g_storage->RollbackTxLocal(txid);
        });
        (void)co_await ReleaseSortTransaction(&transaction);
        co_return Built(AppendSortError(reply_builder, status));
      }
      status = co_await SubmitTaskTo(destination->owner_, [txid] {
        return g_storage->DiscardTxUndoLocal(txid);
      });
      if (!status.ok()) {
        (void)co_await ReleaseSortTransaction(&transaction);
        co_return Built(AppendSortError(reply_builder, status));
      }
      replication->SetCommandArgs(SortReplicationEffects(
          request, destination_name, product->stored_values_));
      replication->SetFinalExpirations(writes);
      replication->Commit();
    }
    status = co_await ReleaseSortTransaction(&transaction);
    if (!status.ok()) {
      co_return Built(AppendSortError(reply_builder, status));
    }
    if (!product->stored_values_.empty()) {
      NotifyListBlockingKey(request, destination_name);
    }
    co_return Built(
        reply_builder.AppendInteger(product->stored_values_.size()));
  }
}

Task<std::string> ExecuteSortCommandLocked(
    const CommandRequest& request, std::span<const SortExecKey> exec_keys,
    std::vector<storage::TxShardWrites>& tx_writes) {
  if (request.kind_ == CommandKind::kSort) {
    MarkReplicationCommandHandled(request);
  }
  auto options = ParseSortOptions(request);
  if (!options.ok()) co_return EncodeSortError(options.status());
  std::vector<LockedKey> keys;
  keys.reserve(exec_keys.size());
  for (const SortExecKey& key : exec_keys) {
    if (key.arg_ >= request.args_.size()) continue;
    AddLockedKey(&keys, request.args_[key.arg_], tx::LockMode::kShared);
  }
  if (options->store_arg_.has_value()) {
    AddLockedKey(&keys, request.args_[*options->store_arg_],
                 tx::LockMode::kExclusive);
  }
  const LockedKey* source_key = FindLockedKey(keys, request.args_[1]);
  if (source_key == nullptr)
    co_return EncodeError("ERR SORT source is missing");
  auto source = co_await ReadSortSourceLocked(request.db_id_, *source_key);
  if (!source.ok()) co_return EncodeSortError(source.status());
  for (const std::string& pattern_key :
       CollectPatternKeys(*options, source->elements_)) {
    if (FindLockedKey(keys, pattern_key) == nullptr) {
      co_return EncodeError(
          "ERR SORT BY/GET pattern keys are not supported inside MULTI");
    }
  }
  auto product =
      co_await BuildSortProduct(request, *options, std::move(*source), keys);
  if (!product.ok()) co_return EncodeSortError(product.status());
  if (!options->store_arg_.has_value()) {
    ReplyBuilder builder(request.resp_version_);
    AppendSortArray(builder, product->reply_values_);
    co_return std::string(builder.View());
  }

  const std::string& destination_name = request.args_[*options->store_arg_];
  const LockedKey* destination = FindLockedKey(keys, destination_name);
  if (destination == nullptr) {
    co_return EncodeError("ERR SORT destination is missing");
  }
  auto prior = co_await SubmitTaskTo(
      destination->owner_, [db = request.db_id_, name = destination->name_,
                            digest = destination->digest_]() {
        return g_storage->ReadRawValueLocked(db, name, digest);
      });
  const bool prior_missing =
      !prior.ok() && prior.status().code() == absl::StatusCode::kNotFound;
  if (!prior.ok() && !prior_missing) co_return EncodeSortError(prior.status());
  auto replaced = co_await ReplaceDestinationLocked(
      request, *destination, product->stored_values_,
      &tx_writes[destination->owner_]);
  if (!replaced.ok()) {
    absl::Status restored;
    if (prior_missing) {
      auto deleted = co_await SubmitTaskTo(
          destination->owner_, [db = request.db_id_, name = destination->name_,
                                digest = destination->digest_,
                                writes = &tx_writes[destination->owner_]] {
            return g_storage->DeleteLocked(db, name, digest, writes);
          });
      restored = deleted.ok() ? absl::OkStatus() : deleted.status();
    } else {
      restored = co_await SubmitTaskTo(
          destination->owner_, [db = request.db_id_, name = destination->name_,
                                digest = destination->digest_, value = &*prior,
                                writes = &tx_writes[destination->owner_]] {
            return g_storage->WriteRawValueLocked(db, name, digest, *value,
                                                  writes);
          });
    }
    co_return EncodeSortError(restored.ok() ? replaced.status() : restored);
  }
  if (*replaced) {
    CaptureReplicationCommand(request, {"DEL", destination_name});
    if (!product->stored_values_.empty()) {
      std::vector<std::string> push{"RPUSH", destination_name};
      push.insert(push.end(), product->stored_values_.begin(),
                  product->stored_values_.end());
      CaptureReplicationCommand(request, std::move(push));
      NotifyListBlockingKey(request, destination_name);
    }
  }
  co_return EncodeInteger(product->stored_values_.size());
}

}  // namespace keylane
