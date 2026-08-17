#include "string_command.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <limits>
#include <new>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <vector>

#include "absl/strings/str_cat.h"
#include "celer/runtime/cross_core.h"
#include "keylane/memory.h"
#include "keylane/redis_parse.h"
#include "keylane/resp.h"
#include "keylane/storage/format.h"
#include "keylane/tx/transaction.h"
#include "keylane/tx/tx_shard.h"

namespace keylane {

namespace {

storage::StorageEngine* g_storage = nullptr;

CommandReply Built(std::string_view encoded) {
  CommandReply reply;
  reply.encoded_ = encoded;
  return reply;
}

std::string StorageError(const absl::Status& status) {
  return status.message().starts_with("WRONGTYPE ")
             ? EncodeError(status.message())
             : EncodeError(absl::StrCat("ERR ", status.message()));
}

absl::Status WrongType() {
  return absl::InvalidArgumentError(
      "WRONGTYPE Operation against a key holding the wrong kind of value");
}

celer::Task<absl::StatusOr<std::optional<storage::RawValue>>>
ReadOptionalStringLocked(std::uint8_t db_id, std::string_view key,
                         const storage::Digest& digest) {
  auto value = co_await g_storage->ReadRawValueLocked(db_id, key, digest);
  if (!value.ok()) {
    if (value.status().code() == absl::StatusCode::kNotFound) {
      co_return std::optional<storage::RawValue>{};
    }
    co_return value.status();
  }
  if (value->value_type_ != storage::ValueType::kString) co_return WrongType();
  co_return std::optional<storage::RawValue>(std::move(*value));
}

std::uint64_t UnixTimeMillis() {
  const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();
  return now > 0 ? static_cast<std::uint64_t>(now) : 0;
}

absl::StatusOr<std::uint64_t> ParseExpireAt(std::string_view text, bool seconds,
                                            bool absolute,
                                            std::string_view command) {
  std::int64_t parsed = 0;
  if (!ParseRedisInt64(text, &parsed)) {
    return absl::InvalidArgumentError(
        "value is not an integer or out of range");
  }
  constexpr std::uint64_t kMaximum =
      static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max());
  if (parsed <= 0 ||
      (seconds && static_cast<std::uint64_t>(parsed) > kMaximum / 1000)) {
    return absl::InvalidArgumentError(
        absl::StrCat("invalid expire time in '", command, "' command"));
  }
  std::uint64_t millis = static_cast<std::uint64_t>(parsed);
  if (seconds) millis *= 1000;
  if (!absolute) {
    const std::uint64_t now = UnixTimeMillis();
    if (millis > kMaximum - now) {
      return absl::InvalidArgumentError(
          absl::StrCat("invalid expire time in '", command, "' command"));
    }
    millis += now;
  }
  return millis;
}

std::string Range(std::string_view value, std::int64_t start,
                  std::int64_t stop) {
  if (start < 0 && stop < 0 && start > stop) return {};
  const std::uint64_t size = value.size();
  auto normalize = [size](std::int64_t index) {
    if (index < 0 && size <= static_cast<std::uint64_t>(INT64_MAX)) {
      if (index < -static_cast<std::int64_t>(size)) return std::int64_t{0};
      return static_cast<std::int64_t>(size) + index;
    }
    return index;
  };
  start = normalize(start);
  stop = normalize(stop);
  if (start < 0) start = 0;
  if (stop < 0) stop = 0;
  if (value.empty() || start > stop ||
      static_cast<std::uint64_t>(start) >= value.size()) {
    return {};
  }
  const std::uint64_t end = std::min<std::uint64_t>(
      static_cast<std::uint64_t>(stop), value.size() - 1);
  return std::string(value.substr(static_cast<std::size_t>(start),
                                  static_cast<std::size_t>(end - start + 1)));
}

celer::Task<std::string> RunStringLocked(const CommandRequest& request,
                                         const storage::Digest& digest,
                                         storage::TxShardWrites* tx) {
  const auto& args = request.args_;
  const std::uint8_t db = request.db_id_;
  const std::string_view key = args[1];

  if (request.kind_ == CommandKind::kSetEx ||
      request.kind_ == CommandKind::kPSetEx) {
    const bool seconds = request.kind_ == CommandKind::kSetEx;
    auto expire_at =
        ParseExpireAt(args[2], seconds, false, seconds ? "setex" : "psetex");
    if (!expire_at.ok()) {
      co_return EncodeError(absl::StrCat("ERR ", expire_at.status().message()));
    }
    storage::SetOptions options;
    options.expire_at_ms_ = *expire_at;
    auto result =
        co_await g_storage->SetLocked(db, key, digest, args[3], options, tx);
    co_return result.ok() ? EncodeSimpleString("OK")
                          : StorageError(result.status());
  }

  if (request.kind_ == CommandKind::kSetNx) {
    storage::SetOptions options;
    options.condition_ = storage::SetCondition::kIfAbsent;
    auto result =
        co_await g_storage->SetLocked(db, key, digest, args[2], options, tx);
    co_return result.ok() ? EncodeInteger(result->applied_ ? 1 : 0)
                          : StorageError(result.status());
  }

  if (request.kind_ == CommandKind::kGetSet) {
    storage::SetOptions options;
    options.return_old_value_ = true;
    auto result =
        co_await g_storage->SetLocked(db, key, digest, args[2], options, tx);
    if (!result.ok()) co_return StorageError(result.status());
    if (!result->old_value_) co_return EncodeNullBulkString();
    const auto bytes = result->old_value_->network_bytes();
    co_return std::string(reinterpret_cast<const char*>(bytes.data()),
                          bytes.size());
  }

  std::int64_t first_integer = 0;
  if ((request.kind_ == CommandKind::kGetRange ||
       request.kind_ == CommandKind::kSubstr ||
       request.kind_ == CommandKind::kSetRange ||
       request.kind_ == CommandKind::kIncrBy ||
       request.kind_ == CommandKind::kDecrBy) &&
      !ParseRedisInt64(args[2], &first_integer)) {
    co_return EncodeError("ERR value is not an integer or out of range");
  }
  std::int64_t second_integer = 0;
  if ((request.kind_ == CommandKind::kGetRange ||
       request.kind_ == CommandKind::kSubstr) &&
      !ParseRedisInt64(args[3], &second_integer)) {
    co_return EncodeError("ERR value is not an integer or out of range");
  }
  if (request.kind_ == CommandKind::kSetRange && first_integer < 0) {
    co_return EncodeError("ERR offset is out of range");
  }
  if (request.kind_ == CommandKind::kGetEx) {
    const bool persist =
        args.size() == 3 && RedisEqualsIgnoreCase(args[2], "persist");
    const bool expiration =
        args.size() == 4 &&
        (RedisEqualsIgnoreCase(args[2], "ex") ||
         RedisEqualsIgnoreCase(args[2], "px") ||
         RedisEqualsIgnoreCase(args[2], "exat") ||
         RedisEqualsIgnoreCase(args[2], "pxat"));
    if (args.size() != 2 && !persist && !expiration) {
      co_return EncodeError("ERR syntax error");
    }
  }

  if (request.kind_ == CommandKind::kGetRange ||
      request.kind_ == CommandKind::kSubstr) {
    auto current = co_await ReadOptionalStringLocked(db, key, digest);
    if (!current.ok()) co_return StorageError(current.status());
    const std::string_view old =
        current->has_value() ? (**current).encoded_ : std::string_view{};
    co_return EncodeBulkString(Range(old, first_integer, second_integer));
  }

  std::string reply;
  auto callback = [&](std::optional<storage::CompactValueView> current)
      -> absl::StatusOr<storage::CompactValueUpdate> {
    const bool exists = current.has_value();
    const std::string_view old =
        exists ? current->encoded_ : std::string_view{};
    const std::uint64_t ttl = exists ? current->expire_at_ms_ : 0;
    auto changed = [](std::string encoded,
                      std::optional<std::uint64_t> expire_at = std::nullopt) {
      const std::size_t size = encoded.size();
      return storage::CompactValueUpdate{
          .changed_ = true,
          .encoded_ = std::move(encoded),
          .logical_size_ = size,
          .expire_at_ms_ = expire_at,
      };
    };

    if (request.kind_ == CommandKind::kGetDel) {
      reply = exists ? EncodeBulkString(old) : EncodeNullBulkString();
      return exists ? storage::CompactValueUpdate{.changed_ = true,
                                                  .erase_ = true,
                                                  .encoded_ = {},
                                                  .logical_size_ = 0,
                                                  .expire_at_ms_ = std::nullopt}
                    : storage::CompactValueUpdate{};
    }

    if (request.kind_ == CommandKind::kGetEx) {
      if (!exists) {
        reply = EncodeNullBulkString();
        return storage::CompactValueUpdate{};
      }
      reply = EncodeBulkString(old);
      if (args.size() == 2) return storage::CompactValueUpdate{};
      if (args.size() == 3) {
        if (ttl == 0) return storage::CompactValueUpdate{};
        return changed(std::string(old), 0);
      }
      const bool ex = RedisEqualsIgnoreCase(args[2], "ex");
      const bool exat = RedisEqualsIgnoreCase(args[2], "exat");
      const bool pxat = RedisEqualsIgnoreCase(args[2], "pxat");
      auto parsed = ParseExpireAt(args[3], ex || exat, exat || pxat, "getex");
      if (!parsed.ok()) return parsed.status();
      if (*parsed <= UnixTimeMillis()) {
        return storage::CompactValueUpdate{.changed_ = true,
                                           .erase_ = true,
                                           .encoded_ = {},
                                           .logical_size_ = 0,
                                           .expire_at_ms_ = std::nullopt};
      }
      return changed(std::string(old), *parsed);
    }

    if (request.kind_ == CommandKind::kSetRange) {
      if (args[3].empty()) {
        reply = EncodeInteger(static_cast<long long>(old.size()));
        return storage::CompactValueUpdate{};
      }
      const std::uint64_t offset = static_cast<std::uint64_t>(first_integer);
      if (offset > storage::kMaxStringBytes ||
          args[3].size() > storage::kMaxStringBytes - offset) {
        return absl::OutOfRangeError(
            "string exceeds maximum allowed size (proto-max-bulk-len)");
      }
      std::string next(old);
      if (next.size() < offset + args[3].size()) {
        next.resize(static_cast<std::size_t>(offset + args[3].size()), '\0');
      }
      next.replace(static_cast<std::size_t>(offset), args[3].size(), args[3]);
      reply = EncodeInteger(static_cast<long long>(next.size()));
      return changed(std::move(next));
    }

    if (request.kind_ == CommandKind::kAppend) {
      if (args[2].size() > storage::kMaxStringBytes - old.size()) {
        return absl::OutOfRangeError(
            "string exceeds maximum allowed size (proto-max-bulk-len)");
      }
      std::string next(old);
      next.append(args[2]);
      reply = EncodeInteger(static_cast<long long>(next.size()));
      return changed(std::move(next));
    }

    if (request.kind_ == CommandKind::kIncrByFloat) {
      long double previous = 0;
      long double increment = 0;
      if ((exists && !ParseRedisLongDouble(old, &previous)) ||
          !ParseRedisLongDouble(args[2], &increment)) {
        return absl::InvalidArgumentError("value is not a valid float");
      }
      const long double result = previous + increment;
      if (!std::isfinite(result)) {
        return absl::InvalidArgumentError(
            "increment would produce NaN or Infinity");
      }
      std::string formatted;
      if (!FormatRedisLongDouble(result, &formatted)) {
        return absl::InternalError("failed to format String float");
      }
      reply = EncodeBulkString(formatted);
      return changed(std::move(formatted));
    }

    std::int64_t previous = 0;
    if (exists && !ParseRedisInt64(old, &previous)) {
      return absl::InvalidArgumentError(
          "value is not an integer or out of range");
    }
    std::int64_t delta = 0;
    switch (request.kind_) {
      case CommandKind::kIncr:
        delta = 1;
        break;
      case CommandKind::kDecr:
        delta = -1;
        break;
      case CommandKind::kIncrBy:
        delta = first_integer;
        break;
      case CommandKind::kDecrBy:
        if (first_integer == std::numeric_limits<std::int64_t>::min()) {
          return absl::InvalidArgumentError("decrement would overflow");
        }
        delta = -first_integer;
        break;
      default:
        return absl::InvalidArgumentError("unsupported String command path");
    }
    std::int64_t result = 0;
    if (__builtin_add_overflow(previous, delta, &result)) {
      return absl::InvalidArgumentError(
          "increment or decrement would overflow");
    }
    reply = EncodeInteger(result);
    return changed(std::to_string(result));
  };

  const absl::Status status = co_await g_storage->ExecuteCompactLocked(
      db, key, digest, storage::ValueType::kString, false, callback, tx);
  co_return status.ok() ? reply : StorageError(status);
}

struct LcsOptions {
  bool length_only_ = false;
  bool indexes_ = false;
  bool with_match_length_ = false;
  std::uint32_t minimum_match_length_ = 0;
};

absl::StatusOr<LcsOptions> ParseLcsOptions(
    const std::vector<std::string>& args) {
  LcsOptions options;
  for (std::size_t i = 3; i < args.size(); ++i) {
    if (RedisEqualsIgnoreCase(args[i], "len")) {
      options.length_only_ = true;
    } else if (RedisEqualsIgnoreCase(args[i], "idx")) {
      options.indexes_ = true;
    } else if (RedisEqualsIgnoreCase(args[i], "withmatchlen")) {
      options.with_match_length_ = true;
    } else if (RedisEqualsIgnoreCase(args[i], "minmatchlen") &&
               i + 1 < args.size()) {
      std::int64_t parsed = 0;
      if (!ParseRedisInt64(args[++i], &parsed)) {
        return absl::InvalidArgumentError(
            "value is not an integer or out of range");
      }
      if (parsed > 0) {
        options.minimum_match_length_ = static_cast<std::uint32_t>(
            std::min<std::int64_t>(parsed, UINT32_MAX));
      }
    } else {
      return absl::InvalidArgumentError("syntax error");
    }
  }
  if (options.length_only_ && options.indexes_) {
    return absl::InvalidArgumentError(
        "If you want both the length and indexes, please just use IDX.");
  }
  return options;
}

struct LcsMatch {
  std::uint32_t a_start_ = 0;
  std::uint32_t a_end_ = 0;
  std::uint32_t b_start_ = 0;
  std::uint32_t b_end_ = 0;
  std::uint32_t length_ = 0;
};

absl::StatusOr<std::string> BuildLcsReply(std::string_view a,
                                          std::string_view b,
                                          const LcsOptions& options) {
  if (a.size() >= UINT32_MAX - 1 || b.size() >= UINT32_MAX - 1) {
    return absl::OutOfRangeError("String too long for LCS");
  }
  const std::uint64_t rows = a.size() + 1;
  const std::uint64_t columns = b.size() + 1;
  if (rows > std::numeric_limits<std::size_t>::max() / columns) {
    return absl::ResourceExhaustedError(
        "Insufficient memory, failed allocating transient memory for LCS");
  }
  const std::size_t cells = static_cast<std::size_t>(rows * columns);
  if (cells > storage::kMaxStringBytes / sizeof(std::uint32_t)) {
    return absl::ResourceExhaustedError(
        "Insufficient memory, transient memory for LCS exceeds "
        "proto-max-bulk-len");
  }
  const std::size_t bytes = cells * sizeof(std::uint32_t);
  if (WouldExceedMemoryLimit(bytes)) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError(
        "Insufficient memory, failed allocating transient memory for LCS");
  }
  std::vector<std::uint32_t> table;
  try {
    table.assign(cells, 0);
  } catch (const std::bad_alloc&) {
    return absl::ResourceExhaustedError(
        "Insufficient memory, failed allocating transient memory for LCS");
  } catch (const std::length_error&) {
    return absl::ResourceExhaustedError(
        "Insufficient memory, failed allocating transient memory for LCS");
  }
  auto at = [&](std::size_t row, std::size_t column) -> std::uint32_t& {
    return table[column + row * columns];
  };
  for (std::size_t i = 1; i <= a.size(); ++i) {
    for (std::size_t j = 1; j <= b.size(); ++j) {
      at(i, j) = a[i - 1] == b[j - 1] ? at(i - 1, j - 1) + 1
                                      : std::max(at(i - 1, j), at(i, j - 1));
    }
  }
  const std::uint32_t length = at(a.size(), b.size());
  if (options.length_only_) return EncodeInteger(length);

  std::string result(length, '\0');
  std::uint32_t result_index = length;
  std::uint32_t i = a.size();
  std::uint32_t j = b.size();
  const std::uint32_t unset = a.size();
  std::uint32_t a_start = unset, a_end = 0, b_start = 0, b_end = 0;
  std::vector<LcsMatch> matches;
  while (i > 0 && j > 0) {
    bool emit = false;
    if (a[i - 1] == b[j - 1]) {
      result[result_index - 1] = a[i - 1];
      if (a_start == unset) {
        a_start = a_end = i - 1;
        b_start = b_end = j - 1;
      } else if (a_start == i && b_start == j) {
        --a_start;
        --b_start;
      } else {
        emit = true;
      }
      if (a_start == 0 || b_start == 0) emit = true;
      --result_index;
      --i;
      --j;
    } else {
      if (at(i - 1, j) > at(i, j - 1))
        --i;
      else
        --j;
      if (a_start != unset) emit = true;
    }
    if (emit) {
      const std::uint32_t match_length = a_end - a_start + 1;
      if (options.minimum_match_length_ == 0 ||
          match_length >= options.minimum_match_length_) {
        matches.push_back({a_start, a_end, b_start, b_end, match_length});
      }
      a_start = unset;
    }
  }
  if (!options.indexes_) return EncodeBulkString(result);

  ReplyBuilder builder;
  builder.AppendArrayHeader(4);
  builder.AppendBulkString("matches");
  builder.AppendArrayHeader(matches.size());
  for (const LcsMatch& match : matches) {
    builder.AppendArrayHeader(options.with_match_length_ ? 3 : 2);
    builder.AppendArrayHeader(2);
    builder.AppendInteger(match.a_start_);
    builder.AppendInteger(match.a_end_);
    builder.AppendArrayHeader(2);
    builder.AppendInteger(match.b_start_);
    builder.AppendInteger(match.b_end_);
    if (options.with_match_length_) builder.AppendInteger(match.length_);
  }
  builder.AppendBulkString("len");
  builder.AppendInteger(length);
  return std::move(builder).Release();
}

struct LcsReadContext {
  const CommandRequest* request_ = nullptr;
  std::string values_[2];
};

celer::Task<absl::Status> LcsReadCallback(void* opaque,
                                          const tx::ShardSlice& slice) {
  auto* context = static_cast<LcsReadContext*>(opaque);
  for (const tx::TxKey& key : slice.keys_) {
    auto value = co_await ReadOptionalStringLocked(
        context->request_->db_id_, context->request_->args_[key.arg_index_],
        key.digest_);
    if (!value.ok()) {
      if (value.status().message().starts_with("WRONGTYPE ")) {
        co_return absl::InvalidArgumentError(
            "The specified keys must contain string values");
      }
      co_return value.status();
    }
    context->values_[key.arg_index_ - 1] =
        value->has_value() ? std::move((**value).encoded_) : std::string{};
    if (context->request_->args_[1] == context->request_->args_[2]) {
      context->values_[0] = context->values_[key.arg_index_ - 1];
      context->values_[1] = context->values_[key.arg_index_ - 1];
    }
  }
  co_return absl::OkStatus();
}

}  // namespace

void InitStringCommandStorage(storage::StorageEngine* engine) {
  g_storage = engine;
}

celer::Task<CommandReply> ExecuteStringCommand(const CommandRequest& request,
                                               ReplyBuilder& reply_builder) {
  const storage::Digest digest = storage::ComputeDigest(request.args_[1]);
  const bool read_only = request.kind_ == CommandKind::kGetRange ||
                         request.kind_ == CommandKind::kSubstr;
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      request.db_id_, tx::FingerprintOf(digest),
      read_only ? tx::LockMode::kShared : tx::LockMode::kExclusive);
  co_return co_await ExecuteStringCommandLocked(request, digest, nullptr,
                                                reply_builder);
}

celer::Task<CommandReply> ExecuteStringCommandLocked(
    const CommandRequest& request, const storage::Digest& digest,
    storage::TxShardWrites* tx, ReplyBuilder& reply_builder) {
  std::string encoded = co_await RunStringLocked(request, digest, tx);
  co_return Built(reply_builder.AppendRaw(encoded));
}

celer::Task<CommandReply> ExecuteLcsCommand(const CommandRequest& request,
                                            ReplyBuilder& reply_builder) {
  tx::Transaction transaction;
  for (std::size_t argument = 1; argument <= 2; ++argument) {
    transaction.AddKey(
        g_storage->OwnerForKey(request.args_[argument]), request.db_id_,
        storage::ComputeDigest(request.args_[argument]),
        static_cast<std::uint32_t>(argument), tx::LockMode::kShared);
  }
  transaction.Seal();
  absl::Status status = co_await transaction.Schedule();
  if (!status.ok()) {
    co_return Built(
        reply_builder.AppendError(absl::StrCat("ERR ", status.message())));
  }
  LcsReadContext context{.request_ = &request, .values_ = {}};
  status = co_await transaction.Execute(&LcsReadCallback, &context, true);
  if (!status.ok()) {
    co_return Built(
        reply_builder.AppendError(absl::StrCat("ERR ", status.message())));
  }
  auto options = ParseLcsOptions(request.args_);
  if (!options.ok()) {
    co_return Built(reply_builder.AppendError(
        absl::StrCat("ERR ", options.status().message())));
  }
  auto result = BuildLcsReply(context.values_[0], context.values_[1], *options);
  co_return result.ok() ? Built(reply_builder.AppendRaw(*result))
                        : Built(reply_builder.AppendError(
                              absl::StrCat("ERR ", result.status().message())));
}

celer::Task<std::string> ExecuteLcsLocked(
    const CommandRequest& request, std::span<const StringExecKey> locked_keys) {
  std::string values[2];
  for (std::size_t argument = 1; argument <= 2; ++argument) {
    const StringExecKey* key = nullptr;
    for (const StringExecKey& candidate : locked_keys) {
      if (candidate.arg_ == argument) {
        key = &candidate;
        break;
      }
    }
    if (key == nullptr) co_return EncodeError("ERR LCS key is missing");
    auto read = [&request, key, argument]() {
      return ReadOptionalStringLocked(request.db_id_, request.args_[argument],
                                      key->digest_);
    };
    auto value = key->owner_ == celer::ThisWorker().id_
                     ? co_await read()
                     : co_await celer::SubmitTaskTo(key->owner_, read);
    if (!value.ok()) {
      if (value.status().message().starts_with("WRONGTYPE ")) {
        co_return EncodeError(
            "ERR The specified keys must contain string values");
      }
      co_return StorageError(value.status());
    }
    values[argument - 1] =
        value->has_value() ? std::move((**value).encoded_) : std::string{};
  }
  if (request.args_[1] == request.args_[2]) values[1] = values[0];
  auto options = ParseLcsOptions(request.args_);
  if (!options.ok()) {
    co_return EncodeError(absl::StrCat("ERR ", options.status().message()));
  }
  auto result = BuildLcsReply(values[0], values[1], *options);
  co_return result.ok()
      ? std::move(*result)
      : EncodeError(absl::StrCat("ERR ", result.status().message()));
}

}  // namespace keylane
