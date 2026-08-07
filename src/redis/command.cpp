#include "keylane/command.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "celer/runtime/cross_core.h"
#include "celer/io/storage.h"
#include "keylane/resp.h"
#include "keylane/storage/engine.h"
#include "keylane/storage/format.h"

namespace keylane {
using namespace celer;

namespace {

storage::StorageEngine* g_storage = nullptr;
bool g_replica_read_only = false;

CommandReply EncodedReply(std::string encoded) {
  CommandReply reply;
  reply.encoded = std::move(encoded);
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

CommandKind MatchCommandKind(std::string_view name) {
  switch (name.size()) {
    case 3:
      if (CmpCaseInsensitive(name, "GET")) return CommandKind::kGet;
      if (CmpCaseInsensitive(name, "SET")) return CommandKind::kSet;
      if (CmpCaseInsensitive(name, "DEL")) return CommandKind::kDel;
      if (CmpCaseInsensitive(name, "TTL")) return CommandKind::kTtl;
      break;
    case 4:
      if (CmpCaseInsensitive(name, "PING")) return CommandKind::kPing;
      if (CmpCaseInsensitive(name, "ECHO")) return CommandKind::kEcho;
      if (CmpCaseInsensitive(name, "INCR")) return CommandKind::kIncr;
      if (CmpCaseInsensitive(name, "SCAN")) return CommandKind::kScan;
      if (CmpCaseInsensitive(name, "PTTL")) return CommandKind::kPttl;
      break;
    case 6:
      if (CmpCaseInsensitive(name, "DBSIZE")) return CommandKind::kDbSize;
      if (CmpCaseInsensitive(name, "EXISTS")) return CommandKind::kExists;
      if (CmpCaseInsensitive(name, "SELECT")) return CommandKind::kSelect;
      if (CmpCaseInsensitive(name, "EXPIRE")) return CommandKind::kExpire;
      if (CmpCaseInsensitive(name, "STRLEN")) return CommandKind::kStrlen;
      break;
    case 7:
      if (CmpCaseInsensitive(name, "FLUSHDB")) return CommandKind::kFlushDb;
      if (CmpCaseInsensitive(name, "PEXPIRE")) return CommandKind::kPExpire;
      if (CmpCaseInsensitive(name, "PERSIST")) return CommandKind::kPersist;
      break;
  }
  return CommandKind::kUnknown;
}

}  // namespace

StatusOr<CommandRequest> BuildCommandRequest(RespCommand command,
                                             std::uint8_t db_id) {
  if (command.args.empty()) {
    return Status(StatusCode::kInvalidArgument, "empty command");
  }

  CommandRequest request;
  request.kind = MatchCommandKind(command.args.front());
  request.db_id = db_id;
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

    case CommandKind::kEcho:
      if (args.size() != 2) {
        reply.encoded =
            EncodeError("ERR wrong number of arguments for 'echo' command");
      } else {
        reply.encoded = EncodeBulkString(args[1]);
      }
      return reply;

    case CommandKind::kSelect: {
      if (args.size() != 2) {
        reply.encoded =
            EncodeError("ERR wrong number of arguments for 'select' command");
        return reply;
      }
      unsigned db_id = 0;
      const char* begin = args[1].data();
      const char* end = begin + args[1].size();
      const auto [parsed_end, error] = std::from_chars(begin, end, db_id);
      if (error != std::errc{} || parsed_end != end ||
          db_id >= storage::kLogicalDatabaseCount) {
        reply.encoded = EncodeError("ERR DB index is out of range");
        return reply;
      }
      reply.encoded = EncodeSimpleString("OK");
      reply.selected_db = static_cast<std::uint8_t>(db_id);
      return reply;
    }

    case CommandKind::kDbSize:
    case CommandKind::kFlushDb:
    case CommandKind::kScan:
    case CommandKind::kGet:
    case CommandKind::kStrlen:
    case CommandKind::kIncr:
    case CommandKind::kSet:
    case CommandKind::kExpire:
    case CommandKind::kPExpire:
    case CommandKind::kPersist:
    case CommandKind::kTtl:
    case CommandKind::kPttl:
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
    co_return EncodedReply(
        EncodeError("ERR wrong number of arguments for 'dbsize' command"));
  }

  std::uint64_t total = 0;
  const std::uint8_t db_id = request.db_id;
  for (unsigned target = 0; target < g_storage->worker_count(); ++target) {
    const std::size_t local_size = co_await SubmitTo(target, [db_id] {
      return g_storage->LocalSize(db_id);
    });
    if (local_size > std::numeric_limits<std::uint64_t>::max() - total) {
      co_return EncodedReply(EncodeError("ERR db size overflow"));
    }
    total += local_size;
  }
  if (total > static_cast<std::uint64_t>(std::numeric_limits<long long>::max())) {
    co_return EncodedReply(
        EncodeError("ERR db size exceeds RESP integer range"));
  }
  co_return EncodedReply(EncodeInteger(static_cast<long long>(total)));
}

constexpr std::uint64_t kDbGateClosed = std::uint64_t{1} << 63;
constexpr std::uint64_t kDbGateCountMask = ~kDbGateClosed;
std::array<std::atomic<std::uint64_t>, storage::kLogicalDatabaseCount>
    g_db_gates{};

bool TryBeginDbOperation(std::uint8_t db_id) noexcept {
  auto& gate = g_db_gates[db_id];
  std::uint64_t state = gate.load(std::memory_order_acquire);
  while ((state & kDbGateClosed) == 0) {
    if (gate.compare_exchange_weak(state, state + 1,
                                   std::memory_order_acq_rel,
                                   std::memory_order_acquire)) {
      return true;
    }
  }
  return false;
}

void EndDbOperation(std::uint8_t db_id) noexcept {
  g_db_gates[db_id].fetch_sub(1, std::memory_order_acq_rel);
}

bool CloseDbGate(std::uint8_t db_id) noexcept {
  auto& gate = g_db_gates[db_id];
  std::uint64_t expected = gate.load(std::memory_order_acquire);
  while ((expected & kDbGateClosed) == 0) {
    if (gate.compare_exchange_weak(expected, expected | kDbGateClosed,
                                   std::memory_order_acq_rel,
                                   std::memory_order_acquire)) {
      return true;
    }
  }
  return false;
}

void OpenDbGate(std::uint8_t db_id) noexcept {
  g_db_gates[db_id].store(0, std::memory_order_release);
}

class DbOperationGuard {
 public:
  explicit DbOperationGuard(std::uint8_t db_id) : db_id_(db_id) {}
  DbOperationGuard(const DbOperationGuard&) = delete;
  DbOperationGuard& operator=(const DbOperationGuard&) = delete;
  ~DbOperationGuard() { EndDbOperation(db_id_); }

 private:
  std::uint8_t db_id_;
};

class DbCloseGuard {
 public:
  explicit DbCloseGuard(std::uint8_t db_id) : db_id_(db_id) {}
  DbCloseGuard(const DbCloseGuard&) = delete;
  DbCloseGuard& operator=(const DbCloseGuard&) = delete;
  ~DbCloseGuard() { OpenDbGate(db_id_); }

 private:
  std::uint8_t db_id_;
};

Task<CommandReply> ExecuteFlushDb(const CommandRequest& request) {
  if (request.args.size() != 1) {
    co_return EncodedReply(
        EncodeError("ERR wrong number of arguments for 'flushdb' command"));
  }
  if (!CloseDbGate(request.db_id)) {
    co_return EncodedReply(
        EncodeError("BUSY another FLUSHDB is already running"));
  }
  DbCloseGuard reopen(request.db_id);
  while ((g_db_gates[request.db_id].load(std::memory_order_acquire) &
          kDbGateCountMask) != 0) {
    Status waited = co_await celer::SleepFor(
        *ThisWorker().self, std::chrono::milliseconds(1));
    if (!waited.ok()) {
      co_return EncodedReply(EncodeError("ERR " + waited.message()));
    }
  }
  Status status = co_await g_storage->FlushDb(request.db_id);
  co_return EncodedReply(status.ok() ? EncodeSimpleString("OK")
                                     : EncodeError("ERR " + status.message()));
}

struct ScanOptions {
  std::uint64_t cursor = 0;
  std::size_t count = 10;
  std::optional<std::string_view> pattern;
};

StatusOr<ScanOptions> ParseScanOptions(const std::vector<std::string>& args) {
  if (args.size() < 2) {
    return Status(StatusCode::kInvalidArgument,
                  "wrong number of arguments for 'scan' command");
  }
  ScanOptions options;
  const char* cursor_begin = args[1].data();
  const char* cursor_end = cursor_begin + args[1].size();
  auto [parsed_cursor, cursor_error] =
      std::from_chars(cursor_begin, cursor_end, options.cursor);
  if (cursor_error != std::errc{} || parsed_cursor != cursor_end) {
    return Status(StatusCode::kInvalidArgument, "invalid cursor");
  }

  for (std::size_t i = 2; i < args.size();) {
    if (CmpCaseInsensitive(args[i], "COUNT") && i + 1 < args.size()) {
      std::uint64_t count = 0;
      const char* begin = args[i + 1].data();
      const char* end = begin + args[i + 1].size();
      auto [parsed, error] = std::from_chars(begin, end, count);
      if (error != std::errc{} || parsed != end || count == 0 ||
          count > std::numeric_limits<std::size_t>::max()) {
        return Status(StatusCode::kInvalidArgument,
                      "value is not an integer or out of range");
      }
      options.count = static_cast<std::size_t>(count);
      i += 2;
      continue;
    }
    if (CmpCaseInsensitive(args[i], "MATCH") && i + 1 < args.size()) {
      options.pattern = args[i + 1];
      i += 2;
      continue;
    }
    return Status(StatusCode::kInvalidArgument, "syntax error");
  }
  return options;
}

bool MatchCharacterClass(std::string_view pattern, std::size_t open,
                         unsigned char value, std::size_t* next,
                         bool* valid) {
  std::size_t i = open + 1;
  bool negate = false;
  if (i < pattern.size() && (pattern[i] == '^' || pattern[i] == '!')) {
    negate = true;
    ++i;
  }
  bool matched = false;
  bool any = false;
  while (i < pattern.size() && pattern[i] != ']') {
    unsigned char first = static_cast<unsigned char>(pattern[i++]);
    if (first == '\\' && i < pattern.size()) {
      first = static_cast<unsigned char>(pattern[i++]);
    }
    any = true;
    if (i + 1 < pattern.size() && pattern[i] == '-' &&
        pattern[i + 1] != ']') {
      ++i;
      unsigned char last = static_cast<unsigned char>(pattern[i++]);
      if (last == '\\' && i < pattern.size()) {
        last = static_cast<unsigned char>(pattern[i++]);
      }
      if (first > last) std::swap(first, last);
      matched = matched || (value >= first && value <= last);
    } else {
      matched = matched || value == first;
    }
  }
  if (i >= pattern.size() || pattern[i] != ']' || !any) {
    *valid = false;
    *next = open + 1;
    return value == static_cast<unsigned char>('[');
  }
  *valid = true;
  *next = i + 1;
  return negate ? !matched : matched;
}

bool GlobMatch(std::string_view pattern, std::string_view text) {
  std::size_t p = 0;
  std::size_t t = 0;
  std::size_t star_pattern = std::string_view::npos;
  std::size_t star_text = 0;
  while (t < text.size()) {
    if (p < pattern.size() && pattern[p] == '*') {
      while (p < pattern.size() && pattern[p] == '*') ++p;
      if (p == pattern.size()) return true;
      star_pattern = p;
      star_text = t;
      continue;
    }

    bool matched = false;
    std::size_t next = p;
    if (p < pattern.size()) {
      if (pattern[p] == '?') {
        matched = true;
        next = p + 1;
      } else if (pattern[p] == '[') {
        bool valid = false;
        matched = MatchCharacterClass(
            pattern, p, static_cast<unsigned char>(text[t]), &next, &valid);
        if (!valid) next = p + 1;
      } else {
        if (pattern[p] == '\\' && p + 1 < pattern.size()) ++p;
        matched = static_cast<unsigned char>(pattern[p]) ==
                  static_cast<unsigned char>(text[t]);
        next = p + 1;
      }
    }
    if (matched) {
      p = next;
      ++t;
      continue;
    }
    if (star_pattern != std::string_view::npos && star_text < text.size()) {
      p = star_pattern;
      t = ++star_text;
      continue;
    }
    return false;
  }
  while (p < pattern.size() && pattern[p] == '*') ++p;
  return p == pattern.size();
}

Task<CommandReply> ExecuteScan(const CommandRequest& request) {
  auto parsed = ParseScanOptions(request.args);
  if (!parsed.ok()) {
    co_return EncodedReply(EncodeError("ERR " + parsed.status().message()));
  }

  constexpr unsigned kPartitionBits = 14;
  constexpr unsigned kLocalBits = 64 - kPartitionBits;
  constexpr std::uint64_t kPackedLocalMask =
      (std::uint64_t{1} << kLocalBits) - 1;
  constexpr std::uint64_t kDroppedLocalMask =
      (std::uint64_t{1} << kPartitionBits) - 1;
  constexpr std::size_t kMaxPartitionsPerCall = 64;
  static_assert(storage::kLogicalStorageShards ==
                (std::uint64_t{1} << kPartitionBits));

  const ScanOptions& options = *parsed;
  unsigned partition_id =
      static_cast<unsigned>(options.cursor >> kLocalBits);
  // ScanHashMap's reverse-bit cursor for a table with at most 2^50 buckets
  // always has 14 zero low bits. Pack its significant high 50 bits below the
  // 14-bit partition id and restore the zeros before scanning the local map.
  std::uint64_t local_cursor =
      (options.cursor & kPackedLocalMask) << kPartitionBits;
  if (options.cursor != 0 &&
      partition_id >= storage::kLogicalStorageShards) {
    co_return EncodedReply(EncodeError("ERR invalid cursor"));
  }

  std::size_t remaining = options.count;
  std::size_t partitions_examined = 0;
  std::vector<std::string> keys;
  while (partition_id < storage::kLogicalStorageShards) {
    const unsigned worker_id = partition_id % g_storage->worker_count();
    storage::ScanBatch batch;
    if (worker_id == ThisWorker().id) {
      batch = g_storage->ScanPartition(
          static_cast<std::uint16_t>(partition_id), request.db_id,
          local_cursor, remaining);
    } else {
      batch = co_await SubmitTo(
          worker_id,
          [partition_id, db_id = request.db_id, local_cursor, remaining] {
            return g_storage->ScanPartition(
                static_cast<std::uint16_t>(partition_id), db_id,
                local_cursor, remaining);
          });
    }
    ++partitions_examined;

    const std::size_t examined = batch.keys.size();
    for (std::string& key : batch.keys) {
      if (!options.pattern.has_value() ||
          GlobMatch(*options.pattern, key)) {
        keys.push_back(std::move(key));
      }
    }

    if (batch.cursor != 0) {
      if ((batch.cursor & kDroppedLocalMask) != 0) {
        co_return EncodedReply(
            EncodeError("ERR local scan cursor overflow"));
      }
      const std::uint64_t cursor =
          (static_cast<std::uint64_t>(partition_id) << kLocalBits) |
          (batch.cursor >> kPartitionBits);
      co_return EncodedReply(EncodeScanReply(cursor, keys));
    }

    ++partition_id;
    local_cursor = 0;
    if (partition_id >= storage::kLogicalStorageShards) {
      co_return EncodedReply(EncodeScanReply(0, keys));
    }
    if (examined >= remaining ||
        partitions_examined >= kMaxPartitionsPerCall) {
      const std::uint64_t cursor =
          static_cast<std::uint64_t>(partition_id) << kLocalBits;
      co_return EncodedReply(EncodeScanReply(cursor, keys));
    }
    remaining -= examined;
  }
  co_return EncodedReply(EncodeScanReply(0, keys));
}

std::uint64_t CommandUnixTimeMillis() noexcept {
  const auto value = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
  return value > 0 ? static_cast<std::uint64_t>(value) : 0;
}

bool ParseInt64(std::string_view text, std::int64_t* value) {
  if (value == nullptr || text.empty()) {
    return false;
  }
  const char* begin = text.data();
  const char* end = begin + text.size();
  const auto [parsed_end, error] = std::from_chars(begin, end, *value);
  return error == std::errc{} && parsed_end == end;
}

std::string EncodeStorageError(const Status& status) {
  if (status.message().starts_with("WRONGTYPE ")) {
    return EncodeError(status.message());
  }
  return EncodeError("ERR " + status.message());
}

StatusOr<storage::SetOptions> ParseSetOptions(
    const std::vector<std::string>& args) {
  storage::SetOptions options;
  bool condition_seen = false;
  bool expiration_seen = false;
  bool get_seen = false;
  const std::uint64_t now_ms = CommandUnixTimeMillis();
  constexpr std::uint64_t kMaxTimestamp =
      static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max());

  for (std::size_t i = 3; i < args.size(); ++i) {
    const std::string_view option = args[i];
    if (CmpCaseInsensitive(option, "NX") ||
        CmpCaseInsensitive(option, "XX")) {
      if (condition_seen) {
        return Status(StatusCode::kInvalidArgument, "syntax error");
      }
      condition_seen = true;
      options.condition = CmpCaseInsensitive(option, "NX")
                              ? storage::SetCondition::kIfAbsent
                              : storage::SetCondition::kIfPresent;
      continue;
    }
    if (CmpCaseInsensitive(option, "GET")) {
      if (get_seen) {
        return Status(StatusCode::kInvalidArgument, "syntax error");
      }
      get_seen = true;
      options.return_old_value = true;
      continue;
    }
    if (CmpCaseInsensitive(option, "KEEPTTL")) {
      if (expiration_seen) {
        return Status(StatusCode::kInvalidArgument, "syntax error");
      }
      expiration_seen = true;
      options.keep_ttl = true;
      continue;
    }

    const bool ex = CmpCaseInsensitive(option, "EX");
    const bool px = CmpCaseInsensitive(option, "PX");
    const bool exat = CmpCaseInsensitive(option, "EXAT");
    const bool pxat = CmpCaseInsensitive(option, "PXAT");
    if (!ex && !px && !exat && !pxat) {
      return Status(StatusCode::kInvalidArgument, "syntax error");
    }
    if (expiration_seen || i + 1 >= args.size()) {
      return Status(StatusCode::kInvalidArgument, "syntax error");
    }
    expiration_seen = true;
    std::int64_t parsed = 0;
    if (!ParseInt64(args[++i], &parsed)) {
      return Status(StatusCode::kInvalidArgument,
                    "value is not an integer or out of range");
    }
    if (parsed <= 0) {
      return Status(StatusCode::kInvalidArgument,
                    "invalid expire time in 'set' command");
    }
    const std::uint64_t amount = static_cast<std::uint64_t>(parsed);
    if (ex || exat) {
      if (amount > kMaxTimestamp / 1000) {
        return Status(StatusCode::kInvalidArgument,
                      "invalid expire time in 'set' command");
      }
    }
    const std::uint64_t millis = (ex || exat) ? amount * 1000 : amount;
    if (ex || px) {
      if (millis > kMaxTimestamp - now_ms) {
        return Status(StatusCode::kInvalidArgument,
                      "invalid expire time in 'set' command");
      }
      options.expire_at_ms = now_ms + millis;
    } else {
      options.expire_at_ms = millis;
    }
  }
  return options;
}

StatusOr<storage::ExpirationCondition> ParseExpirationCondition(
    const std::vector<std::string>& args) {
  if (args.size() == 3) {
    return storage::ExpirationCondition::kNone;
  }
  if (args.size() != 4) {
    return Status(StatusCode::kInvalidArgument, "syntax error");
  }
  if (CmpCaseInsensitive(args[3], "NX")) {
    return storage::ExpirationCondition::kIfNoExpiration;
  }
  if (CmpCaseInsensitive(args[3], "XX")) {
    return storage::ExpirationCondition::kIfHasExpiration;
  }
  if (CmpCaseInsensitive(args[3], "GT")) {
    return storage::ExpirationCondition::kIfGreater;
  }
  if (CmpCaseInsensitive(args[3], "LT")) {
    return storage::ExpirationCondition::kIfLess;
  }
  return Status(StatusCode::kInvalidArgument, "syntax error");
}

Task<CommandReply> ExecuteStorageCommand(const CommandRequest& request,
                                         ReadLatencyTrace* read_trace = nullptr) {
  CommandReply reply;
  const auto& args = request.args;
  switch (request.kind) {
    case CommandKind::kGet: {
      if (args.size() != 2) {
        reply.encoded =
            EncodeError("ERR wrong number of arguments for 'get' command");
        co_return reply;
      }
      auto value =
          co_await g_storage->Get(request.db_id, args[1], read_trace);
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
      if (args.size() < 3) {
        reply.encoded =
            EncodeError("ERR wrong number of arguments for 'set' command");
        co_return reply;
      }
      auto options = ParseSetOptions(args);
      if (!options.ok()) {
        reply.encoded = EncodeError("ERR " + options.status().message());
        co_return reply;
      }
      auto result = co_await g_storage->Set(request.db_id, args[1], args[2],
                                            *options);
      if (!result.ok()) {
        reply.encoded = EncodeStorageError(result.status());
        co_return reply;
      }
      if (options->return_old_value) {
        if (result->old_value.has_value()) {
          reply.disk_value.emplace(std::move(*result->old_value));
        } else {
          reply.encoded = EncodeNullBulkString();
        }
      } else {
        reply.encoded = result->applied ? EncodeSimpleString("OK")
                                        : EncodeNullBulkString();
      }
      co_return reply;
    }

    case CommandKind::kStrlen: {
      if (args.size() != 2) {
        reply.encoded =
            EncodeError("ERR wrong number of arguments for 'strlen' command");
        co_return reply;
      }
      auto length =
          co_await g_storage->StringLength(request.db_id, args[1]);
      if (!length.ok()) {
        if (length.status().code() == StatusCode::kNotFound) {
          reply.encoded = EncodeInteger(0);
        } else {
          reply.encoded = EncodeStorageError(length.status());
        }
      } else if (*length > static_cast<std::uint64_t>(
                                std::numeric_limits<long long>::max())) {
        reply.encoded = EncodeError("ERR String length exceeds RESP range");
      } else {
        reply.encoded = EncodeInteger(static_cast<long long>(*length));
      }
      co_return reply;
    }

    case CommandKind::kTtl:
    case CommandKind::kPttl: {
      const bool milliseconds = request.kind == CommandKind::kPttl;
      if (args.size() != 2) {
        reply.encoded = EncodeError(
            std::string("ERR wrong number of arguments for '") +
            (milliseconds ? "pttl" : "ttl") + "' command");
        co_return reply;
      }
      const storage::ExpirationInfo info =
          co_await g_storage->GetExpiration(request.db_id, args[1]);
      if (!info.exists) {
        reply.encoded = EncodeInteger(-2);
      } else if (info.expire_at_ms == 0) {
        reply.encoded = EncodeInteger(-1);
      } else {
        const std::uint64_t now_ms = CommandUnixTimeMillis();
        const std::uint64_t remaining =
            info.expire_at_ms > now_ms ? info.expire_at_ms - now_ms : 0;
        const std::uint64_t output = milliseconds ? remaining
                                                   : remaining / 1000;
        reply.encoded = EncodeInteger(static_cast<long long>(output));
      }
      co_return reply;
    }

    case CommandKind::kExpire:
    case CommandKind::kPExpire: {
      const bool milliseconds = request.kind == CommandKind::kPExpire;
      if (args.size() < 3 || args.size() > 4) {
        reply.encoded = EncodeError(
            std::string("ERR wrong number of arguments for '") +
            (milliseconds ? "pexpire" : "expire") + "' command");
        co_return reply;
      }
      std::int64_t duration = 0;
      if (!ParseInt64(args[2], &duration)) {
        reply.encoded =
            EncodeError("ERR value is not an integer or out of range");
        co_return reply;
      }
      auto condition = ParseExpirationCondition(args);
      if (!condition.ok()) {
        reply.encoded = EncodeError("ERR syntax error");
        co_return reply;
      }
      const std::uint64_t now_ms = CommandUnixTimeMillis();
      constexpr std::uint64_t kMaxTimestamp =
          static_cast<std::uint64_t>(
              std::numeric_limits<std::int64_t>::max());
      std::uint64_t expire_at_ms = 1;
      if (duration > 0) {
        const std::uint64_t amount = static_cast<std::uint64_t>(duration);
        const std::uint64_t factor = milliseconds ? 1 : 1000;
        if (amount > (kMaxTimestamp - now_ms) / factor) {
          reply.encoded =
              EncodeError("ERR invalid expire time in 'expire' command");
          co_return reply;
        }
        expire_at_ms = now_ms + amount * factor;
      }
      auto updated = co_await g_storage->UpdateExpiration(
          request.db_id, args[1], expire_at_ms, *condition);
      reply.encoded = updated.ok()
                          ? EncodeInteger(*updated ? 1 : 0)
                          : EncodeStorageError(updated.status());
      co_return reply;
    }

    case CommandKind::kPersist: {
      if (args.size() != 2) {
        reply.encoded =
            EncodeError("ERR wrong number of arguments for 'persist' command");
        co_return reply;
      }
      auto updated = co_await g_storage->UpdateExpiration(
          request.db_id, args[1], 0,
          storage::ExpirationCondition::kIfHasExpiration);
      reply.encoded = updated.ok()
                          ? EncodeInteger(*updated ? 1 : 0)
                          : EncodeStorageError(updated.status());
      co_return reply;
    }

    case CommandKind::kIncr: {
      if (args.size() != 2) {
        reply.encoded =
            EncodeError("ERR wrong number of arguments for 'incr' command");
        co_return reply;
      }
      auto value = co_await g_storage->Increment(request.db_id, args[1]);
      if (!value.ok()) {
        if (value.status().message().starts_with("WRONGTYPE ")) {
          reply.encoded = EncodeError(value.status().message());
        } else {
          reply.encoded =
              value.status().code() == StatusCode::kInvalidArgument
                  ? EncodeError(
                        "ERR value is not an integer or out of range")
                  : EncodeError("ERR " + value.status().message());
        }
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
    co_return EncodedReply(EncodeError(
        is_del ? "ERR wrong number of arguments for 'del' command"
               : "ERR wrong number of arguments for 'exists' command"));
  }

  long long count = 0;
  for (std::size_t i = 1; i < args.size(); ++i) {
    const std::string_view key = args[i];
    const unsigned target = ShardForKey(key);
    bool hit = false;
    if (target == ThisWorker().id) {
      if (is_del) {
        auto deleted = co_await g_storage->Delete(request.db_id, key);
        if (!deleted.ok()) {
          co_return EncodedReply(
              EncodeError("ERR " + deleted.status().message()));
        }
        hit = *deleted;
      } else {
        hit = co_await g_storage->Exists(request.db_id, key);
      }
    } else {
      if (is_del) {
        auto deleted = co_await SubmitTaskTo(
            target, [db_id = request.db_id, key]() -> Task<StatusOr<bool>> {
              co_return co_await g_storage->Delete(db_id, key);
            });
        if (!deleted.ok()) {
          co_return EncodedReply(
              EncodeError("ERR " + deleted.status().message()));
        }
        hit = *deleted;
      } else {
        hit = co_await SubmitTaskTo(
            target, [db_id = request.db_id, key]() -> Task<bool> {
              co_return co_await g_storage->Exists(db_id, key);
            });
      }
    }
    if (hit) {
      ++count;
    }
  }
  co_return EncodedReply(EncodeInteger(count));
}

}  // namespace

void InitStorage(storage::StorageEngine* engine, bool replica_read_only) {
  g_storage = engine;
  g_replica_read_only = replica_read_only;
}

Task<CommandReply> ExecuteCommand(const CommandRequest& request) {
  const auto& args = request.args;
  const bool mutating = request.kind == CommandKind::kSet ||
                        request.kind == CommandKind::kIncr ||
                        request.kind == CommandKind::kDel ||
                        request.kind == CommandKind::kExpire ||
                        request.kind == CommandKind::kPExpire ||
                        request.kind == CommandKind::kPersist ||
                        request.kind == CommandKind::kFlushDb;
  if (g_replica_read_only && mutating) {
    co_return EncodedReply(EncodeError(
        "READONLY You can't write against a read only replica."));
  }
  if (request.kind == CommandKind::kFlushDb) {
    co_return co_await ExecuteFlushDb(request);
  }

  const bool uses_db = request.kind == CommandKind::kDbSize ||
                       request.kind == CommandKind::kScan ||
                       request.kind == CommandKind::kDel ||
                       request.kind == CommandKind::kExists ||
                       request.kind == CommandKind::kGet ||
                       request.kind == CommandKind::kStrlen ||
                       request.kind == CommandKind::kSet ||
                       request.kind == CommandKind::kIncr ||
                       request.kind == CommandKind::kExpire ||
                       request.kind == CommandKind::kPExpire ||
                       request.kind == CommandKind::kPersist ||
                       request.kind == CommandKind::kTtl ||
                       request.kind == CommandKind::kPttl;
  if (uses_db && !TryBeginDbOperation(request.db_id)) {
    co_return EncodedReply(
        EncodeError("TRYAGAIN FLUSHDB is in progress"));
  }
  std::optional<DbOperationGuard> db_guard;
  if (uses_db) {
    db_guard.emplace(request.db_id);
  }

  switch (request.kind) {
    case CommandKind::kDbSize:
      co_return co_await ExecuteDbSize(request);

    case CommandKind::kScan:
      co_return co_await ExecuteScan(request);

    case CommandKind::kDel:
    case CommandKind::kExists:
      co_return co_await RouteMultiKey(request);

    case CommandKind::kGet:
    case CommandKind::kStrlen:
    case CommandKind::kSet:
    case CommandKind::kIncr:
    case CommandKind::kExpire:
    case CommandKind::kPExpire:
    case CommandKind::kPersist:
    case CommandKind::kTtl:
    case CommandKind::kPttl:
      if (args.size() >= 2) {
        const unsigned target = ShardForKey(args[1]);
#if KEYLANE_ENABLE_READ_LATENCY_TRACE
        if (request.kind == CommandKind::kGet) {
          ReadLatencyTrace trace;
          trace.request_start_ns = ReadTraceNowNanos();
          trace.remote = target != ThisWorker().id;
          CommandReply reply;
          if (trace.remote) {
            reply = co_await SubmitTaskTo(
                target, [&request, &trace]() -> Task<CommandReply> {
                  trace.owner_start_ns = ReadTraceNowNanos();
                  CommandReply result =
                      co_await ExecuteStorageCommand(request, &trace);
                  trace.owner_done_ns = ReadTraceNowNanos();
                  co_return result;
                });
          } else {
            trace.owner_start_ns = trace.request_start_ns;
            reply = co_await ExecuteStorageCommand(request, &trace);
            trace.owner_done_ns = ReadTraceNowNanos();
          }
          trace.origin_resume_ns = ReadTraceNowNanos();
          reply.read_trace = trace;
          co_return reply;
        }
#endif
        if (target != ThisWorker().id) {
          co_return co_await SubmitTaskTo(
              target, [&request]() -> Task<CommandReply> {
                co_return co_await ExecuteStorageCommand(request);
              });
        }
      }
      co_return co_await ExecuteStorageCommand(request);

    case CommandKind::kFlushDb:
      // Handled before the DB operation gate above.
      co_return EncodedReply(EncodeError("ERR internal FLUSHDB routing error"));

    default:  // PING, SELECT, local single-key path, unknown
      co_return ExecuteLocalCommand(request);
  }
}

}  // namespace keylane
