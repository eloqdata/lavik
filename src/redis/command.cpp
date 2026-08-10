#include "keylane/command.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "absl/strings/str_cat.h"
#include "celer/io/storage.h"
#include "celer/runtime/cross_core.h"
#include "celer/runtime/cycle_clock.h"
#include "celer/runtime/worker.h"
#include "keylane/command_table.h"
#include "keylane/memory.h"
#include "keylane/metrics.h"
#include "keylane/resp.h"
#include "keylane/session.h"
#include "keylane/storage/engine.h"
#include "keylane/storage/format.h"
#include "keylane/tx/transaction.h"
#include "keylane/tx/tx_shard.h"

namespace keylane {
using namespace celer;

namespace {

storage::StorageEngine* g_storage = nullptr;
bool g_replica_read_only = false;
std::uint16_t g_server_port = 0;
unsigned g_server_threads = 0;
std::chrono::steady_clock::time_point g_server_start;

constexpr std::size_t kEstimatedIndexBytesPerKey = 512;

std::size_t SaturatingAdd(std::size_t left, std::size_t right) noexcept {
  return right > std::numeric_limits<std::size_t>::max() - left
             ? std::numeric_limits<std::size_t>::max()
             : left + right;
}

std::size_t RequestArgumentBytes(const CommandRequest& request) noexcept {
  std::size_t result = sizeof(CommandRequest);
  for (const std::string& argument : request.args_) {
    result = SaturatingAdd(result, argument.size());
  }
  return result;
}

std::size_t EstimatedMemoryGrowth(const CommandRequest& request) noexcept {
  std::size_t keys = 0;
  switch (request.kind_) {
    case CommandKind::kSet:
    case CommandKind::kIncr:
      keys = 1;
      break;
    case CommandKind::kMSet:
      keys = request.args_.size() > 1 ? (request.args_.size() - 1) / 2 : 0;
      break;
    default:
      return 0;
  }
  const std::size_t index_bytes =
      keys > std::numeric_limits<std::size_t>::max() /
                  kEstimatedIndexBytesPerKey
          ? std::numeric_limits<std::size_t>::max()
          : keys * kEstimatedIndexBytesPerKey;
  // Include parsed arguments because the cached allocator sample can precede
  // this request by up to 100ms. Actual process growth is reconciled from RSS
  // by the background sampler.
  return SaturatingAdd(RequestArgumentBytes(request), index_bytes);
}

bool RejectForMemory(std::size_t additional_bytes) noexcept {
  if (additional_bytes == 0 || !WouldExceedMemoryLimit(additional_bytes)) {
    return false;
  }
  RecordMemoryRejection();
  return true;
}

std::string_view AppendOomError(ReplyBuilder& reply_builder) {
  return reply_builder.AppendError(
      "OOM command not allowed when used memory > 'maxmemory'.");
}

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

}  // namespace

absl::StatusOr<CommandRequest> BuildCommandRequest(RespCommand command,
                                                   std::uint8_t db_id) {
  if (command.args_.empty()) {
    return absl::Status(absl::StatusCode::kInvalidArgument, "empty command");
  }

  CommandRequest request;
  request.spec_ = FindCommand(command.args_.front());
  request.kind_ =
      request.spec_ != nullptr ? request.spec_->kind_ : CommandKind::kUnknown;
  request.db_id_ = db_id;
  request.args_ = std::move(command.args_);
  return request;
}

namespace {

CommandReply ExecuteSimpleLocalCommand(const CommandRequest& request,
                                       ReplyBuilder& reply_builder) {
  CommandReply reply;
  const auto& args = request.args_;

  switch (request.kind_) {
    case CommandKind::kPing:
      if (args.size() == 1) {
        reply.encoded_ = reply_builder.AppendSimpleString("PONG");
      } else if (args.size() == 2) {
        reply.encoded_ = reply_builder.AppendBulkString(args[1]);
      } else {
        reply.encoded_ = reply_builder.AppendError(
            "ERR wrong number of arguments for 'ping' command");
      }
      return reply;

    case CommandKind::kEcho:
      if (args.size() != 2) {
        reply.encoded_ = reply_builder.AppendError(
            "ERR wrong number of arguments for 'echo' command");
      } else {
        reply.encoded_ = reply_builder.AppendBulkString(args[1]);
      }
      return reply;

    case CommandKind::kUnwatch:
      // Inside EXEC this is a no-op: the transaction consumes the watches
      // itself. Outside MULTI, DispatchCommand clears them before this runs.
      reply.encoded_ = reply_builder.AppendSimpleString("OK");
      return reply;

    case CommandKind::kSelect: {
      if (args.size() != 2) {
        reply.encoded_ = reply_builder.AppendError(
            "ERR wrong number of arguments for 'select' command");
        return reply;
      }
      unsigned db_id = 0;
      const char* begin = args[1].data();
      const char* end = begin + args[1].size();
      const auto [parsed_end, error] = std::from_chars(begin, end, db_id);
      if (error != std::errc{} || parsed_end != end ||
          db_id >= storage::kLogicalDatabaseCount) {
        reply.encoded_ =
            reply_builder.AppendError("ERR DB index is out of range");
        return reply;
      }
      reply.encoded_ = reply_builder.AppendSimpleString("OK");
      reply.selected_db_ = static_cast<std::uint8_t>(db_id);
      return reply;
    }

    default:
      reply.encoded_ = reply_builder.AppendError("ERR unknown command '" +
                                                 args.front() + "'");
      return reply;
  }
}

Task<CommandReply> ExecuteDbSize(const CommandRequest& request,
                                 ReplyBuilder& reply_builder) {
  if (request.args_.size() != 1) {
    co_return BuiltReply(reply_builder.AppendError(
        "ERR wrong number of arguments for 'dbsize' command"));
  }

  std::uint64_t total = 0;
  const std::uint8_t db_id = request.db_id_;
  for (unsigned target = 0; target < g_storage->worker_count(); ++target) {
    const std::size_t local_size = co_await SubmitTo(
        target, [db_id] { return g_storage->LocalSize(db_id); });
    if (local_size > std::numeric_limits<std::uint64_t>::max() - total) {
      co_return BuiltReply(reply_builder.AppendError("ERR db size overflow"));
    }
    total += local_size;
  }
  if (total >
      static_cast<std::uint64_t>(std::numeric_limits<long long>::max())) {
    co_return BuiltReply(
        reply_builder.AppendError("ERR db size exceeds RESP integer range"));
  }
  co_return BuiltReply(
      reply_builder.AppendInteger(static_cast<long long>(total)));
}

bool ParseUint64(std::string_view text, std::uint64_t* value) {
  if (text.empty()) {
    return false;
  }
  const char* begin = text.data();
  const char* end = begin + text.size();
  const auto [parsed_end, error] = std::from_chars(begin, end, *value);
  return error == std::errc{} && parsed_end == end;
}

absl::StatusOr<std::uint32_t> ParseDailySecond(std::string_view text) {
  std::array<std::uint64_t, 3> parts{};
  std::size_t count = 0;
  while (!text.empty() && count < parts.size()) {
    const std::size_t separator = text.find(':');
    const std::string_view part = text.substr(0, separator);
    if (!ParseUint64(part, &parts[count++])) {
      return absl::Status(absl::StatusCode::kInvalidArgument,
                          "daily time must be HH:MM or HH:MM:SS");
    }
    if (separator == std::string_view::npos) {
      text = {};
    } else {
      text.remove_prefix(separator + 1);
      if (text.empty()) {
        return absl::Status(absl::StatusCode::kInvalidArgument,
                            "daily time must be HH:MM or HH:MM:SS");
      }
    }
  }
  if (!text.empty() || (count != 2 && count != 3) || parts[0] >= 24 ||
      parts[1] >= 60 || parts[2] >= 60) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "daily time must be HH:MM or HH:MM:SS");
  }
  return static_cast<std::uint32_t>(parts[0] * 3600 + parts[1] * 60 + parts[2]);
}

std::string FormatDailySecond(std::uint32_t daily_second) {
  const std::uint32_t hour = daily_second / 3600;
  const std::uint32_t minute = (daily_second % 3600) / 60;
  const std::uint32_t second = daily_second % 60;
  auto two_digits = [](std::uint32_t value) {
    return value < 10 ? absl::StrCat("0", value) : absl::StrCat(value);
  };
  return absl::StrCat(two_digits(hour), ":", two_digits(minute), ":",
                      two_digits(second));
}

std::string_view TombRaiderModeName(storage::TombRaiderMode mode) {
  switch (mode) {
    case storage::TombRaiderMode::kOff:
      return "off";
    case storage::TombRaiderMode::kInterval:
      return "interval";
    case storage::TombRaiderMode::kDaily:
      return "daily";
  }
  return "off";
}

Task<CommandReply> ExecuteTombRaider(const CommandRequest& request,
                                     ReplyBuilder& reply_builder) {
  const auto& args = request.args_;
  if (args.size() < 2 || args.size() > 3) {
    co_return BuiltReply(reply_builder.AppendError(
        "ERR wrong number of arguments for 'tombraider' command"));
  }

  if (CmpCaseInsensitive(args[1], "STATUS")) {
    if (args.size() != 2) {
      co_return BuiltReply(reply_builder.AppendError("ERR syntax error"));
    }
    const storage::TombRaiderTotals status = g_storage->TombRaiderStats();
    co_return BuiltReply(reply_builder.AppendBulkString(absl::StrCat(
        "mode=", TombRaiderModeName(status.mode_), " interval_ms=",
        status.interval_ms_, " block_sleep_ms=", status.block_sleep_ms_,
        " daily=", FormatDailySecond(status.daily_second_),
        " timezone=local running=", status.running_ ? 1 : 0)));
  }

  storage::TombRaiderConfigUpdate update;
  if (CmpCaseInsensitive(args[1], "ON") && args.size() == 2) {
    update.action_ = storage::TombRaiderConfigAction::kOn;
  } else if (CmpCaseInsensitive(args[1], "OFF") && args.size() == 2) {
    update.action_ = storage::TombRaiderConfigAction::kOff;
  } else if (CmpCaseInsensitive(args[1], "INTERVAL") && args.size() == 3) {
    update.action_ = storage::TombRaiderConfigAction::kInterval;
    if (!ParseUint64(args[2], &update.value_) || update.value_ == 0) {
      co_return BuiltReply(reply_builder.AppendError(
          "ERR value is not an integer or out of range"));
    }
  } else if ((CmpCaseInsensitive(args[1], "BLOCK-SLEEP") ||
              CmpCaseInsensitive(args[1], "SLEEP")) &&
             args.size() == 3) {
    update.action_ = storage::TombRaiderConfigAction::kBlockSleep;
    if (!ParseUint64(args[2], &update.value_) ||
        update.value_ > std::numeric_limits<std::uint32_t>::max()) {
      co_return BuiltReply(reply_builder.AppendError(
          "ERR value is not an integer or out of range"));
    }
  } else if (CmpCaseInsensitive(args[1], "DAILY") && args.size() == 3) {
    auto daily_second = ParseDailySecond(args[2]);
    if (!daily_second.ok()) {
      co_return BuiltReply(reply_builder.AppendError(
          absl::StrCat("ERR ", daily_second.status().message())));
    }
    update.action_ = storage::TombRaiderConfigAction::kDaily;
    update.value_ = *daily_second;
  } else {
    co_return BuiltReply(reply_builder.AppendError("ERR syntax error"));
  }

  const absl::Status configured =
      co_await g_storage->ConfigureTombRaider(update);
  co_return configured.ok() ? BuiltReply(reply_builder.AppendSimpleString("OK"))
                            : BuiltReply(reply_builder.AppendError(
                                  absl::StrCat("ERR ", configured.message())));
}

Task<CommandReply> ExecuteDefrag(const CommandRequest& request,
                                ReplyBuilder& reply_builder) {
  const auto& args = request.args_;
  if (args.size() < 2 || args.size() > 3) {
    co_return BuiltReply(reply_builder.AppendError(
        "ERR wrong number of arguments for 'defrag' command"));
  }

  if (CmpCaseInsensitive(args[1], "STATUS")) {
    if (args.size() != 2) {
      co_return BuiltReply(reply_builder.AppendError("ERR syntax error"));
    }
    const storage::DefragTotals status = g_storage->DefragStats();
    co_return BuiltReply(reply_builder.AppendBulkString(absl::StrCat(
        "paused=", status.paused_ ? 1 : 0, " max_active_per_device=",
        status.max_active_per_device_,
        " block_sleep_ms=", status.block_sleep_ms_, " record_sleep_us=",
        status.record_sleep_us_, " active_total=", status.active_,
        " pending_total=", status.pending_)));
  }

  storage::DefragConfigUpdate update;
  if (CmpCaseInsensitive(args[1], "PAUSE") && args.size() == 2) {
    update.action_ = storage::DefragConfigAction::kPause;
  } else if (CmpCaseInsensitive(args[1], "RESUME") && args.size() == 2) {
    update.action_ = storage::DefragConfigAction::kResume;
  } else if ((CmpCaseInsensitive(args[1], "MAX-ACTIVE") ||
       CmpCaseInsensitive(args[1], "CONCURRENCY")) &&
      args.size() == 3) {
    update.action_ = storage::DefragConfigAction::kMaxActivePerDevice;
    if (!ParseUint64(args[2], &update.value_) || update.value_ == 0 ||
        update.value_ > 8) {
      co_return BuiltReply(reply_builder.AppendError(
          "ERR value is not an integer or out of range"));
    }
  } else if ((CmpCaseInsensitive(args[1], "BLOCK-SLEEP-MS") ||
              CmpCaseInsensitive(args[1], "BLOCK-SLEEP") ||
              CmpCaseInsensitive(args[1], "SLEEP")) &&
             args.size() == 3) {
    update.action_ = storage::DefragConfigAction::kBlockSleep;
    if (!ParseUint64(args[2], &update.value_) ||
        update.value_ > std::numeric_limits<std::uint32_t>::max()) {
      co_return BuiltReply(reply_builder.AppendError(
          "ERR value is not an integer or out of range"));
    }
  } else if ((CmpCaseInsensitive(args[1], "RECORD-SLEEP-US") ||
              CmpCaseInsensitive(args[1], "RECORD-SLEEP")) &&
             args.size() == 3) {
    update.action_ = storage::DefragConfigAction::kRecordSleep;
    if (!ParseUint64(args[2], &update.value_) ||
        update.value_ > std::numeric_limits<std::uint32_t>::max()) {
      co_return BuiltReply(reply_builder.AppendError(
          "ERR value is not an integer or out of range"));
    }
  } else {
    co_return BuiltReply(reply_builder.AppendError("ERR syntax error"));
  }

  const absl::Status configured = co_await g_storage->ConfigureDefrag(update);
  co_return configured.ok() ? BuiltReply(reply_builder.AppendSimpleString("OK"))
                            : BuiltReply(reply_builder.AppendError(
                                  absl::StrCat("ERR ", configured.message())));
}

constexpr std::uint64_t kDbGateClosed = std::uint64_t{1} << 63;
constexpr std::uint64_t kDbGateCountMask = ~kDbGateClosed;
std::array<std::atomic<std::uint64_t>, storage::kLogicalDatabaseCount>
    g_db_gates{};

bool TryBeginDbOperation(std::uint8_t db_id) noexcept {
  auto& gate = g_db_gates[db_id];
  std::uint64_t state = gate.load(std::memory_order_acquire);
  while ((state & kDbGateClosed) == 0) {
    if (gate.compare_exchange_weak(state, state + 1, std::memory_order_acq_rel,
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
  // Clear only the closed bit. After a completed drain the count bits are
  // zero anyway; on an early exit (today only worker shutdown) in-flight
  // operations still hold their counts, and zeroing those would let their
  // EndDbOperation underflow the gate into a permanently-closed value.
  g_db_gates[db_id].fetch_and(~kDbGateClosed, std::memory_order_acq_rel);
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

// Gates several databases at once (EXEC spanning databases via SELECT); the
// destructor releases whatever was successfully begun.
class MultiDbOperationGuard {
 public:
  MultiDbOperationGuard() = default;
  MultiDbOperationGuard(const MultiDbOperationGuard&) = delete;
  MultiDbOperationGuard& operator=(const MultiDbOperationGuard&) = delete;
  ~MultiDbOperationGuard() {
    for (const std::uint8_t db : dbs_) {
      EndDbOperation(db);
    }
  }

  bool Add(std::uint8_t db_id) {
    if (!TryBeginDbOperation(db_id)) {
      return false;
    }
    dbs_.push_back(db_id);
    return true;
  }

 private:
  std::vector<std::uint8_t> dbs_;
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

class MultiDbCloseGuard {
 public:
  MultiDbCloseGuard() = default;
  MultiDbCloseGuard(const MultiDbCloseGuard&) = delete;
  MultiDbCloseGuard& operator=(const MultiDbCloseGuard&) = delete;
  ~MultiDbCloseGuard() {
    for (std::size_t i = 0; i < count_; ++i) {
      OpenDbGate(dbs_[i]);
    }
  }

  bool Add(std::uint8_t db_id) {
    if (!CloseDbGate(db_id)) {
      return false;
    }
    dbs_[count_++] = db_id;
    return true;
  }

 private:
  std::array<std::uint8_t, storage::kLogicalDatabaseCount> dbs_{};
  std::size_t count_ = 0;
};

Task<CommandReply> ExecuteFlush(const CommandRequest& request,
                                ReplyBuilder& reply_builder) {
  const std::string_view command_name =
      request.kind_ == CommandKind::kFlushAll ? "flushall" : "flushdb";
  bool wait_for_reclaim = true;
  if (request.args_.size() == 2) {
    if (CmpCaseInsensitive(request.args_[1], "ASYNC")) {
      wait_for_reclaim = false;
    } else if (!CmpCaseInsensitive(request.args_[1], "SYNC")) {
      co_return BuiltReply(reply_builder.AppendError("ERR syntax error"));
    }
  } else if (request.args_.size() != 1) {
    co_return BuiltReply(reply_builder.AppendError(absl::StrCat(
        "ERR wrong number of arguments for '", command_name, "' command")));
  }

  std::vector<std::uint8_t> dbs;
  if (request.kind_ == CommandKind::kFlushAll) {
    dbs.reserve(storage::kLogicalDatabaseCount);
    for (std::uint8_t db_id = 0; db_id < storage::kLogicalDatabaseCount;
         ++db_id) {
      dbs.push_back(db_id);
    }
  } else {
    dbs.push_back(request.db_id_);
  }

  absl::Status detached = absl::OkStatus();
  {
    // Close the whole target set before draining any one database. FLUSHALL
    // therefore has one exclusion window across all databases rather than
    // allowing writes into an already-detached database while it advances the
    // remaining epochs.
    MultiDbCloseGuard reopen;
    for (const std::uint8_t db_id : dbs) {
      if (!reopen.Add(db_id)) {
        co_return BuiltReply(reply_builder.AppendError(
            "BUSY another database flush is already running"));
      }
    }

    for (const std::uint8_t db_id : dbs) {
      while ((g_db_gates[db_id].load(std::memory_order_acquire) &
              kDbGateCountMask) != 0) {
        absl::Status waited = co_await celer::SleepFor(
            *ThisWorker().self_, std::chrono::milliseconds(1));
        if (!waited.ok()) {
          co_return BuiltReply(reply_builder.AppendError(
              absl::StrCat("ERR ", waited.message())));
        }
      }
    }

    for (const std::uint8_t db_id : dbs) {
      detached = co_await g_storage->FlushDbDetach(db_id);
      if (!detached.ok()) {
        break;
      }
    }
  }

  absl::Status reclaimed = co_await g_storage->FlushDbReclaim(wait_for_reclaim);
  if (!detached.ok()) {
    co_return BuiltReply(
        reply_builder.AppendError(absl::StrCat("ERR ", detached.message())));
  }
  co_return reclaimed.ok() ? BuiltReply(reply_builder.AppendSimpleString("OK"))
                           : BuiltReply(reply_builder.AppendError(
                                 absl::StrCat("ERR ", reclaimed.message())));
}

struct ScanOptions {
  std::uint64_t cursor_ = 0;
  std::size_t count_ = 10;
  std::optional<std::string_view> pattern_;
  std::optional<std::string_view> type_;
};

absl::StatusOr<ScanOptions> ParseScanOptions(
    const std::vector<std::string>& args) {
  if (args.size() < 2) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "wrong number of arguments for 'scan' command");
  }
  ScanOptions options;
  const char* cursor_begin = args[1].data();
  const char* cursor_end = cursor_begin + args[1].size();
  auto [parsed_cursor, cursor_error] =
      std::from_chars(cursor_begin, cursor_end, options.cursor_);
  if (cursor_error != std::errc{} || parsed_cursor != cursor_end) {
    return absl::Status(absl::StatusCode::kInvalidArgument, "invalid cursor");
  }

  for (std::size_t i = 2; i < args.size();) {
    if (CmpCaseInsensitive(args[i], "COUNT") && i + 1 < args.size()) {
      std::uint64_t count = 0;
      const char* begin = args[i + 1].data();
      const char* end = begin + args[i + 1].size();
      auto [parsed, error] = std::from_chars(begin, end, count);
      if (error != std::errc{} || parsed != end || count == 0 ||
          count > std::numeric_limits<std::size_t>::max()) {
        return absl::Status(absl::StatusCode::kInvalidArgument,
                            "value is not an integer or out of range");
      }
      options.count_ = static_cast<std::size_t>(count);
      i += 2;
      continue;
    }
    if (CmpCaseInsensitive(args[i], "MATCH") && i + 1 < args.size()) {
      options.pattern_ = args[i + 1];
      i += 2;
      continue;
    }
    if (CmpCaseInsensitive(args[i], "TYPE") && i + 1 < args.size()) {
      options.type_ = args[i + 1];
      i += 2;
      continue;
    }
    return absl::Status(absl::StatusCode::kInvalidArgument, "syntax error");
  }
  return options;
}

bool MatchCharacterClass(std::string_view pattern, std::size_t open,
                         unsigned char value, std::size_t* next, bool* valid) {
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
    if (i + 1 < pattern.size() && pattern[i] == '-' && pattern[i + 1] != ']') {
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

Task<CommandReply> ExecuteScan(const CommandRequest& request,
                               ReplyBuilder& reply_builder) {
  auto parsed = ParseScanOptions(request.args_);
  if (!parsed.ok()) {
    co_return BuiltReply(reply_builder.AppendError(
        absl::StrCat("ERR ", parsed.status().message())));
  }

  constexpr unsigned kPartitionBits = 14;
  constexpr unsigned kLocalBits = 64 - kPartitionBits;
  constexpr std::uint64_t kPackedLocalMask =
      (std::uint64_t{1} << kLocalBits) - 1;
  constexpr std::uint64_t kDroppedLocalMask =
      (std::uint64_t{1} << kPartitionBits) - 1;
  // COUNT is the per-call work hint: it also bounds how many (mostly
  // empty) partitions one call may examine, so large COUNTs sweep the
  // keyspace in few round trips.
  const std::size_t max_partitions_per_call = std::clamp<std::size_t>(
      parsed->count_, 64, storage::kLogicalStorageShards);
  static_assert(storage::kLogicalStorageShards ==
                (std::uint64_t{1} << kPartitionBits));

  const ScanOptions& options = *parsed;
  unsigned partition_id = static_cast<unsigned>(options.cursor_ >> kLocalBits);
  // ScanHashMap's reverse-bit cursor for a table with at most 2^50 buckets
  // always has 14 zero low bits. Pack its significant high 50 bits below the
  // 14-bit partition id and restore the zeros before scanning the local map.
  std::uint64_t local_cursor = (options.cursor_ & kPackedLocalMask)
                               << kPartitionBits;
  if (options.cursor_ != 0 && partition_id >= storage::kLogicalStorageShards) {
    co_return BuiltReply(reply_builder.AppendError("ERR invalid cursor"));
  }

  std::size_t remaining = options.count_;
  std::size_t partitions_examined = 0;
  std::vector<std::string> keys;
  while (partition_id < storage::kLogicalStorageShards) {
    const unsigned worker_id = partition_id % g_storage->worker_count();
    absl::StatusOr<storage::ScanBatch> scanned;
    if (worker_id == ThisWorker().id_) {
      scanned = co_await g_storage->ScanPartition(
          static_cast<std::uint16_t>(partition_id), request.db_id_,
          local_cursor, remaining);
    } else {
      scanned = co_await celer::SubmitTaskTo(
          worker_id,
          [partition_id, db_id = request.db_id_, local_cursor,
           remaining]() -> Task<absl::StatusOr<storage::ScanBatch>> {
            co_return co_await g_storage->ScanPartition(
                static_cast<std::uint16_t>(partition_id), db_id, local_cursor,
                remaining);
          });
    }
    if (!scanned.ok()) {
      co_return BuiltReply(reply_builder.AppendError(
          absl::StrCat("ERR ", scanned.status().message())));
    }
    storage::ScanBatch batch = std::move(*scanned);
    ++partitions_examined;

    const std::size_t examined = batch.keys_.size();
    const bool type_matches = !options.type_.has_value() ||
                              CmpCaseInsensitive(*options.type_, "string");
    for (std::string& key : batch.keys_) {
      if (type_matches && (!options.pattern_.has_value() ||
                           GlobMatch(*options.pattern_, key))) {
        keys.push_back(std::move(key));
      }
    }

    if (batch.cursor_ != 0) {
      if ((batch.cursor_ & kDroppedLocalMask) != 0) {
        co_return BuiltReply(
            reply_builder.AppendError("ERR local scan cursor overflow"));
      }
      const std::uint64_t cursor =
          (static_cast<std::uint64_t>(partition_id) << kLocalBits) |
          (batch.cursor_ >> kPartitionBits);
      co_return BuiltReply(EncodeScanReply(reply_builder, cursor, keys));
    }

    ++partition_id;
    local_cursor = 0;
    if (partition_id >= storage::kLogicalStorageShards) {
      co_return BuiltReply(EncodeScanReply(reply_builder, 0, keys));
    }
    if (examined >= remaining ||
        partitions_examined >= max_partitions_per_call) {
      const std::uint64_t cursor = static_cast<std::uint64_t>(partition_id)
                                   << kLocalBits;
      co_return BuiltReply(EncodeScanReply(reply_builder, cursor, keys));
    }
    remaining -= examined;
  }
  co_return BuiltReply(EncodeScanReply(reply_builder, 0, keys));
}

std::uint64_t CommandUnixTimeMillis() noexcept;

// KEYS streams its reply in bounded memory. RESP2 arrays announce their
// element count first, so the keyspace must hold still between the counting
// pass and the emitting pass: the database gate is closed (like FLUSHDB) and
// expiration writes are quiesced, with a fixed liveness timestamp shared by
// both passes. State is dropped when the reply finishes or the connection
// dies, reopening the gate either way.
struct KeysStreamState {
  explicit KeysStreamState(std::uint8_t db_id) : db_(db_id), guard_(db_id) {}
  ~KeysStreamState() {
    // Resume only a pause this KEYS actually took: the pause nests across
    // overlapping KEYS on other databases, and the early-error path drops
    // the state before ever quiescing.
    if (expiration_quiesced_) {
      g_storage->ResumeExpiration();
    }
  }

  std::uint8_t db_;
  bool expiration_quiesced_ = false;
  DbCloseGuard guard_;
  std::string pattern_;
  std::uint64_t now_ms_ = 0;
  unsigned worker_ = 0;     // worker currently being drained
  unsigned partition_ = 0;  // absolute partition id owned by `worker`
  std::uint64_t cursor_ = 0;
};

// One bounded batch on `worker`: walks that worker's own partitions locally
// (one cross-core round trip per batch, not per partition), encoding matches
// or just counting them. Yields periodically so other databases' traffic on
// the worker keeps flowing.
struct KeysWorkerBatch {
  absl::Status status_;
  std::string payload_;
  std::uint64_t matches_ = 0;
  unsigned partition_ = 0;
  std::uint64_t cursor_ = 0;
  bool worker_done_ = false;
};

Task<KeysWorkerBatch> KeysBatchOnWorker(
    std::uint8_t db, unsigned worker, unsigned partition, std::uint64_t cursor,
    std::uint64_t now_ms, const std::string* pattern, bool count_only) {
  co_return co_await celer::SubmitTaskTo(
      worker, [=]() -> Task<KeysWorkerBatch> {
        constexpr std::size_t kChunkBytes = 64 * 1024;
        const unsigned stride = g_storage->worker_count();
        KeysWorkerBatch batch;
        batch.partition_ = partition == 0 ? worker : partition;
        batch.cursor_ = cursor;
        unsigned scanned = 0;
        while (batch.partition_ < storage::kLogicalStorageShards &&
               batch.payload_.size() < kChunkBytes) {
          // The byte budget keeps one step from blowing past the chunk
          // bound with large key names; overshoot is one bucket chain.
          auto scanned_step = co_await g_storage->ScanPartition(
              static_cast<std::uint16_t>(batch.partition_), db, batch.cursor_,
              512, now_ms,
              count_only ? kChunkBytes : kChunkBytes - batch.payload_.size());
          if (!scanned_step.ok()) {
            batch.status_ = scanned_step.status();
            co_return batch;
          }
          storage::ScanBatch step = std::move(*scanned_step);
          for (const std::string& key : step.keys_) {
            if (*pattern == "*" || GlobMatch(*pattern, key)) {
              if (count_only) {
                ++batch.matches_;
              } else {
                batch.payload_ +=
                    "$" + std::to_string(key.size()) + "\r\n" + key + "\r\n";
              }
            }
          }
          if (step.cursor_ == 0) {
            batch.partition_ += stride;
            batch.cursor_ = 0;
          } else {
            batch.cursor_ = step.cursor_;
          }
          if (++scanned % 256 == 0) {
            co_await celer::Yield(*ThisWorker().self_);
          }
        }
        batch.worker_done_ = batch.partition_ >= storage::kLogicalStorageShards;
        co_return batch;
      });
}

Task<absl::StatusOr<std::string>> NextKeysChunk(
    std::shared_ptr<KeysStreamState> state) {
  while (state->worker_ < g_storage->worker_count()) {
    KeysWorkerBatch batch = co_await KeysBatchOnWorker(
        state->db_, state->worker_, state->partition_, state->cursor_,
        state->now_ms_, &state->pattern_, /*count_only=*/false);
    if (!batch.status_.ok()) {
      co_return batch.status_;
    }
    if (batch.worker_done_) {
      ++state->worker_;
      state->partition_ = 0;
      state->cursor_ = 0;
    } else {
      state->partition_ = batch.partition_;
      state->cursor_ = batch.cursor_;
    }
    if (!batch.payload_.empty()) {
      co_return std::move(batch.payload_);
    }
  }
  co_return std::string();
}

Task<CommandReply> ExecuteKeys(const CommandRequest& request,
                               ReplyBuilder& reply_builder) {
  if (request.args_.size() != 2) {
    co_return BuiltReply(reply_builder.AppendError(
        "ERR wrong number of arguments for 'keys' command"));
  }
  const std::uint8_t db = request.db_id_;
  if (!CloseDbGate(db)) {
    co_return BuiltReply(reply_builder.AppendError(
        "BUSY another operation is holding the database"));
  }
  auto state = std::make_shared<KeysStreamState>(db);
  state->pattern_ = request.args_[1];
  state->now_ms_ = CommandUnixTimeMillis();
  // Drain in-flight commands, then freeze expiration writes: from here to the
  // end of the stream the keyspace cannot change, so the counted N is exact.
  while ((g_db_gates[db].load(std::memory_order_acquire) & kDbGateCountMask) !=
         0) {
    absl::Status waited = co_await celer::SleepFor(
        *ThisWorker().self_, std::chrono::milliseconds(1));
    if (!waited.ok()) {
      co_return BuiltReply(
          reply_builder.AppendError(absl::StrCat("ERR ", waited.message())));
    }
  }
  absl::Status quiesced = co_await g_storage->QuiesceExpiration();
  if (!quiesced.ok()) {
    co_return BuiltReply(
        reply_builder.AppendError(absl::StrCat("ERR ", quiesced.message())));
  }
  state->expiration_quiesced_ = true;

  // Counting pass over the frozen keyspace: one batched walk per worker.
  std::uint64_t matches = 0;
  for (unsigned worker = 0; worker < g_storage->worker_count(); ++worker) {
    unsigned partition = 0;
    std::uint64_t cursor = 0;
    for (;;) {
      KeysWorkerBatch batch = co_await KeysBatchOnWorker(
          db, worker, partition, cursor, state->now_ms_, &state->pattern_,
          /*count_only=*/true);
      if (!batch.status_.ok()) {
        co_return BuiltReply(reply_builder.AppendError(
            absl::StrCat("ERR ", batch.status_.message())));
      }
      matches += batch.matches_;
      if (batch.worker_done_) {
        break;
      }
      partition = batch.partition_;
      cursor = batch.cursor_;
    }
  }

  CommandReply reply = BuiltReply(reply_builder.AppendArrayHeader(matches));
  reply.chunks_ = [state]() { return NextKeysChunk(state); };
  co_return reply;
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

std::string EncodeStorageError(const absl::Status& status) {
  if (status.message().starts_with("WRONGTYPE ")) {
    return EncodeError(status.message());
  }
  return EncodeError(absl::StrCat("ERR ", status.message()));
}

std::string_view AppendStorageError(ReplyBuilder& reply_builder,
                                    const absl::Status& status) {
  return status.message().starts_with("WRONGTYPE ")
             ? reply_builder.AppendError(status.message())
             : reply_builder.AppendError("ERR ", status.message());
}

absl::StatusOr<storage::SetOptions> ParseSetOptions(
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
    if (CmpCaseInsensitive(option, "NX") || CmpCaseInsensitive(option, "XX")) {
      if (condition_seen) {
        return absl::Status(absl::StatusCode::kInvalidArgument, "syntax error");
      }
      condition_seen = true;
      options.condition_ = CmpCaseInsensitive(option, "NX")
                               ? storage::SetCondition::kIfAbsent
                               : storage::SetCondition::kIfPresent;
      continue;
    }
    if (CmpCaseInsensitive(option, "GET")) {
      if (get_seen) {
        return absl::Status(absl::StatusCode::kInvalidArgument, "syntax error");
      }
      get_seen = true;
      options.return_old_value_ = true;
      continue;
    }
    if (CmpCaseInsensitive(option, "KEEPTTL")) {
      if (expiration_seen) {
        return absl::Status(absl::StatusCode::kInvalidArgument, "syntax error");
      }
      expiration_seen = true;
      options.keep_ttl_ = true;
      continue;
    }

    const bool ex = CmpCaseInsensitive(option, "EX");
    const bool px = CmpCaseInsensitive(option, "PX");
    const bool exat = CmpCaseInsensitive(option, "EXAT");
    const bool pxat = CmpCaseInsensitive(option, "PXAT");
    if (!ex && !px && !exat && !pxat) {
      return absl::Status(absl::StatusCode::kInvalidArgument, "syntax error");
    }
    if (expiration_seen || i + 1 >= args.size()) {
      return absl::Status(absl::StatusCode::kInvalidArgument, "syntax error");
    }
    expiration_seen = true;
    std::int64_t parsed = 0;
    if (!ParseInt64(args[++i], &parsed)) {
      return absl::Status(absl::StatusCode::kInvalidArgument,
                          "value is not an integer or out of range");
    }
    if (parsed <= 0) {
      return absl::Status(absl::StatusCode::kInvalidArgument,
                          "invalid expire time in 'set' command");
    }
    const std::uint64_t amount = static_cast<std::uint64_t>(parsed);
    if (ex || exat) {
      if (amount > kMaxTimestamp / 1000) {
        return absl::Status(absl::StatusCode::kInvalidArgument,
                            "invalid expire time in 'set' command");
      }
    }
    const std::uint64_t millis = (ex || exat) ? amount * 1000 : amount;
    if (ex || px) {
      if (millis > kMaxTimestamp - now_ms) {
        return absl::Status(absl::StatusCode::kInvalidArgument,
                            "invalid expire time in 'set' command");
      }
      options.expire_at_ms_ = now_ms + millis;
    } else {
      options.expire_at_ms_ = millis;
    }
  }
  return options;
}

absl::StatusOr<storage::ExpirationCondition> ParseExpirationCondition(
    const std::vector<std::string>& args) {
  if (args.size() == 3) {
    return storage::ExpirationCondition::kNone;
  }
  if (args.size() != 4) {
    return absl::Status(absl::StatusCode::kInvalidArgument, "syntax error");
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
  return absl::Status(absl::StatusCode::kInvalidArgument, "syntax error");
}

Task<CommandReply> ExecuteStorageCommand(
    const CommandRequest& request, ReplyBuilder& reply_builder,
    ReadLatencyTrace* read_trace = nullptr) {
  CommandReply reply;
  const auto& args = request.args_;
  switch (request.kind_) {
    case CommandKind::kGet: {
      if (args.size() != 2) {
        reply.encoded_ = reply_builder.AppendError(
            "ERR wrong number of arguments for 'get' command");
        co_return reply;
      }
      auto value = co_await g_storage->Get(request.db_id_, args[1], read_trace);
      if (!value.ok()) {
        if (value.status().code() == absl::StatusCode::kNotFound) {
          reply.encoded_ = reply_builder.AppendNullBulkString();
        } else {
          reply.encoded_ = reply_builder.AppendError(
              absl::StrCat("ERR ", value.status().message()));
        }
      } else {
        reply.disk_value_.emplace(std::move(*value));
      }
      co_return reply;
    }

    case CommandKind::kSet: {
      if (args.size() < 3) {
        reply.encoded_ = reply_builder.AppendError(
            "ERR wrong number of arguments for 'set' command");
        co_return reply;
      }
      auto options = ParseSetOptions(args);
      if (!options.ok()) {
        reply.encoded_ = reply_builder.AppendError(
            absl::StrCat("ERR ", options.status().message()));
        co_return reply;
      }
      auto result =
          co_await g_storage->Set(request.db_id_, args[1], args[2], *options);
      if (!result.ok()) {
        reply.encoded_ = AppendStorageError(reply_builder, result.status());
        co_return reply;
      }
      if (options->return_old_value_) {
        if (result->old_value_.has_value()) {
          reply.disk_value_.emplace(std::move(*result->old_value_));
        } else {
          reply.encoded_ = reply_builder.AppendNullBulkString();
        }
      } else {
        reply.encoded_ = result->applied_
                             ? reply_builder.AppendSimpleString("OK")
                             : reply_builder.AppendNullBulkString();
      }
      co_return reply;
    }

    case CommandKind::kStrlen: {
      if (args.size() != 2) {
        reply.encoded_ = reply_builder.AppendError(
            "ERR wrong number of arguments for 'strlen' command");
        co_return reply;
      }
      auto length = co_await g_storage->StringLength(request.db_id_, args[1]);
      if (!length.ok()) {
        if (length.status().code() == absl::StatusCode::kNotFound) {
          reply.encoded_ = reply_builder.AppendInteger(0);
        } else {
          reply.encoded_ = AppendStorageError(reply_builder, length.status());
        }
      } else if (*length > static_cast<std::uint64_t>(
                               std::numeric_limits<long long>::max())) {
        reply.encoded_ =
            reply_builder.AppendError("ERR String length exceeds RESP range");
      } else {
        reply.encoded_ =
            reply_builder.AppendInteger(static_cast<long long>(*length));
      }
      co_return reply;
    }

    case CommandKind::kTtl:
    case CommandKind::kPttl: {
      const bool milliseconds = request.kind_ == CommandKind::kPttl;
      if (args.size() != 2) {
        reply.encoded_ = reply_builder.AppendError(
            std::string("ERR wrong number of arguments for '") +
            (milliseconds ? "pttl" : "ttl") + "' command");
        co_return reply;
      }
      const storage::ExpirationInfo info =
          co_await g_storage->GetExpiration(request.db_id_, args[1]);
      if (!info.exists_) {
        reply.encoded_ = reply_builder.AppendInteger(-2);
      } else if (info.expire_at_ms_ == 0) {
        reply.encoded_ = reply_builder.AppendInteger(-1);
      } else {
        const std::uint64_t now_ms = CommandUnixTimeMillis();
        const std::uint64_t remaining =
            info.expire_at_ms_ > now_ms ? info.expire_at_ms_ - now_ms : 0;
        const std::uint64_t output =
            milliseconds ? remaining : remaining / 1000;
        reply.encoded_ =
            reply_builder.AppendInteger(static_cast<long long>(output));
      }
      co_return reply;
    }

    case CommandKind::kExpire:
    case CommandKind::kPExpire: {
      const bool milliseconds = request.kind_ == CommandKind::kPExpire;
      if (args.size() < 3 || args.size() > 4) {
        reply.encoded_ = reply_builder.AppendError(
            std::string("ERR wrong number of arguments for '") +
            (milliseconds ? "pexpire" : "expire") + "' command");
        co_return reply;
      }
      std::int64_t duration = 0;
      if (!ParseInt64(args[2], &duration)) {
        reply.encoded_ = reply_builder.AppendError(
            "ERR value is not an integer or out of range");
        co_return reply;
      }
      auto condition = ParseExpirationCondition(args);
      if (!condition.ok()) {
        reply.encoded_ = reply_builder.AppendError("ERR syntax error");
        co_return reply;
      }
      const std::uint64_t now_ms = CommandUnixTimeMillis();
      constexpr std::uint64_t kMaxTimestamp =
          static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max());
      std::uint64_t expire_at_ms = 1;
      if (duration > 0) {
        const std::uint64_t amount = static_cast<std::uint64_t>(duration);
        const std::uint64_t factor = milliseconds ? 1 : 1000;
        if (amount > (kMaxTimestamp - now_ms) / factor) {
          reply.encoded_ = reply_builder.AppendError(
              "ERR invalid expire time in 'expire' command");
          co_return reply;
        }
        expire_at_ms = now_ms + amount * factor;
      }
      auto updated = co_await g_storage->UpdateExpiration(
          request.db_id_, args[1], expire_at_ms, *condition);
      reply.encoded_ =
          updated.ok() ? reply_builder.AppendInteger(*updated ? 1 : 0)
                       : AppendStorageError(reply_builder, updated.status());
      co_return reply;
    }

    case CommandKind::kPersist: {
      if (args.size() != 2) {
        reply.encoded_ = reply_builder.AppendError(
            "ERR wrong number of arguments for 'persist' command");
        co_return reply;
      }
      auto updated = co_await g_storage->UpdateExpiration(
          request.db_id_, args[1], 0,
          storage::ExpirationCondition::kIfHasExpiration);
      reply.encoded_ =
          updated.ok() ? reply_builder.AppendInteger(*updated ? 1 : 0)
                       : AppendStorageError(reply_builder, updated.status());
      co_return reply;
    }

    case CommandKind::kIncr: {
      if (args.size() != 2) {
        reply.encoded_ = reply_builder.AppendError(
            "ERR wrong number of arguments for 'incr' command");
        co_return reply;
      }
      auto value = co_await g_storage->Increment(request.db_id_, args[1]);
      if (!value.ok()) {
        if (value.status().message().starts_with("WRONGTYPE ")) {
          reply.encoded_ = reply_builder.AppendError(value.status().message());
        } else {
          reply.encoded_ =
              value.status().code() == absl::StatusCode::kInvalidArgument
                  ? reply_builder.AppendError(
                        "ERR value is not an integer or out of range")
                  : reply_builder.AppendError(
                        absl::StrCat("ERR ", value.status().message()));
        }
      } else {
        reply.encoded_ = reply_builder.AppendInteger(*value);
      }
      co_return reply;
    }

    default:
      co_return ExecuteSimpleLocalCommand(request, reply_builder);
  }
}

// INFO: Redis-shaped sections built from what keylane actually tracks. The
// Transactions section surfaces the VLL scheduler counters.
Task<CommandReply> ExecuteInfo(const CommandRequest& request,
                               ReplyBuilder& reply_builder) {
  std::string section = "default";
  if (request.args_.size() == 2) {
    section = request.args_[1];
    for (char& c : section) {
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
  }
  const bool all =
      section == "default" || section == "all" || section == "everything";
  auto wants = [&](std::string_view name) { return all || section == name; };

  std::optional<WorkerMetricsSnapshot> runtime_metrics;
  if (wants("clients") || wants("stats")) {
    runtime_metrics = co_await CollectWorkerMetrics();
  }

  std::string info;
  if (wants("server")) {
    const auto uptime = std::chrono::duration_cast<std::chrono::seconds>(
                            std::chrono::steady_clock::now() - g_server_start)
                            .count();
    info += "# Server\r\n";
    info += "keylane_version:0.1.0\r\n";
    info += "process_id:" + std::to_string(::getpid()) + "\r\n";
    info += "tcp_port:" + std::to_string(g_server_port) + "\r\n";
    info += "worker_threads:" + std::to_string(g_server_threads) + "\r\n";
    info += "uptime_in_seconds:" + std::to_string(uptime) + "\r\n\r\n";
  }
  if (wants("clients")) {
    info += "# Clients\r\n";
    info += "connected_clients:" +
            std::to_string(runtime_metrics->connected_clients_) + "\r\n\r\n";
  }
  if (wants("memory")) {
    RefreshMemoryDiagnostics();
    const MemoryStats memory = GetMemoryStats();
    const double fragmentation =
        memory.committed_bytes_ == 0
            ? 0.0
            : static_cast<double>(memory.rss_bytes_) /
                  static_cast<double>(memory.committed_bytes_);
    info += "# Memory\r\n";
    info += "used_memory:" + std::to_string(memory.used_bytes_) + "\r\n";
    info +=
        "used_memory_human:" + HumanReadableMemory(memory.used_bytes_) + "\r\n";
    info += "used_memory_rss:" + std::to_string(memory.rss_bytes_) + "\r\n";
    info += "used_memory_rss_human:" + HumanReadableMemory(memory.rss_bytes_) +
            "\r\n";
    info +=
        "used_memory_peak:" + std::to_string(memory.peak_used_bytes_) + "\r\n";
    info += "used_memory_peak_human:" +
            HumanReadableMemory(memory.peak_used_bytes_) + "\r\n";
    info += "maxmemory:" + std::to_string(memory.max_bytes_) + "\r\n";
    info +=
        "maxmemory_human:" + HumanReadableMemory(memory.max_bytes_) + "\r\n";
    info += "maxmemory_policy:noeviction\r\n";
    info +=
        "allocator_allocated:" + std::to_string(memory.used_bytes_) + "\r\n";
    info +=
        "allocator_active:" + std::to_string(memory.committed_bytes_) + "\r\n";
    info += "allocator_resident:" + std::to_string(memory.rss_bytes_) + "\r\n";
    info +=
        "allocator_reserved:" + std::to_string(memory.reserved_bytes_) + "\r\n";
    info += "mem_fragmentation_ratio:" + absl::StrCat(fragmentation) + "\r\n";
    info +=
        "oom_rejected_commands:" + std::to_string(memory.rejected_commands_) +
        "\r\n\r\n";
  }
  if (wants("stats")) {
    const storage::TombRaiderTotals raider = g_storage->TombRaiderStats();
    const storage::DefragTotals defrag = g_storage->DefragStats();
    info += "# Stats\r\n";
    info += "total_commands_processed:" +
            std::to_string(runtime_metrics->TotalCalls()) + "\r\n";
    info += "tomb_raider_rounds:" + std::to_string(raider.rounds_) + "\r\n";
    info += "tomb_raider_reaped:" + std::to_string(raider.reaped_) + "\r\n";
    info +=
        "tomb_raider_refreshed:" + std::to_string(raider.refreshed_) + "\r\n";
    info += std::string("tomb_raider_enabled:") +
            (raider.enabled_ ? "1\r\n" : "0\r\n");
    info += std::string("tomb_raider_running:") +
            (raider.running_ ? "1\r\n" : "0\r\n");
    info +=
        "tomb_raider_mode:" + std::string(TombRaiderModeName(raider.mode_)) +
        "\r\n";
    info += "tomb_raider_interval_ms:" + std::to_string(raider.interval_ms_) +
            "\r\n";
    info +=
        "tomb_raider_block_sleep_ms:" + std::to_string(raider.block_sleep_ms_) +
        "\r\n";
    info += "tomb_raider_daily_second:" + std::to_string(raider.daily_second_) +
            "\r\n\r\n";
    info += "defrag_max_active_per_device:" +
            std::to_string(defrag.max_active_per_device_) + "\r\n";
    info += std::string("defrag_paused:") +
            (defrag.paused_ ? "1\r\n" : "0\r\n");
    info += "defrag_block_sleep_ms:" +
            std::to_string(defrag.block_sleep_ms_) + "\r\n";
    info += "defrag_record_sleep_us:" +
            std::to_string(defrag.record_sleep_us_) + "\r\n";
    info += "defrag_active:" + std::to_string(defrag.active_) + "\r\n";
    info += "defrag_pending:" + std::to_string(defrag.pending_) + "\r\n\r\n";
  }
  if (wants("replication")) {
    info += "# Replication\r\n";
    info += std::string("role:") + (g_replica_read_only ? "slave" : "master") +
            "\r\n\r\n";
  }
  if (wants("transactions")) {
    struct ShardStats {
      std::uint64_t fastpath_ = 0;
      std::uint64_t queued_ = 0;
    };
    std::uint64_t fastpath = 0;
    std::uint64_t queued = 0;
    tx::TxRuntime* runtime = tx::TxRuntime::Get();
    for (unsigned target = 0; target < runtime->shard_count(); ++target) {
      const ShardStats stats = co_await SubmitTo(target, [] {
        tx::TxShard& shard = tx::CurrentTxShard();
        return ShardStats{shard.fastpath_runs(), shard.queued_runs()};
      });
      fastpath += stats.fastpath_;
      queued += stats.queued_;
    }
    info += "# Transactions\r\n";
    info += "tx_fastpath_runs:" + std::to_string(fastpath) + "\r\n";
    info += "tx_queued_runs:" + std::to_string(queued) + "\r\n";
    info += "tx_schedule_retries:" +
            std::to_string(
                runtime->schedule_retries_.load(std::memory_order_relaxed)) +
            "\r\n";
    info += "tx_ids_allocated:" +
            std::to_string(runtime->next_txid_.load(std::memory_order_relaxed) -
                           1) +
            "\r\n\r\n";
  }
  if (wants("keyspace")) {
    info += "# Keyspace\r\n";
    for (unsigned db = 0; db < storage::kLogicalDatabaseCount; ++db) {
      std::uint64_t keys = 0;
      for (unsigned target = 0; target < g_storage->worker_count(); ++target) {
        keys += co_await SubmitTo(target, [db] {
          return g_storage->LocalSize(static_cast<std::uint8_t>(db));
        });
      }
      if (keys != 0) {
        info += "db" + std::to_string(db) + ":keys=" + std::to_string(keys) +
                "\r\n";
      }
    }
    info += "\r\n";
  }
  co_return BuiltReply(reply_builder.AppendBulkString(info));
}

// Runs one single-key command body against pre-acquired locks, returning the
// encoded reply. Mirrors ExecuteStorageCommand's semantics; arity was already
// validated when the command was queued.
Task<std::string> RunSingleKeyLocked(std::uint8_t db_id,
                                     const CommandRequest& request,
                                     const storage::Digest& digest,
                                     storage::TxShardWrites* tx) {
  const auto& args = request.args_;
  switch (request.kind_) {
    case CommandKind::kGet: {
      auto value = co_await g_storage->GetLocked(db_id, args[1], digest);
      if (value.ok()) {
        const auto bytes = value->network_bytes();
        co_return std::string(reinterpret_cast<const char*>(bytes.data()),
                              bytes.size());
      }
      co_return value.status().code() == absl::StatusCode::kNotFound
          ? EncodeNullBulkString()
          : EncodeError(absl::StrCat("ERR ", value.status().message()));
    }

    case CommandKind::kSet: {
      auto options = ParseSetOptions(args);
      if (!options.ok()) {
        co_return EncodeError(absl::StrCat("ERR ", options.status().message()));
      }
      auto result = co_await g_storage->SetLocked(db_id, args[1], digest,
                                                  args[2], *options, tx);
      if (!result.ok()) {
        co_return EncodeStorageError(result.status());
      }
      if (options->return_old_value_) {
        if (result->old_value_.has_value()) {
          const auto bytes = result->old_value_->network_bytes();
          co_return std::string(reinterpret_cast<const char*>(bytes.data()),
                                bytes.size());
        }
        co_return EncodeNullBulkString();
      }
      co_return result->applied_ ? EncodeSimpleString("OK")
                                 : EncodeNullBulkString();
    }

    case CommandKind::kStrlen: {
      auto length =
          co_await g_storage->StringLengthLocked(db_id, args[1], digest);
      if (!length.ok()) {
        co_return length.status().code() == absl::StatusCode::kNotFound
            ? EncodeInteger(0)
            : EncodeStorageError(length.status());
      }
      if (*length >
          static_cast<std::uint64_t>(std::numeric_limits<long long>::max())) {
        co_return EncodeError("ERR String length exceeds RESP range");
      }
      co_return EncodeInteger(static_cast<long long>(*length));
    }

    case CommandKind::kTtl:
    case CommandKind::kPttl: {
      const bool milliseconds = request.kind_ == CommandKind::kPttl;
      const storage::ExpirationInfo info =
          co_await g_storage->GetExpirationLocked(db_id, args[1], digest);
      if (!info.exists_) {
        co_return EncodeInteger(-2);
      }
      if (info.expire_at_ms_ == 0) {
        co_return EncodeInteger(-1);
      }
      const std::uint64_t now_ms = CommandUnixTimeMillis();
      const std::uint64_t remaining =
          info.expire_at_ms_ > now_ms ? info.expire_at_ms_ - now_ms : 0;
      co_return EncodeInteger(
          static_cast<long long>(milliseconds ? remaining : remaining / 1000));
    }

    case CommandKind::kExpire:
    case CommandKind::kPExpire: {
      const bool milliseconds = request.kind_ == CommandKind::kPExpire;
      std::int64_t duration = 0;
      if (!ParseInt64(args[2], &duration)) {
        co_return EncodeError("ERR value is not an integer or out of range");
      }
      auto condition = ParseExpirationCondition(args);
      if (!condition.ok()) {
        co_return EncodeError("ERR syntax error");
      }
      const std::uint64_t now_ms = CommandUnixTimeMillis();
      constexpr std::uint64_t kMaxTimestamp =
          static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max());
      std::uint64_t expire_at_ms = 1;
      if (duration > 0) {
        const std::uint64_t amount = static_cast<std::uint64_t>(duration);
        const std::uint64_t factor = milliseconds ? 1 : 1000;
        if (amount > (kMaxTimestamp - now_ms) / factor) {
          co_return EncodeError("ERR invalid expire time in 'expire' command");
        }
        expire_at_ms = now_ms + amount * factor;
      }
      auto updated = co_await g_storage->UpdateExpirationLocked(
          db_id, args[1], digest, expire_at_ms, *condition, tx);
      co_return updated.ok() ? EncodeInteger(*updated ? 1 : 0)
                             : EncodeStorageError(updated.status());
    }

    case CommandKind::kPersist: {
      auto updated = co_await g_storage->UpdateExpirationLocked(
          db_id, args[1], digest, 0,
          storage::ExpirationCondition::kIfHasExpiration, tx);
      co_return updated.ok() ? EncodeInteger(*updated ? 1 : 0)
                             : EncodeStorageError(updated.status());
    }

    case CommandKind::kIncr: {
      auto value =
          co_await g_storage->IncrementLocked(db_id, args[1], digest, tx);
      if (value.ok()) {
        co_return EncodeInteger(*value);
      }
      if (value.status().message().starts_with("WRONGTYPE ")) {
        co_return EncodeError(value.status().message());
      }
      co_return value.status().code() == absl::StatusCode::kInvalidArgument
          ? EncodeError("ERR value is not an integer or out of range")
          : EncodeError(absl::StrCat("ERR ", value.status().message()));
    }

    default:
      co_return EncodeError("ERR command is not allowed in transactions");
  }
}

// Joins per-key reader coroutines spawned on one shard. Everything runs on
// the owning worker thread, so plain counters suffice; the waiter resumes
// via its own worker's ready queue once the last read lands.
struct ShardReadJoin {
  std::size_t pending_ = 0;
  std::coroutine_handle<> waiter_;
  absl::Status error_;

  void Complete(absl::Status status) {
    if (!status.ok() && error_.ok()) {
      error_ = std::move(status);
    }
    if (--pending_ == 0 && waiter_) {
      auto handle = waiter_;
      waiter_ = {};
      ThisWorker().self_->Enqueue(handle);
    }
  }

  auto Join() {
    struct Awaiter {
      ShardReadJoin* join_;
      bool await_ready() const { return join_->pending_ == 0; }
      void await_suspend(std::coroutine_handle<> handle) {
        join_->waiter_ = handle;
      }
      void await_resume() const {}
    };
    return Awaiter{this};
  }
};

// One concurrent MGET read: locks are already held for the whole hop, and
// distinct keys live in distinct blocks, so per-key disk reads overlap
// instead of accumulating latency serially.
Task<absl::Status> ReadFrameIntoSlot(std::uint8_t db, const std::string* key,
                                     storage::Digest digest,
                                     std::optional<std::string>* slot,
                                     ShardReadJoin* join) {
  auto value = co_await g_storage->GetLocked(db, *key, digest);
  absl::Status status = absl::OkStatus();
  if (value.ok()) {
    const auto bytes = value->network_bytes();
    slot->emplace(reinterpret_cast<const char*>(bytes.data()), bytes.size());
  } else if (value.status().code() != absl::StatusCode::kNotFound) {
    status = value.status();
  }
  join->Complete(std::move(status));
  co_return absl::OkStatus();
}

// Shared context of one multi-key command's transaction. Shard callbacks
// write disjoint reply slots (MGET) or bump the shared counter (DEL/EXISTS)
// before the hop barrier; the coordinator assembles the reply afterwards.
struct MultiKeyContext {
  const CommandRequest* request_ = nullptr;
  std::vector<std::optional<std::string>>
      frames_;                      // MGET: encoded bulk per slot
  std::atomic<long long> hits_{0};  // DEL / EXISTS
  // Multi-key atomic write: per-worker receipts, non-empty only for tagged
  // writes (MSET / multi-key DEL). Each shard touches only its own slot.
  std::vector<storage::TxShardWrites> tx_writes_;
  // Set by the coordinator between the execute and finish hops of a tagged
  // multi-shard write: any shard failed, so every shard must undo.
  bool rollback_ = false;
};

// Second hop of a tagged multi-shard write, riding the releasing round: the
// locks are still held, so undoing (or discarding the journal) here is
// invisible to every other client — readers can never observe the aborted
// values.
Task<absl::Status> MultiKeyFinishCallback(void* context,
                                          const tx::ShardSlice&) {
  auto* ctx = static_cast<MultiKeyContext*>(context);
  const std::uint64_t txid = ctx->tx_writes_.front().txid_;
  if (ctx->rollback_) {
    co_return co_await g_storage->RollbackTxLocal(txid);
  }
  co_return co_await g_storage->DiscardTxUndoLocal(txid);
}

// Detached commit chain for one multi-key write: waits for every shard's
// tagged data to be durable, then appends the kTxCommit record. The client
// reply never waits for this — losing the commit before it lands drops the
// whole transaction at recovery, which relaxed durability already allows;
// what it can never do is keep half of it.
Task<absl::Status> RunTxCommit(std::uint64_t txid,
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
  if (shards.empty()) {
    co_return absl::OkStatus();
  }
  absl::Status committed =
      co_await g_storage->CommitTxWrites(txid, std::move(shards));
  if (!committed.ok()) {
    spdlog::warn("transaction {} commit append failed: {}", txid,
                 committed.message());
  }
  co_return absl::OkStatus();
}

Task<absl::Status> MultiKeyShardCallback(void* context,
                                         const tx::ShardSlice& slice) {
  auto* ctx = static_cast<MultiKeyContext*>(context);
  const auto& args = ctx->request_->args_;
  if (ctx->request_->kind_ == CommandKind::kMGet && slice.keys_.size() > 1) {
    // Overlap this shard's disk reads instead of awaiting them one by one.
    ShardReadJoin join;
    join.pending_ = slice.keys_.size();
    for (const tx::TxKey& key : slice.keys_) {
      SpawnOnCurrentWorker(ReadFrameIntoSlot(
          ctx->request_->db_id_, &args[key.arg_index_], key.digest_,
          &ctx->frames_[key.arg_index_ - 1], &join));
    }
    co_await join.Join();
    co_return join.error_;
  }
  for (const tx::TxKey& key : slice.keys_) {
    const std::string& name = args[key.arg_index_];
    switch (ctx->request_->kind_) {
      case CommandKind::kMSet: {
        auto result = co_await g_storage->SetLocked(
            ctx->request_->db_id_, name, key.digest_, args[key.arg_index_ + 1],
            {},
            ctx->tx_writes_.empty() ? nullptr
                                    : &ctx->tx_writes_[ThisWorker().id_]);
        if (!result.ok()) {
          if (!ctx->tx_writes_.empty()) {
            (void)co_await g_storage->RollbackTxLocal(
                ctx->tx_writes_.front().txid_);
          }
          co_return result.status();
        }
        break;
      }
      case CommandKind::kMGet: {
        auto value = co_await g_storage->GetLocked(ctx->request_->db_id_, name,
                                                   key.digest_);
        if (value.ok()) {
          const auto bytes = value->network_bytes();
          ctx->frames_[key.arg_index_ - 1].emplace(
              reinterpret_cast<const char*>(bytes.data()), bytes.size());
        } else if (value.status().code() != absl::StatusCode::kNotFound) {
          co_return value.status();
        }
        break;
      }
      case CommandKind::kDel: {
        auto deleted = co_await g_storage->DeleteLocked(
            ctx->request_->db_id_, name, key.digest_,
            ctx->tx_writes_.empty() ? nullptr
                                    : &ctx->tx_writes_[ThisWorker().id_]);
        if (!deleted.ok()) {
          if (!ctx->tx_writes_.empty()) {
            (void)co_await g_storage->RollbackTxLocal(
                ctx->tx_writes_.front().txid_);
          }
          co_return deleted.status();
        }
        if (*deleted) {
          ctx->hits_.fetch_add(1, std::memory_order_relaxed);
        }
        break;
      }
      case CommandKind::kExists:
      default: {
        if (co_await g_storage->ExistsLocked(ctx->request_->db_id_, name,
                                             key.digest_)) {
          ctx->hits_.fetch_add(1, std::memory_order_relaxed);
        }
        break;
      }
    }
  }
  co_return absl::OkStatus();
}

// DEL / EXISTS / MSET / MGET run as one transaction: every key locked up
// front (across all owning shards), one hop where each shard works its
// slice, locks released when the hop completes.
Task<CommandReply> ExecuteMultiKey(const CommandRequest& request,
                                   ReplyBuilder& reply_builder) {
  const auto& args = request.args_;
  auto keys = DetermineKeys(*request.spec_, args.size());
  if (!keys.ok()) {
    co_return BuiltReply(reply_builder.AppendError(
        absl::StrCat("ERR ", keys.status().message())));
  }
  if (request.kind_ == CommandKind::kMSet && args.size() % 2 != 1) {
    co_return BuiltReply(reply_builder.AppendError(
        "ERR wrong number of arguments for 'mset' command"));
  }

  const bool write = (request.spec_->flags_ & kCmdWrite) != 0;
  tx::Transaction txn;
  for (std::size_t i = keys->first_; i <= keys->last_; i += keys->step_) {
    txn.AddKey(ShardForKey(args[i]), request.db_id_,
               storage::ComputeDigest(args[i]), static_cast<std::uint32_t>(i),
               write ? tx::LockMode::kExclusive : tx::LockMode::kShared);
  }
  txn.Seal();

  MultiKeyContext ctx;
  ctx.request_ = &request;
  if (request.kind_ == CommandKind::kMGet) {
    ctx.frames_.resize(keys->count());
  }
  std::uint64_t write_txid = 0;
  if (write && keys->count() > 1) {
    write_txid = storage::StorageEngine::AllocateWriteTxid();
    ctx.tx_writes_.resize(g_storage->worker_count());
    for (auto& shard : ctx.tx_writes_) {
      shard.txid_ = write_txid;
      shard.collect_undo_ = true;
    }
  }

  absl::Status scheduled = co_await txn.Schedule();
  if (!scheduled.ok()) {
    co_return BuiltReply(
        reply_builder.AppendError(absl::StrCat("ERR ", scheduled.message())));
  }
  // A tagged multi-shard write holds every shard's locks across a second
  // hop, so a mid-transaction storage failure can be undone before any other
  // client sees it. Single-shard transactions self-roll-back inside their
  // one hop (Execute requires release there), and reads have nothing to
  // undo.
  const bool two_hop = write_txid != 0 && !txn.single_shard();
  absl::Status status =
      co_await txn.Execute(&MultiKeyShardCallback, &ctx, !two_hop);
  if (two_hop) {
    ctx.rollback_ = !status.ok();
    absl::Status finish =
        co_await txn.Execute(&MultiKeyFinishCallback, &ctx, true);
    if (!finish.ok() && status.ok()) {
      status = finish;
    }
  }
  if (!status.ok()) {
    // Runtime state is already rolled back, and no commit record is ever
    // appended: recovery treats every record this write tagged as an aborted
    // prepare and drops it, so neither a reader nor a crash can observe half
    // of the command.
    co_return BuiltReply(AppendStorageError(reply_builder, status));
  }
  if (write_txid != 0) {
    g_storage->NoteTxCommitStarted();
    SpawnOnCurrentWorker(RunTxCommit(write_txid, std::move(ctx.tx_writes_)));
  }

  switch (request.kind_) {
    case CommandKind::kMSet:
      co_return BuiltReply(reply_builder.AppendSimpleString("OK"));
    case CommandKind::kMGet: {
      reply_builder.AppendArrayHeader(ctx.frames_.size());
      for (const auto& frame : ctx.frames_) {
        reply_builder.AppendRaw(frame.has_value() ? *frame : "$-1\r\n");
      }
      co_return BuiltReply(reply_builder.View());
    }
    default:
      co_return BuiltReply(reply_builder.AppendInteger(
          ctx.hits_.load(std::memory_order_relaxed)));
  }
}

// One key of a queued EXEC command, with everything precomputed on the
// coordinator: digest, owning shard, argument position, reply slot.
struct ExecKey {
  storage::Digest digest_;
  std::uint16_t owner_ = 0;
  std::uint16_t arg_ = 0;
  std::uint16_t slot_ = 0;
  tx::LockMode mode_ = tx::LockMode::kShared;
  std::uint8_t db_ = 0;
};

// One squashed run of consecutive keyed commands [begin, end): every shard
// executes its keys of every command in queue order within a single hop.
// Sinks are per command (indexed by i - begin); shards write disjoint reply
// slots, per-command atomic counters, and record rare per-command errors
// under a mutex.
struct ExecRunContext {
  const std::vector<CommandRequest>* queued_ = nullptr;
  // EXEC-wide per-worker write receipts (worker-indexed); each shard touches
  // only its own slot. Null for read-only transactions.
  storage::TxShardWrites* tx_writes_ = nullptr;
  const std::vector<std::vector<ExecKey>>* cmd_keys_ = nullptr;
  std::vector<std::string>* replies_ = nullptr;
  std::size_t begin_ = 0;
  std::size_t end_ = 0;
  std::vector<std::vector<std::optional<std::string>>> mget_;
  std::unique_ptr<std::atomic<long long>[]> counters_;
  std::mutex error_mutex_;
  std::vector<absl::Status> errors_;
};

// Builds the replies of a completed run from its per-command sinks.
// Single-key commands already wrote their slots on the owning shard.
void AssembleRunReplies(ExecRunContext& run) {
  for (std::size_t i = run.begin_; i < run.end_; ++i) {
    const std::size_t local = i - run.begin_;
    if (!run.errors_[local].ok()) {
      (*run.replies_)[i] = EncodeStorageError(run.errors_[local]);
      continue;
    }
    switch ((*run.queued_)[i].kind_) {
      case CommandKind::kMSet:
        (*run.replies_)[i] = EncodeSimpleString("OK");
        break;
      case CommandKind::kMGet: {
        std::string reply =
            "*" + std::to_string(run.mget_[local].size()) + "\r\n";
        for (const auto& frame : run.mget_[local]) {
          reply += frame.has_value() ? *frame : "$-1\r\n";
        }
        (*run.replies_)[i] = std::move(reply);
        break;
      }
      case CommandKind::kDel:
      case CommandKind::kExists:
        (*run.replies_)[i] =
            EncodeInteger(run.counters_[local].load(std::memory_order_relaxed));
        break;
      default:
        break;
    }
  }
}

// Prepares the sinks of one squashed run over [begin, end).
void InitExecRun(ExecRunContext& run, const std::vector<CommandRequest>& queued,
                 const std::vector<std::vector<ExecKey>>& cmd_keys,
                 std::vector<std::string>& replies, std::size_t begin,
                 std::size_t end) {
  run.queued_ = &queued;
  run.cmd_keys_ = &cmd_keys;
  run.replies_ = &replies;
  run.begin_ = begin;
  run.end_ = end;
  const std::size_t count = end - begin;
  run.counters_ = std::make_unique<std::atomic<long long>[]>(count);
  run.errors_.assign(count, absl::OkStatus());
  run.mget_.resize(count);
  for (std::size_t i = begin; i < end; ++i) {
    if (queued[i].kind_ == CommandKind::kMGet) {
      run.mget_[i - begin].assign(cmd_keys[i].size(), std::nullopt);
    }
  }
}

// A hop that does nothing but acquire (and keep) every shard's holds, so the
// coordinator can act at the transaction's position in the serial order
// before running any command.
Task<absl::Status> ArmOnlyShardCallback(void*, const tx::ShardSlice&) {
  co_return absl::OkStatus();
}

// One EXEC hop = one squashed run: every shard executes its keys of each
// command in [begin, end) in queue order (same-key commands share an owner,
// so their relative order is preserved). A command's failure is recorded and
// the remaining commands still run, matching Redis's continue-on-error
// transaction semantics.
Task<absl::Status> ExecRunShardCallback(void* context, const tx::ShardSlice&) {
  auto* ctx = static_cast<ExecRunContext*>(context);
  const unsigned self = ThisWorker().id_;
  for (std::size_t i = ctx->begin_; i < ctx->end_; ++i) {
    const CommandRequest& cmd = (*ctx->queued_)[i];
    const auto& args = cmd.args_;
    const auto& keys = (*ctx->cmd_keys_)[i];
    const std::size_t local = i - ctx->begin_;
    auto record_error = [&](absl::Status status) {
      std::lock_guard<std::mutex> lock(ctx->error_mutex_);
      if (ctx->errors_[local].ok()) {
        ctx->errors_[local] = std::move(status);
      }
    };

    if (cmd.kind_ == CommandKind::kMGet) {
      std::size_t mine = 0;
      for (const ExecKey& key : keys) {
        mine += key.owner_ == self ? 1 : 0;
      }
      if (mine > 1) {
        ShardReadJoin join;
        join.pending_ = mine;
        for (const ExecKey& key : keys) {
          if (key.owner_ == self) {
            SpawnOnCurrentWorker(
                ReadFrameIntoSlot(cmd.db_id_, &args[key.arg_], key.digest_,
                                  &ctx->mget_[local][key.slot_], &join));
          }
        }
        co_await join.Join();
        if (!join.error_.ok()) {
          record_error(std::move(join.error_));
        }
        continue;
      }
    }

    for (const ExecKey& key : keys) {
      if (key.owner_ != self) {
        continue;
      }
      bool command_failed = false;
      storage::TxShardWrites* tx =
          ctx->tx_writes_ == nullptr ? nullptr : &ctx->tx_writes_[self];
      switch (cmd.kind_) {
        case CommandKind::kMSet: {
          auto result = co_await g_storage->SetLocked(
              cmd.db_id_, args[key.arg_], key.digest_, args[key.arg_ + 1], {},
              tx);
          if (!result.ok()) {
            record_error(result.status());
            command_failed = true;
          }
          break;
        }
        case CommandKind::kMGet: {
          auto value = co_await g_storage->GetLocked(cmd.db_id_, args[key.arg_],
                                                     key.digest_);
          if (value.ok()) {
            const auto bytes = value->network_bytes();
            ctx->mget_[local][key.slot_].emplace(
                reinterpret_cast<const char*>(bytes.data()), bytes.size());
          } else if (value.status().code() != absl::StatusCode::kNotFound) {
            record_error(value.status());
            command_failed = true;
          }
          break;
        }
        case CommandKind::kDel: {
          auto deleted = co_await g_storage->DeleteLocked(
              cmd.db_id_, args[key.arg_], key.digest_, tx);
          if (!deleted.ok()) {
            record_error(deleted.status());
            command_failed = true;
          } else if (*deleted) {
            ctx->counters_[local].fetch_add(1, std::memory_order_relaxed);
          }
          break;
        }
        case CommandKind::kExists: {
          if (co_await g_storage->ExistsLocked(cmd.db_id_, args[key.arg_],
                                               key.digest_)) {
            ctx->counters_[local].fetch_add(1, std::memory_order_relaxed);
          }
          break;
        }
        default:
          // Single-key command: the sole owner runs the full body and writes
          // the reply slot directly (errors self-encode).
          (*ctx->replies_)[i] =
              co_await RunSingleKeyLocked(cmd.db_id_, cmd, key.digest_, tx);
          break;
      }
      if (command_failed) {
        break;  // abandon this command's remaining keys; run the next one
      }
    }
  }
  co_return absl::OkStatus();
}

// The union lock set of an EXEC, deduplicated per fingerprint with
// exclusive-if-any-writer, as TxShard::AcquireKeys requires.
std::vector<tx::KeyRef> DedupExecLocks(
    const std::vector<std::vector<ExecKey>>& cmd_keys) {
  std::vector<tx::KeyRef> refs;
  for (const auto& keys : cmd_keys) {
    for (const ExecKey& key : keys) {
      const tx::LockFp fp = tx::FingerprintOf(key.digest_);
      bool merged = false;
      for (tx::KeyRef& ref : refs) {
        if (ref.fp_ == fp && ref.db_ == key.db_) {
          if (key.mode_ == tx::LockMode::kExclusive) {
            ref.mode_ = tx::LockMode::kExclusive;
          }
          merged = true;
          break;
        }
      }
      if (!merged) {
        refs.push_back(tx::KeyRef{fp, key.mode_, key.db_});
      }
    }
  }
  return refs;
}

// Registers each key on its owning shard with a liveness snapshot taken
// there; duplicates of an already-watched (db, fp) keep the first snapshot.
Task<CommandReply> ExecuteWatch(ConnectionContext& ctx,
                                const CommandRequest& request,
                                ReplyBuilder& reply_builder) {
  auto keys = DetermineKeys(*request.spec_, request.args_.size());
  if (!keys.ok()) {
    co_return BuiltReply(reply_builder.AppendError(
        absl::StrCat("ERR ", keys.status().message())));
  }
  for (std::size_t i = keys->first_; i <= keys->last_; i += keys->step_) {
    const std::uint8_t db = ctx.selected_db_;
    const storage::Digest digest = storage::ComputeDigest(request.args_[i]);
    const tx::LockFp fp = tx::FingerprintOf(digest);
    bool already = false;
    for (const auto& watched : ctx.watched_) {
      if (watched.db_ == db && watched.key_ == request.args_[i]) {
        already = true;
        break;
      }
    }
    if (already) {
      continue;
    }
    const std::uint16_t owner =
        static_cast<std::uint16_t>(ShardForKey(request.args_[i]));
    const bool live = co_await celer::SubmitTaskTo(
        owner,
        [key = std::string(request.args_[i]), db, digest, fp,
         conn = ctx.conn_id_]() -> Task<bool> {
          tx::CurrentTxShard().Watch(db, fp, conn);
          co_return co_await g_storage->KeyLive(db, key, digest);
        });
    ctx.watched_.push_back(ConnectionContext::WatchedKey{
        .key_ = request.args_[i],
        .digest_ = digest,
        .fp_ = fp,
        .owner_ = owner,
        .db_ = db,
        .live_ = live,
    });
  }
  co_return BuiltReply(reply_builder.AppendSimpleString("OK"));
}

// True when every watched key is unmarked and still matches its WATCH-time
// liveness. Runs on each key's owning shard; callers hold whatever locks the
// transaction needs before asking. The liveness snapshot travels with the
// key, not the shard entry, so keys sharing a fingerprint are each compared
// against their own snapshot.
Task<bool> CheckConnectionWatches(const ConnectionContext& ctx) {
  for (const auto& watched : ctx.watched_) {
    const bool clean = co_await celer::SubmitTaskTo(
        watched.owner_,
        [key = watched.key_, db = watched.db_, digest = watched.digest_,
         fp = watched.fp_, live = watched.live_,
         conn = ctx.conn_id_]() -> Task<bool> {
          if (!tx::CurrentTxShard().WatchClean(db, fp, conn)) {
            co_return false;
          }
          co_return co_await g_storage->KeyLive(db, key, digest) == live;
        });
    if (!clean) {
      co_return false;
    }
  }
  co_return true;
}

// EXEC consumes the connection's watches whatever its outcome.
Task<absl::Status> DropWatches(ConnectionContext& ctx) {
  for (const auto& watched : ctx.watched_) {
    co_await SubmitTo(watched.owner_, [db = watched.db_, fp = watched.fp_,
                                       conn = ctx.conn_id_]() {
      tx::CurrentTxShard().Unwatch(db, fp, conn);
      return true;
    });
  }
  ctx.watched_.clear();
  co_return absl::OkStatus();
}

Task<CommandReply> ExecuteExec(ConnectionContext& ctx,
                               ReplyBuilder& reply_builder) {
  const std::vector<CommandRequest> queued = std::move(ctx.queued_);
  const bool dirty = ctx.multi_dirty_;
  ctx.ResetMulti();
  if (dirty) {
    co_await DropWatches(ctx);
    co_return BuiltReply(reply_builder.AppendError(
        "EXECABORT Transaction discarded because of previous errors."));
  }
  if (queued.empty()) {
    const bool clean =
        ctx.watched_.empty() || co_await CheckConnectionWatches(ctx);
    co_await DropWatches(ctx);
    co_return BuiltReply(reply_builder.AppendRaw(clean ? "*0\r\n" : "*-1\r\n"));
  }

  std::size_t exec_memory_growth = 0;
  for (const CommandRequest& command : queued) {
    exec_memory_growth =
        SaturatingAdd(exec_memory_growth, EstimatedMemoryGrowth(command));
  }
  if (RejectForMemory(exec_memory_growth)) {
    co_await DropWatches(ctx);
    co_return BuiltReply(AppendOomError(reply_builder));
  }

  // Precompute every queued command's keys: digests, owners, slots, modes.
  std::vector<std::vector<ExecKey>> cmd_keys(queued.size());
  std::vector<std::uint8_t> dbs;  // distinct databases with keyed commands
  for (std::size_t i = 0; i < queued.size(); ++i) {
    const CommandRequest& cmd = queued[i];
    if (cmd.spec_ == nullptr || cmd.spec_->first_key_ == 0) {
      continue;
    }
    auto keys = DetermineKeys(*cmd.spec_, cmd.args_.size());
    if (!keys.ok()) {
      // Unreachable today: queueing ran the same check on the same spec and
      // arity. Kept defensive, and EXEC must consume the connection's
      // watches whatever its outcome — leaving them registered would
      // false-abort every later EXEC on this connection.
      co_await DropWatches(ctx);
      co_return BuiltReply(reply_builder.AppendError(
          absl::StrCat("ERR ", keys.status().message())));
    }
    const bool write = (cmd.spec_->flags_ & kCmdWrite) != 0;
    std::uint16_t slot = 0;
    for (std::size_t a = keys->first_; a <= keys->last_; a += keys->step_) {
      cmd_keys[i].push_back(ExecKey{
          .digest_ = storage::ComputeDigest(cmd.args_[a]),
          .owner_ = static_cast<std::uint16_t>(ShardForKey(cmd.args_[a])),
          .arg_ = static_cast<std::uint16_t>(a),
          .slot_ = slot++,
          .mode_ = write ? tx::LockMode::kExclusive : tx::LockMode::kShared,
          .db_ = cmd.db_id_,
      });
    }
    if (std::find(dbs.begin(), dbs.end(), cmd.db_id_) == dbs.end()) {
      dbs.push_back(cmd.db_id_);
    }
  }

  std::vector<std::string> replies(queued.size());
  std::optional<std::uint8_t> select_db;
  auto run_keyless = [&](const CommandRequest& cmd) {
    ReplyBuilder local_builder;
    CommandReply local = ExecuteSimpleLocalCommand(cmd, local_builder);
    if (local.selected_db_.has_value()) {
      select_db = local.selected_db_;
    }
    return std::string(local.encoded_);
  };

  if (!dbs.empty()) {
    MultiDbOperationGuard db_guard;
    for (const std::uint8_t db : dbs) {
      if (!db_guard.Add(db)) {
        co_await DropWatches(ctx);
        co_return BuiltReply(reply_builder.AppendError(
            "TRYAGAIN database flush is in progress"));
      }
    }

    // One write id for the whole EXEC: every record any of its commands
    // writes carries it, and one commit record at the end covers them all.
    // Read-only transactions collect no fences and append no commit.
    const std::uint64_t exec_txid = storage::StorageEngine::AllocateWriteTxid();
    std::vector<storage::TxShardWrites> tx_writes(g_storage->worker_count());
    for (auto& shard : tx_writes) {
      shard.txid_ = exec_txid;
    }

    // Distinct owners across the whole transaction.
    std::vector<std::uint16_t> owners;
    for (const auto& keys : cmd_keys) {
      for (const ExecKey& key : keys) {
        if (std::find(owners.begin(), owners.end(), key.owner_) ==
            owners.end()) {
          owners.push_back(key.owner_);
        }
      }
    }

    if (owners.size() == 1) {
      // Whole transaction on one shard: hop once, take the fast-path guard
      // over the union lock set, run every command inline.
      const std::vector<tx::KeyRef> refs = DedupExecLocks(cmd_keys);
      bool watch_aborted = false;
      absl::Status status =
          co_await SubmitTaskTo(owners.front(), [&]() -> Task<absl::Status> {
            auto guard = co_await tx::CurrentTxShard().AcquireKeys(
                std::span<const tx::KeyRef>(refs));
            if (!ctx.watched_.empty() &&
                !co_await CheckConnectionWatches(ctx)) {
              watch_aborted = true;
              co_return absl::OkStatus();
            }
            std::size_t i = 0;
            while (i < queued.size()) {
              const CommandRequest& cmd = queued[i];
              if (cmd_keys[i].empty()) {
                if (cmd.kind_ == CommandKind::kInfo) {
                  ReplyBuilder info_builder;
                  replies[i] =
                      (co_await ExecuteInfo(cmd, info_builder)).encoded_;
                } else {
                  replies[i] = run_keyless(cmd);
                }
                ++i;
                continue;
              }
              std::size_t end = i + 1;
              while (end < queued.size() && !cmd_keys[end].empty()) {
                ++end;
              }
              ExecRunContext run;
              InitExecRun(run, queued, cmd_keys, replies, i, end);
              run.tx_writes_ = tx_writes.data();
              (void)co_await ExecRunShardCallback(&run,
                                                  tx::ShardSlice{.keys_ = {}});
              AssembleRunReplies(run);
              i = end;
            }
            co_return absl::OkStatus();
          });
      if (!status.ok()) {
        co_await DropWatches(ctx);
        co_return BuiltReply(
            reply_builder.AppendError(absl::StrCat("ERR ", status.message())));
      }
      if (watch_aborted) {
        co_await DropWatches(ctx);
        co_return BuiltReply(reply_builder.AppendRaw("*-1\r\n"));
      }
      g_storage->NoteTxCommitStarted();
      SpawnOnCurrentWorker(RunTxCommit(exec_txid, std::move(tx_writes)));
    } else {
      tx::Transaction txn;
      for (const auto& keys : cmd_keys) {
        for (const ExecKey& key : keys) {
          txn.AddKey(key.owner_, key.db_, key.digest_, key.arg_, key.mode_);
        }
      }
      txn.Seal();
      absl::Status scheduled = co_await txn.Schedule();
      if (!scheduled.ok()) {
        co_await DropWatches(ctx);
        co_return BuiltReply(reply_builder.AppendError(
            absl::StrCat("ERR ", scheduled.message())));
      }
      if (!ctx.watched_.empty()) {
        // Schedule only records a queue position; conflicting transactions
        // ordered ahead of this EXEC have not necessarily run, and their
        // writes would land after a check taken now. Run an empty hop first:
        // it returns once every shard has this EXEC's holds, i.e. once
        // everything serialized before it has committed, which is the point
        // the watch check is defined at (and where the single-shard path
        // already takes it).
        absl::Status armed =
            co_await txn.Execute(&ArmOnlyShardCallback, nullptr,
                                 /*release=*/false);
        if (!armed.ok()) {
          (void)co_await txn.Release();
          co_await DropWatches(ctx);
          co_return BuiltReply(
              reply_builder.AppendError(absl::StrCat("ERR ", armed.message())));
        }
        if (!co_await CheckConnectionWatches(ctx)) {
          (void)co_await txn.Release();
          co_await DropWatches(ctx);
          co_return BuiltReply(reply_builder.AppendRaw("*-1\r\n"));
        }
      }
      // Squashed execution: each hop covers a whole run of consecutive keyed
      // commands — every shard works its keys of every command in the run in
      // queue order. Keyless commands break runs, preserving their position
      // in the serial order.
      std::size_t i = 0;
      while (i < queued.size()) {
        const CommandRequest& cmd = queued[i];
        if (cmd_keys[i].empty()) {
          if (cmd.kind_ == CommandKind::kInfo) {
            ReplyBuilder info_builder;
            replies[i] = (co_await ExecuteInfo(cmd, info_builder)).encoded_;
          } else {
            replies[i] = run_keyless(cmd);
          }
          ++i;
          continue;
        }
        std::size_t end = i + 1;
        while (end < queued.size() && !cmd_keys[end].empty()) {
          ++end;
        }
        ExecRunContext run;
        InitExecRun(run, queued, cmd_keys, replies, i, end);
        run.tx_writes_ = tx_writes.data();
        absl::Status hop = co_await txn.Execute(&ExecRunShardCallback, &run,
                                                /*release=*/false);
        if (!hop.ok()) {
          for (std::size_t j = i; j < end; ++j) {
            replies[j] = EncodeStorageError(hop);
          }
        } else {
          AssembleRunReplies(run);
        }
        i = end;
      }
      absl::Status released = co_await txn.Release();
      if (!released.ok()) {
        // Defensive: the no-op release hop cannot fail today. If it ever
        // can, the watches must still be consumed — EXEC ends them whatever
        // its outcome, and stale entries would falsely abort every later
        // EXEC on this connection.
        co_await DropWatches(ctx);
        co_return BuiltReply(reply_builder.AppendError(
            absl::StrCat("ERR ", released.message())));
      }
      g_storage->NoteTxCommitStarted();
      SpawnOnCurrentWorker(RunTxCommit(exec_txid, std::move(tx_writes)));
    }
  } else {
    // Keyless-only transaction.
    if (!ctx.watched_.empty() && !co_await CheckConnectionWatches(ctx)) {
      co_await DropWatches(ctx);
      co_return BuiltReply(reply_builder.AppendRaw("*-1\r\n"));
    }
    for (std::size_t i = 0; i < queued.size(); ++i) {
      if (queued[i].kind_ == CommandKind::kInfo) {
        ReplyBuilder info_builder;
        replies[i] = (co_await ExecuteInfo(queued[i], info_builder)).encoded_;
      } else {
        replies[i] = run_keyless(queued[i]);
      }
    }
  }

  co_await DropWatches(ctx);
  reply_builder.AppendArrayHeader(replies.size());
  for (const std::string& reply : replies) {
    reply_builder.AppendRaw(reply);
  }
  CommandReply reply = BuiltReply(reply_builder.View());
  reply.selected_db_ = select_db;
  co_return reply;
}

}  // namespace

void InitStorage(storage::StorageEngine* engine, bool replica_read_only) {
  g_storage = engine;
  g_replica_read_only = replica_read_only;
}

void SetServerInfo(std::uint16_t port, unsigned thread_count) {
  g_server_port = port;
  g_server_threads = thread_count;
  g_server_start = std::chrono::steady_clock::now();
}

void ConnectionOpened() noexcept { RecordConnectionOpened(); }

void ConnectionClosed() noexcept { RecordConnectionClosed(); }

Task<CommandReply> DispatchCommandImpl(ConnectionContext& ctx,
                                       CommandRequest request,
                                       ReplyBuilder& reply_builder) {
  const CommandKind kind = request.kind_;
  if (ctx.in_multi_) {
    switch (kind) {
      case CommandKind::kMulti:
        co_return BuiltReply(
            reply_builder.AppendError("ERR MULTI calls can not be nested"));
      case CommandKind::kWatch:
        co_return BuiltReply(
            reply_builder.AppendError("ERR WATCH inside MULTI is not allowed"));
      case CommandKind::kDiscard:
        ctx.ResetMulti();
        co_await DropWatches(ctx);
        co_return BuiltReply(reply_builder.AppendSimpleString("OK"));
      case CommandKind::kExec:
        co_return co_await ExecuteExec(ctx, reply_builder);
      default:
        break;
    }
    // Queue-time validation: errors reply immediately and doom the EXEC.
    if (request.spec_ == nullptr) {
      ctx.multi_dirty_ = true;
      co_return BuiltReply(reply_builder.AppendError(
          "ERR unknown command '" + request.args_.front() + "'"));
    }
    const CommandSpec& spec = *request.spec_;
    auto keys = DetermineKeys(spec, request.args_.size());
    if (!keys.ok() ||
        (kind == CommandKind::kMSet && request.args_.size() % 2 != 1)) {
      ctx.multi_dirty_ = true;
      co_return BuiltReply(
          reply_builder.AppendError("ERR wrong number of arguments for '" +
                                    std::string(spec.name_) + "' command"));
    }
    if (g_replica_read_only && (spec.flags_ & kCmdWrite) != 0) {
      ctx.multi_dirty_ = true;
      co_return BuiltReply(reply_builder.AppendError(
          "READONLY You can't write against a read only replica."));
    }
    if ((spec.flags_ & kCmdGlobal) != 0) {
      ctx.multi_dirty_ = true;
      co_return BuiltReply(
          reply_builder.AppendError("ERR " + std::string(spec.name_) +
                                    " is not allowed in transactions"));
    }
    if (kind == CommandKind::kSelect) {
      // Validated by running it: SELECT inside MULTI moves the database for
      // the commands queued after it.
      ReplyBuilder local_builder;
      CommandReply local = ExecuteSimpleLocalCommand(request, local_builder);
      if (!local.selected_db_.has_value()) {
        ctx.multi_dirty_ = true;
        co_return BuiltReply(reply_builder.AppendRaw(local.encoded_));
      }
      ctx.multi_db_ = *local.selected_db_;
    }
    if (RejectForMemory(RequestArgumentBytes(request))) {
      ctx.multi_dirty_ = true;
      co_return BuiltReply(AppendOomError(reply_builder));
    }
    request.db_id_ = ctx.multi_db_;
    ctx.queued_.push_back(std::move(request));
    co_return BuiltReply(reply_builder.AppendSimpleString("QUEUED"));
  }

  switch (kind) {
    case CommandKind::kMulti:
      ctx.in_multi_ = true;
      ctx.multi_dirty_ = false;
      ctx.multi_db_ = ctx.selected_db_;
      co_return BuiltReply(reply_builder.AppendSimpleString("OK"));
    case CommandKind::kExec:
      co_return BuiltReply(reply_builder.AppendError("ERR EXEC without MULTI"));
    case CommandKind::kDiscard:
      co_return BuiltReply(
          reply_builder.AppendError("ERR DISCARD without MULTI"));
    case CommandKind::kWatch:
      if (request.spec_ == nullptr) {
        break;
      }
      co_return co_await ExecuteWatch(ctx, request, reply_builder);
    case CommandKind::kUnwatch:
      co_await DropWatches(ctx);
      co_return BuiltReply(reply_builder.AppendSimpleString("OK"));
    default:
      break;
  }
  co_return co_await ExecuteCommand(request, reply_builder);
}

Task<CommandReply> DispatchCommand(ConnectionContext& ctx,
                                   CommandRequest request,
                                   ReplyBuilder& reply_builder) {
  const CommandKind kind = request.kind_;
  const std::uint64_t started = celer::ReadCycleCounter();
  CommandReply reply =
      co_await DispatchCommandImpl(ctx, std::move(request), reply_builder);
  RecordCommandMetric(kind, celer::ReadCycleCounter() - started);
  co_return reply;
}

Task<absl::Status> ReleaseConnectionWatches(ConnectionContext& ctx) {
  co_return co_await DropWatches(ctx);
}

Task<CommandReply> ExecuteCommand(const CommandRequest& request,
                                  ReplyBuilder& reply_builder) {
  const auto& args = request.args_;
  const std::uint32_t cmd_flags =
      request.spec_ != nullptr ? request.spec_->flags_ : 0u;
  if (g_replica_read_only && (cmd_flags & kCmdWrite) != 0) {
    co_return BuiltReply(reply_builder.AppendError(
        "READONLY You can't write against a read only replica."));
  }
  if (RejectForMemory(EstimatedMemoryGrowth(request))) {
    co_return BuiltReply(AppendOomError(reply_builder));
  }
  if (request.kind_ == CommandKind::kFlushDb ||
      request.kind_ == CommandKind::kFlushAll) {
    co_return co_await ExecuteFlush(request, reply_builder);
  }

  const bool uses_db = (cmd_flags & kCmdUsesDbGate) != 0;
  if (uses_db && !TryBeginDbOperation(request.db_id_)) {
    co_return BuiltReply(
        reply_builder.AppendError("TRYAGAIN database flush is in progress"));
  }
  std::optional<DbOperationGuard> db_guard;
  if (uses_db) {
    db_guard.emplace(request.db_id_);
  }

  switch (request.kind_) {
    case CommandKind::kInfo:
      co_return co_await ExecuteInfo(request, reply_builder);

    case CommandKind::kKeys:
      co_return co_await ExecuteKeys(request, reply_builder);

    case CommandKind::kDbSize:
      co_return co_await ExecuteDbSize(request, reply_builder);

    case CommandKind::kScan:
      co_return co_await ExecuteScan(request, reply_builder);

    case CommandKind::kTombRaider:
      co_return co_await ExecuteTombRaider(request, reply_builder);

    case CommandKind::kDefrag:
      co_return co_await ExecuteDefrag(request, reply_builder);

    case CommandKind::kDel:
    case CommandKind::kExists:
    case CommandKind::kMSet:
    case CommandKind::kMGet:
      co_return co_await ExecuteMultiKey(request, reply_builder);

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
                target,
                [&request, &reply_builder, &trace]() -> Task<CommandReply> {
                  trace.owner_start_ns = ReadTraceNowNanos();
                  CommandReply result = co_await ExecuteStorageCommand(
                      request, reply_builder, &trace);
                  trace.owner_done_ns = ReadTraceNowNanos();
                  co_return result;
                });
          } else {
            trace.owner_start_ns = trace.request_start_ns;
            reply =
                co_await ExecuteStorageCommand(request, reply_builder, &trace);
            trace.owner_done_ns = ReadTraceNowNanos();
          }
          trace.origin_resume_ns = ReadTraceNowNanos();
          reply.read_trace = trace;
          co_return reply;
        }
#endif
        if (target != ThisWorker().id_) {
          co_return co_await SubmitTaskTo(
              target, [&request, &reply_builder]() -> Task<CommandReply> {
                co_return co_await ExecuteStorageCommand(request,
                                                         reply_builder);
              });
        }
      }
      co_return co_await ExecuteStorageCommand(request, reply_builder);

    case CommandKind::kFlushDb:
    case CommandKind::kFlushAll:
      // Handled before the DB operation gate above.
      co_return BuiltReply(
          reply_builder.AppendError("ERR internal flush routing error"));

    default:
      co_return ExecuteSimpleLocalCommand(request, reply_builder);
  }
}

}  // namespace keylane
