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

#include "celer/runtime/cross_core.h"
#include "celer/runtime/worker.h"
#include "celer/io/storage.h"
#include "keylane/command_table.h"
#include "keylane/resp.h"
#include "keylane/session.h"
#include "keylane/tx/transaction.h"
#include "keylane/tx/tx_shard.h"
#include "keylane/storage/engine.h"
#include "keylane/storage/format.h"

namespace keylane {
using namespace celer;

namespace {

storage::StorageEngine* g_storage = nullptr;
bool g_replica_read_only = false;
std::uint16_t g_server_port = 0;
unsigned g_server_threads = 0;
std::chrono::steady_clock::time_point g_server_start;
std::atomic<std::uint64_t> g_connected_clients{0};
std::atomic<std::uint64_t> g_commands_processed{0};

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

}  // namespace

StatusOr<CommandRequest> BuildCommandRequest(RespCommand command,
                                             std::uint8_t db_id) {
  if (command.args.empty()) {
    return Status(StatusCode::kInvalidArgument, "empty command");
  }

  CommandRequest request;
  request.spec = FindCommand(command.args.front());
  request.kind =
      request.spec != nullptr ? request.spec->kind : CommandKind::kUnknown;
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

    case CommandKind::kUnwatch:
      // Inside EXEC this is a no-op: the transaction consumes the watches
      // itself. Outside MULTI, DispatchCommand clears them before this runs.
      reply.encoded = EncodeSimpleString("OK");
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

Task<CommandReply> ExecuteFlushDb(const CommandRequest& request) {
  bool wait_for_reclaim = true;
  if (request.args.size() == 2) {
    if (CmpCaseInsensitive(request.args[1], "ASYNC")) {
      wait_for_reclaim = false;
    } else if (!CmpCaseInsensitive(request.args[1], "SYNC")) {
      co_return EncodedReply(EncodeError("ERR syntax error"));
    }
  } else if (request.args.size() != 1) {
    co_return EncodedReply(
        EncodeError("ERR wrong number of arguments for 'flushdb' command"));
  }

  Status detached = Status::Ok();
  {
    // The gate closes the database to every other command, so it covers only
    // the phase that has to be exclusive: draining commands already in flight,
    // and swapping the indexes out. Reclaiming what was swapped out runs below
    // with the database open again, since nothing can reach it any more.
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
    detached = co_await g_storage->FlushDbDetach(request.db_id);
  }
  if (!detached.ok()) {
    co_return EncodedReply(EncodeError("ERR " + detached.message()));
  }

  Status reclaimed = co_await g_storage->FlushDbReclaim(wait_for_reclaim);
  co_return EncodedReply(reclaimed.ok()
                             ? EncodeSimpleString("OK")
                             : EncodeError("ERR " + reclaimed.message()));
}

struct ScanOptions {
  std::uint64_t cursor = 0;
  std::size_t count = 10;
  std::optional<std::string_view> pattern;
  std::optional<std::string_view> type;
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
    if (CmpCaseInsensitive(args[i], "TYPE") && i + 1 < args.size()) {
      options.type = args[i + 1];
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
  // COUNT is the per-call work hint: it also bounds how many (mostly
  // empty) partitions one call may examine, so large COUNTs sweep the
  // keyspace in few round trips.
  const std::size_t max_partitions_per_call =
      std::clamp<std::size_t>(parsed->count, 64,
                              storage::kLogicalStorageShards);
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
    const bool type_matches =
        !options.type.has_value() ||
        CmpCaseInsensitive(*options.type, "string");
    for (std::string& key : batch.keys) {
      if (type_matches &&
          (!options.pattern.has_value() ||
           GlobMatch(*options.pattern, key))) {
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
        partitions_examined >= max_partitions_per_call) {
      const std::uint64_t cursor =
          static_cast<std::uint64_t>(partition_id) << kLocalBits;
      co_return EncodedReply(EncodeScanReply(cursor, keys));
    }
    remaining -= examined;
  }
  co_return EncodedReply(EncodeScanReply(0, keys));
}

std::uint64_t CommandUnixTimeMillis() noexcept;

// KEYS streams its reply in bounded memory. RESP2 arrays announce their
// element count first, so the keyspace must hold still between the counting
// pass and the emitting pass: the database gate is closed (like FLUSHDB) and
// expiration writes are quiesced, with a fixed liveness timestamp shared by
// both passes. State is dropped when the reply finishes or the connection
// dies, reopening the gate either way.
struct KeysStreamState {
  explicit KeysStreamState(std::uint8_t db_id) : db(db_id), guard(db_id) {}
  ~KeysStreamState() {
    // Resume only a pause this KEYS actually took: the pause nests across
    // overlapping KEYS on other databases, and the early-error path drops
    // the state before ever quiescing.
    if (expiration_quiesced) {
      g_storage->ResumeExpiration();
    }
  }

  std::uint8_t db;
  bool expiration_quiesced = false;
  DbCloseGuard guard;
  std::string pattern;
  std::uint64_t now_ms = 0;
  unsigned worker = 0;        // worker currently being drained
  unsigned partition = 0;     // absolute partition id owned by `worker`
  std::uint64_t cursor = 0;
};

// One bounded batch on `worker`: walks that worker's own partitions locally
// (one cross-core round trip per batch, not per partition), encoding matches
// or just counting them. Yields periodically so other databases' traffic on
// the worker keeps flowing.
struct KeysWorkerBatch {
  std::string payload;
  std::uint64_t matches = 0;
  unsigned partition = 0;
  std::uint64_t cursor = 0;
  bool worker_done = false;
};

Task<KeysWorkerBatch> KeysBatchOnWorker(std::uint8_t db, unsigned worker,
                                        unsigned partition,
                                        std::uint64_t cursor,
                                        std::uint64_t now_ms,
                                        const std::string* pattern,
                                        bool count_only) {
  co_return co_await celer::SubmitTaskTo(
      worker, [=]() -> Task<KeysWorkerBatch> {
        constexpr std::size_t kChunkBytes = 64 * 1024;
        const unsigned stride = g_storage->worker_count();
        KeysWorkerBatch batch;
        batch.partition = partition == 0 ? worker : partition;
        batch.cursor = cursor;
        unsigned scanned = 0;
        while (batch.partition < storage::kLogicalStorageShards &&
               batch.payload.size() < kChunkBytes) {
          storage::ScanBatch step = g_storage->ScanPartition(
              static_cast<std::uint16_t>(batch.partition), db, batch.cursor,
              512, now_ms);
          for (const std::string& key : step.keys) {
            if (*pattern == "*" || GlobMatch(*pattern, key)) {
              if (count_only) {
                ++batch.matches;
              } else {
                batch.payload += "$" + std::to_string(key.size()) + "\r\n" +
                                 key + "\r\n";
              }
            }
          }
          if (step.cursor == 0) {
            batch.partition += stride;
            batch.cursor = 0;
          } else {
            batch.cursor = step.cursor;
          }
          if (++scanned % 256 == 0) {
            co_await celer::Yield(*ThisWorker().self);
          }
        }
        batch.worker_done =
            batch.partition >= storage::kLogicalStorageShards;
        co_return batch;
      });
}

Task<StatusOr<std::string>> NextKeysChunk(
    std::shared_ptr<KeysStreamState> state) {
  while (state->worker < g_storage->worker_count()) {
    KeysWorkerBatch batch = co_await KeysBatchOnWorker(
        state->db, state->worker, state->partition, state->cursor,
        state->now_ms, &state->pattern, /*count_only=*/false);
    if (batch.worker_done) {
      ++state->worker;
      state->partition = 0;
      state->cursor = 0;
    } else {
      state->partition = batch.partition;
      state->cursor = batch.cursor;
    }
    if (!batch.payload.empty()) {
      co_return std::move(batch.payload);
    }
  }
  co_return std::string();
}

Task<CommandReply> ExecuteKeys(const CommandRequest& request) {
  if (request.args.size() != 2) {
    co_return EncodedReply(
        EncodeError("ERR wrong number of arguments for 'keys' command"));
  }
  const std::uint8_t db = request.db_id;
  if (!CloseDbGate(db)) {
    co_return EncodedReply(
        EncodeError("BUSY another operation is holding the database"));
  }
  auto state = std::make_shared<KeysStreamState>(db);
  state->pattern = request.args[1];
  state->now_ms = CommandUnixTimeMillis();
  // Drain in-flight commands, then freeze expiration writes: from here to the
  // end of the stream the keyspace cannot change, so the counted N is exact.
  while ((g_db_gates[db].load(std::memory_order_acquire) &
          kDbGateCountMask) != 0) {
    Status waited = co_await celer::SleepFor(*ThisWorker().self,
                                             std::chrono::milliseconds(1));
    if (!waited.ok()) {
      co_return EncodedReply(EncodeError("ERR " + waited.message()));
    }
  }
  Status quiesced = co_await g_storage->QuiesceExpiration();
  if (!quiesced.ok()) {
    co_return EncodedReply(EncodeError("ERR " + quiesced.message()));
  }
  state->expiration_quiesced = true;

  // Counting pass over the frozen keyspace: one batched walk per worker.
  std::uint64_t matches = 0;
  for (unsigned worker = 0; worker < g_storage->worker_count(); ++worker) {
    unsigned partition = 0;
    std::uint64_t cursor = 0;
    for (;;) {
      KeysWorkerBatch batch = co_await KeysBatchOnWorker(
          db, worker, partition, cursor, state->now_ms, &state->pattern,
          /*count_only=*/true);
      matches += batch.matches;
      if (batch.worker_done) {
        break;
      }
      partition = batch.partition;
      cursor = batch.cursor;
    }
  }

  CommandReply reply =
      EncodedReply("*" + std::to_string(matches) + "\r\n");
  reply.chunks = [state]() { return NextKeysChunk(state); };
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

// INFO: Redis-shaped sections built from what keylane actually tracks. The
// Transactions section surfaces the VLL scheduler counters.
Task<CommandReply> ExecuteInfo(const CommandRequest& request) {
  std::string section = "default";
  if (request.args.size() == 2) {
    section = request.args[1];
    for (char& c : section) {
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
  }
  const bool all = section == "default" || section == "all" ||
                   section == "everything";
  auto wants = [&](std::string_view name) { return all || section == name; };

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
            std::to_string(
                g_connected_clients.load(std::memory_order_relaxed)) +
            "\r\n\r\n";
  }
  if (wants("stats")) {
    info += "# Stats\r\n";
    info += "total_commands_processed:" +
            std::to_string(
                g_commands_processed.load(std::memory_order_relaxed)) +
            "\r\n\r\n";
  }
  if (wants("replication")) {
    info += "# Replication\r\n";
    info += std::string("role:") +
            (g_replica_read_only ? "slave" : "master") + "\r\n\r\n";
  }
  if (wants("transactions")) {
    struct ShardStats {
      std::uint64_t fastpath = 0;
      std::uint64_t queued = 0;
    };
    std::uint64_t fastpath = 0;
    std::uint64_t queued = 0;
    tx::TxRuntime* runtime = tx::TxRuntime::Get();
    for (unsigned target = 0; target < runtime->shard_count(); ++target) {
      const ShardStats stats = co_await SubmitTo(target, [] {
        tx::TxShard& shard = tx::CurrentTxShard();
        return ShardStats{shard.fastpath_runs(), shard.queued_runs()};
      });
      fastpath += stats.fastpath;
      queued += stats.queued;
    }
    info += "# Transactions\r\n";
    info += "tx_fastpath_runs:" + std::to_string(fastpath) + "\r\n";
    info += "tx_queued_runs:" + std::to_string(queued) + "\r\n";
    info += "tx_schedule_retries:" +
            std::to_string(
                runtime->schedule_retries.load(std::memory_order_relaxed)) +
            "\r\n";
    info += "tx_ids_allocated:" +
            std::to_string(
                runtime->next_txid.load(std::memory_order_relaxed) - 1) +
            "\r\n\r\n";
  }
  if (wants("keyspace")) {
    info += "# Keyspace\r\n";
    for (unsigned db = 0; db < storage::kLogicalDatabaseCount; ++db) {
      std::uint64_t keys = 0;
      for (unsigned target = 0; target < g_storage->worker_count();
           ++target) {
        keys += co_await SubmitTo(
            target, [db] { return g_storage->LocalSize(
                               static_cast<std::uint8_t>(db)); });
      }
      if (keys != 0) {
        info += "db" + std::to_string(db) + ":keys=" + std::to_string(keys) +
                "\r\n";
      }
    }
    info += "\r\n";
  }
  co_return EncodedReply(EncodeBulkString(info));
}

// Runs one single-key command body against pre-acquired locks, returning the
// encoded reply. Mirrors ExecuteStorageCommand's semantics; arity was already
// validated when the command was queued.
Task<std::string> RunSingleKeyLocked(std::uint8_t db_id,
                                     const CommandRequest& request,
                                     const storage::Digest& digest) {
  const auto& args = request.args;
  switch (request.kind) {
    case CommandKind::kGet: {
      auto value = co_await g_storage->GetLocked(db_id, args[1], digest);
      if (value.ok()) {
        const auto bytes = value->network_bytes();
        co_return std::string(reinterpret_cast<const char*>(bytes.data()),
                              bytes.size());
      }
      co_return value.status().code() == StatusCode::kNotFound
          ? EncodeNullBulkString()
          : EncodeError("ERR " + value.status().message());
    }

    case CommandKind::kSet: {
      auto options = ParseSetOptions(args);
      if (!options.ok()) {
        co_return EncodeError("ERR " + options.status().message());
      }
      auto result = co_await g_storage->SetLocked(db_id, args[1], digest,
                                                  args[2], *options);
      if (!result.ok()) {
        co_return EncodeStorageError(result.status());
      }
      if (options->return_old_value) {
        if (result->old_value.has_value()) {
          const auto bytes = result->old_value->network_bytes();
          co_return std::string(reinterpret_cast<const char*>(bytes.data()),
                                bytes.size());
        }
        co_return EncodeNullBulkString();
      }
      co_return result->applied ? EncodeSimpleString("OK")
                                : EncodeNullBulkString();
    }

    case CommandKind::kStrlen: {
      auto length =
          co_await g_storage->StringLengthLocked(db_id, args[1], digest);
      if (!length.ok()) {
        co_return length.status().code() == StatusCode::kNotFound
            ? EncodeInteger(0)
            : EncodeStorageError(length.status());
      }
      if (*length > static_cast<std::uint64_t>(
                        std::numeric_limits<long long>::max())) {
        co_return EncodeError("ERR String length exceeds RESP range");
      }
      co_return EncodeInteger(static_cast<long long>(*length));
    }

    case CommandKind::kTtl:
    case CommandKind::kPttl: {
      const bool milliseconds = request.kind == CommandKind::kPttl;
      const storage::ExpirationInfo info =
          co_await g_storage->GetExpirationLocked(db_id, args[1], digest);
      if (!info.exists) {
        co_return EncodeInteger(-2);
      }
      if (info.expire_at_ms == 0) {
        co_return EncodeInteger(-1);
      }
      const std::uint64_t now_ms = CommandUnixTimeMillis();
      const std::uint64_t remaining =
          info.expire_at_ms > now_ms ? info.expire_at_ms - now_ms : 0;
      co_return EncodeInteger(static_cast<long long>(
          milliseconds ? remaining : remaining / 1000));
    }

    case CommandKind::kExpire:
    case CommandKind::kPExpire: {
      const bool milliseconds = request.kind == CommandKind::kPExpire;
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
          db_id, args[1], digest, expire_at_ms, *condition);
      co_return updated.ok() ? EncodeInteger(*updated ? 1 : 0)
                             : EncodeStorageError(updated.status());
    }

    case CommandKind::kPersist: {
      auto updated = co_await g_storage->UpdateExpirationLocked(
          db_id, args[1], digest, 0,
          storage::ExpirationCondition::kIfHasExpiration);
      co_return updated.ok() ? EncodeInteger(*updated ? 1 : 0)
                             : EncodeStorageError(updated.status());
    }

    case CommandKind::kIncr: {
      auto value = co_await g_storage->IncrementLocked(db_id, args[1], digest);
      if (value.ok()) {
        co_return EncodeInteger(*value);
      }
      if (value.status().message().starts_with("WRONGTYPE ")) {
        co_return EncodeError(value.status().message());
      }
      co_return value.status().code() == StatusCode::kInvalidArgument
          ? EncodeError("ERR value is not an integer or out of range")
          : EncodeError("ERR " + value.status().message());
    }

    default:
      co_return EncodeError("ERR command is not allowed in transactions");
  }
}

// Joins per-key reader coroutines spawned on one shard. Everything runs on
// the owning worker thread, so plain counters suffice; the waiter resumes
// via its own worker's ready queue once the last read lands.
struct ShardReadJoin {
  std::size_t pending = 0;
  std::coroutine_handle<> waiter;
  Status error;

  void Complete(Status status) {
    if (!status.ok() && error.ok()) {
      error = std::move(status);
    }
    if (--pending == 0 && waiter) {
      auto handle = waiter;
      waiter = {};
      ThisWorker().self->Enqueue(handle);
    }
  }

  auto Join() {
    struct Awaiter {
      ShardReadJoin* join;
      bool await_ready() const { return join->pending == 0; }
      void await_suspend(std::coroutine_handle<> handle) {
        join->waiter = handle;
      }
      void await_resume() const {}
    };
    return Awaiter{this};
  }
};

// One concurrent MGET read: locks are already held for the whole hop, and
// distinct keys live in distinct blocks, so per-key disk reads overlap
// instead of accumulating latency serially.
Task<Status> ReadFrameIntoSlot(std::uint8_t db, const std::string* key,
                               storage::Digest digest,
                               std::optional<std::string>* slot,
                               ShardReadJoin* join) {
  auto value = co_await g_storage->GetLocked(db, *key, digest);
  Status status = Status::Ok();
  if (value.ok()) {
    const auto bytes = value->network_bytes();
    slot->emplace(reinterpret_cast<const char*>(bytes.data()), bytes.size());
  } else if (value.status().code() != StatusCode::kNotFound) {
    status = value.status();
  }
  join->Complete(std::move(status));
  co_return Status::Ok();
}

// Shared context of one multi-key command's transaction. Shard callbacks
// write disjoint reply slots (MGET) or bump the shared counter (DEL/EXISTS)
// before the hop barrier; the coordinator assembles the reply afterwards.
struct MultiKeyContext {
  const CommandRequest* request = nullptr;
  std::vector<std::optional<std::string>> frames;  // MGET: encoded bulk per slot
  std::atomic<long long> hits{0};                  // DEL / EXISTS
};

Task<Status> MultiKeyShardCallback(void* context,
                                   const tx::ShardSlice& slice) {
  auto* ctx = static_cast<MultiKeyContext*>(context);
  const auto& args = ctx->request->args;
  if (ctx->request->kind == CommandKind::kMGet && slice.keys.size() > 1) {
    // Overlap this shard's disk reads instead of awaiting them one by one.
    ShardReadJoin join;
    join.pending = slice.keys.size();
    for (const tx::TxKey& key : slice.keys) {
      SpawnOnCurrentWorker(ReadFrameIntoSlot(
          ctx->request->db_id, &args[key.arg_index], key.digest,
          &ctx->frames[key.arg_index - 1], &join));
    }
    co_await join.Join();
    co_return join.error;
  }
  for (const tx::TxKey& key : slice.keys) {
    const std::string& name = args[key.arg_index];
    switch (ctx->request->kind) {
      case CommandKind::kMSet: {
        auto result = co_await g_storage->SetLocked(
            ctx->request->db_id, name, key.digest, args[key.arg_index + 1],
            {});
        if (!result.ok()) {
          co_return result.status();
        }
        break;
      }
      case CommandKind::kMGet: {
        auto value = co_await g_storage->GetLocked(ctx->request->db_id,
                                                   name, key.digest);
        if (value.ok()) {
          const auto bytes = value->network_bytes();
          ctx->frames[key.arg_index - 1].emplace(
              reinterpret_cast<const char*>(bytes.data()), bytes.size());
        } else if (value.status().code() != StatusCode::kNotFound) {
          co_return value.status();
        }
        break;
      }
      case CommandKind::kDel: {
        auto deleted =
            co_await g_storage->DeleteLocked(ctx->request->db_id, name,
                                             key.digest);
        if (!deleted.ok()) {
          co_return deleted.status();
        }
        if (*deleted) {
          ctx->hits.fetch_add(1, std::memory_order_relaxed);
        }
        break;
      }
      case CommandKind::kExists:
      default: {
        if (co_await g_storage->ExistsLocked(ctx->request->db_id, name,
                                             key.digest)) {
          ctx->hits.fetch_add(1, std::memory_order_relaxed);
        }
        break;
      }
    }
  }
  co_return Status::Ok();
}

// DEL / EXISTS / MSET / MGET run as one transaction: every key locked up
// front (across all owning shards), one hop where each shard works its
// slice, locks released when the hop completes.
Task<CommandReply> ExecuteMultiKey(const CommandRequest& request) {
  const auto& args = request.args;
  auto keys = DetermineKeys(*request.spec, args.size());
  if (!keys.ok()) {
    co_return EncodedReply(EncodeError("ERR " + keys.status().message()));
  }
  if (request.kind == CommandKind::kMSet && args.size() % 2 != 1) {
    co_return EncodedReply(
        EncodeError("ERR wrong number of arguments for 'mset' command"));
  }

  const bool write = (request.spec->flags & kCmdWrite) != 0;
  tx::Transaction txn;
  for (std::size_t i = keys->first; i <= keys->last; i += keys->step) {
    txn.AddKey(ShardForKey(args[i]), request.db_id,
               storage::ComputeDigest(args[i]),
               static_cast<std::uint32_t>(i),
               write ? tx::LockMode::kExclusive : tx::LockMode::kShared);
  }
  txn.Seal();

  MultiKeyContext ctx;
  ctx.request = &request;
  if (request.kind == CommandKind::kMGet) {
    ctx.frames.resize(keys->count());
  }

  Status scheduled = co_await txn.Schedule();
  if (!scheduled.ok()) {
    co_return EncodedReply(EncodeError("ERR " + scheduled.message()));
  }
  Status status = co_await txn.Execute(&MultiKeyShardCallback, &ctx, true);
  if (!status.ok()) {
    co_return EncodedReply(EncodeStorageError(status));
  }

  switch (request.kind) {
    case CommandKind::kMSet:
      co_return EncodedReply(EncodeSimpleString("OK"));
    case CommandKind::kMGet: {
      std::string reply =
          "*" + std::to_string(ctx.frames.size()) + "\r\n";
      for (const auto& frame : ctx.frames) {
        reply += frame.has_value() ? *frame : "$-1\r\n";
      }
      co_return EncodedReply(std::move(reply));
    }
    default:
      co_return EncodedReply(
          EncodeInteger(ctx.hits.load(std::memory_order_relaxed)));
  }
}

// One key of a queued EXEC command, with everything precomputed on the
// coordinator: digest, owning shard, argument position, reply slot.
struct ExecKey {
  storage::Digest digest;
  std::uint16_t owner = 0;
  std::uint16_t arg = 0;
  std::uint16_t slot = 0;
  tx::LockMode mode = tx::LockMode::kShared;
  std::uint8_t db = 0;
};

// One squashed run of consecutive keyed commands [begin, end): every shard
// executes its keys of every command in queue order within a single hop.
// Sinks are per command (indexed by i - begin); shards write disjoint reply
// slots, per-command atomic counters, and record rare per-command errors
// under a mutex.
struct ExecRunContext {
  const std::vector<CommandRequest>* queued = nullptr;
  const std::vector<std::vector<ExecKey>>* cmd_keys = nullptr;
  std::vector<std::string>* replies = nullptr;
  std::size_t begin = 0;
  std::size_t end = 0;
  std::vector<std::vector<std::optional<std::string>>> mget;
  std::unique_ptr<std::atomic<long long>[]> counters;
  std::mutex error_mutex;
  std::vector<Status> errors;
};

// Builds the replies of a completed run from its per-command sinks.
// Single-key commands already wrote their slots on the owning shard.
void AssembleRunReplies(ExecRunContext& run) {
  for (std::size_t i = run.begin; i < run.end; ++i) {
    const std::size_t local = i - run.begin;
    if (!run.errors[local].ok()) {
      (*run.replies)[i] = EncodeStorageError(run.errors[local]);
      continue;
    }
    switch ((*run.queued)[i].kind) {
      case CommandKind::kMSet:
        (*run.replies)[i] = EncodeSimpleString("OK");
        break;
      case CommandKind::kMGet: {
        std::string reply =
            "*" + std::to_string(run.mget[local].size()) + "\r\n";
        for (const auto& frame : run.mget[local]) {
          reply += frame.has_value() ? *frame : "$-1\r\n";
        }
        (*run.replies)[i] = std::move(reply);
        break;
      }
      case CommandKind::kDel:
      case CommandKind::kExists:
        (*run.replies)[i] = EncodeInteger(
            run.counters[local].load(std::memory_order_relaxed));
        break;
      default:
        break;
    }
  }
}

// Prepares the sinks of one squashed run over [begin, end).
void InitExecRun(ExecRunContext& run,
                 const std::vector<CommandRequest>& queued,
                 const std::vector<std::vector<ExecKey>>& cmd_keys,
                 std::vector<std::string>& replies, std::size_t begin,
                 std::size_t end) {
  run.queued = &queued;
  run.cmd_keys = &cmd_keys;
  run.replies = &replies;
  run.begin = begin;
  run.end = end;
  const std::size_t count = end - begin;
  run.counters = std::make_unique<std::atomic<long long>[]>(count);
  run.errors.assign(count, Status::Ok());
  run.mget.resize(count);
  for (std::size_t i = begin; i < end; ++i) {
    if (queued[i].kind == CommandKind::kMGet) {
      run.mget[i - begin].assign(cmd_keys[i].size(), std::nullopt);
    }
  }
}

// A hop that does nothing but acquire (and keep) every shard's holds, so the
// coordinator can act at the transaction's position in the serial order
// before running any command.
Task<Status> ArmOnlyShardCallback(void*, const tx::ShardSlice&) {
  co_return Status::Ok();
}

// One EXEC hop = one squashed run: every shard executes its keys of each
// command in [begin, end) in queue order (same-key commands share an owner,
// so their relative order is preserved). A command's failure is recorded and
// the remaining commands still run, matching Redis's continue-on-error
// transaction semantics.
Task<Status> ExecRunShardCallback(void* context, const tx::ShardSlice&) {
  auto* ctx = static_cast<ExecRunContext*>(context);
  const unsigned self = ThisWorker().id;
  for (std::size_t i = ctx->begin; i < ctx->end; ++i) {
    const CommandRequest& cmd = (*ctx->queued)[i];
    const auto& args = cmd.args;
    const auto& keys = (*ctx->cmd_keys)[i];
    const std::size_t local = i - ctx->begin;
    auto record_error = [&](Status status) {
      std::lock_guard<std::mutex> lock(ctx->error_mutex);
      if (ctx->errors[local].ok()) {
        ctx->errors[local] = std::move(status);
      }
    };

    if (cmd.kind == CommandKind::kMGet) {
      std::size_t mine = 0;
      for (const ExecKey& key : keys) {
        mine += key.owner == self ? 1 : 0;
      }
      if (mine > 1) {
        ShardReadJoin join;
        join.pending = mine;
        for (const ExecKey& key : keys) {
          if (key.owner == self) {
            SpawnOnCurrentWorker(ReadFrameIntoSlot(
                cmd.db_id, &args[key.arg], key.digest,
                &ctx->mget[local][key.slot], &join));
          }
        }
        co_await join.Join();
        if (!join.error.ok()) {
          record_error(std::move(join.error));
        }
        continue;
      }
    }

    for (const ExecKey& key : keys) {
      if (key.owner != self) {
        continue;
      }
      bool command_failed = false;
      switch (cmd.kind) {
        case CommandKind::kMSet: {
          auto result = co_await g_storage->SetLocked(
              cmd.db_id, args[key.arg], key.digest, args[key.arg + 1], {});
          if (!result.ok()) {
            record_error(result.status());
            command_failed = true;
          }
          break;
        }
        case CommandKind::kMGet: {
          auto value = co_await g_storage->GetLocked(cmd.db_id, args[key.arg],
                                                     key.digest);
          if (value.ok()) {
            const auto bytes = value->network_bytes();
            ctx->mget[local][key.slot].emplace(
                reinterpret_cast<const char*>(bytes.data()), bytes.size());
          } else if (value.status().code() != StatusCode::kNotFound) {
            record_error(value.status());
            command_failed = true;
          }
          break;
        }
        case CommandKind::kDel: {
          auto deleted = co_await g_storage->DeleteLocked(
              cmd.db_id, args[key.arg], key.digest);
          if (!deleted.ok()) {
            record_error(deleted.status());
            command_failed = true;
          } else if (*deleted) {
            ctx->counters[local].fetch_add(1, std::memory_order_relaxed);
          }
          break;
        }
        case CommandKind::kExists: {
          if (co_await g_storage->ExistsLocked(cmd.db_id, args[key.arg],
                                               key.digest)) {
            ctx->counters[local].fetch_add(1, std::memory_order_relaxed);
          }
          break;
        }
        default:
          // Single-key command: the sole owner runs the full body and writes
          // the reply slot directly (errors self-encode).
          (*ctx->replies)[i] =
              co_await RunSingleKeyLocked(cmd.db_id, cmd, key.digest);
          break;
      }
      if (command_failed) {
        break;  // abandon this command's remaining keys; run the next one
      }
    }
  }
  co_return Status::Ok();
}

// The union lock set of an EXEC, deduplicated per fingerprint with
// exclusive-if-any-writer, as TxShard::AcquireKeys requires.
std::vector<tx::KeyRef> DedupExecLocks(
    const std::vector<std::vector<ExecKey>>& cmd_keys) {
  std::vector<tx::KeyRef> refs;
  for (const auto& keys : cmd_keys) {
    for (const ExecKey& key : keys) {
      const tx::LockFp fp = tx::FingerprintOf(key.digest);
      bool merged = false;
      for (tx::KeyRef& ref : refs) {
        if (ref.fp == fp && ref.db == key.db) {
          if (key.mode == tx::LockMode::kExclusive) {
            ref.mode = tx::LockMode::kExclusive;
          }
          merged = true;
          break;
        }
      }
      if (!merged) {
        refs.push_back(tx::KeyRef{fp, key.mode, key.db});
      }
    }
  }
  return refs;
}

// Registers each key on its owning shard with a liveness snapshot taken
// there; duplicates of an already-watched (db, fp) keep the first snapshot.
Task<CommandReply> ExecuteWatch(ConnectionContext& ctx,
                                const CommandRequest& request) {
  auto keys = DetermineKeys(*request.spec, request.args.size());
  if (!keys.ok()) {
    co_return EncodedReply(EncodeError("ERR " + keys.status().message()));
  }
  for (std::size_t i = keys->first; i <= keys->last; i += keys->step) {
    const std::uint8_t db = ctx.selected_db;
    const storage::Digest digest = storage::ComputeDigest(request.args[i]);
    const tx::LockFp fp = tx::FingerprintOf(digest);
    bool already = false;
    for (const auto& watched : ctx.watched) {
      if (watched.db == db && watched.key == request.args[i]) {
        already = true;
        break;
      }
    }
    if (already) {
      continue;
    }
    const std::uint16_t owner =
        static_cast<std::uint16_t>(ShardForKey(request.args[i]));
    const bool live = co_await SubmitTo(
        owner, [key = std::string(request.args[i]), db, digest, fp,
                conn = ctx.conn_id]() {
          tx::CurrentTxShard().Watch(db, fp, conn);
          return g_storage->KeyLive(db, key, digest);
        });
    ctx.watched.push_back(ConnectionContext::WatchedKey{
        .key = request.args[i],
        .digest = digest,
        .fp = fp,
        .owner = owner,
        .db = db,
        .live = live,
    });
  }
  co_return EncodedReply(EncodeSimpleString("OK"));
}

// True when every watched key is unmarked and still matches its WATCH-time
// liveness. Runs on each key's owning shard; callers hold whatever locks the
// transaction needs before asking. The liveness snapshot travels with the
// key, not the shard entry, so keys sharing a fingerprint are each compared
// against their own snapshot.
Task<bool> CheckConnectionWatches(const ConnectionContext& ctx) {
  for (const auto& watched : ctx.watched) {
    const bool clean = co_await SubmitTo(
        watched.owner,
        [key = watched.key, db = watched.db, digest = watched.digest,
         fp = watched.fp, live = watched.live, conn = ctx.conn_id]() {
          return tx::CurrentTxShard().WatchClean(db, fp, conn) &&
                 g_storage->KeyLive(db, key, digest) == live;
        });
    if (!clean) {
      co_return false;
    }
  }
  co_return true;
}

// EXEC consumes the connection's watches whatever its outcome.
Task<Status> DropWatches(ConnectionContext& ctx) {
  for (const auto& watched : ctx.watched) {
    co_await SubmitTo(watched.owner,
                      [db = watched.db, fp = watched.fp,
                       conn = ctx.conn_id]() {
                        tx::CurrentTxShard().Unwatch(db, fp, conn);
                        return true;
                      });
  }
  ctx.watched.clear();
  co_return Status::Ok();
}

Task<CommandReply> ExecuteExec(ConnectionContext& ctx) {
  const std::vector<CommandRequest> queued = std::move(ctx.queued);
  const bool dirty = ctx.multi_dirty;
  ctx.ResetMulti();
  if (dirty) {
    co_await DropWatches(ctx);
    co_return EncodedReply(EncodeError(
        "EXECABORT Transaction discarded because of previous errors."));
  }
  if (queued.empty()) {
    const bool clean =
        ctx.watched.empty() || co_await CheckConnectionWatches(ctx);
    co_await DropWatches(ctx);
    co_return EncodedReply(clean ? "*0\r\n" : "*-1\r\n");
  }

  // Precompute every queued command's keys: digests, owners, slots, modes.
  std::vector<std::vector<ExecKey>> cmd_keys(queued.size());
  std::vector<std::uint8_t> dbs;  // distinct databases with keyed commands
  for (std::size_t i = 0; i < queued.size(); ++i) {
    const CommandRequest& cmd = queued[i];
    if (cmd.spec == nullptr || cmd.spec->first_key == 0) {
      continue;
    }
    auto keys = DetermineKeys(*cmd.spec, cmd.args.size());
    if (!keys.ok()) {
      co_return EncodedReply(EncodeError("ERR " + keys.status().message()));
    }
    const bool write = (cmd.spec->flags & kCmdWrite) != 0;
    std::uint16_t slot = 0;
    for (std::size_t a = keys->first; a <= keys->last; a += keys->step) {
      cmd_keys[i].push_back(ExecKey{
          .digest = storage::ComputeDigest(cmd.args[a]),
          .owner = static_cast<std::uint16_t>(ShardForKey(cmd.args[a])),
          .arg = static_cast<std::uint16_t>(a),
          .slot = slot++,
          .mode = write ? tx::LockMode::kExclusive : tx::LockMode::kShared,
          .db = cmd.db_id,
      });
    }
    if (std::find(dbs.begin(), dbs.end(), cmd.db_id) == dbs.end()) {
      dbs.push_back(cmd.db_id);
    }
  }

  std::vector<std::string> replies(queued.size());
  std::optional<std::uint8_t> select_db;
  auto run_keyless = [&](const CommandRequest& cmd) {
    CommandReply local = ExecuteLocalCommand(cmd);
    if (local.selected_db.has_value()) {
      select_db = local.selected_db;
    }
    return std::move(local.encoded);
  };

  if (!dbs.empty()) {
    MultiDbOperationGuard db_guard;
    for (const std::uint8_t db : dbs) {
      if (!db_guard.Add(db)) {
        co_await DropWatches(ctx);
        co_return EncodedReply(
            EncodeError("TRYAGAIN FLUSHDB is in progress"));
      }
    }

    // Distinct owners across the whole transaction.
    std::vector<std::uint16_t> owners;
    for (const auto& keys : cmd_keys) {
      for (const ExecKey& key : keys) {
        if (std::find(owners.begin(), owners.end(), key.owner) ==
            owners.end()) {
          owners.push_back(key.owner);
        }
      }
    }

    if (owners.size() == 1) {
      // Whole transaction on one shard: hop once, take the fast-path guard
      // over the union lock set, run every command inline.
      const std::vector<tx::KeyRef> refs = DedupExecLocks(cmd_keys);
      bool watch_aborted = false;
      Status status = co_await SubmitTaskTo(
          owners.front(), [&]() -> Task<Status> {
            auto guard = co_await tx::CurrentTxShard().AcquireKeys(
                std::span<const tx::KeyRef>(refs));
            if (!ctx.watched.empty() &&
                !co_await CheckConnectionWatches(ctx)) {
              watch_aborted = true;
              co_return Status::Ok();
            }
            std::size_t i = 0;
            while (i < queued.size()) {
              const CommandRequest& cmd = queued[i];
              if (cmd_keys[i].empty()) {
                if (cmd.kind == CommandKind::kInfo) {
                  replies[i] = (co_await ExecuteInfo(cmd)).encoded;
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
              (void)co_await ExecRunShardCallback(&run,
                                                  tx::ShardSlice{.keys = {}});
              AssembleRunReplies(run);
              i = end;
            }
            co_return Status::Ok();
          });
      if (!status.ok()) {
        co_await DropWatches(ctx);
        co_return EncodedReply(EncodeError("ERR " + status.message()));
      }
      if (watch_aborted) {
        co_await DropWatches(ctx);
        co_return EncodedReply("*-1\r\n");
      }
    } else {
      tx::Transaction txn;
      for (const auto& keys : cmd_keys) {
        for (const ExecKey& key : keys) {
          txn.AddKey(key.owner, key.db, key.digest, key.arg, key.mode);
        }
      }
      txn.Seal();
      Status scheduled = co_await txn.Schedule();
      if (!scheduled.ok()) {
        co_await DropWatches(ctx);
        co_return EncodedReply(EncodeError("ERR " + scheduled.message()));
      }
      if (!ctx.watched.empty()) {
        // Schedule only records a queue position; conflicting transactions
        // ordered ahead of this EXEC have not necessarily run, and their
        // writes would land after a check taken now. Run an empty hop first:
        // it returns once every shard has this EXEC's holds, i.e. once
        // everything serialized before it has committed, which is the point
        // the watch check is defined at (and where the single-shard path
        // already takes it).
        Status armed = co_await txn.Execute(&ArmOnlyShardCallback, nullptr,
                                            /*release=*/false);
        if (!armed.ok()) {
          (void)co_await txn.Release();
          co_await DropWatches(ctx);
          co_return EncodedReply(EncodeError("ERR " + armed.message()));
        }
        if (!co_await CheckConnectionWatches(ctx)) {
          (void)co_await txn.Release();
          co_await DropWatches(ctx);
          co_return EncodedReply("*-1\r\n");
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
          if (cmd.kind == CommandKind::kInfo) {
            replies[i] = (co_await ExecuteInfo(cmd)).encoded;
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
        Status hop = co_await txn.Execute(&ExecRunShardCallback, &run,
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
      Status released = co_await txn.Release();
      if (!released.ok()) {
        // Defensive: the no-op release hop cannot fail today. If it ever
        // can, the watches must still be consumed — EXEC ends them whatever
        // its outcome, and stale entries would falsely abort every later
        // EXEC on this connection.
        co_await DropWatches(ctx);
        co_return EncodedReply(EncodeError("ERR " + released.message()));
      }
    }
  } else {
    // Keyless-only transaction.
    if (!ctx.watched.empty() && !co_await CheckConnectionWatches(ctx)) {
      co_await DropWatches(ctx);
      co_return EncodedReply("*-1\r\n");
    }
    for (std::size_t i = 0; i < queued.size(); ++i) {
      if (queued[i].kind == CommandKind::kInfo) {
        replies[i] = (co_await ExecuteInfo(queued[i])).encoded;
      } else {
        replies[i] = run_keyless(queued[i]);
      }
    }
  }

  co_await DropWatches(ctx);
  std::string encoded = "*" + std::to_string(replies.size()) + "\r\n";
  for (const std::string& reply : replies) {
    encoded += reply;
  }
  CommandReply reply = EncodedReply(std::move(encoded));
  reply.selected_db = select_db;
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

void ConnectionOpened() noexcept {
  g_connected_clients.fetch_add(1, std::memory_order_relaxed);
}

void ConnectionClosed() noexcept {
  g_connected_clients.fetch_sub(1, std::memory_order_relaxed);
}

Task<CommandReply> DispatchCommand(ConnectionContext& ctx,
                                   CommandRequest request) {
  g_commands_processed.fetch_add(1, std::memory_order_relaxed);
  const CommandKind kind = request.kind;
  if (ctx.in_multi) {
    switch (kind) {
      case CommandKind::kMulti:
        co_return EncodedReply(
            EncodeError("ERR MULTI calls can not be nested"));
      case CommandKind::kWatch:
        co_return EncodedReply(
            EncodeError("ERR WATCH inside MULTI is not allowed"));
      case CommandKind::kDiscard:
        ctx.ResetMulti();
        co_await DropWatches(ctx);
        co_return EncodedReply(EncodeSimpleString("OK"));
      case CommandKind::kExec:
        co_return co_await ExecuteExec(ctx);
      default:
        break;
    }
    // Queue-time validation: errors reply immediately and doom the EXEC.
    if (request.spec == nullptr) {
      ctx.multi_dirty = true;
      co_return EncodedReply(
          EncodeError("ERR unknown command '" + request.args.front() + "'"));
    }
    const CommandSpec& spec = *request.spec;
    auto keys = DetermineKeys(spec, request.args.size());
    if (!keys.ok() ||
        (kind == CommandKind::kMSet && request.args.size() % 2 != 1)) {
      ctx.multi_dirty = true;
      co_return EncodedReply(EncodeError(
          "ERR wrong number of arguments for '" + std::string(spec.name) +
          "' command"));
    }
    if (g_replica_read_only && (spec.flags & kCmdWrite) != 0) {
      ctx.multi_dirty = true;
      co_return EncodedReply(EncodeError(
          "READONLY You can't write against a read only replica."));
    }
    if ((spec.flags & kCmdGlobal) != 0) {
      ctx.multi_dirty = true;
      co_return EncodedReply(
          EncodeError("ERR " + std::string(spec.name) +
                      " is not allowed in transactions"));
    }
    if (kind == CommandKind::kSelect) {
      // Validated by running it: SELECT inside MULTI moves the database for
      // the commands queued after it.
      CommandReply local = ExecuteLocalCommand(request);
      if (!local.selected_db.has_value()) {
        ctx.multi_dirty = true;
        co_return local;
      }
      ctx.multi_db = *local.selected_db;
    }
    request.db_id = ctx.multi_db;
    ctx.queued.push_back(std::move(request));
    co_return EncodedReply(EncodeSimpleString("QUEUED"));
  }

  switch (kind) {
    case CommandKind::kMulti:
      ctx.in_multi = true;
      ctx.multi_dirty = false;
      ctx.multi_db = ctx.selected_db;
      co_return EncodedReply(EncodeSimpleString("OK"));
    case CommandKind::kExec:
      co_return EncodedReply(EncodeError("ERR EXEC without MULTI"));
    case CommandKind::kDiscard:
      co_return EncodedReply(EncodeError("ERR DISCARD without MULTI"));
    case CommandKind::kWatch:
      if (request.spec == nullptr) {
        break;
      }
      co_return co_await ExecuteWatch(ctx, request);
    case CommandKind::kUnwatch:
      co_await DropWatches(ctx);
      co_return EncodedReply(EncodeSimpleString("OK"));
    default:
      break;
  }
  co_return co_await ExecuteCommand(request);
}

Task<celer::Status> ReleaseConnectionWatches(ConnectionContext& ctx) {
  co_return co_await DropWatches(ctx);
}

Task<CommandReply> ExecuteCommand(const CommandRequest& request) {
  const auto& args = request.args;
  const std::uint32_t cmd_flags =
      request.spec != nullptr ? request.spec->flags : 0u;
  if (g_replica_read_only && (cmd_flags & kCmdWrite) != 0) {
    co_return EncodedReply(EncodeError(
        "READONLY You can't write against a read only replica."));
  }
  if (request.kind == CommandKind::kFlushDb) {
    co_return co_await ExecuteFlushDb(request);
  }

  const bool uses_db = (cmd_flags & kCmdUsesDbGate) != 0;
  if (uses_db && !TryBeginDbOperation(request.db_id)) {
    co_return EncodedReply(
        EncodeError("TRYAGAIN FLUSHDB is in progress"));
  }
  std::optional<DbOperationGuard> db_guard;
  if (uses_db) {
    db_guard.emplace(request.db_id);
  }

  switch (request.kind) {
    case CommandKind::kInfo:
      co_return co_await ExecuteInfo(request);

    case CommandKind::kKeys:
      co_return co_await ExecuteKeys(request);

    case CommandKind::kDbSize:
      co_return co_await ExecuteDbSize(request);

    case CommandKind::kScan:
      co_return co_await ExecuteScan(request);

    case CommandKind::kDel:
    case CommandKind::kExists:
    case CommandKind::kMSet:
    case CommandKind::kMGet:
      co_return co_await ExecuteMultiKey(request);

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
