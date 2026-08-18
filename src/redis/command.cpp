#include "keylane/command.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/strings/str_cat.h"
#include "celer/io/storage.h"
#include "celer/runtime/cross_core.h"
#include "celer/runtime/cycle_clock.h"
#include "celer/runtime/worker.h"
#include "hash_command.h"
#include "keylane/command_table.h"
#include "keylane/expiration.h"
#include "keylane/glob.h"
#include "keylane/memory.h"
#include "keylane/metrics.h"
#include "keylane/random_sample.h"
#include "keylane/redis_parse.h"
#include "keylane/replication.h"
#include "keylane/replication_command.h"
#include "keylane/resp.h"
#include "keylane/session.h"
#include "keylane/storage/engine.h"
#include "keylane/storage/format.h"
#include "keylane/tx/transaction.h"
#include "keylane/tx/tx_shard.h"
#include "list_command.h"
#include "set_command.h"
#include "stream_command.h"
#include "string_command.h"
#include "zset_command.h"

namespace keylane {
using namespace celer;

namespace {

storage::StorageEngine* g_storage = nullptr;
ReplicationManager* g_replication = nullptr;
bool g_replica_read_only = false;
std::uint16_t g_server_port = 0;
unsigned g_server_threads = 0;
std::string g_server_bind_ip = "127.0.0.1";
std::chrono::steady_clock::time_point g_server_start;

constexpr std::size_t kEstimatedIndexBytesPerKey = 512;

bool CmpCaseInsensitive(std::string_view a, std::string_view b);
std::string_view AppendStorageError(ReplyBuilder& reply_builder,
                                    const absl::Status& status);

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
    case CommandKind::kSetEx:
    case CommandKind::kPSetEx:
    case CommandKind::kSetNx:
    case CommandKind::kSetRange:
    case CommandKind::kSetBit:
    case CommandKind::kBitField:
    case CommandKind::kBitOp:
    case CommandKind::kGetSet:
    case CommandKind::kAppend:
    case CommandKind::kIncrBy:
    case CommandKind::kIncrByFloat:
    case CommandKind::kDecr:
    case CommandKind::kDecrBy:
    case CommandKind::kLPush:
    case CommandKind::kLPushX:
    case CommandKind::kRPush:
    case CommandKind::kRPushX:
    case CommandKind::kLSet:
    case CommandKind::kLInsert:
    case CommandKind::kHSet:
    case CommandKind::kHMSet:
    case CommandKind::kHSetNx:
    case CommandKind::kHIncrBy:
    case CommandKind::kHIncrByFloat:
    case CommandKind::kSAdd:
    case CommandKind::kZAdd:
    case CommandKind::kZIncrBy:
    case CommandKind::kGeoAdd:
    case CommandKind::kXAdd:
    case CommandKind::kIncr:
    case CommandKind::kCopy:
      keys = 1;
      break;
    case CommandKind::kXGroup:
      if (request.args_.size() > 1 &&
          (CmpCaseInsensitive(request.args_[1], "create") ||
           CmpCaseInsensitive(request.args_[1], "createconsumer")))
        keys = 1;
      else
        return 0;
      break;
    case CommandKind::kLMove:
    case CommandKind::kRPopLPush:
    case CommandKind::kBLMove:
    case CommandKind::kBRPopLPush:
    case CommandKind::kSMove:
      keys = 2;
      break;
    case CommandKind::kSDiffStore:
    case CommandKind::kSInterStore:
    case CommandKind::kSUnionStore:
      keys = request.args_.size() > 1 ? request.args_.size() - 1 : 0;
      break;
    case CommandKind::kZDiffStore:
    case CommandKind::kZInterStore:
    case CommandKind::kZUnionStore:
    case CommandKind::kZRangeStore:
    case CommandKind::kGeoRadius:
    case CommandKind::kGeoRadiusByMember:
    case CommandKind::kGeoSearchStore:
      keys = 1;
      break;
    case CommandKind::kMSet:
    case CommandKind::kMSetNx:
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

absl::StatusOr<ReplicaOfRequest> ParseReplicaOfRequest(
    std::span<const std::string> args) {
  if (args.size() != 3) {
    return absl::InvalidArgumentError(
        "wrong number of arguments for 'replicaof' command");
  }
  if (CmpCaseInsensitive(args[1], "NO") &&
      CmpCaseInsensitive(args[2], "ONE")) {
    return ReplicaOfRequest{};
  }
  std::uint64_t port = 0;
  const auto* begin = args[2].data();
  const auto* end = begin + args[2].size();
  const auto parsed = std::from_chars(begin, end, port);
  if (parsed.ec != std::errc{} || parsed.ptr != end || port == 0 ||
      port > 65535 || args[1].empty()) {
    return absl::InvalidArgumentError("invalid upstream host or port");
  }
  return ReplicaOfRequest{std::string(args[1]),
                          static_cast<std::uint16_t>(port)};
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

Task<CommandReply> ExecuteReplicaOf(const CommandRequest& request,
                                    ReplyBuilder& reply_builder) {
  auto parsed = ParseReplicaOfRequest(request.args_);
  if (!parsed.ok()) {
    co_return BuiltReply(reply_builder.AppendError(
        absl::StrCat("ERR ", parsed.status().message())));
  }
  if (g_replication == nullptr) {
    co_return BuiltReply(
        reply_builder.AppendError("ERR replication backend is unavailable"));
  }
  std::optional<ReplicaOfConfig> upstream;
  if (parsed->host_.has_value()) {
    upstream = ReplicaOfConfig{*parsed->host_, parsed->port_};
  }
  absl::Status configured =
      co_await g_replication->SetUpstream(std::move(upstream));
  if (!configured.ok()) {
    co_return BuiltReply(
        reply_builder.AppendError(absl::StrCat("ERR ", configured.message())));
  }
  co_return BuiltReply(reply_builder.AppendSimpleString("OK"));
}

std::string ClusterNodeAddress(std::string_view host, std::uint16_t port) {
  if (host.find(':') != std::string_view::npos &&
      !(host.starts_with('[') && host.ends_with(']'))) {
    return absl::StrCat("[", host, "]:", port, "@0");
  }
  return absl::StrCat(host, ":", port, "@0");
}

std::string_view ClusterSlotsHost(std::string_view host) {
  // A wildcard bind address is not a routable endpoint. Redis clients treat an
  // empty primary host as "use the address of the startup node".
  return host == "0.0.0.0" || host == "::" ? std::string_view{} : host;
}

void AppendClusterSlotsNode(ReplyBuilder& reply_builder, std::string_view host,
                            std::uint16_t port, std::string_view node_id) {
  reply_builder.AppendArrayHeader(3);
  reply_builder.AppendBulkString(host);
  reply_builder.AppendInteger(port);
  reply_builder.AppendBulkString(node_id);
}

CommandReply BuildClusterSlotsReply(const ReplicationStatus& replication,
                                    ReplyBuilder& reply_builder) {
  constexpr long long kFirstClusterSlot = 0;
  constexpr long long kLastClusterSlot = 16383;

  if (replication.role_ == ReplicationRole::kMaster) {
    std::size_t online_replicas = 0;
    for (const DownstreamReplicaStatus& replica :
         replication.downstream_replicas_) {
      online_replicas += replica.online_ ? 1 : 0;
    }
    reply_builder.AppendArrayHeader(1);
    reply_builder.AppendArrayHeader(3 + online_replicas);
    reply_builder.AppendInteger(kFirstClusterSlot);
    reply_builder.AppendInteger(kLastClusterSlot);
    AppendClusterSlotsNode(reply_builder, ClusterSlotsHost(g_server_bind_ip),
                           g_server_port, replication.local_node_id_);
    for (const DownstreamReplicaStatus& replica :
         replication.downstream_replicas_) {
      if (!replica.online_) continue;
      AppendClusterSlotsNode(reply_builder, replica.host_, replica.port_,
                             replica.node_id_);
    }
    return BuiltReply(reply_builder.View());
  }

  if (!replication.upstream_.has_value()) {
    return BuiltReply(reply_builder.AppendArrayHeader(0));
  }

  const bool advertise_local_replica =
      replication.role_ == ReplicationRole::kOnline &&
      g_server_bind_ip != "0.0.0.0" && g_server_bind_ip != "::";
  reply_builder.AppendArrayHeader(1);
  reply_builder.AppendArrayHeader(advertise_local_replica ? 4 : 3);
  reply_builder.AppendInteger(kFirstClusterSlot);
  reply_builder.AppendInteger(kLastClusterSlot);
  AppendClusterSlotsNode(
      reply_builder, replication.upstream_->host_,
      replication.upstream_->port_,
      replication.upstream_node_id_.value_or(std::string{}));
  if (advertise_local_replica) {
    AppendClusterSlotsNode(reply_builder, g_server_bind_ip, g_server_port,
                           replication.local_node_id_);
  }
  return BuiltReply(reply_builder.View());
}

std::optional<std::string> ReplicaMovedError(
    const ConnectionContext& ctx, const CommandRequest& request) {
  if (g_replication == nullptr || !g_replication->is_replica() ||
      request.spec_ == nullptr ||
      (request.spec_->flags_ & kCmdNoKeys) != 0) {
    return std::nullopt;
  }
  const bool write = (request.spec_->flags_ & kCmdWrite) != 0;
  if (!write && ctx.cluster_readonly_) {
    return std::nullopt;
  }
  absl::StatusOr<KeyIndexView> keys =
      DetermineKeys(*request.spec_, request.args_);
  if (!keys.ok() || keys->empty()) {
    return std::nullopt;
  }
  const ReplicationStatus replication = g_replication->status();
  if (!replication.upstream_.has_value()) {
    return std::nullopt;
  }
  const std::uint16_t slot =
      storage::RedisSlot(request.args_[keys->first_]);
  return absl::StrCat("MOVED ", slot, " ", replication.upstream_->host_, ":",
                      replication.upstream_->port_);
}

Task<CommandReply> ExecuteCluster(const CommandRequest& request,
                                  ReplyBuilder& reply_builder) {
  if (request.args_.size() != 2) {
    co_return BuiltReply(reply_builder.AppendError(
        "ERR wrong number of arguments for 'cluster' command"));
  }
  const ReplicationStatus replication =
      g_replication != nullptr ? g_replication->status() : ReplicationStatus{};
  if (CmpCaseInsensitive(request.args_[1], "SLOTS")) {
    co_return BuildClusterSlotsReply(replication, reply_builder);
  }
  if (!CmpCaseInsensitive(request.args_[1], "NODES")) {
    co_return BuiltReply(reply_builder.AppendError(
        "ERR only CLUSTER NODES and CLUSTER SLOTS are supported"));
  }
  std::string nodes;
  const std::string local_address =
      ClusterNodeAddress(g_server_bind_ip, g_server_port);
  if (replication.role_ == ReplicationRole::kMaster) {
    nodes += replication.local_node_id_ + " " + local_address +
             " myself,master - 0 0 1 connected 0-16383\n";
    for (const DownstreamReplicaStatus& replica :
         replication.downstream_replicas_) {
      nodes += replica.node_id_ + " " +
               ClusterNodeAddress(replica.host_, replica.port_) + " slave " +
               replication.local_node_id_ + " 0 0 1 " +
               (replica.online_ ? "connected\n" : "disconnected\n");
    }
  } else {
    const std::string master_id =
        replication.upstream_node_id_.value_or("-");
    if (replication.upstream_.has_value() &&
        replication.upstream_node_id_.has_value()) {
      nodes += *replication.upstream_node_id_ + " " +
               ClusterNodeAddress(replication.upstream_->host_,
                                  replication.upstream_->port_) +
               " master - 0 0 1 " +
               (replication.role_ == ReplicationRole::kOnline
                    ? "connected 0-16383\n"
                    : "disconnected 0-16383\n");
    }
    nodes += replication.local_node_id_ + " " + local_address +
             " myself,slave " + master_id + " 0 0 1 " +
             (replication.role_ == ReplicationRole::kOnline ? "connected\n"
                                                             : "disconnected\n");
  }
  co_return BuiltReply(reply_builder.AppendBulkString(nodes));
}

void AppendCommandFlags(ReplyBuilder& reply_builder,
                        const CommandSpec& command) {
  std::uint64_t count = 0;
  count += (command.flags_ & kCmdWrite) != 0 ? 1 : 0;
  count += (command.flags_ & kCmdReadOnly) != 0 ? 1 : 0;
  count += (command.flags_ & kCmdMovableKeys) != 0 ? 1 : 0;
  count += (command.flags_ & kCmdMayBlock) != 0 ? 1 : 0;
  reply_builder.AppendArrayHeader(count);
  if ((command.flags_ & kCmdWrite) != 0) {
    reply_builder.AppendBulkString("write");
  }
  if ((command.flags_ & kCmdReadOnly) != 0) {
    reply_builder.AppendBulkString("readonly");
  }
  if ((command.flags_ & kCmdMovableKeys) != 0) {
    reply_builder.AppendBulkString("movablekeys");
  }
  if ((command.flags_ & kCmdMayBlock) != 0) {
    reply_builder.AppendBulkString("blocking");
  }
}

CommandReply BuildCommandMetadataReply(ReplyBuilder& reply_builder) {
  const std::span<const CommandSpec> commands = CommandSpecs();
  reply_builder.AppendArrayHeader(commands.size());
  for (const CommandSpec& command : commands) {
    reply_builder.AppendArrayHeader(7);
    reply_builder.AppendBulkString(command.name_);
    const bool fixed_arity = command.max_args_ == command.min_args_;
    const long long arity = fixed_arity
                                ? static_cast<long long>(command.min_args_)
                                : -static_cast<long long>(command.min_args_);
    reply_builder.AppendInteger(arity);
    AppendCommandFlags(reply_builder, command);
    reply_builder.AppendInteger(command.first_key_);
    reply_builder.AppendInteger(command.last_key_);
    reply_builder.AppendInteger(command.key_step_);
    reply_builder.AppendArrayHeader(0);  // ACL categories
  }
  return BuiltReply(reply_builder.View());
}

Task<CommandReply> ExecuteCommandIntrospection(const CommandRequest& request,
                                               ReplyBuilder& reply_builder) {
  if (request.args_.size() == 1) {
    co_return BuildCommandMetadataReply(reply_builder);
  }
  if (CmpCaseInsensitive(request.args_[1], "COUNT") &&
      request.args_.size() == 2) {
    co_return BuiltReply(reply_builder.AppendInteger(CommandSpecs().size()));
  }
  if (CmpCaseInsensitive(request.args_[1], "GETKEYS") &&
      request.args_.size() >= 3) {
    const std::span<const std::string> command_args(request.args_.data() + 2,
                                                    request.args_.size() - 2);
    const CommandSpec* command = FindCommand(command_args.front());
    if (command == nullptr) {
      co_return BuiltReply(reply_builder.AppendError(
          "ERR Invalid arguments specified for command"));
    }
    absl::StatusOr<KeyIndexView> keys = DetermineKeys(*command, command_args);
    if (!keys.ok()) {
      co_return BuiltReply(reply_builder.AppendError(
          "ERR Invalid arguments specified for command"));
    }
    reply_builder.AppendArrayHeader(keys->count());
    for (std::uint16_t index = keys->first_; !keys->empty() && index <= keys->last_;
         index = static_cast<std::uint16_t>(index + keys->step_)) {
      reply_builder.AppendBulkString(command_args[index]);
      if (keys->last_ - index < keys->step_) break;
    }
    co_return BuiltReply(reply_builder.View());
  }
  co_return BuiltReply(reply_builder.AppendError(
      "ERR unknown subcommand or wrong number of arguments for 'command'"));
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

Task<CommandReply> ExecuteRandomKey(const CommandRequest& request,
                                    ReplyBuilder& reply_builder) {
  std::vector<std::uint64_t> populations(g_storage->worker_count());
  std::uint64_t total = 0;
  for (unsigned worker = 0; worker < g_storage->worker_count(); ++worker) {
    const std::size_t local = co_await SubmitTo(
        worker, [db = request.db_id_] { return g_storage->LocalSize(db); });
    populations[worker] = local;
    if (local > std::numeric_limits<std::uint64_t>::max() - total) {
      co_return BuiltReply(reply_builder.AppendError("ERR db size overflow"));
    }
    total += local;
  }

  // A worker's live count can temporarily include expired records awaiting
  // their tombstone. Remove an empty result from this draw and retry the
  // remaining workers; RandomKeyLocal itself filters those stale entries.
  constexpr unsigned kMaximumTransientRetries = 100;
  unsigned transient_retries = 0;
  while (total != 0) {
    std::uint64_t rank = RandomRank(total, RandomSampleGenerator());
    unsigned selected = 0;
    for (; selected < populations.size(); ++selected) {
      if (rank < populations[selected]) break;
      rank -= populations[selected];
    }
    if (selected == populations.size()) break;
    auto key = co_await SubmitTaskTo(selected, [db = request.db_id_] {
      return g_storage->RandomKeyLocal(db);
    });
    if (!key.ok()) {
      if (key.status().code() == absl::StatusCode::kAborted &&
          transient_retries++ < kMaximumTransientRetries) {
        continue;
      }
      co_return BuiltReply(AppendStorageError(reply_builder, key.status()));
    }
    if (key->has_value()) {
      co_return BuiltReply(reply_builder.AppendBulkString(**key));
    }
    total -= populations[selected];
    populations[selected] = 0;
  }
  co_return BuiltReply(reply_builder.AppendNullBulkString());
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

constexpr std::string_view kSnapshotReadConcurrencyConfig =
    "replication-snapshot-read-concurrency";
constexpr std::string_view kDefragPausedConfig = "defrag-paused";
constexpr std::string_view kDefragMaxActiveConfig =
    "defrag-max-active-per-device";
constexpr std::string_view kDefragSleepConfig = "defrag-sleep-ms";
constexpr std::string_view kDefragRecordSleepConfig =
    "defrag-record-sleep-us";
constexpr std::string_view kTombRaiderModeConfig = "tomb-raider-mode";
constexpr std::string_view kTombRaiderIntervalConfig =
    "tomb-raider-interval-ms";
constexpr std::string_view kTombRaiderSleepConfig = "tomb-raider-sleep-ms";
constexpr std::string_view kTombRaiderDailyTimeConfig =
    "tomb-raider-daily-time";

enum class RuntimeConfigKey : std::uint8_t {
  kSnapshotReadConcurrency,
  kDefragPaused,
  kDefragMaxActive,
  kDefragSleep,
  kDefragRecordSleep,
  kTombRaiderMode,
  kTombRaiderInterval,
  kTombRaiderSleep,
  kTombRaiderDailyTime,
};

struct RuntimeConfigDescriptor {
  std::string_view name_;
  RuntimeConfigKey key_;
};

// CONFIG command metadata only. Execution paths never consult this table;
// replication, defrag, and tomb-raider load their owning atomics directly.
constexpr std::array kRuntimeConfigs{
    RuntimeConfigDescriptor{kSnapshotReadConcurrencyConfig,
                            RuntimeConfigKey::kSnapshotReadConcurrency},
    RuntimeConfigDescriptor{kDefragPausedConfig,
                            RuntimeConfigKey::kDefragPaused},
    RuntimeConfigDescriptor{kDefragMaxActiveConfig,
                            RuntimeConfigKey::kDefragMaxActive},
    RuntimeConfigDescriptor{kDefragSleepConfig,
                            RuntimeConfigKey::kDefragSleep},
    RuntimeConfigDescriptor{kDefragRecordSleepConfig,
                            RuntimeConfigKey::kDefragRecordSleep},
    RuntimeConfigDescriptor{kTombRaiderModeConfig,
                            RuntimeConfigKey::kTombRaiderMode},
    RuntimeConfigDescriptor{kTombRaiderIntervalConfig,
                            RuntimeConfigKey::kTombRaiderInterval},
    RuntimeConfigDescriptor{kTombRaiderSleepConfig,
                            RuntimeConfigKey::kTombRaiderSleep},
    RuntimeConfigDescriptor{kTombRaiderDailyTimeConfig,
                            RuntimeConfigKey::kTombRaiderDailyTime},
};

absl::StatusOr<std::uint32_t> ParseDailySecond(std::string_view text);
std::string FormatDailySecond(std::uint32_t daily_second);
std::string_view TombRaiderModeName(storage::TombRaiderMode mode);

std::optional<bool> ParseConfigYesNo(std::string_view value) {
  if (CmpCaseInsensitive(value, "yes")) return true;
  if (CmpCaseInsensitive(value, "no")) return false;
  return std::nullopt;
}

std::string NormalizeConfigPattern(std::string_view pattern) {
  std::string lower(pattern);
  for (char& value : lower) {
    if (value >= 'A' && value <= 'Z') value += 'a' - 'A';
  }
  return lower;
}

Task<CommandReply> ExecuteConfig(const CommandRequest& request,
                                 ReplyBuilder& reply_builder) {
  const auto& args = request.args_;
  if (CmpCaseInsensitive(args[1], "GET") && args.size() == 3) {
    const std::string pattern = NormalizeConfigPattern(args[2]);
    std::vector<const RuntimeConfigDescriptor*> matches;
    matches.reserve(kRuntimeConfigs.size());
    for (const RuntimeConfigDescriptor& config : kRuntimeConfigs) {
      if (config.key_ == RuntimeConfigKey::kSnapshotReadConcurrency &&
          g_replication == nullptr) {
        continue;
      }
      if (RedisGlobMatch(pattern, config.name_)) {
        matches.push_back(&config);
      }
    }
    std::optional<storage::DefragTotals> defrag;
    std::optional<storage::TombRaiderTotals> tomb_raider;
    auto value_of = [&](RuntimeConfigKey key) -> std::string {
      switch (key) {
        case RuntimeConfigKey::kSnapshotReadConcurrency:
          return std::to_string(g_replication->snapshot_read_concurrency());
        case RuntimeConfigKey::kDefragPaused:
        case RuntimeConfigKey::kDefragMaxActive:
        case RuntimeConfigKey::kDefragSleep:
        case RuntimeConfigKey::kDefragRecordSleep:
          if (!defrag.has_value()) defrag.emplace(g_storage->DefragStats());
          if (key == RuntimeConfigKey::kDefragPaused)
            return defrag->paused_ ? "yes" : "no";
          if (key == RuntimeConfigKey::kDefragMaxActive)
            return std::to_string(defrag->max_active_per_device_);
          if (key == RuntimeConfigKey::kDefragSleep)
            return std::to_string(defrag->block_sleep_ms_);
          return std::to_string(defrag->record_sleep_us_);
        case RuntimeConfigKey::kTombRaiderMode:
        case RuntimeConfigKey::kTombRaiderInterval:
        case RuntimeConfigKey::kTombRaiderSleep:
        case RuntimeConfigKey::kTombRaiderDailyTime:
          if (!tomb_raider.has_value())
            tomb_raider.emplace(g_storage->TombRaiderStats());
          if (key == RuntimeConfigKey::kTombRaiderMode)
            return std::string(TombRaiderModeName(tomb_raider->mode_));
          if (key == RuntimeConfigKey::kTombRaiderInterval)
            return std::to_string(tomb_raider->interval_ms_);
          if (key == RuntimeConfigKey::kTombRaiderSleep)
            return std::to_string(tomb_raider->block_sleep_ms_);
          return FormatDailySecond(tomb_raider->daily_second_);
      }
      return {};
    };
    reply_builder.AppendArrayHeader(matches.size() * 2);
    for (const RuntimeConfigDescriptor* config : matches) {
      reply_builder.AppendBulkString(config->name_);
      reply_builder.AppendBulkString(value_of(config->key_));
    }
    co_return BuiltReply(reply_builder.View());
  }
  if (CmpCaseInsensitive(args[1], "SET") && args.size() == 4) {
    const RuntimeConfigDescriptor* config = nullptr;
    for (const RuntimeConfigDescriptor& candidate : kRuntimeConfigs) {
      if (CmpCaseInsensitive(args[2], candidate.name_)) {
        config = &candidate;
        break;
      }
    }
    if (config == nullptr) {
      co_return BuiltReply(reply_builder.AppendError(absl::StrCat(
          "ERR Unsupported CONFIG parameter: ", args[2])));
    }
    absl::Status configured;
    std::uint64_t value = 0;
    if (config->key_ == RuntimeConfigKey::kSnapshotReadConcurrency) {
      if (g_replication == nullptr) {
        configured = absl::FailedPreconditionError(
            "replication backend is unavailable");
      } else if (!ParseUint64(args[3], &value) ||
                 value > std::numeric_limits<unsigned>::max()) {
        configured = absl::InvalidArgumentError(
            "value is not an integer or out of range");
      } else {
        configured = g_replication->SetSnapshotReadConcurrency(
            static_cast<unsigned>(value));
      }
    } else if (config->key_ == RuntimeConfigKey::kDefragPaused) {
      const std::optional<bool> paused = ParseConfigYesNo(args[3]);
      if (!paused.has_value()) {
        configured = absl::InvalidArgumentError("value must be 'yes' or 'no'");
      } else {
        configured = co_await g_storage->ConfigureDefrag(storage::DefragConfigUpdate{
            .action_ = *paused ? storage::DefragConfigAction::kPause
                               : storage::DefragConfigAction::kResume});
      }
    } else if (config->key_ == RuntimeConfigKey::kDefragMaxActive) {
      if (!ParseUint64(args[3], &value)) {
        configured = absl::InvalidArgumentError(
            "value is not an integer or out of range");
      } else {
        configured = co_await g_storage->ConfigureDefrag(
            storage::DefragConfigUpdate{
                .action_ = storage::DefragConfigAction::kMaxActivePerDevice,
                .value_ = value});
      }
    } else if (config->key_ == RuntimeConfigKey::kDefragSleep ||
               config->key_ == RuntimeConfigKey::kDefragRecordSleep) {
      if (!ParseUint64(args[3], &value) ||
          value > std::numeric_limits<std::uint32_t>::max()) {
        configured = absl::InvalidArgumentError(
            "value is not an integer or out of range");
      } else {
        configured = co_await g_storage->ConfigureDefrag(
            storage::DefragConfigUpdate{
                .action_ = config->key_ == RuntimeConfigKey::kDefragSleep
                               ? storage::DefragConfigAction::kBlockSleep
                               : storage::DefragConfigAction::kRecordSleep,
                .value_ = value});
      }
    } else if (config->key_ == RuntimeConfigKey::kTombRaiderInterval) {
      if (!ParseUint64(args[3], &value)) {
        configured = absl::InvalidArgumentError(
            "value is not an integer or out of range");
      } else {
        configured = co_await g_storage->ConfigureTombRaider(
            storage::TombRaiderConfigUpdate{
                .action_ = value == 0
                               ? storage::TombRaiderConfigAction::kOff
                               : storage::TombRaiderConfigAction::kInterval,
                .value_ = value});
      }
    } else if (config->key_ == RuntimeConfigKey::kTombRaiderSleep) {
      if (!ParseUint64(args[3], &value) ||
          value > std::numeric_limits<std::uint32_t>::max()) {
        configured = absl::InvalidArgumentError(
            "value is not an integer or out of range");
      } else {
        configured = co_await g_storage->ConfigureTombRaider(
            storage::TombRaiderConfigUpdate{
                .action_ = storage::TombRaiderConfigAction::kBlockSleep,
                .value_ = value});
      }
    } else if (config->key_ == RuntimeConfigKey::kTombRaiderDailyTime) {
      auto daily_second = ParseDailySecond(args[3]);
      if (!daily_second.ok()) {
        configured = daily_second.status();
      } else {
        configured = co_await g_storage->ConfigureTombRaider(
            storage::TombRaiderConfigUpdate{
                .action_ = storage::TombRaiderConfigAction::kDaily,
                .value_ = *daily_second});
      }
    } else if (config->key_ == RuntimeConfigKey::kTombRaiderMode) {
      storage::TombRaiderConfigUpdate update;
      const storage::TombRaiderTotals current = g_storage->TombRaiderStats();
      if (CmpCaseInsensitive(args[3], "off")) {
        update.action_ = storage::TombRaiderConfigAction::kOff;
      } else if (CmpCaseInsensitive(args[3], "on")) {
        update.action_ = storage::TombRaiderConfigAction::kOn;
      } else if (CmpCaseInsensitive(args[3], "interval")) {
        update.action_ = storage::TombRaiderConfigAction::kInterval;
        update.value_ = current.interval_ms_;
      } else if (CmpCaseInsensitive(args[3], "daily")) {
        update.action_ = storage::TombRaiderConfigAction::kDaily;
        update.value_ = current.daily_second_;
      } else {
        configured = absl::InvalidArgumentError(
            "value must be 'off', 'on', 'interval', or 'daily'");
      }
      if (configured.ok()) {
        configured = co_await g_storage->ConfigureTombRaider(update);
      }
    }
    co_return configured.ok()
                  ? BuiltReply(reply_builder.AppendSimpleString("OK"))
                  : BuiltReply(reply_builder.AppendError(
                        absl::StrCat("ERR ", configured.message())));
  }
  co_return BuiltReply(reply_builder.AppendError("ERR syntax error"));
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
        "paused=", status.paused_ ? 1 : 0,
        " max_active_per_device=", status.max_active_per_device_,
        " block_sleep_ms=", status.block_sleep_ms_,
        " record_sleep_us=", status.record_sleep_us_,
        " active_total=", status.active_, " pending_total=", status.pending_)));
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
  ~DbOperationGuard() { Release(); }

  void Release() noexcept {
    if (!active_) return;
    active_ = false;
    EndDbOperation(db_id_);
  }

 private:
  std::uint8_t db_id_;
  bool active_ = true;
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
      if (!request.replication_origin_ && g_storage->ReplicationLogActive()) {
        detached = co_await g_storage->PublishFlushDbReplication(
            db_id, g_storage->DbEpoch(db_id));
        if (!detached.ok()) break;
      }
    }
  }

  // Blocking commands do not hold the database gate while suspended. Wake
  // them after the detached database becomes visible so predicates depending
  // on metadata (notably XREADGROUP's group existence) are re-evaluated.
  for (const std::uint8_t db_id : dbs) {
    (void)co_await NotifyBlockingDb(db_id);
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

std::string_view ValueTypeName(storage::ValueType type) {
  switch (type) {
    case storage::ValueType::kString:
      return "string";
    case storage::ValueType::kList:
      return "list";
    case storage::ValueType::kSet:
      return "set";
    case storage::ValueType::kSortedSet:
      return "zset";
    case storage::ValueType::kHash:
      return "hash";
    case storage::ValueType::kStream:
      return "stream";
    case storage::ValueType::kNone:
      return "none";
  }
  return "none";
}

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

    if (batch.keys_.size() != batch.value_types_.size()) {
      co_return BuiltReply(
          reply_builder.AppendError("ERR scan key/type metadata mismatch"));
    }
    const std::size_t examined = batch.keys_.size();
    for (std::size_t i = 0; i < batch.keys_.size(); ++i) {
      std::string& key = batch.keys_[i];
      const bool type_matches =
          !options.type_.has_value() ||
          CmpCaseInsensitive(*options.type_,
                             ValueTypeName(batch.value_types_[i]));
      if (type_matches &&
          (!options.pattern_.has_value() || *options.pattern_ == "*" ||
           RedisGlobMatch(*options.pattern_, key))) {
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
            if (*pattern == "*" || RedisGlobMatch(*pattern, key)) {
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
  state->now_ms_ = RedisUnixTimeMillis();
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

bool ParseInt64(std::string_view text, std::int64_t* value) {
  if (value == nullptr || text.empty()) {
    return false;
  }
  const char* begin = text.data();
  const char* end = begin + text.size();
  const auto [parsed_end, error] = std::from_chars(begin, end, *value);
  return error == std::errc{} && parsed_end == end;
}

absl::Status ValidateBlockingTimeout(std::string_view text) {
  double timeout = 0;
  if (!ParseRedisDouble(text, &timeout)) {
    return absl::InvalidArgumentError("timeout is not a float or out of range");
  }
  if (timeout < 0) {
    return absl::InvalidArgumentError("timeout is negative");
  }
  if (static_cast<long double>(timeout) * 1000.0L >
      static_cast<long double>(std::numeric_limits<std::int64_t>::max())) {
    return absl::InvalidArgumentError("timeout is out of range");
  }
  return absl::OkStatus();
}

std::string_view AppendStorageError(ReplyBuilder& reply_builder,
                                    const absl::Status& status);

struct NegativeRandomStreamOptions {
  bool hash_ = false;
  bool zset_ = false;
  bool with_values_ = false;
  std::uint64_t count_ = 0;
};

std::optional<NegativeRandomStreamOptions> ParseNegativeRandomStream(
    const CommandRequest& request) {
  const auto& args = request.args_;
  if (request.kind_ == CommandKind::kHRandField) {
    if (args.size() < 3 || args.size() > 4) return std::nullopt;
  } else if (request.kind_ == CommandKind::kSRandMember) {
    if (args.size() != 3) return std::nullopt;
  } else if (request.kind_ == CommandKind::kZRandMember) {
    if (args.size() < 3 || args.size() > 4) return std::nullopt;
  } else {
    return std::nullopt;
  }

  std::int64_t count = 0;
  if (!ParseInt64(args[2], &count) || count >= 0 ||
      count == std::numeric_limits<std::int64_t>::min()) {
    return std::nullopt;
  }
  const std::uint64_t magnitude = static_cast<std::uint64_t>(-count);
  if (magnitude <= kRandomSampleBatchLimit) return std::nullopt;
  NegativeRandomStreamOptions options{
      .hash_ = request.kind_ == CommandKind::kHRandField,
      .zset_ = request.kind_ == CommandKind::kZRandMember,
      .with_values_ = false,
      .count_ = magnitude,
  };
  if (args.size() == 4) {
    const std::string_view option = options.zset_ ? "WITHSCORES" : "WITHVALUES";
    if (!CmpCaseInsensitive(args[3], option) ||
        magnitude > static_cast<std::uint64_t>(
                        std::numeric_limits<std::int64_t>::max()) /
                        2) {
      return std::nullopt;
    }
    options.with_values_ = true;
  }
  return options;
}

// Keep one immutable command-time view and generate the with-replacement
// result in bounded batches. This preserves the command's atomic read view
// without retaining a DB gate or key lock across socket backpressure.
struct NegativeRandomStreamState {
  NegativeRandomStreamState(std::uint8_t db_id,
                            NegativeRandomStreamOptions stream_options,
                            std::string key, unsigned owner)
      : db_(db_id),
        options_(stream_options),
        remaining_(stream_options.count_),
        key_(std::move(key)),
        owner_(owner),
        digest_(storage::ComputeDigest(key_)) {}

  std::uint8_t db_ = 0;
  NegativeRandomStreamOptions options_;
  std::uint64_t remaining_ = 0;
  std::string key_;
  unsigned owner_ = 0;
  storage::Digest digest_{};
  std::uint64_t now_ms_ = 0;
  std::uint64_t population_ = 0;
  std::vector<std::string> values_;
};

Task<absl::StatusOr<storage::HashResult>> BeginNegativeRandomStream(
    const std::shared_ptr<NegativeRandomStreamState>& state) {
  auto begin = [state]() -> Task<absl::StatusOr<storage::HashResult>> {
    tx::TxShard::Guard guard = co_await tx::CurrentTxShard().AcquireKey(
        state->db_, tx::FingerprintOf(state->digest_), tx::LockMode::kShared);
    storage::HashOperation operation;
    operation.kind_ = state->options_.hash_ && state->options_.with_values_
                          ? storage::HashOperationKind::kGetAll
                          : storage::HashOperationKind::kKeys;
    operation.now_ms_ = state->now_ms_;
    absl::StatusOr<storage::HashResult> snapshot;
    if (state->options_.zset_) {
      snapshot = co_await ZSetRandomSnapshotLocked(
          state->db_, state->key_, state->digest_, state->options_.with_values_,
          nullptr, state->now_ms_);
    } else if (state->options_.hash_) {
      snapshot = co_await g_storage->ExecuteHashLocked(
          state->db_, state->key_, state->digest_, operation, nullptr);
    } else {
      snapshot = co_await g_storage->ExecuteSetLocked(
          state->db_, state->key_, state->digest_, operation, nullptr);
    }
    if (!snapshot.ok() || snapshot->length_ == 0) co_return snapshot;

    const std::size_t tuple_width = state->options_.with_values_ ? 2 : 1;
    if (snapshot->length_ >
            std::numeric_limits<std::size_t>::max() / tuple_width ||
        snapshot->values_.size() != snapshot->length_ * tuple_width) {
      co_return absl::InternalError(
          "random snapshot disagrees with collection length");
    }
    state->population_ = snapshot->length_;
    state->values_.reserve(snapshot->values_.size());
    for (auto& value : snapshot->values_) {
      if (!value.has_value())
        co_return absl::InternalError(
            "random snapshot contains a missing value");
      state->values_.push_back(std::move(*value));
    }
    co_return snapshot;
  };
  if (state->owner_ != ThisWorker().id_) {
    co_return co_await celer::SubmitTaskTo(state->owner_, std::move(begin));
  }
  co_return co_await begin();
}

Task<absl::StatusOr<std::string>> NextNegativeRandomChunk(
    std::shared_ptr<NegativeRandomStreamState> state) {
  if (state->remaining_ == 0) co_return std::string();
  const std::uint64_t count =
      std::min(state->remaining_, kRandomSampleBatchLimit);
  const std::size_t tuple_width = state->options_.with_values_ ? 2 : 1;
  std::string payload;
  for (std::uint64_t i = 0; i < count; ++i) {
    const std::uint64_t rank =
        RandomRank(state->population_, RandomSampleGenerator());
    const std::size_t offset = static_cast<std::size_t>(rank) * tuple_width;
    for (std::size_t field = 0; field < tuple_width; ++field) {
      payload += EncodeBulkString(state->values_[offset + field]);
    }
  }
  state->remaining_ -= count;
  co_return payload;
}

Task<CommandReply> ExecuteNegativeRandomStream(
    const CommandRequest& request, NegativeRandomStreamOptions options,
    ReplyBuilder& reply_builder) {
  const std::uint8_t db = request.db_id_;
  const unsigned owner = ShardForKey(request.args_[1]);
  auto state = std::make_shared<NegativeRandomStreamState>(
      db, std::move(options), request.args_[1], owner);
  if (!TryBeginDbOperation(db)) {
    co_return BuiltReply(
        reply_builder.AppendError("TRYAGAIN database flush is in progress"));
  }
  DbOperationGuard initial_db_guard(db);
  state->now_ms_ = RedisUnixTimeMillis();
  absl::StatusOr<storage::HashResult> length =
      co_await BeginNegativeRandomStream(state);
  if (!length.ok()) {
    co_return BuiltReply(AppendStorageError(reply_builder, length.status()));
  }
  if (length->length_ == 0) {
    co_return BuiltReply(reply_builder.AppendArrayHeader(0));
  }
  // The initial lookup is complete. Socket backpressure may retain the reply
  // for an arbitrary duration, so retain neither the DB gate nor the key lock.
  // Later batches sample only the immutable in-memory view.
  initial_db_guard.Release();

  std::uint64_t reply_elements = state->remaining_;
  if (state->options_.with_values_) reply_elements *= 2;
  CommandReply reply =
      BuiltReply(reply_builder.AppendArrayHeader(reply_elements));
  reply.chunks_ = [state]() { return NextNegativeRandomChunk(state); };
  co_return reply;
}

// EXEC must preserve the view observed at the command's position in the
// transaction, but it must not materialize a negative-count reply whose size
// is controlled by the client. Capture the compact collection once while the
// transaction owns its locks, then sample that immutable view in bounded
// chunks after EXEC has released every DB/key guard.
struct TransactionalRandomStreamState {
  NegativeRandomStreamOptions options_;
  std::uint64_t remaining_ = 0;
  std::uint64_t population_ = 0;
  std::vector<std::string> values_;
};

struct OwnedStreamReply {
  std::string encoded_;
  ReplyChunkSource chunks_;
};

Task<absl::StatusOr<std::string>> NextTransactionalRandomChunk(
    std::shared_ptr<TransactionalRandomStreamState> state) {
  if (state->remaining_ == 0) co_return std::string();
  const std::uint64_t count =
      std::min(state->remaining_, kRandomSampleBatchLimit);
  const std::size_t tuple_width = state->options_.with_values_ ? 2 : 1;
  std::string payload;
  for (std::uint64_t i = 0; i < count; ++i) {
    const std::uint64_t rank =
        RandomRank(state->population_, RandomSampleGenerator());
    const std::size_t offset = static_cast<std::size_t>(rank) * tuple_width;
    for (std::size_t field = 0; field < tuple_width; ++field) {
      payload += EncodeBulkString(state->values_[offset + field]);
    }
  }
  state->remaining_ -= count;
  co_return payload;
}

Task<absl::StatusOr<OwnedStreamReply>>
PrepareTransactionalNegativeRandomStreamLocked(
    const CommandRequest& request, const storage::Digest& digest,
    storage::TxShardWrites* tx, NegativeRandomStreamOptions options) {
  storage::HashOperation operation;
  operation.kind_ = options.hash_ && options.with_values_
                        ? storage::HashOperationKind::kGetAll
                        : storage::HashOperationKind::kKeys;
  operation.now_ms_ = RedisUnixTimeMillis();

  absl::StatusOr<storage::HashResult> snapshot;
  if (options.zset_) {
    snapshot = co_await ZSetRandomSnapshotLocked(
        request.db_id_, request.args_[1], digest, options.with_values_, tx,
        operation.now_ms_);
  } else if (options.hash_) {
    snapshot = co_await g_storage->ExecuteHashLocked(
        request.db_id_, request.args_[1], digest, operation, tx);
  } else {
    snapshot = co_await g_storage->ExecuteSetLocked(
        request.db_id_, request.args_[1], digest, operation, tx);
  }
  if (!snapshot.ok()) co_return snapshot.status();
  if (snapshot->length_ == 0) {
    co_return OwnedStreamReply{.encoded_ = "*0\r\n", .chunks_ = {}};
  }

  const std::size_t tuple_width = options.with_values_ ? 2 : 1;
  if (snapshot->length_ >
          std::numeric_limits<std::size_t>::max() / tuple_width ||
      snapshot->values_.size() != snapshot->length_ * tuple_width) {
    co_return absl::InternalError(
        "random snapshot disagrees with collection length");
  }
  auto state = std::make_shared<TransactionalRandomStreamState>();
  state->options_ = options;
  state->remaining_ = options.count_;
  state->population_ = snapshot->length_;
  state->values_.reserve(snapshot->values_.size());
  for (auto& value : snapshot->values_) {
    if (!value.has_value()) {
      co_return absl::InternalError("random snapshot contains a missing value");
    }
    state->values_.push_back(std::move(*value));
  }

  const std::uint64_t elements =
      options.with_values_ ? options.count_ * 2 : options.count_;
  OwnedStreamReply reply;
  reply.encoded_ = "*" + std::to_string(elements) + "\r\n";
  reply.chunks_ = [state]() { return NextTransactionalRandomChunk(state); };
  co_return reply;
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

bool IsMissingStringValue(const absl::Status& status) {
  return status.code() == absl::StatusCode::kNotFound ||
         status.message().starts_with("WRONGTYPE ");
}

absl::StatusOr<storage::SetOptions> ParseSetOptions(
    const std::vector<std::string>& args) {
  storage::SetOptions options;
  bool condition_seen = false;
  bool expiration_seen = false;
  bool get_seen = false;
  const std::uint64_t now_ms = RedisUnixTimeMillis();
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

absl::StatusOr<std::uint64_t> ParseExpirationDeadline(CommandKind kind,
                                                      std::string_view text) {
  const bool seconds =
      kind == CommandKind::kExpire || kind == CommandKind::kExpireAt;
  const bool absolute =
      kind == CommandKind::kExpireAt || kind == CommandKind::kPExpireAt;
  const std::string_view command = kind == CommandKind::kExpire    ? "expire"
                                   : kind == CommandKind::kPExpire ? "pexpire"
                                   : kind == CommandKind::kExpireAt
                                       ? "expireat"
                                       : "pexpireat";
  return ParseRedisExpirationDeadline(
      text, seconds, absolute, command,
      PastExpirationPolicy::kExpireImmediately);
}

long long ExpirationReplySeconds(std::uint64_t milliseconds) {
  const std::uint64_t rounded =
      milliseconds / 1000 + (milliseconds % 1000 >= 500 ? 1 : 0);
  return static_cast<long long>(rounded);
}

Task<CommandReply> ExecuteStorageCommand(
    const CommandRequest& request, ReplyBuilder& reply_builder,
    ReadLatencyTrace* read_trace = nullptr,
    SetLatencyTrace* set_trace = nullptr) {
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
          reply.encoded_ = AppendStorageError(reply_builder, value.status());
        }
      } else {
        reply.disk_value_.emplace(std::move(*value));
      }
      co_return reply;
    }

    case CommandKind::kType: {
      const storage::ExpirationInfo info =
          co_await g_storage->GetExpiration(request.db_id_, args[1]);
      reply.encoded_ = reply_builder.AppendSimpleString(
          info.exists_ ? ValueTypeName(info.value_type_) : "none");
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
      std::optional<storage::ReplicationCommandAppend> replication;
      if (!request.replication_origin_ && g_storage->ReplicationLogActive()) {
        replication.emplace();
        replication->args_ = {"SET", args[1], args[2]};
      }
      if (set_trace != nullptr) {
        set_trace->replication_ = replication.has_value();
      }
      auto result = co_await g_storage->Set(
          request.db_id_, args[1], args[2], *options,
          replication ? &*replication : nullptr, set_trace);
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

    case CommandKind::kAppend:
    case CommandKind::kDecr:
    case CommandKind::kDecrBy:
    case CommandKind::kGetDel:
    case CommandKind::kGetEx:
    case CommandKind::kGetRange:
    case CommandKind::kGetSet:
    case CommandKind::kIncr:
    case CommandKind::kIncrBy:
    case CommandKind::kIncrByFloat:
    case CommandKind::kPSetEx:
    case CommandKind::kSetEx:
    case CommandKind::kSetNx:
    case CommandKind::kSetRange:
    case CommandKind::kSubstr:
      co_return co_await ExecuteStringCommand(request, reply_builder);

    case CommandKind::kGetBit:
    case CommandKind::kSetBit:
    case CommandKind::kBitCount:
    case CommandKind::kBitPos:
    case CommandKind::kBitField:
    case CommandKind::kBitFieldRo:
      co_return co_await ExecuteBitmapCommand(request, reply_builder);

    case CommandKind::kLPush:
    case CommandKind::kLPushX:
    case CommandKind::kRPush:
    case CommandKind::kRPushX:
    case CommandKind::kLPop:
    case CommandKind::kRPop:
    case CommandKind::kLLen:
    case CommandKind::kLIndex:
    case CommandKind::kLRange:
    case CommandKind::kLSet:
    case CommandKind::kLInsert:
    case CommandKind::kLRem:
    case CommandKind::kLTrim:
    case CommandKind::kLPos:
      co_return co_await ExecuteSingleListCommand(request, reply_builder);

    case CommandKind::kHSet:
    case CommandKind::kHMSet:
    case CommandKind::kHSetNx:
    case CommandKind::kHGet:
    case CommandKind::kHMGet:
    case CommandKind::kHDel:
    case CommandKind::kHLen:
    case CommandKind::kHExists:
    case CommandKind::kHGetAll:
    case CommandKind::kHKeys:
    case CommandKind::kHVals:
    case CommandKind::kHStrlen:
    case CommandKind::kHIncrBy:
    case CommandKind::kHIncrByFloat:
    case CommandKind::kHRandField:
    case CommandKind::kHScan:
      co_return co_await ExecuteHashCommand(request, reply_builder);

    case CommandKind::kSAdd:
    case CommandKind::kSCard:
    case CommandKind::kSIsMember:
    case CommandKind::kSMembers:
    case CommandKind::kSMIsMember:
    case CommandKind::kSPop:
    case CommandKind::kSRandMember:
    case CommandKind::kSRem:
    case CommandKind::kSScan:
      co_return co_await ExecuteSetCommand(request, reply_builder);

    case CommandKind::kZAdd:
    case CommandKind::kZCard:
    case CommandKind::kZCount:
    case CommandKind::kZIncrBy:
    case CommandKind::kZLexCount:
    case CommandKind::kZMScore:
    case CommandKind::kZPopMax:
    case CommandKind::kZPopMin:
    case CommandKind::kZRandMember:
    case CommandKind::kZRange:
    case CommandKind::kZRangeByLex:
    case CommandKind::kZRangeByScore:
    case CommandKind::kZRank:
    case CommandKind::kZRem:
    case CommandKind::kZRemRangeByLex:
    case CommandKind::kZRemRangeByRank:
    case CommandKind::kZRemRangeByScore:
    case CommandKind::kZRevRange:
    case CommandKind::kZRevRangeByLex:
    case CommandKind::kZRevRangeByScore:
    case CommandKind::kZRevRank:
    case CommandKind::kZScan:
    case CommandKind::kZScore:
    case CommandKind::kGeoAdd:
    case CommandKind::kGeoDist:
    case CommandKind::kGeoHash:
    case CommandKind::kGeoPos:
    case CommandKind::kGeoRadiusRo:
    case CommandKind::kGeoRadiusByMemberRo:
    case CommandKind::kGeoSearch:
      co_return co_await ExecuteZSetCommand(request, reply_builder);

    case CommandKind::kXAdd:
    case CommandKind::kXDel:
    case CommandKind::kXLen:
    case CommandKind::kXRange:
    case CommandKind::kXRevRange:
    case CommandKind::kXTrim:
    case CommandKind::kXSetId:
    case CommandKind::kXGroup:
    case CommandKind::kXAck:
    case CommandKind::kXPending:
    case CommandKind::kXClaim:
    case CommandKind::kXAutoClaim:
    case CommandKind::kXInfo:
    case CommandKind::kXRead:
    case CommandKind::kXReadGroup:
      co_return co_await ExecuteStreamCommand(request, reply_builder);

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
    case CommandKind::kPttl:
    case CommandKind::kExpireTime:
    case CommandKind::kPExpireTime: {
      const bool milliseconds = request.kind_ == CommandKind::kPttl ||
                                request.kind_ == CommandKind::kPExpireTime;
      const bool absolute = request.kind_ == CommandKind::kExpireTime ||
                            request.kind_ == CommandKind::kPExpireTime;
      if (args.size() != 2) {
        reply.encoded_ = reply_builder.AppendError(
            std::string("ERR wrong number of arguments for '") +
            std::string(request.spec_->name_) + "' command");
        co_return reply;
      }
      const storage::ExpirationInfo info =
          co_await g_storage->GetExpiration(request.db_id_, args[1]);
      if (!info.exists_) {
        reply.encoded_ = reply_builder.AppendInteger(-2);
      } else if (info.expire_at_ms_ == 0) {
        reply.encoded_ = reply_builder.AppendInteger(-1);
      } else {
        const std::uint64_t now_ms = RedisUnixTimeMillis();
        const std::uint64_t value =
            absolute
                ? info.expire_at_ms_
                : (info.expire_at_ms_ > now_ms ? info.expire_at_ms_ - now_ms
                                               : 0);
        reply.encoded_ = reply_builder.AppendInteger(
            milliseconds ? static_cast<long long>(value)
                         : ExpirationReplySeconds(value));
      }
      co_return reply;
    }

    case CommandKind::kExpire:
    case CommandKind::kPExpire:
    case CommandKind::kExpireAt:
    case CommandKind::kPExpireAt: {
      if (args.size() < 3 || args.size() > 4) {
        reply.encoded_ = reply_builder.AppendError(
            std::string("ERR wrong number of arguments for '") +
            std::string(request.spec_->name_) + "' command");
        co_return reply;
      }
      auto condition = ParseExpirationCondition(args);
      if (!condition.ok()) {
        reply.encoded_ = reply_builder.AppendError("ERR syntax error");
        co_return reply;
      }
      auto expire_at_ms = ParseExpirationDeadline(request.kind_, args[2]);
      if (!expire_at_ms.ok()) {
        reply.encoded_ = reply_builder.AppendError(
            absl::StrCat("ERR ", expire_at_ms.status().message()));
        co_return reply;
      }
      auto updated = co_await g_storage->UpdateExpiration(
          request.db_id_, args[1], *expire_at_ms, *condition);
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
    const storage::StorageDurabilityStats durability =
        co_await g_storage->DurabilityStats();
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
    info +=
        std::string("defrag_paused:") + (defrag.paused_ ? "1\r\n" : "0\r\n");
    info += "defrag_block_sleep_ms:" + std::to_string(defrag.block_sleep_ms_) +
            "\r\n";
    info +=
        "defrag_record_sleep_us:" + std::to_string(defrag.record_sleep_us_) +
        "\r\n";
    info += "defrag_active:" + std::to_string(defrag.active_) + "\r\n";
    info += "defrag_pending:" + std::to_string(defrag.pending_) + "\r\n\r\n";
    info += "storage_dirty_staging_bytes:" +
            std::to_string(durability.dirty_staging_bytes_) + "\r\n";
    info += "storage_expiration_pause_count:" +
            std::to_string(g_storage->ExpirationPauseCount()) + "\r\n";
    info += "storage_flushes_pending:" +
            std::to_string(durability.flushes_pending_) + "\r\n";
    info += "storage_tx_commits_pending:" +
            std::to_string(durability.tx_commits_pending_) + "\r\n";
    info += std::string("storage_durability_pending:") +
            (durability.pending() ? "1\r\n\r\n" : "0\r\n\r\n");
  }
  if (wants("replication")) {
    const ReplicationStatus replication =
        g_replication != nullptr ? g_replication->status() : ReplicationStatus{};
    info += "# Replication\r\n";
    info += "role:" +
            std::string(replication.role_ == ReplicationRole::kMaster
                            ? "master"
                            : "slave") +
            "\r\n";
    info += "keylane_replication_state:" +
            std::string(ReplicationRoleName(replication.role_)) + "\r\n";
    info += "keylane_replication_generation:" +
            std::to_string(replication.generation_) + "\r\n";
    info += "master_replid:" +
            (replication.upstream_node_id_.has_value()
                 ? *replication.upstream_node_id_
                 : replication.local_node_id_) +
            "\r\n";
    if (replication.role_ == ReplicationRole::kMaster) {
      info += "connected_slaves:" +
              std::to_string(replication.downstream_replicas_.size()) +
              "\r\n";
      for (std::size_t index = 0;
           index < replication.downstream_replicas_.size(); ++index) {
        const DownstreamReplicaStatus& replica =
            replication.downstream_replicas_[index];
        info += "slave" + std::to_string(index) + ":ip=" + replica.host_ +
                ",port=" + std::to_string(replica.port_) + ",state=" +
                (replica.online_ ? "online" : "sync") + ",offset=" +
                std::to_string(replica.min_lsn_) + ",lag=0\r\n";
      }
    }
    if (replication.upstream_.has_value()) {
      info += "master_host:" + replication.upstream_->host_ + "\r\n";
      info += "master_port:" + std::to_string(replication.upstream_->port_) +
              "\r\n";
      info += "master_link_status:" +
              std::string(replication.role_ == ReplicationRole::kOnline
                              ? "up\r\n"
                              : "down\r\n");
      info += "keylane_source_workers:" +
              std::to_string(replication.source_worker_count_) + "\r\n";
      info += "keylane_connected_flows:" +
              std::to_string(replication.connected_flows_) + "\r\n";
      info += std::string("slave_read_only:") +
              (g_replication != nullptr &&
                       g_replication->replica_read_only()
                   ? "1\r\n"
                   : "0\r\n");
      info += std::string("master_sync_in_progress:") +
              (replication.role_ == ReplicationRole::kOnline ? "0\r\n"
                                                              : "1\r\n");
    }
    info += "\r\n";
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
                                     storage::TxShardWrites* tx,
                                     ReplyChunkSource* reply_chunks) {
  const auto& args = request.args_;
  if (auto options = ParseNegativeRandomStream(request); options.has_value()) {
    auto streamed = co_await PrepareTransactionalNegativeRandomStreamLocked(
        request, digest, tx, *options);
    if (!streamed.ok()) co_return EncodeStorageError(streamed.status());
    if (reply_chunks != nullptr) {
      *reply_chunks = std::move(streamed->chunks_);
    }
    co_return std::move(streamed->encoded_);
  }
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

    case CommandKind::kType: {
      const storage::ExpirationInfo info =
          co_await g_storage->GetExpirationLocked(db_id, args[1], digest);
      co_return EncodeSimpleString(
          info.exists_ ? ValueTypeName(info.value_type_) : "none");
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

    case CommandKind::kAppend:
    case CommandKind::kDecr:
    case CommandKind::kDecrBy:
    case CommandKind::kGetDel:
    case CommandKind::kGetEx:
    case CommandKind::kGetRange:
    case CommandKind::kGetSet:
    case CommandKind::kIncr:
    case CommandKind::kIncrBy:
    case CommandKind::kIncrByFloat:
    case CommandKind::kPSetEx:
    case CommandKind::kSetEx:
    case CommandKind::kSetNx:
    case CommandKind::kSetRange:
    case CommandKind::kSubstr: {
      ReplyBuilder string_reply_builder;
      CommandReply reply = co_await ExecuteStringCommandLocked(
          request, digest, tx, string_reply_builder);
      co_return std::string(reply.encoded_);
    }

    case CommandKind::kGetBit:
    case CommandKind::kSetBit:
    case CommandKind::kBitCount:
    case CommandKind::kBitPos:
    case CommandKind::kBitField:
    case CommandKind::kBitFieldRo: {
      ReplyBuilder bitmap_reply_builder;
      CommandReply reply = co_await ExecuteBitmapCommandLocked(
          request, digest, tx, bitmap_reply_builder);
      co_return std::string(reply.encoded_);
    }

    case CommandKind::kLPush:
    case CommandKind::kLPushX:
    case CommandKind::kRPush:
    case CommandKind::kRPushX:
    case CommandKind::kLPop:
    case CommandKind::kRPop:
    case CommandKind::kLLen:
    case CommandKind::kLIndex:
    case CommandKind::kLRange:
    case CommandKind::kLSet:
    case CommandKind::kLInsert:
    case CommandKind::kLRem:
    case CommandKind::kLTrim:
    case CommandKind::kLPos: {
      ReplyBuilder list_reply_builder;
      CommandReply reply = co_await ExecuteSingleListCommandLocked(
          request, digest, tx, list_reply_builder);
      co_return std::string(reply.encoded_);
    }

    case CommandKind::kHSet:
    case CommandKind::kHMSet:
    case CommandKind::kHSetNx:
    case CommandKind::kHGet:
    case CommandKind::kHMGet:
    case CommandKind::kHDel:
    case CommandKind::kHLen:
    case CommandKind::kHExists:
    case CommandKind::kHGetAll:
    case CommandKind::kHKeys:
    case CommandKind::kHVals:
    case CommandKind::kHStrlen:
    case CommandKind::kHIncrBy:
    case CommandKind::kHIncrByFloat:
    case CommandKind::kHRandField:
    case CommandKind::kHScan: {
      ReplyBuilder hash_reply_builder;
      CommandReply reply = co_await ExecuteHashCommandLocked(
          request, digest, tx, hash_reply_builder);
      co_return std::string(reply.encoded_);
    }

    case CommandKind::kSAdd:
    case CommandKind::kSCard:
    case CommandKind::kSIsMember:
    case CommandKind::kSMembers:
    case CommandKind::kSMIsMember:
    case CommandKind::kSPop:
    case CommandKind::kSRandMember:
    case CommandKind::kSRem:
    case CommandKind::kSScan: {
      ReplyBuilder set_reply_builder;
      CommandReply reply = co_await ExecuteSetCommandLocked(request, digest, tx,
                                                            set_reply_builder);
      co_return std::string(reply.encoded_);
    }

    case CommandKind::kZAdd:
    case CommandKind::kZCard:
    case CommandKind::kZCount:
    case CommandKind::kZIncrBy:
    case CommandKind::kZLexCount:
    case CommandKind::kZMScore:
    case CommandKind::kZPopMax:
    case CommandKind::kZPopMin:
    case CommandKind::kZRandMember:
    case CommandKind::kZRange:
    case CommandKind::kZRangeByLex:
    case CommandKind::kZRangeByScore:
    case CommandKind::kZRank:
    case CommandKind::kZRem:
    case CommandKind::kZRemRangeByLex:
    case CommandKind::kZRemRangeByRank:
    case CommandKind::kZRemRangeByScore:
    case CommandKind::kZRevRange:
    case CommandKind::kZRevRangeByLex:
    case CommandKind::kZRevRangeByScore:
    case CommandKind::kZRevRank:
    case CommandKind::kZScan:
    case CommandKind::kZScore:
    case CommandKind::kGeoAdd:
    case CommandKind::kGeoDist:
    case CommandKind::kGeoHash:
    case CommandKind::kGeoPos:
    case CommandKind::kGeoRadiusRo:
    case CommandKind::kGeoRadiusByMemberRo:
    case CommandKind::kGeoSearch: {
      ReplyBuilder zset_reply_builder;
      CommandReply reply = co_await ExecuteZSetCommandLocked(
          request, digest, tx, zset_reply_builder);
      co_return std::string(reply.encoded_);
    }

    case CommandKind::kXAdd:
    case CommandKind::kXDel:
    case CommandKind::kXLen:
    case CommandKind::kXRange:
    case CommandKind::kXRevRange:
    case CommandKind::kXTrim:
    case CommandKind::kXSetId:
    case CommandKind::kXGroup:
    case CommandKind::kXAck:
    case CommandKind::kXPending:
    case CommandKind::kXClaim:
    case CommandKind::kXAutoClaim:
    case CommandKind::kXInfo: {
      ReplyBuilder stream_reply_builder;
      CommandReply reply = co_await ExecuteStreamCommandLocked(
          request, digest, tx, stream_reply_builder);
      co_return std::string(reply.encoded_);
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
    case CommandKind::kPttl:
    case CommandKind::kExpireTime:
    case CommandKind::kPExpireTime: {
      const bool milliseconds = request.kind_ == CommandKind::kPttl ||
                                request.kind_ == CommandKind::kPExpireTime;
      const bool absolute = request.kind_ == CommandKind::kExpireTime ||
                            request.kind_ == CommandKind::kPExpireTime;
      const storage::ExpirationInfo info =
          co_await g_storage->GetExpirationLocked(db_id, args[1], digest);
      if (!info.exists_) {
        co_return EncodeInteger(-2);
      }
      if (info.expire_at_ms_ == 0) {
        co_return EncodeInteger(-1);
      }
      const std::uint64_t now_ms = RedisUnixTimeMillis();
      const std::uint64_t value =
          absolute
              ? info.expire_at_ms_
              : (info.expire_at_ms_ > now_ms ? info.expire_at_ms_ - now_ms : 0);
      co_return EncodeInteger(milliseconds ? static_cast<long long>(value)
                                           : ExpirationReplySeconds(value));
    }

    case CommandKind::kExpire:
    case CommandKind::kPExpire:
    case CommandKind::kExpireAt:
    case CommandKind::kPExpireAt: {
      auto condition = ParseExpirationCondition(args);
      if (!condition.ok()) {
        co_return EncodeError("ERR syntax error");
      }
      auto expire_at_ms = ParseExpirationDeadline(request.kind_, args[2]);
      if (!expire_at_ms.ok()) {
        co_return EncodeError(
            absl::StrCat("ERR ", expire_at_ms.status().message()));
      }
      auto updated = co_await g_storage->UpdateExpirationLocked(
          db_id, args[1], digest, *expire_at_ms, *condition, tx);
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
  } else if (!IsMissingStringValue(value.status())) {
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
    // Command-local undo must always be settled before the EXEC-wide commit
    // chain is detached. Committing while this flag is armed means a failure
    // path forgot to restore its receipt checkpoint.
    assert(!shard.collect_undo_);
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

using TwoPhaseCallback =
    Task<absl::Status> (*)(void*, const tx::ShardSlice&);

Task<absl::Status> ReleaseHeldKeys(void*, const tx::ShardSlice&);

struct TwoPhaseResult {
  absl::Status status_;
  std::uint64_t txid_ = 0;
  bool skipped_ = false;
};

template <typename Context>
Task<absl::Status> TwoPhaseFinishCallback(void* opaque,
                                          const tx::ShardSlice&) {
  auto* context = static_cast<Context*>(opaque);
  const std::uint64_t txid = context->writes_.front().txid_;
  if (context->rollback_) co_return co_await g_storage->RollbackTxLocal(txid);
  co_return co_await g_storage->DiscardTxUndoLocal(txid);
}

template <typename Context, typename ShouldSkip>
Task<TwoPhaseResult> ExecuteTwoPhaseWrite(
    tx::Transaction& transaction, Context* context,
    TwoPhaseCallback read_callback, TwoPhaseCallback write_callback,
    TwoPhaseCallback single_shard_callback, ShouldSkip should_skip) {
  absl::Status status = co_await transaction.Schedule();
  if (!status.ok()) co_return TwoPhaseResult{std::move(status)};

  const std::uint64_t txid = storage::StorageEngine::AllocateWriteTxid();
  context->writes_.resize(g_storage->worker_count());
  for (storage::TxShardWrites& writes : context->writes_) {
    writes.txid_ = txid;
    writes.collect_undo_ = true;
  }
  auto disarm_undo = [&] {
    for (storage::TxShardWrites& writes : context->writes_) {
      writes.collect_undo_ = false;
    }
  };

  if (transaction.single_shard()) {
    status = co_await transaction.Execute(single_shard_callback, context, true);
    disarm_undo();
    const bool skipped = status.ok() && should_skip(*context);
    co_return TwoPhaseResult{std::move(status), txid, skipped};
  }

  status = co_await transaction.Execute(read_callback, context, false);
  if (!status.ok() || should_skip(*context)) {
    absl::Status released =
        co_await transaction.Execute(&ReleaseHeldKeys, nullptr, true);
    disarm_undo();
    if (status.ok() && !released.ok()) status = std::move(released);
    const bool skipped = status.ok();
    co_return TwoPhaseResult{std::move(status), txid, skipped};
  }

  status = co_await transaction.Execute(write_callback, context, false);
  context->rollback_ = !status.ok();
  absl::Status finished = co_await transaction.Execute(
      &TwoPhaseFinishCallback<Context>, context, true);
  if (status.ok() && !finished.ok()) status = std::move(finished);
  disarm_undo();
  co_return TwoPhaseResult{std::move(status), txid, false};
}

struct RenameContext {
  const CommandRequest* request_ = nullptr;
  storage::RawValue source_;
  bool destination_exists_ = false;
  bool rollback_ = false;
  std::vector<storage::TxShardWrites> writes_;
};

Task<absl::Status> RenameReadCallback(void* opaque,
                                      const tx::ShardSlice& slice) {
  auto* context = static_cast<RenameContext*>(opaque);
  for (const tx::TxKey& key : slice.keys_) {
    const std::string& name = context->request_->args_[key.arg_index_];
    if (key.arg_index_ == 1) {
      auto source = co_await g_storage->ReadRawValueLocked(
          context->request_->db_id_, name, key.digest_);
      if (!source.ok()) co_return source.status();
      context->source_ = std::move(*source);
    } else if (key.arg_index_ == 2) {
      context->destination_exists_ = co_await g_storage->ExistsLocked(
          context->request_->db_id_, name, key.digest_);
    }
  }
  co_return absl::OkStatus();
}

Task<absl::Status> RenameWriteCallback(void* opaque,
                                       const tx::ShardSlice& slice) {
  auto* context = static_cast<RenameContext*>(opaque);
  for (const tx::TxKey& key : slice.keys_) {
    const std::string& name = context->request_->args_[key.arg_index_];
    storage::TxShardWrites* writes = &context->writes_[celer::ThisWorker().id_];
    if (key.arg_index_ == 1) {
      auto deleted = co_await g_storage->DeleteLocked(
          context->request_->db_id_, name, key.digest_, writes);
      if (!deleted.ok()) co_return deleted.status();
      if (!*deleted) co_return absl::NotFoundError("no such key");
    } else if (key.arg_index_ == 2) {
      absl::Status written = co_await g_storage->WriteRawValueLocked(
          context->request_->db_id_, name, key.digest_, context->source_,
          writes);
      if (!written.ok()) co_return written;
    }
  }
  co_return absl::OkStatus();
}

Task<absl::Status> RenameSingleShardCallback(void* opaque,
                                             const tx::ShardSlice& slice) {
  auto* context = static_cast<RenameContext*>(opaque);
  const tx::TxKey* source_key = nullptr;
  const tx::TxKey* destination_key = nullptr;
  for (const tx::TxKey& key : slice.keys_) {
    if (key.arg_index_ == 1) source_key = &key;
    if (key.arg_index_ == 2) destination_key = &key;
  }
  if (source_key == nullptr || destination_key == nullptr) {
    co_return absl::InternalError("RENAME key routing is incomplete");
  }
  const auto& args = context->request_->args_;
  auto source = co_await g_storage->ReadRawValueLocked(
      context->request_->db_id_, args[1], source_key->digest_);
  if (!source.ok()) co_return source.status();
  context->source_ = std::move(*source);
  context->destination_exists_ = co_await g_storage->ExistsLocked(
      context->request_->db_id_, args[2], destination_key->digest_);
  if (context->request_->kind_ == CommandKind::kRenameNx &&
      context->destination_exists_) {
    co_return absl::OkStatus();
  }

  storage::TxShardWrites& writes = context->writes_[celer::ThisWorker().id_];
  absl::Status written = co_await g_storage->WriteRawValueLocked(
      context->request_->db_id_, args[2], destination_key->digest_,
      context->source_, &writes);
  if (written.ok()) {
    auto deleted = co_await g_storage->DeleteLocked(
        context->request_->db_id_, args[1], source_key->digest_, &writes);
    if (!deleted.ok()) {
      written = deleted.status();
    } else if (!*deleted) {
      written = absl::NotFoundError("no such key");
    }
  }
  writes.collect_undo_ = false;
  absl::Status finished =
      written.ok() ? co_await g_storage->DiscardTxUndoLocal(writes.txid_)
                   : co_await g_storage->RollbackTxLocal(writes.txid_, &writes);
  co_return written.ok() ? finished : (finished.ok() ? written : finished);
}

Task<absl::Status> ReleaseHeldKeys(void*, const tx::ShardSlice&) {
  co_return absl::OkStatus();
}

void NotifyRenamedValue(std::uint8_t db_id, std::string_view key,
                        storage::ValueType type) {
  if (type == storage::ValueType::kList) {
    NotifyListBlockingKey(db_id, key);
  } else if (type == storage::ValueType::kSortedSet) {
    NotifyZSetBlockingKey(db_id, key);
  } else if (type == storage::ValueType::kStream) {
    NotifyStreamBlockingKey(db_id, key);
  }
}

Task<CommandReply> ExecuteRename(const CommandRequest& request,
                                 ReplyBuilder& reply_builder) {
  const auto& args = request.args_;
  const bool nx = request.kind_ == CommandKind::kRenameNx;
  if (args[1] == args[2]) {
    const unsigned owner = ShardForKey(args[1]);
    const storage::ExpirationInfo info = co_await SubmitTaskTo(
        owner, [db = request.db_id_, key = std::string(args[1])]() {
          return g_storage->GetExpiration(db, key);
        });
    if (!info.exists_) {
      co_return BuiltReply(reply_builder.AppendError("ERR no such key"));
    }
    co_return BuiltReply(nx ? reply_builder.AppendInteger(0)
                            : reply_builder.AppendSimpleString("OK"));
  }

  tx::Transaction transaction;
  for (std::size_t argument = 1; argument <= 2; ++argument) {
    transaction.AddKey(ShardForKey(args[argument]), request.db_id_,
                       storage::ComputeDigest(args[argument]),
                       static_cast<std::uint32_t>(argument),
                       tx::LockMode::kExclusive);
  }
  transaction.Seal();
  RenameContext context;
  context.request_ = &request;
  TwoPhaseResult execution = co_await ExecuteTwoPhaseWrite(
      transaction, &context, &RenameReadCallback, &RenameWriteCallback,
      &RenameSingleShardCallback,
      [nx](const RenameContext& value) {
        return nx && value.destination_exists_;
      });
  absl::Status status = std::move(execution.status_);
  if (!status.ok()) {
    if (status.code() == absl::StatusCode::kNotFound) {
      co_return BuiltReply(reply_builder.AppendError("ERR no such key"));
    }
    co_return BuiltReply(AppendStorageError(reply_builder, status));
  }
  if (execution.skipped_) {
    co_return BuiltReply(reply_builder.AppendInteger(0));
  }

  g_storage->NoteTxCommitStarted();
  SpawnOnCurrentWorker(
      RunTxCommit(execution.txid_, std::move(context.writes_)));
  NotifyRenamedValue(request.db_id_, args[2], context.source_.value_type_);
  co_return BuiltReply(nx ? reply_builder.AppendInteger(1)
                          : reply_builder.AppendSimpleString("OK"));
}

struct CopyOptions {
  std::uint8_t destination_db_ = 0;
  bool replace_ = false;
};

absl::StatusOr<CopyOptions> ParseCopyOptions(const CommandRequest& request) {
  CopyOptions options{.destination_db_ = request.db_id_};
  const auto& args = request.args_;
  for (std::size_t i = 3; i < args.size();) {
    if (CmpCaseInsensitive(args[i], "replace")) {
      options.replace_ = true;
      ++i;
      continue;
    }
    if (CmpCaseInsensitive(args[i], "db") && i + 1 < args.size()) {
      std::int64_t db = 0;
      if (!ParseRedisInt64(args[i + 1], &db) ||
          db < std::numeric_limits<int>::min() ||
          db > std::numeric_limits<int>::max()) {
        return absl::InvalidArgumentError(
            "value is not an integer or out of range");
      }
      if (db < 0 || db >= storage::kLogicalDatabaseCount) {
        return absl::InvalidArgumentError("DB index is out of range");
      }
      options.destination_db_ = static_cast<std::uint8_t>(db);
      i += 2;
      continue;
    }
    return absl::InvalidArgumentError("syntax error");
  }
  return options;
}

struct CopyContext {
  const CommandRequest* request_ = nullptr;
  CopyOptions options_;
  std::optional<storage::RawValue> source_;
  bool destination_exists_ = false;
  bool copied_ = false;
  bool rollback_ = false;
  std::vector<storage::TxShardWrites> writes_;
};

Task<absl::Status> CopyReadCallback(void* opaque, const tx::ShardSlice& slice) {
  auto* context = static_cast<CopyContext*>(opaque);
  for (const tx::TxKey& key : slice.keys_) {
    const std::string& name = context->request_->args_[key.arg_index_];
    if (key.arg_index_ == 1) {
      auto source =
          co_await g_storage->ReadRawValueLocked(key.db_, name, key.digest_);
      if (source.ok()) {
        context->source_.emplace(std::move(*source));
      } else if (source.status().code() != absl::StatusCode::kNotFound) {
        co_return source.status();
      }
    } else if (key.arg_index_ == 2) {
      context->destination_exists_ =
          co_await g_storage->ExistsLocked(key.db_, name, key.digest_);
    }
  }
  co_return absl::OkStatus();
}

Task<absl::Status> CopyWriteCallback(void* opaque,
                                     const tx::ShardSlice& slice) {
  auto* context = static_cast<CopyContext*>(opaque);
  for (const tx::TxKey& key : slice.keys_) {
    if (key.arg_index_ != 2) continue;
    storage::TxShardWrites* writes = &context->writes_[celer::ThisWorker().id_];
    absl::Status status = co_await g_storage->WriteRawValueLocked(
        key.db_, context->request_->args_[2], key.digest_, *context->source_,
        writes);
    if (!status.ok()) co_return status;
    context->copied_ = true;
  }
  co_return absl::OkStatus();
}

Task<absl::Status> CopySingleShardCallback(void* opaque,
                                           const tx::ShardSlice& slice) {
  auto* context = static_cast<CopyContext*>(opaque);
  absl::Status status = co_await CopyReadCallback(opaque, slice);
  if (status.ok() && context->source_.has_value() &&
      (!context->destination_exists_ || context->options_.replace_)) {
    status = co_await CopyWriteCallback(opaque, slice);
  }
  storage::TxShardWrites& writes = context->writes_[celer::ThisWorker().id_];
  writes.collect_undo_ = false;
  absl::Status finished =
      status.ok() ? co_await g_storage->DiscardTxUndoLocal(writes.txid_)
                  : co_await g_storage->RollbackTxLocal(writes.txid_, &writes);
  co_return status.ok() ? finished : (finished.ok() ? status : finished);
}

Task<CommandReply> ExecuteCopy(const CommandRequest& request,
                               ReplyBuilder& reply_builder) {
  auto options = ParseCopyOptions(request);
  if (!options.ok()) {
    co_return BuiltReply(reply_builder.AppendError(
        absl::StrCat("ERR ", options.status().message())));
  }
  const auto& args = request.args_;
  if (request.db_id_ == options->destination_db_ && args[1] == args[2]) {
    co_return BuiltReply(reply_builder.AppendError(
        "ERR source and destination objects are the same"));
  }

  MultiDbOperationGuard db_guard;
  if (!db_guard.Add(request.db_id_) ||
      (options->destination_db_ != request.db_id_ &&
       !db_guard.Add(options->destination_db_))) {
    co_return BuiltReply(
        reply_builder.AppendError("TRYAGAIN database flush is in progress"));
  }

  tx::Transaction transaction;
  transaction.AddKey(ShardForKey(args[1]), request.db_id_,
                     storage::ComputeDigest(args[1]), 1, tx::LockMode::kShared);
  transaction.AddKey(ShardForKey(args[2]), options->destination_db_,
                     storage::ComputeDigest(args[2]), 2,
                     tx::LockMode::kExclusive);
  transaction.Seal();
  CopyContext context;
  context.request_ = &request;
  context.options_ = *options;
  TwoPhaseResult execution = co_await ExecuteTwoPhaseWrite(
      transaction, &context, &CopyReadCallback, &CopyWriteCallback,
      &CopySingleShardCallback, [](const CopyContext& value) {
        return !value.source_.has_value() ||
               (value.destination_exists_ && !value.options_.replace_);
      });
  absl::Status status = std::move(execution.status_);
  if (!status.ok()) {
    co_return BuiltReply(AppendStorageError(reply_builder, status));
  }
  if (execution.skipped_ || !context.copied_) {
    co_return BuiltReply(reply_builder.AppendInteger(0));
  }

  g_storage->NoteTxCommitStarted();
  SpawnOnCurrentWorker(
      RunTxCommit(execution.txid_, std::move(context.writes_)));
  NotifyRenamedValue(options->destination_db_, args[2],
                     context.source_->value_type_);
  co_return BuiltReply(reply_builder.AppendInteger(1));
}

struct MSetNxContext {
  const CommandRequest* request_ = nullptr;
  std::atomic<bool> exists_{false};
  bool rollback_ = false;
  std::vector<storage::TxShardWrites> writes_;
};

Task<absl::Status> MSetNxCheckCallback(void* opaque,
                                       const tx::ShardSlice& slice) {
  auto* context = static_cast<MSetNxContext*>(opaque);
  for (const tx::TxKey& key : slice.keys_) {
    if (co_await g_storage->ExistsLocked(
            context->request_->db_id_, context->request_->args_[key.arg_index_],
            key.digest_)) {
      context->exists_.store(true, std::memory_order_relaxed);
    }
  }
  co_return absl::OkStatus();
}

Task<absl::Status> MSetNxWriteLocal(MSetNxContext* context) {
  const auto& args = context->request_->args_;
  const unsigned owner = ThisWorker().id_;
  for (std::size_t argument = 1; argument < args.size(); argument += 2) {
    if (ShardForKey(args[argument]) != owner) continue;
    const storage::Digest digest = storage::ComputeDigest(args[argument]);
    auto result = co_await g_storage->SetLocked(
        context->request_->db_id_, args[argument], digest, args[argument + 1],
        {}, &context->writes_[owner]);
    if (!result.ok()) co_return result.status();
  }
  co_return absl::OkStatus();
}

Task<absl::Status> MSetNxWriteCallback(void* opaque, const tx::ShardSlice&) {
  co_return co_await MSetNxWriteLocal(static_cast<MSetNxContext*>(opaque));
}

Task<absl::Status> MSetNxSingleShardCallback(void* opaque,
                                             const tx::ShardSlice& slice) {
  auto* context = static_cast<MSetNxContext*>(opaque);
  absl::Status checked = co_await MSetNxCheckCallback(opaque, slice);
  if (!checked.ok() || context->exists_.load(std::memory_order_relaxed)) {
    co_return checked;
  }
  absl::Status written = co_await MSetNxWriteLocal(context);
  storage::TxShardWrites& writes = context->writes_[celer::ThisWorker().id_];
  writes.collect_undo_ = false;
  absl::Status finished =
      written.ok() ? co_await g_storage->DiscardTxUndoLocal(writes.txid_)
                   : co_await g_storage->RollbackTxLocal(writes.txid_, &writes);
  co_return written.ok() ? finished : (finished.ok() ? written : finished);
}

Task<CommandReply> ExecuteMSetNx(const CommandRequest& request,
                                 ReplyBuilder& reply_builder) {
  const auto& args = request.args_;
  if (args.size() < 3 || args.size() % 2 == 0) {
    co_return BuiltReply(reply_builder.AppendError(
        "ERR wrong number of arguments for 'msetnx' command"));
  }
  tx::Transaction transaction;
  for (std::size_t argument = 1; argument < args.size(); argument += 2) {
    transaction.AddKey(ShardForKey(args[argument]), request.db_id_,
                       storage::ComputeDigest(args[argument]),
                       static_cast<std::uint32_t>(argument),
                       tx::LockMode::kExclusive);
  }
  transaction.Seal();
  MSetNxContext context;
  context.request_ = &request;
  TwoPhaseResult execution = co_await ExecuteTwoPhaseWrite(
      transaction, &context, &MSetNxCheckCallback, &MSetNxWriteCallback,
      &MSetNxSingleShardCallback, [](const MSetNxContext& value) {
        return value.exists_.load(std::memory_order_relaxed);
      });
  absl::Status status = std::move(execution.status_);
  if (!status.ok()) {
    co_return BuiltReply(AppendStorageError(reply_builder, status));
  }
  if (execution.skipped_) {
    co_return BuiltReply(reply_builder.AppendInteger(0));
  }
  g_storage->NoteTxCommitStarted();
  SpawnOnCurrentWorker(
      RunTxCommit(execution.txid_, std::move(context.writes_)));
  co_return BuiltReply(reply_builder.AppendInteger(1));
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
        } else if (!IsMissingStringValue(value.status())) {
          co_return value.status();
        }
        break;
      }
      case CommandKind::kDel:
      case CommandKind::kUnlink: {
        std::optional<storage::ReplicationCommandAppend> replication;
        if (!ctx->request_->replication_origin_ &&
            ctx->tx_writes_.empty() && g_storage->ReplicationLogActive()) {
          replication.emplace();
          replication->args_ = {"DEL", name};
        }
        auto deleted = co_await g_storage->DeleteLocked(
            ctx->request_->db_id_, name, key.digest_,
            ctx->tx_writes_.empty() ? nullptr
                                    : &ctx->tx_writes_[ThisWorker().id_],
            replication ? &*replication : nullptr);
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
      case CommandKind::kTouch:
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
  auto keys = DetermineKeys(*request.spec_, args);
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
    // The two-hop path already discarded these journals in its finish hop;
    // the single-shard path has no finish hop. Settle both uniformly before
    // handing the receipts to the detached commit chain.
    for (unsigned owner = 0; owner < ctx.tx_writes_.size(); ++owner) {
      storage::TxShardWrites& shard = ctx.tx_writes_[owner];
      if (!shard.collect_undo_) continue;
      if (!shard.fences_.empty() || !shard.retirements_.empty()) {
        absl::Status discarded =
            co_await SubmitTaskTo(owner, [txid = write_txid] {
              return g_storage->DiscardTxUndoLocal(txid);
            });
        if (!discarded.ok()) {
          co_return BuiltReply(AppendStorageError(reply_builder, discarded));
        }
      }
      shard.collect_undo_ = false;
    }
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

bool IsExecSequentialListPop(CommandKind kind) {
  return kind == CommandKind::kLMPop || kind == CommandKind::kBLMPop ||
         kind == CommandKind::kBLPop || kind == CommandKind::kBRPop;
}

bool IsExecSequentialListMove(CommandKind kind) {
  return kind == CommandKind::kLMove || kind == CommandKind::kRPopLPush ||
         kind == CommandKind::kBLMove || kind == CommandKind::kBRPopLPush;
}

bool IsExecSequentialSetMulti(CommandKind kind) {
  return kind == CommandKind::kSDiff || kind == CommandKind::kSDiffStore ||
         kind == CommandKind::kSInter || kind == CommandKind::kSInterCard ||
         kind == CommandKind::kSInterStore || kind == CommandKind::kSMove ||
         kind == CommandKind::kSUnion || kind == CommandKind::kSUnionStore;
}

bool IsExecSequentialZSetMulti(CommandKind kind) {
  return kind == CommandKind::kZMPop || kind == CommandKind::kBZMPop ||
         kind == CommandKind::kBZPopMin || kind == CommandKind::kBZPopMax ||
         kind == CommandKind::kZDiff || kind == CommandKind::kZDiffStore ||
         kind == CommandKind::kZInter || kind == CommandKind::kZInterCard ||
         kind == CommandKind::kZInterStore || kind == CommandKind::kZUnion ||
         kind == CommandKind::kZUnionStore ||
         kind == CommandKind::kZRangeStore || kind == CommandKind::kGeoRadius ||
         kind == CommandKind::kGeoRadiusByMember ||
         kind == CommandKind::kGeoSearchStore;
}

bool IsExecSequentialRename(CommandKind kind) {
  return kind == CommandKind::kRename || kind == CommandKind::kRenameNx;
}

bool IsExecSequentialCopy(CommandKind kind) {
  return kind == CommandKind::kCopy;
}

bool IsExecSequentialStringMulti(CommandKind kind) {
  return kind == CommandKind::kMSetNx || kind == CommandKind::kLcs ||
         kind == CommandKind::kBitOp;
}

std::optional<std::uint16_t> GeoStoreDestinationArg(
    const CommandRequest& command) {
  if (command.kind_ != CommandKind::kGeoRadius &&
      command.kind_ != CommandKind::kGeoRadiusByMember) {
    return std::nullopt;
  }
  const std::size_t begin = command.kind_ == CommandKind::kGeoRadius ? 6 : 5;
  for (std::size_t i = begin; i + 1 < command.args_.size(); ++i) {
    if (CmpCaseInsensitive(command.args_[i], "store") ||
        CmpCaseInsensitive(command.args_[i], "storedist")) {
      if (i + 1 <= std::numeric_limits<std::uint16_t>::max())
        return static_cast<std::uint16_t>(i + 1);
      return std::nullopt;
    }
  }
  return std::nullopt;
}

bool IsExecSequentialStreamRead(CommandKind kind) {
  return kind == CommandKind::kXRead || kind == CommandKind::kXReadGroup;
}

enum class ExecSequentialFamily : std::uint8_t {
  kNone,
  kListPop,
  kListMove,
  kRename,
  kCopy,
  kStringMulti,
  kSetMulti,
  kZSetMulti,
  kStreamRead,
};

ExecSequentialFamily ClassifyExecSequential(CommandKind kind) {
  if (IsExecSequentialListPop(kind)) return ExecSequentialFamily::kListPop;
  if (IsExecSequentialListMove(kind)) return ExecSequentialFamily::kListMove;
  if (IsExecSequentialRename(kind)) return ExecSequentialFamily::kRename;
  if (IsExecSequentialCopy(kind)) return ExecSequentialFamily::kCopy;
  if (IsExecSequentialStringMulti(kind))
    return ExecSequentialFamily::kStringMulti;
  if (IsExecSequentialSetMulti(kind)) return ExecSequentialFamily::kSetMulti;
  if (IsExecSequentialZSetMulti(kind)) return ExecSequentialFamily::kZSetMulti;
  if (IsExecSequentialStreamRead(kind))
    return ExecSequentialFamily::kStreamRead;
  return ExecSequentialFamily::kNone;
}

Task<std::string> ExecuteExecSequentialZSetMulti(
    const CommandRequest& command, const std::vector<ExecKey>& keys,
    std::vector<storage::TxShardWrites>& tx_writes) {
  std::vector<ZSetExecKey> zset_keys;
  zset_keys.reserve(keys.size());
  for (const ExecKey& key : keys) {
    zset_keys.push_back(ZSetExecKey{
        .digest_ = key.digest_, .owner_ = key.owner_, .arg_ = key.arg_});
  }
  if (command.kind_ == CommandKind::kZMPop ||
      command.kind_ == CommandKind::kBZMPop ||
      command.kind_ == CommandKind::kBZPopMin ||
      command.kind_ == CommandKind::kBZPopMax) {
    co_return co_await ExecuteZSetMultiPopLocked(command, zset_keys, tx_writes);
  }
  co_return co_await ExecuteZSetMultiKeyLocked(command, zset_keys, tx_writes);
}

Task<std::string> ExecuteExecSequentialStreamRead(
    const CommandRequest& command, const std::vector<ExecKey>& keys,
    std::vector<storage::TxShardWrites>& tx_writes) {
  std::vector<StreamExecKey> stream_keys;
  stream_keys.reserve(keys.size());
  for (const ExecKey& key : keys) {
    stream_keys.push_back(StreamExecKey{
        .digest_ = key.digest_, .owner_ = key.owner_, .arg_ = key.arg_});
  }
  co_return co_await ExecuteStreamReadLocked(command, stream_keys, tx_writes);
}

// EXEC uses one write receipt per shard for all commands. A command such as
// LMOVE or SMOVE can still need command-local undo after its first half was
// staged. Its rollback appends a same-txid compensation record, so the final
// EXEC commit preserves both earlier successful commands and the restored
// state across recovery.
struct ExecWriteCheckpoint {
  unsigned owner_ = 0;
};

std::vector<unsigned> UniqueOwners(std::initializer_list<unsigned> owners) {
  std::vector<unsigned> unique;
  for (unsigned owner : owners) {
    if (std::find(unique.begin(), unique.end(), owner) == unique.end())
      unique.push_back(owner);
  }
  return unique;
}

Task<absl::Status> BeginExecCommandUndo(
    const std::vector<unsigned>& owners,
    std::vector<storage::TxShardWrites>& writes,
    std::vector<ExecWriteCheckpoint>* checkpoints) {
  checkpoints->clear();
  checkpoints->reserve(owners.size());
  for (unsigned owner : owners) {
    storage::TxShardWrites& shard = writes[owner];
    checkpoints->push_back(ExecWriteCheckpoint{
        .owner_ = owner,
    });
    absl::Status discarded = co_await SubmitTaskTo(owner, [txid = shard.txid_] {
      return g_storage->DiscardTxUndoLocal(txid);
    });
    if (!discarded.ok()) {
      for (const ExecWriteCheckpoint& checkpoint : *checkpoints)
        writes[checkpoint.owner_].collect_undo_ = false;
      checkpoints->clear();
      co_return discarded;
    }
    shard.collect_undo_ = true;
  }
  co_return absl::OkStatus();
}

Task<absl::Status> FinishExecCommandUndo(
    const std::vector<ExecWriteCheckpoint>& checkpoints,
    std::vector<storage::TxShardWrites>& writes, bool rollback) {
  absl::Status first_error = absl::OkStatus();
  for (const ExecWriteCheckpoint& checkpoint : checkpoints) {
    storage::TxShardWrites& shard = writes[checkpoint.owner_];
    // Compensation appends must not recursively enter the undo journal.
    shard.collect_undo_ = false;
    absl::Status finished = co_await SubmitTaskTo(
        checkpoint.owner_, [txid = shard.txid_, rollback, shard = &shard] {
          return rollback ? g_storage->RollbackTxLocal(txid, shard)
                          : g_storage->DiscardTxUndoLocal(txid);
        });
    if (!finished.ok() && first_error.ok()) first_error = finished;
  }
  co_return first_error;
}

Task<std::string> ExecuteExecSequentialRename(
    const CommandRequest& command, const std::vector<ExecKey>& keys,
    std::vector<storage::TxShardWrites>& tx_writes) {
  const auto& args = command.args_;
  const bool nx = command.kind_ == CommandKind::kRenameNx;
  auto find_key = [&](std::size_t argument) -> const ExecKey* {
    for (const ExecKey& key : keys) {
      if (key.arg_ == argument) return &key;
    }
    return nullptr;
  };
  const ExecKey* source = find_key(1);
  const ExecKey* destination = find_key(2);
  if (source == nullptr) co_return EncodeError("ERR no such key");
  if (args[1] == args[2]) {
    auto info = co_await SubmitTaskTo(
        source->owner_, [db = command.db_id_, key = std::string(args[1]),
                         digest = source->digest_]() {
          return g_storage->GetExpirationLocked(db, key, digest);
        });
    if (!info.exists_) co_return EncodeError("ERR no such key");
    co_return nx ? EncodeInteger(0) : EncodeSimpleString("OK");
  }
  if (destination == nullptr) {
    co_return EncodeError("ERR RENAME destination key is missing");
  }

  auto raw = co_await SubmitTaskTo(
      source->owner_, [db = command.db_id_, key = std::string(args[1]),
                       digest = source->digest_]() {
        return g_storage->ReadRawValueLocked(db, key, digest);
      });
  if (!raw.ok()) {
    if (raw.status().code() == absl::StatusCode::kNotFound)
      co_return EncodeError("ERR no such key");
    co_return EncodeStorageError(raw.status());
  }
  if (nx) {
    const bool destination_exists = co_await SubmitTaskTo(
        destination->owner_, [db = command.db_id_, key = std::string(args[2]),
                              digest = destination->digest_]() {
          return g_storage->ExistsLocked(db, key, digest);
        });
    if (destination_exists) co_return EncodeInteger(0);
  }

  const std::vector<unsigned> owners =
      UniqueOwners({source->owner_, destination->owner_});
  std::vector<ExecWriteCheckpoint> checkpoints;
  absl::Status undo =
      co_await BeginExecCommandUndo(owners, tx_writes, &checkpoints);
  if (!undo.ok()) co_return EncodeStorageError(undo);

  absl::Status written = co_await SubmitTaskTo(
      destination->owner_, [db = command.db_id_, key = std::string(args[2]),
                            digest = destination->digest_, value = &*raw,
                            writes = &tx_writes[destination->owner_]] {
        return g_storage->WriteRawValueLocked(db, key, digest, *value, writes);
      });
  if (!written.ok()) {
    absl::Status rolled_back =
        co_await FinishExecCommandUndo(checkpoints, tx_writes, true);
    co_return EncodeStorageError(rolled_back.ok() ? written : rolled_back);
  }
  auto deleted = co_await SubmitTaskTo(
      source->owner_,
      [db = command.db_id_, key = std::string(args[1]),
       digest = source->digest_, writes = &tx_writes[source->owner_]] {
        return g_storage->DeleteLocked(db, key, digest, writes);
      });
  if (!deleted.ok() || !*deleted) {
    const absl::Status failure =
        deleted.ok() ? absl::NotFoundError("key not found") : deleted.status();
    absl::Status rolled_back =
        co_await FinishExecCommandUndo(checkpoints, tx_writes, true);
    co_return EncodeStorageError(rolled_back.ok() ? failure : rolled_back);
  }
  absl::Status completed =
      co_await FinishExecCommandUndo(checkpoints, tx_writes, false);
  if (!completed.ok()) co_return EncodeStorageError(completed);
  NotifyRenamedValue(command.db_id_, args[2], raw->value_type_);
  co_return nx ? EncodeInteger(1) : EncodeSimpleString("OK");
}

Task<std::string> ExecuteExecSequentialCopy(
    const CommandRequest& command, const std::vector<ExecKey>& keys,
    std::vector<storage::TxShardWrites>& tx_writes) {
  auto options = ParseCopyOptions(command);
  if (!options.ok()) {
    co_return EncodeError(absl::StrCat("ERR ", options.status().message()));
  }
  const auto& args = command.args_;
  auto find_key = [&](std::size_t argument) -> const ExecKey* {
    for (const ExecKey& key : keys) {
      if (key.arg_ == argument) return &key;
    }
    return nullptr;
  };
  const ExecKey* source = find_key(1);
  const ExecKey* destination = find_key(2);
  if (source == nullptr || destination == nullptr) {
    co_return EncodeError("ERR syntax error");
  }
  if (source->db_ == destination->db_ && args[1] == args[2]) {
    co_return EncodeError("ERR source and destination objects are the same");
  }

  auto raw = co_await SubmitTaskTo(
      source->owner_, [db = source->db_, key = std::string(args[1]),
                       digest = source->digest_]() {
        return g_storage->ReadRawValueLocked(db, key, digest);
      });
  if (!raw.ok()) {
    if (raw.status().code() == absl::StatusCode::kNotFound) {
      co_return EncodeInteger(0);
    }
    co_return EncodeStorageError(raw.status());
  }
  const bool destination_exists = co_await SubmitTaskTo(
      destination->owner_, [db = destination->db_, key = std::string(args[2]),
                            digest = destination->digest_]() {
        return g_storage->ExistsLocked(db, key, digest);
      });
  if (destination_exists && !options->replace_) {
    co_return EncodeInteger(0);
  }

  const std::vector<unsigned> owners =
      UniqueOwners({source->owner_, destination->owner_});
  std::vector<ExecWriteCheckpoint> checkpoints;
  absl::Status undo =
      co_await BeginExecCommandUndo(owners, tx_writes, &checkpoints);
  if (!undo.ok()) co_return EncodeStorageError(undo);
  absl::Status written = co_await SubmitTaskTo(
      destination->owner_, [db = destination->db_, key = std::string(args[2]),
                            digest = destination->digest_, value = &*raw,
                            writes = &tx_writes[destination->owner_]] {
        return g_storage->WriteRawValueLocked(db, key, digest, *value, writes);
      });
  if (!written.ok()) {
    absl::Status rolled_back =
        co_await FinishExecCommandUndo(checkpoints, tx_writes, true);
    co_return EncodeStorageError(rolled_back.ok() ? written : rolled_back);
  }
  absl::Status completed =
      co_await FinishExecCommandUndo(checkpoints, tx_writes, false);
  if (!completed.ok()) co_return EncodeStorageError(completed);
  NotifyRenamedValue(destination->db_, args[2], raw->value_type_);
  co_return EncodeInteger(1);
}

Task<std::string> ExecuteExecSequentialStringMulti(
    const CommandRequest& command, const std::vector<ExecKey>& keys,
    std::vector<storage::TxShardWrites>& tx_writes) {
  if (command.kind_ == CommandKind::kLcs ||
      command.kind_ == CommandKind::kBitOp) {
    std::vector<StringExecKey> string_keys;
    string_keys.reserve(keys.size());
    for (const ExecKey& key : keys) {
      string_keys.push_back(StringExecKey{
          .digest_ = key.digest_, .owner_ = key.owner_, .arg_ = key.arg_});
    }
    if (command.kind_ == CommandKind::kBitOp) {
      co_return co_await ExecuteBitOpLocked(command, string_keys, tx_writes);
    }
    co_return co_await ExecuteLcsLocked(command, string_keys);
  }

  const auto& args = command.args_;
  auto find_key = [&](std::size_t argument) -> const ExecKey* {
    for (const ExecKey& key : keys) {
      if (key.arg_ == argument) return &key;
    }
    return nullptr;
  };
  for (std::size_t argument = 1; argument < args.size(); argument += 2) {
    const ExecKey* key = find_key(argument);
    if (key == nullptr) co_return EncodeError("ERR MSETNX key is missing");
    auto exists = [db = command.db_id_, name = std::string(args[argument]),
                   digest = key->digest_]() {
      return g_storage->ExistsLocked(db, name, digest);
    };
    const bool present = key->owner_ == ThisWorker().id_
                             ? co_await exists()
                             : co_await SubmitTaskTo(key->owner_, exists);
    if (present) co_return EncodeInteger(0);
  }

  std::vector<unsigned> owners;
  owners.reserve(keys.size());
  for (const ExecKey& key : keys) owners.push_back(key.owner_);
  std::sort(owners.begin(), owners.end());
  owners.erase(std::unique(owners.begin(), owners.end()), owners.end());
  std::vector<ExecWriteCheckpoint> checkpoints;
  absl::Status undo =
      co_await BeginExecCommandUndo(owners, tx_writes, &checkpoints);
  if (!undo.ok()) co_return EncodeStorageError(undo);

  for (std::size_t argument = 1; argument < args.size(); argument += 2) {
    const ExecKey* key = find_key(argument);
    if (key == nullptr) {
      absl::Status rolled_back =
          co_await FinishExecCommandUndo(checkpoints, tx_writes, true);
      co_return rolled_back.ok() ? EncodeError("ERR MSETNX key is missing")
                                 : EncodeStorageError(rolled_back);
    }
    auto write = [db = command.db_id_, name = std::string(args[argument]),
                  value = std::string(args[argument + 1]),
                  digest = key->digest_, writes = &tx_writes[key->owner_]]() {
      return g_storage->SetLocked(db, name, digest, value, {}, writes);
    };
    auto result = key->owner_ == ThisWorker().id_
                      ? co_await write()
                      : co_await SubmitTaskTo(key->owner_, write);
    if (!result.ok()) {
      absl::Status rolled_back =
          co_await FinishExecCommandUndo(checkpoints, tx_writes, true);
      co_return EncodeStorageError(rolled_back.ok() ? result.status()
                                                    : rolled_back);
    }
  }
  absl::Status completed =
      co_await FinishExecCommandUndo(checkpoints, tx_writes, false);
  co_return completed.ok() ? EncodeInteger(1) : EncodeStorageError(completed);
}

Task<std::string> ExecuteExecSequentialSetMulti(
    const CommandRequest& command, const std::vector<ExecKey>& keys,
    std::vector<storage::TxShardWrites>& tx_writes) {
  const auto& args = command.args_;
  auto find_key = [&](std::size_t argument) -> const ExecKey* {
    for (const ExecKey& key : keys) {
      if (key.arg_ == argument) return &key;
    }
    return nullptr;
  };
  auto run_set = [&](const ExecKey& key, storage::HashOperation operation,
                     bool write) -> Task<absl::StatusOr<storage::HashResult>> {
    auto execute = [&]() {
      return g_storage->ExecuteSetLocked(
          command.db_id_, args[key.arg_], key.digest_, operation,
          write ? &tx_writes[key.owner_] : nullptr);
    };
    if (key.owner_ == ThisWorker().id_) {
      co_return co_await execute();
    }
    co_return co_await SubmitTaskTo(key.owner_, execute);
  };
  auto read_members = [&](std::size_t argument)
      -> Task<absl::StatusOr<std::vector<std::string>>> {
    const ExecKey* key = find_key(argument);
    if (key == nullptr) co_return absl::InternalError("Set key is missing");
    storage::HashOperation operation;
    operation.kind_ = storage::HashOperationKind::kKeys;
    auto result = co_await run_set(*key, std::move(operation), false);
    if (!result.ok()) co_return result.status();
    std::vector<std::string> members;
    members.reserve(result->values_.size());
    for (auto& member : result->values_) {
      members.push_back(std::move(*member));
    }
    co_return members;
  };

  if (command.kind_ == CommandKind::kSMove) {
    const ExecKey* source_key = find_key(1);
    if (source_key == nullptr) {
      co_return EncodeError("ERR Set source key is missing");
    }
    storage::HashOperation contains;
    contains.kind_ = storage::HashOperationKind::kGet;
    contains.fields_.push_back(args[3]);
    auto source = co_await run_set(*source_key, std::move(contains), false);
    if (!source.ok()) co_return EncodeStorageError(source.status());
    if (!source->key_exists_) co_return EncodeInteger(0);
    const bool source_contains =
        !source->values_.empty() && source->values_.front().has_value();
    if (args[1] == args[2]) co_return EncodeInteger(source_contains ? 1 : 0);
    const ExecKey* destination = find_key(2);
    if (destination == nullptr) {
      co_return EncodeError("ERR Set destination key is missing");
    }
    storage::HashOperation validate;
    validate.kind_ = storage::HashOperationKind::kLength;
    auto checked = co_await run_set(*destination, std::move(validate), false);
    if (!checked.ok()) co_return EncodeStorageError(checked.status());
    if (!source_contains) co_return EncodeInteger(0);
    const std::vector<unsigned> owners =
        UniqueOwners({source_key->owner_, destination->owner_});
    std::vector<ExecWriteCheckpoint> checkpoints;
    absl::Status undo =
        co_await BeginExecCommandUndo(owners, tx_writes, &checkpoints);
    if (!undo.ok()) co_return EncodeStorageError(undo);
    storage::HashOperation remove;
    remove.kind_ = storage::HashOperationKind::kDelete;
    remove.fields_.push_back(args[3]);
    auto removed = co_await run_set(*source_key, std::move(remove), true);
    if (!removed.ok()) {
      absl::Status rolled_back =
          co_await FinishExecCommandUndo(checkpoints, tx_writes, true);
      co_return EncodeStorageError(rolled_back.ok() ? removed.status()
                                                    : rolled_back);
    }
    storage::HashOperation add;
    add.kind_ = storage::HashOperationKind::kSet;
    add.fields_.push_back(args[3]);
    add.values_.push_back({});
    auto added = co_await run_set(*destination, std::move(add), true);
    if (!added.ok()) {
      absl::Status rolled_back =
          co_await FinishExecCommandUndo(checkpoints, tx_writes, true);
      co_return EncodeStorageError(rolled_back.ok() ? added.status()
                                                    : rolled_back);
    }
    absl::Status completed =
        co_await FinishExecCommandUndo(checkpoints, tx_writes, false);
    if (!completed.ok()) co_return EncodeStorageError(completed);
    co_return EncodeInteger(1);
  }

  const bool store = command.kind_ == CommandKind::kSDiffStore ||
                     command.kind_ == CommandKind::kSInterStore ||
                     command.kind_ == CommandKind::kSUnionStore;
  const bool intersection = command.kind_ == CommandKind::kSInter ||
                            command.kind_ == CommandKind::kSInterCard ||
                            command.kind_ == CommandKind::kSInterStore;
  const bool difference = command.kind_ == CommandKind::kSDiff ||
                          command.kind_ == CommandKind::kSDiffStore;
  const std::size_t first_source =
      command.kind_ == CommandKind::kSInterCard ? 2 : (store ? 2 : 1);
  const std::size_t last_source = keys.back().arg_;
  std::uint64_t cardinality_limit = 0;
  if (command.kind_ == CommandKind::kSInterCard) {
    for (std::size_t option = last_source + 1; option < args.size();
         option += 2) {
      if (option + 1 >= args.size() ||
          !CmpCaseInsensitive(args[option], "limit")) {
        co_return EncodeError("ERR syntax error");
      }
      std::int64_t parsed_limit = 0;
      const std::string_view text = args[option + 1];
      const auto parsed =
          std::from_chars(text.data(), text.data() + text.size(), parsed_limit);
      if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size() ||
          parsed_limit < 0) {
        co_return EncodeError("ERR LIMIT can't be negative");
      }
      cardinality_limit = static_cast<std::uint64_t>(parsed_limit);
    }
  }
  std::vector<std::vector<std::string>> sources;
  sources.reserve(last_source - first_source + 1);
  for (std::size_t argument = first_source; argument <= last_source;
       ++argument) {
    auto members = co_await read_members(argument);
    if (!members.ok()) co_return EncodeStorageError(members.status());
    sources.push_back(std::move(*members));
  }
  absl::flat_hash_set<std::string> output;
  for (const std::string& member : sources.front()) output.insert(member);
  if (intersection) {
    for (std::size_t i = 1; i < sources.size(); ++i) {
      absl::flat_hash_set<std::string_view> current;
      current.reserve(sources[i].size());
      for (const std::string& member : sources[i]) current.insert(member);
      for (auto it = output.begin(); it != output.end();) {
        if (!current.contains(*it)) {
          auto remove = it++;
          output.erase(remove);
        } else {
          ++it;
        }
      }
    }
  } else if (difference) {
    for (std::size_t i = 1; i < sources.size(); ++i) {
      for (const std::string& member : sources[i]) output.erase(member);
    }
  } else {
    for (std::size_t i = 1; i < sources.size(); ++i) {
      for (const std::string& member : sources[i]) output.insert(member);
    }
  }

  if (command.kind_ == CommandKind::kSInterCard) {
    const std::uint64_t cardinality = output.size();
    co_return EncodeInteger(static_cast<long long>(
        cardinality_limit == 0 ? cardinality
                               : std::min(cardinality, cardinality_limit)));
  }

  if (store) {
    const ExecKey* destination = find_key(1);
    if (destination == nullptr) {
      co_return EncodeError("ERR Set destination key is missing");
    }
    const std::vector<unsigned> owners = UniqueOwners({destination->owner_});
    std::vector<ExecWriteCheckpoint> checkpoints;
    absl::Status undo =
        co_await BeginExecCommandUndo(owners, tx_writes, &checkpoints);
    if (!undo.ok()) co_return EncodeStorageError(undo);
    auto remove_destination = [&]() {
      return g_storage->DeleteLocked(command.db_id_, args[1],
                                     destination->digest_,
                                     &tx_writes[destination->owner_]);
    };
    absl::StatusOr<bool> deleted;
    if (destination->owner_ == ThisWorker().id_) {
      deleted = co_await remove_destination();
    } else {
      deleted = co_await SubmitTaskTo(destination->owner_, remove_destination);
    }
    if (!deleted.ok()) {
      absl::Status rolled_back =
          co_await FinishExecCommandUndo(checkpoints, tx_writes, true);
      co_return EncodeStorageError(rolled_back.ok() ? deleted.status()
                                                    : rolled_back);
    }
    if (!output.empty()) {
      storage::HashOperation add;
      add.kind_ = storage::HashOperationKind::kSet;
      add.fields_.reserve(output.size());
      add.values_.reserve(output.size());
      for (const std::string& member : output) {
        add.fields_.push_back(member);
        add.values_.push_back({});
      }
      auto added = co_await run_set(*destination, std::move(add), true);
      if (!added.ok()) {
        absl::Status rolled_back =
            co_await FinishExecCommandUndo(checkpoints, tx_writes, true);
        co_return EncodeStorageError(rolled_back.ok() ? added.status()
                                                      : rolled_back);
      }
    }
    absl::Status completed =
        co_await FinishExecCommandUndo(checkpoints, tx_writes, false);
    if (!completed.ok()) co_return EncodeStorageError(completed);
    co_return EncodeInteger(static_cast<long long>(output.size()));
  }

  std::vector<std::string> ordered(output.begin(), output.end());
  std::sort(ordered.begin(), ordered.end());
  ReplyBuilder builder;
  builder.AppendArrayHeader(ordered.size());
  for (const std::string& member : ordered) builder.AppendBulkString(member);
  co_return std::string(builder.View());
}

Task<std::string> ExecuteExecSequentialListPop(
    const CommandRequest& command, const std::vector<ExecKey>& keys,
    std::vector<storage::TxShardWrites>& tx_writes) {
  if (keys.empty()) co_return EncodeError("ERR syntax error");
  const auto& args = command.args_;
  bool left = command.kind_ != CommandKind::kBRPop;
  std::uint64_t count = 1;
  const bool nested = command.kind_ == CommandKind::kLMPop ||
                      command.kind_ == CommandKind::kBLMPop;
  if (command.kind_ == CommandKind::kBLPop ||
      command.kind_ == CommandKind::kBRPop) {
    const absl::Status timeout = ValidateBlockingTimeout(args.back());
    if (!timeout.ok()) {
      co_return EncodeError(absl::StrCat("ERR ", timeout.message()));
    }
  }
  if (command.kind_ == CommandKind::kBLMPop) {
    const absl::Status timeout = ValidateBlockingTimeout(args[1]);
    if (!timeout.ok()) {
      co_return EncodeError(absl::StrCat("ERR ", timeout.message()));
    }
  }
  if (nested) {
    const std::size_t direction = keys.back().arg_ + 1;
    if (direction >= args.size()) co_return EncodeError("ERR syntax error");
    if (CmpCaseInsensitive(args[direction], "left")) {
      left = true;
    } else if (CmpCaseInsensitive(args[direction], "right")) {
      left = false;
    } else {
      co_return EncodeError("ERR syntax error");
    }
    const std::size_t trailing = args.size() - (direction + 1);
    if (trailing != 0) {
      if (trailing != 2 || !CmpCaseInsensitive(args[direction + 1], "count")) {
        co_return EncodeError("ERR syntax error");
      }
      const std::string_view text = args[direction + 2];
      const auto parsed =
          std::from_chars(text.data(), text.data() + text.size(), count);
      if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size() ||
          count == 0) {
        co_return EncodeError("ERR count should be greater than 0");
      }
    }
  }

  for (const ExecKey& key : keys) {
    auto pop = [&]() -> Task<absl::StatusOr<storage::ListResult>> {
      storage::ListOperation operation;
      operation.kind_ = left ? storage::ListOperationKind::kPopLeft
                             : storage::ListOperationKind::kPopRight;
      operation.count_ = count;
      operation.count_provided_ = true;
      co_return co_await g_storage->ExecuteListLocked(
          command.db_id_, args[key.arg_], key.digest_, operation,
          &tx_writes[key.owner_]);
    };
    absl::StatusOr<storage::ListResult> result;
    if (key.owner_ == ThisWorker().id_) {
      result = co_await pop();
    } else {
      result = co_await SubmitTaskTo(key.owner_, pop);
    }
    if (!result.ok()) co_return EncodeStorageError(result.status());
    if (result->values_.empty()) continue;
    ReplyBuilder builder;
    builder.AppendArrayHeader(2);
    builder.AppendBulkString(args[key.arg_]);
    if (nested) {
      builder.AppendArrayHeader(result->values_.size());
      for (const std::string& value : result->values_) {
        builder.AppendBulkString(value);
      }
    } else {
      builder.AppendBulkString(result->values_.front());
    }
    co_return std::string(builder.View());
  }
  co_return "*-1\r\n";
}

Task<std::string> ExecuteExecSequentialListMove(
    const CommandRequest& command, const std::vector<ExecKey>& keys,
    std::vector<storage::TxShardWrites>& tx_writes) {
  const auto& args = command.args_;
  if (keys.size() < 2 || args.size() < 3) {
    co_return EncodeError("ERR syntax error");
  }
  auto find_key = [&](std::size_t argument) -> const ExecKey* {
    for (const ExecKey& key : keys) {
      if (key.arg_ == argument) return &key;
    }
    return nullptr;
  };
  const ExecKey* source = find_key(1);
  const ExecKey* destination = find_key(2);
  if (source == nullptr || destination == nullptr) {
    co_return EncodeError("ERR List move key is missing");
  }

  bool source_left = false;
  bool destination_left = true;
  if (command.kind_ == CommandKind::kLMove ||
      command.kind_ == CommandKind::kBLMove) {
    if (args.size() < 5) co_return EncodeError("ERR syntax error");
    if (CmpCaseInsensitive(args[3], "left")) {
      source_left = true;
    } else if (!CmpCaseInsensitive(args[3], "right")) {
      co_return EncodeError("ERR syntax error");
    }
    if (CmpCaseInsensitive(args[4], "left")) {
      destination_left = true;
    } else if (CmpCaseInsensitive(args[4], "right")) {
      destination_left = false;
    } else {
      co_return EncodeError("ERR syntax error");
    }
  }

  if (command.kind_ == CommandKind::kBLMove ||
      command.kind_ == CommandKind::kBRPopLPush) {
    const std::size_t timeout_arg =
        command.kind_ == CommandKind::kBLMove ? 5 : 3;
    if (timeout_arg >= args.size()) co_return EncodeError("ERR syntax error");
    const absl::Status timeout = ValidateBlockingTimeout(args[timeout_arg]);
    if (!timeout.ok()) {
      co_return EncodeError(absl::StrCat("ERR ", timeout.message()));
    }
  }

  auto run_list = [&](const ExecKey& key, storage::ListOperation operation,
                      bool write) -> Task<absl::StatusOr<storage::ListResult>> {
    auto execute = [&]() {
      return g_storage->ExecuteListLocked(
          command.db_id_, args[key.arg_], key.digest_, operation,
          write ? &tx_writes[key.owner_] : nullptr);
    };
    if (key.owner_ == ThisWorker().id_) {
      co_return co_await execute();
    }
    co_return co_await SubmitTaskTo(key.owner_, execute);
  };

  if (args[1] == args[2]) {
    storage::ListOperation move;
    move.kind_ = storage::ListOperationKind::kMoveWithin;
    move.first_ = source_left ? 1 : 0;
    move.second_ = destination_left ? 1 : 0;
    auto moved = co_await run_list(*source, std::move(move), true);
    if (!moved.ok()) co_return EncodeStorageError(moved.status());
    if (moved->values_.empty()) co_return EncodeNullBulkString();
    NotifyListBlockingKey(command.db_id_, args[1]);
    co_return EncodeBulkString(moved->values_.front());
  }

  // Validate the destination before mutating the source.  In particular, a
  // WRONGTYPE reply must not turn a failed move into a committed pop.
  storage::ListOperation validate;
  validate.kind_ = storage::ListOperationKind::kLength;
  auto checked = co_await run_list(*destination, std::move(validate), false);
  if (!checked.ok()) co_return EncodeStorageError(checked.status());

  // EXEC normally has no runtime undo because command errors do not roll back
  // earlier commands. LMOVE is one command spanning two writes, however: keep
  // undo only for its pop/push pair so a failed destination append restores
  // the source without touching earlier successful EXEC commands.
  const std::vector<unsigned> owners =
      UniqueOwners({source->owner_, destination->owner_});
  std::vector<ExecWriteCheckpoint> checkpoints;
  absl::Status undo =
      co_await BeginExecCommandUndo(owners, tx_writes, &checkpoints);
  if (!undo.ok()) co_return EncodeStorageError(undo);

  storage::ListOperation pop;
  pop.kind_ = source_left ? storage::ListOperationKind::kPopLeft
                          : storage::ListOperationKind::kPopRight;
  pop.count_ = 1;
  auto popped = co_await run_list(*source, std::move(pop), true);
  if (!popped.ok()) {
    absl::Status rolled_back =
        co_await FinishExecCommandUndo(checkpoints, tx_writes, true);
    co_return EncodeStorageError(rolled_back.ok() ? popped.status()
                                                  : rolled_back);
  }
  if (popped->values_.empty()) {
    absl::Status completed =
        co_await FinishExecCommandUndo(checkpoints, tx_writes, false);
    if (!completed.ok()) co_return EncodeStorageError(completed);
    co_return EncodeNullBulkString();
  }

  storage::ListOperation push;
  push.kind_ = destination_left ? storage::ListOperationKind::kPushLeft
                                : storage::ListOperationKind::kPushRight;
  push.values_.push_back(popped->values_.front());
  auto pushed = co_await run_list(*destination, std::move(push), true);
  if (!pushed.ok()) {
    absl::Status rolled_back =
        co_await FinishExecCommandUndo(checkpoints, tx_writes, true);
    co_return EncodeStorageError(rolled_back.ok() ? pushed.status()
                                                  : rolled_back);
  }
  absl::Status completed =
      co_await FinishExecCommandUndo(checkpoints, tx_writes, false);
  if (!completed.ok()) co_return EncodeStorageError(completed);
  NotifyListBlockingKey(command.db_id_, args[1]);
  NotifyListBlockingKey(command.db_id_, args[2]);
  co_return EncodeBulkString(popped->values_.front());
}

Task<std::string> ExecuteExecSequentialCommand(
    ExecSequentialFamily family, const CommandRequest& command,
    const std::vector<ExecKey>& keys,
    std::vector<storage::TxShardWrites>& tx_writes) {
  switch (family) {
    case ExecSequentialFamily::kListPop:
      co_return co_await ExecuteExecSequentialListPop(command, keys, tx_writes);
    case ExecSequentialFamily::kListMove:
      co_return co_await ExecuteExecSequentialListMove(command, keys,
                                                       tx_writes);
    case ExecSequentialFamily::kRename:
      co_return co_await ExecuteExecSequentialRename(command, keys, tx_writes);
    case ExecSequentialFamily::kCopy:
      co_return co_await ExecuteExecSequentialCopy(command, keys, tx_writes);
    case ExecSequentialFamily::kStringMulti:
      co_return co_await ExecuteExecSequentialStringMulti(command, keys,
                                                          tx_writes);
    case ExecSequentialFamily::kSetMulti:
      co_return co_await ExecuteExecSequentialSetMulti(command, keys,
                                                       tx_writes);
    case ExecSequentialFamily::kZSetMulti:
      co_return co_await ExecuteExecSequentialZSetMulti(command, keys,
                                                        tx_writes);
    case ExecSequentialFamily::kStreamRead:
      co_return co_await ExecuteExecSequentialStreamRead(command, keys,
                                                         tx_writes);
    case ExecSequentialFamily::kNone:
      co_return EncodeError("ERR internal EXEC sequential routing error");
  }
  co_return EncodeError("ERR internal EXEC sequential routing error");
}

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
  std::vector<ReplyChunkSource>* reply_chunks_ = nullptr;
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
      case CommandKind::kUnlink:
      case CommandKind::kExists:
      case CommandKind::kTouch:
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
                 std::vector<std::string>& replies,
                 std::vector<ReplyChunkSource>& reply_chunks, std::size_t begin,
                 std::size_t end) {
  run.queued_ = &queued;
  run.cmd_keys_ = &cmd_keys;
  run.replies_ = &replies;
  run.reply_chunks_ = &reply_chunks;
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
          } else if (!IsMissingStringValue(value.status())) {
            record_error(value.status());
            command_failed = true;
          }
          break;
        }
        case CommandKind::kDel:
        case CommandKind::kUnlink: {
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
        case CommandKind::kExists:
        case CommandKind::kTouch: {
          if (co_await g_storage->ExistsLocked(cmd.db_id_, args[key.arg_],
                                               key.digest_)) {
            ctx->counters_[local].fetch_add(1, std::memory_order_relaxed);
          }
          break;
        }
        default:
          // Single-key command: the sole owner runs the full body and writes
          // the reply slot directly (errors self-encode).
          (*ctx->replies_)[i] = co_await RunSingleKeyLocked(
              cmd.db_id_, cmd, key.digest_, tx, &(*ctx->reply_chunks_)[i]);
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
  auto keys = DetermineKeys(*request.spec_, request.args_);
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

struct ExecReplyStreamState {
  std::vector<std::string> replies_;
  std::vector<ReplyChunkSource> chunks_;
  std::size_t index_ = 0;
  bool reply_header_sent_ = false;
};

Task<absl::StatusOr<std::string>> NextExecReplyChunk(
    std::shared_ptr<ExecReplyStreamState> state) {
  while (state->index_ < state->replies_.size()) {
    if (!state->reply_header_sent_) {
      state->reply_header_sent_ = true;
      co_return std::move(state->replies_[state->index_]);
    }
    ReplyChunkSource& source = state->chunks_[state->index_];
    if (source) {
      absl::StatusOr<std::string> chunk = co_await source();
      if (!chunk.ok()) co_return chunk.status();
      if (!chunk->empty()) co_return chunk;
      source = {};
    }
    ++state->index_;
    state->reply_header_sent_ = false;
  }
  co_return std::string();
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
  std::vector<std::string> key_errors(queued.size());
  std::vector<std::uint8_t> dbs;  // distinct databases with keyed commands
  for (std::size_t i = 0; i < queued.size(); ++i) {
    const CommandRequest& cmd = queued[i];
    if (cmd.spec_ == nullptr || (cmd.spec_->flags_ & kCmdNoKeys) != 0) {
      if (cmd.kind_ == CommandKind::kRandomKey &&
          std::find(dbs.begin(), dbs.end(), cmd.db_id_) == dbs.end()) {
        dbs.push_back(cmd.db_id_);
      }
      continue;
    }
    auto keys = DetermineKeys(*cmd.spec_, cmd.args_);
    if (!keys.ok()) {
      // Movable-key commands derive their key set from argument values.
      // Redis queues value errors and reports them in the EXEC result instead
      // of treating them as queue-time arity failures.
      key_errors[i] =
          EncodeError(absl::StrCat("ERR ", keys.status().message()));
      continue;
    }
    const bool write = (cmd.spec_->flags_ & kCmdWrite) != 0;
    std::uint16_t slot = 0;
    if (cmd.kind_ == CommandKind::kCopy) {
      auto options = ParseCopyOptions(cmd);
      if (!options.ok()) {
        key_errors[i] =
            EncodeError(absl::StrCat("ERR ", options.status().message()));
        continue;
      }
      cmd_keys[i].push_back(ExecKey{
          .digest_ = storage::ComputeDigest(cmd.args_[1]),
          .owner_ = static_cast<std::uint16_t>(ShardForKey(cmd.args_[1])),
          .arg_ = 1,
          .slot_ = slot++,
          .mode_ = tx::LockMode::kShared,
          .db_ = cmd.db_id_,
      });
      cmd_keys[i].push_back(ExecKey{
          .digest_ = storage::ComputeDigest(cmd.args_[2]),
          .owner_ = static_cast<std::uint16_t>(ShardForKey(cmd.args_[2])),
          .arg_ = 2,
          .slot_ = slot++,
          .mode_ = tx::LockMode::kExclusive,
          .db_ = options->destination_db_,
      });
      for (const std::uint8_t db : {cmd.db_id_, options->destination_db_}) {
        if (std::find(dbs.begin(), dbs.end(), db) == dbs.end()) {
          dbs.push_back(db);
        }
      }
      continue;
    }
    const bool zset_store = cmd.kind_ == CommandKind::kZDiffStore ||
                            cmd.kind_ == CommandKind::kZInterStore ||
                            cmd.kind_ == CommandKind::kZUnionStore ||
                            cmd.kind_ == CommandKind::kZRangeStore ||
                            cmd.kind_ == CommandKind::kGeoSearchStore ||
                            GeoStoreDestinationArg(cmd).has_value();
    const bool bitop_store = cmd.kind_ == CommandKind::kBitOp;
    const std::uint16_t zset_destination =
        GeoStoreDestinationArg(cmd).value_or(1);
    if (zset_store) {
      cmd_keys[i].push_back(ExecKey{
          .digest_ = storage::ComputeDigest(cmd.args_[zset_destination]),
          .owner_ = static_cast<std::uint16_t>(
              ShardForKey(cmd.args_[zset_destination])),
          .arg_ = zset_destination,
          .slot_ = slot++,
          .mode_ = tx::LockMode::kExclusive,
          .db_ = cmd.db_id_,
      });
    }
    if (bitop_store) {
      cmd_keys[i].push_back(ExecKey{
          .digest_ = storage::ComputeDigest(cmd.args_[2]),
          .owner_ = static_cast<std::uint16_t>(ShardForKey(cmd.args_[2])),
          .arg_ = 2,
          .slot_ = slot++,
          .mode_ = tx::LockMode::kExclusive,
          .db_ = cmd.db_id_,
      });
    }
    for (std::size_t a = keys->first_; a <= keys->last_; a += keys->step_) {
      if (zset_store && a == zset_destination) continue;
      if (bitop_store && a == 2) continue;
      cmd_keys[i].push_back(ExecKey{
          .digest_ = storage::ComputeDigest(cmd.args_[a]),
          .owner_ = static_cast<std::uint16_t>(ShardForKey(cmd.args_[a])),
          .arg_ = static_cast<std::uint16_t>(a),
          .slot_ = slot++,
          .mode_ = bitop_store ? tx::LockMode::kShared
                               : (write ? tx::LockMode::kExclusive
                                        : tx::LockMode::kShared),
          .db_ = cmd.db_id_,
      });
    }
    if (std::find(dbs.begin(), dbs.end(), cmd.db_id_) == dbs.end()) {
      dbs.push_back(cmd.db_id_);
    }
  }

  std::vector<std::string> replies(queued.size());
  std::vector<ReplyChunkSource> reply_chunks(queued.size());
  std::optional<std::uint8_t> select_db;
  auto run_keyless = [&](const CommandRequest& cmd) -> Task<std::string> {
    ReplyBuilder local_builder;
    CommandReply local;
    if (cmd.kind_ == CommandKind::kInfo) {
      local = co_await ExecuteInfo(cmd, local_builder);
    } else if (cmd.kind_ == CommandKind::kRandomKey) {
      local = co_await ExecuteRandomKey(cmd, local_builder);
    } else if (cmd.kind_ == CommandKind::kXGroup ||
               cmd.kind_ == CommandKind::kXInfo) {
      local = co_await ExecuteStreamCommand(cmd, local_builder);
    } else {
      local = ExecuteSimpleLocalCommand(cmd, local_builder);
    }
    if (local.selected_db_.has_value()) {
      select_db = local.selected_db_;
    }
    co_return std::string(local.encoded_);
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
              if (!key_errors[i].empty()) {
                replies[i] = key_errors[i];
                ++i;
                continue;
              }
              if (cmd_keys[i].empty()) {
                replies[i] = co_await run_keyless(cmd);
                ++i;
                continue;
              }
              const ExecSequentialFamily sequential =
                  ClassifyExecSequential(cmd.kind_);
              if (sequential != ExecSequentialFamily::kNone) {
                replies[i] = co_await ExecuteExecSequentialCommand(
                    sequential, cmd, cmd_keys[i], tx_writes);
                ++i;
                continue;
              }
              std::size_t end = i + 1;
              while (end < queued.size() && !cmd_keys[end].empty() &&
                     ClassifyExecSequential(queued[end].kind_) ==
                         ExecSequentialFamily::kNone) {
                ++end;
              }
              ExecRunContext run;
              InitExecRun(run, queued, cmd_keys, replies, reply_chunks, i, end);
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
      // Acquire and retain every shard's holds before the coordinator runs
      // argument-ordered commands such as LMPOP one key at a time.
      absl::Status armed = co_await txn.Execute(&ArmOnlyShardCallback, nullptr,
                                                /*release=*/false);
      if (!armed.ok()) {
        (void)co_await txn.Release();
        co_await DropWatches(ctx);
        co_return BuiltReply(
            reply_builder.AppendError(absl::StrCat("ERR ", armed.message())));
      }
      if (!ctx.watched_.empty()) {
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
        if (!key_errors[i].empty()) {
          replies[i] = key_errors[i];
          ++i;
          continue;
        }
        if (cmd_keys[i].empty()) {
          replies[i] = co_await run_keyless(cmd);
          ++i;
          continue;
        }
        const ExecSequentialFamily sequential =
            ClassifyExecSequential(cmd.kind_);
        if (sequential != ExecSequentialFamily::kNone) {
          replies[i] = co_await ExecuteExecSequentialCommand(
              sequential, cmd, cmd_keys[i], tx_writes);
          ++i;
          continue;
        }
        std::size_t end = i + 1;
        while (end < queued.size() && !cmd_keys[end].empty() &&
               ClassifyExecSequential(queued[end].kind_) ==
                   ExecSequentialFamily::kNone) {
          ++end;
        }
        ExecRunContext run;
        InitExecRun(run, queued, cmd_keys, replies, reply_chunks, i, end);
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
      if (!key_errors[i].empty()) {
        replies[i] = key_errors[i];
      } else {
        replies[i] = co_await run_keyless(queued[i]);
      }
    }
  }

  co_await DropWatches(ctx);
  const bool streamed = std::any_of(
      reply_chunks.begin(), reply_chunks.end(),
      [](const ReplyChunkSource& source) { return static_cast<bool>(source); });
  reply_builder.AppendArrayHeader(replies.size());
  if (!streamed) {
    for (const std::string& reply : replies) {
      reply_builder.AppendRaw(reply);
    }
  }
  CommandReply reply = BuiltReply(reply_builder.View());
  if (streamed) {
    auto state = std::make_shared<ExecReplyStreamState>();
    state->replies_ = std::move(replies);
    state->chunks_ = std::move(reply_chunks);
    reply.chunks_ = [state]() { return NextExecReplyChunk(state); };
  }
  reply.selected_db_ = select_db;
  co_return reply;
}

}  // namespace

bool TryBeginCommandDbOperation(std::uint8_t db_id) noexcept {
  return TryBeginDbOperation(db_id);
}

void EndCommandDbOperation(std::uint8_t db_id) noexcept {
  EndDbOperation(db_id);
}

void InitStorage(storage::StorageEngine* engine, ReplicationManager* replication) {
  g_storage = engine;
  InitBlockingWaitStorage(engine);
  InitHashCommandStorage(engine);
  InitListCommandStorage(engine);
  InitSetCommandStorage(engine);
  InitStringCommandStorage(engine);
  InitStreamCommandStorage(engine);
  InitZSetCommandStorage(engine);
  g_replication = replication;
  g_replica_read_only = replication != nullptr && replication->is_replica();
}

void SetServerInfo(std::string bind_ip, std::uint16_t port,
                   unsigned thread_count) {
  g_server_bind_ip = std::move(bind_ip);
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
  if (request.spec_ != nullptr) {
    const CommandSpec& spec = *request.spec_;
    const std::size_t argc = request.args_.size();
    if (argc < spec.min_args_ ||
        (spec.max_args_ != 0 && argc > spec.max_args_)) {
      if (ctx.in_multi_) ctx.multi_dirty_ = true;
      co_return BuiltReply(
          reply_builder.AppendError("ERR wrong number of arguments for '" +
                                    std::string(spec.name_) + "' command"));
    }
  }
  if (std::optional<std::string> moved = ReplicaMovedError(ctx, request);
      moved.has_value()) {
    if (ctx.in_multi_) ctx.multi_dirty_ = true;
    co_return BuiltReply(reply_builder.AppendError(*moved));
  }
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
    // Queue-time validation is limited to structural errors. Movable-key
    // commands may have invalid numeric/key-count argument values; Redis
    // queues those and returns the error as one element of EXEC.
    if (request.spec_ == nullptr) {
      ctx.multi_dirty_ = true;
      co_return BuiltReply(reply_builder.AppendError(
          "ERR unknown command '" + request.args_.front() + "'"));
    }
    const CommandSpec& spec = *request.spec_;
    if ((kind == CommandKind::kMSet || kind == CommandKind::kMSetNx) &&
        request.args_.size() % 2 != 1) {
      ctx.multi_dirty_ = true;
      co_return BuiltReply(
          reply_builder.AppendError("ERR wrong number of arguments for '" +
                                    std::string(spec.name_) + "' command"));
    }
    const bool reject_writes =
        g_replication != nullptr ? g_replication->reject_writes()
                                 : g_replica_read_only;
    if (reject_writes && (spec.flags_ & kCmdWrite) != 0) {
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
    case CommandKind::kReadOnly:
      ctx.cluster_readonly_ = true;
      co_return BuiltReply(reply_builder.AppendSimpleString("OK"));
    case CommandKind::kReadWrite:
      ctx.cluster_readonly_ = false;
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
  const bool replication_origin = request.replication_origin_;
  const auto& args = request.args_;
  const std::uint32_t cmd_flags =
      request.spec_ != nullptr ? request.spec_->flags_ : 0u;
  const bool reject_writes =
      g_replication != nullptr ? g_replication->reject_writes()
                               : g_replica_read_only;
  if (!replication_origin && reject_writes &&
      (cmd_flags & kCmdWrite) != 0) {
    co_return BuiltReply(reply_builder.AppendError(
        "READONLY You can't write against a read only replica."));
  }
  if (!replication_origin && RejectForMemory(EstimatedMemoryGrowth(request))) {
    co_return BuiltReply(AppendOomError(reply_builder));
  }
  if (request.kind_ == CommandKind::kFlushDb ||
      request.kind_ == CommandKind::kFlushAll) {
    co_return co_await ExecuteFlush(request, reply_builder);
  }

  const bool uses_db = (cmd_flags & kCmdUsesDbGate) != 0;
  const std::optional<NegativeRandomStreamOptions> random_stream =
      ParseNegativeRandomStream(request);
  const bool manages_own_db_gate = (cmd_flags & kCmdMayBlock) != 0 ||
                                   random_stream.has_value() ||
                                   request.kind_ == CommandKind::kCopy;
  if (uses_db && !manages_own_db_gate && !TryBeginDbOperation(request.db_id_)) {
    co_return BuiltReply(
        reply_builder.AppendError("TRYAGAIN database flush is in progress"));
  }
  std::optional<DbOperationGuard> db_guard;
  if (uses_db && !manages_own_db_gate) {
    db_guard.emplace(request.db_id_);
  }

  if (random_stream.has_value()) {
    co_return co_await ExecuteNegativeRandomStream(request, *random_stream,
                                                   reply_builder);
  }

  switch (request.kind_) {
    case CommandKind::kReplicaOf:
      co_return co_await ExecuteReplicaOf(request, reply_builder);

    case CommandKind::kConfig:
      co_return co_await ExecuteConfig(request, reply_builder);

    case CommandKind::kInfo:
      co_return co_await ExecuteInfo(request, reply_builder);

    case CommandKind::kCluster:
      co_return co_await ExecuteCluster(request, reply_builder);

    case CommandKind::kCommand:
      co_return co_await ExecuteCommandIntrospection(request, reply_builder);

    case CommandKind::kKeys:
      co_return co_await ExecuteKeys(request, reply_builder);

    case CommandKind::kDbSize:
      co_return co_await ExecuteDbSize(request, reply_builder);

    case CommandKind::kRandomKey:
      co_return co_await ExecuteRandomKey(request, reply_builder);

    case CommandKind::kScan:
      co_return co_await ExecuteScan(request, reply_builder);

    case CommandKind::kTombRaider:
      co_return co_await ExecuteTombRaider(request, reply_builder);

    case CommandKind::kDefrag:
      co_return co_await ExecuteDefrag(request, reply_builder);

    case CommandKind::kRename:
    case CommandKind::kRenameNx:
      co_return co_await ExecuteRename(request, reply_builder);

    case CommandKind::kCopy:
      co_return co_await ExecuteCopy(request, reply_builder);

    case CommandKind::kMSetNx:
      co_return co_await ExecuteMSetNx(request, reply_builder);

    case CommandKind::kLcs:
      co_return co_await ExecuteLcsCommand(request, reply_builder);

    case CommandKind::kBitOp:
      co_return co_await ExecuteBitOpCommand(request, reply_builder);

    case CommandKind::kDel:
    case CommandKind::kUnlink:
    case CommandKind::kExists:
    case CommandKind::kTouch:
    case CommandKind::kMSet:
    case CommandKind::kMGet:
      co_return co_await ExecuteMultiKey(request, reply_builder);

    case CommandKind::kSDiff:
    case CommandKind::kSDiffStore:
    case CommandKind::kSInter:
    case CommandKind::kSInterCard:
    case CommandKind::kSInterStore:
    case CommandKind::kSMove:
    case CommandKind::kSUnion:
    case CommandKind::kSUnionStore:
      co_return co_await ExecuteSetMultiKey(request, reply_builder);

    case CommandKind::kZDiff:
    case CommandKind::kZDiffStore:
    case CommandKind::kZInter:
    case CommandKind::kZInterCard:
    case CommandKind::kZInterStore:
    case CommandKind::kZUnion:
    case CommandKind::kZUnionStore:
    case CommandKind::kZRangeStore:
    case CommandKind::kGeoRadius:
    case CommandKind::kGeoRadiusByMember:
    case CommandKind::kGeoSearchStore:
    case CommandKind::kZMPop:
      co_return co_await ExecuteZSetMultiKey(request, reply_builder);

    case CommandKind::kLMove:
    case CommandKind::kRPopLPush:
    case CommandKind::kLMPop:
      co_return co_await ExecuteListMultiKey(request, reply_builder);

    case CommandKind::kBLPop:
    case CommandKind::kBRPop:
    case CommandKind::kBLMove:
    case CommandKind::kBRPopLPush:
    case CommandKind::kBLMPop:
      co_return co_await ExecuteBlockingListCommand(request, reply_builder);

    case CommandKind::kBZMPop:
    case CommandKind::kBZPopMax:
    case CommandKind::kBZPopMin:
      co_return co_await ExecuteBlockingZSetCommand(request, reply_builder);

    case CommandKind::kXGroup:
    case CommandKind::kXInfo:
      if (args.size() >= 3) {
        const unsigned target = ShardForKey(args[2]);
        if (target != ThisWorker().id_) {
          co_return co_await SubmitTaskTo(
              target, [&request, &reply_builder]() -> Task<CommandReply> {
                co_return co_await ExecuteStorageCommand(request,
                                                         reply_builder);
              });
        }
      }
      co_return co_await ExecuteStorageCommand(request, reply_builder);

    case CommandKind::kXRead:
    case CommandKind::kXReadGroup:
      // The Stream handler parses the movable key list and dispatches each
      // key to its owner; args[1] is an option (or GROUP), not a key.
      co_return co_await ExecuteStorageCommand(request, reply_builder);

    case CommandKind::kGet:
    case CommandKind::kGetDel:
    case CommandKind::kGetEx:
    case CommandKind::kGetRange:
    case CommandKind::kGetSet:
    case CommandKind::kAppend:
    case CommandKind::kGetBit:
    case CommandKind::kSetBit:
    case CommandKind::kBitCount:
    case CommandKind::kBitPos:
    case CommandKind::kBitField:
    case CommandKind::kBitFieldRo:
    case CommandKind::kStrlen:
    case CommandKind::kSet:
    case CommandKind::kSetEx:
    case CommandKind::kPSetEx:
    case CommandKind::kSetNx:
    case CommandKind::kSetRange:
    case CommandKind::kSubstr:
    case CommandKind::kLPush:
    case CommandKind::kLPushX:
    case CommandKind::kRPush:
    case CommandKind::kRPushX:
    case CommandKind::kLPop:
    case CommandKind::kRPop:
    case CommandKind::kLLen:
    case CommandKind::kLIndex:
    case CommandKind::kLRange:
    case CommandKind::kLSet:
    case CommandKind::kLInsert:
    case CommandKind::kLRem:
    case CommandKind::kLTrim:
    case CommandKind::kLPos:
    case CommandKind::kHSet:
    case CommandKind::kHMSet:
    case CommandKind::kHSetNx:
    case CommandKind::kHGet:
    case CommandKind::kHMGet:
    case CommandKind::kHDel:
    case CommandKind::kHLen:
    case CommandKind::kHExists:
    case CommandKind::kHGetAll:
    case CommandKind::kHKeys:
    case CommandKind::kHVals:
    case CommandKind::kHStrlen:
    case CommandKind::kHIncrBy:
    case CommandKind::kHIncrByFloat:
    case CommandKind::kHRandField:
    case CommandKind::kHScan:
    case CommandKind::kSAdd:
    case CommandKind::kSCard:
    case CommandKind::kSIsMember:
    case CommandKind::kSMembers:
    case CommandKind::kSMIsMember:
    case CommandKind::kSPop:
    case CommandKind::kSRandMember:
    case CommandKind::kSRem:
    case CommandKind::kSScan:
    case CommandKind::kZAdd:
    case CommandKind::kZCard:
    case CommandKind::kZCount:
    case CommandKind::kZIncrBy:
    case CommandKind::kZLexCount:
    case CommandKind::kZMScore:
    case CommandKind::kZPopMax:
    case CommandKind::kZPopMin:
    case CommandKind::kZRandMember:
    case CommandKind::kZRange:
    case CommandKind::kZRangeByLex:
    case CommandKind::kZRangeByScore:
    case CommandKind::kZRank:
    case CommandKind::kZRem:
    case CommandKind::kZRemRangeByLex:
    case CommandKind::kZRemRangeByRank:
    case CommandKind::kZRemRangeByScore:
    case CommandKind::kZRevRange:
    case CommandKind::kZRevRangeByLex:
    case CommandKind::kZRevRangeByScore:
    case CommandKind::kZRevRank:
    case CommandKind::kZScan:
    case CommandKind::kZScore:
    case CommandKind::kGeoAdd:
    case CommandKind::kGeoDist:
    case CommandKind::kGeoHash:
    case CommandKind::kGeoPos:
    case CommandKind::kGeoRadiusRo:
    case CommandKind::kGeoRadiusByMemberRo:
    case CommandKind::kGeoSearch:
    case CommandKind::kXAdd:
    case CommandKind::kXDel:
    case CommandKind::kXLen:
    case CommandKind::kXRange:
    case CommandKind::kXRevRange:
    case CommandKind::kXTrim:
    case CommandKind::kXSetId:
    case CommandKind::kXAck:
    case CommandKind::kXPending:
    case CommandKind::kXClaim:
    case CommandKind::kXAutoClaim:
    case CommandKind::kIncr:
    case CommandKind::kIncrBy:
    case CommandKind::kIncrByFloat:
    case CommandKind::kDecr:
    case CommandKind::kDecrBy:
    case CommandKind::kExpire:
    case CommandKind::kPExpire:
    case CommandKind::kExpireAt:
    case CommandKind::kPExpireAt:
    case CommandKind::kPersist:
    case CommandKind::kTtl:
    case CommandKind::kPttl:
    case CommandKind::kExpireTime:
    case CommandKind::kPExpireTime:
    case CommandKind::kType:
      if (args.size() >= 2) {
        const unsigned target = ShardForKey(args[1]);
#if KEYLANE_ENABLE_READ_LATENCY_TRACE
        if (request.kind_ == CommandKind::kGet) {
          ReadLatencyTrace trace;
          trace.request_start_ns_ = ReadTraceNowNanos();
          trace.remote_ = target != ThisWorker().id_;
          CommandReply reply;
          if (trace.remote_) {
            reply = co_await SubmitTaskTo(
                target,
                [&request, &reply_builder, &trace]() -> Task<CommandReply> {
                  trace.owner_start_ns_ = ReadTraceNowNanos();
                  CommandReply result = co_await ExecuteStorageCommand(
                      request, reply_builder, &trace);
                  trace.owner_done_ns_ = ReadTraceNowNanos();
                  co_return result;
                });
          } else {
            trace.owner_start_ns_ = trace.request_start_ns_;
            reply =
                co_await ExecuteStorageCommand(request, reply_builder, &trace);
            trace.owner_done_ns_ = ReadTraceNowNanos();
          }
          trace.origin_resume_ns_ = ReadTraceNowNanos();
          reply.read_trace_ = trace;
          co_return reply;
        }
#endif
#if KEYLANE_ENABLE_SET_LATENCY_TRACE
        if (request.kind_ == CommandKind::kSet) {
          SetLatencyTrace trace;
          trace.request_start_ns_ = SetTraceNowNanos();
          trace.remote_ = target != ThisWorker().id_;
          CommandReply reply;
          if (trace.remote_) {
            reply = co_await SubmitTaskTo(
                target,
                [&request, &reply_builder, &trace]() -> Task<CommandReply> {
                  trace.owner_start_ns_ = SetTraceNowNanos();
                  CommandReply result = co_await ExecuteStorageCommand(
                      request, reply_builder, nullptr, &trace);
                  trace.owner_done_ns_ = SetTraceNowNanos();
                  co_return result;
                });
          } else {
            trace.owner_start_ns_ = trace.request_start_ns_;
            reply = co_await ExecuteStorageCommand(request, reply_builder,
                                                   nullptr, &trace);
            trace.owner_done_ns_ = SetTraceNowNanos();
          }
          trace.origin_resume_ns_ = SetTraceNowNanos();
          reply.set_trace_ = trace;
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

Task<absl::Status> ApplyReplicatedCommand(const ReplicatedCommand& command) {
  RespCommand wire{.args_ = command.args_};
  auto request = BuildCommandRequest(std::move(wire), command.db_id_);
  if (!request.ok()) co_return request.status();
  request->replication_origin_ = true;
  const bool canonical_set = request->kind_ == CommandKind::kSet &&
                             (request->args_.size() == 3 ||
                              (request->args_.size() == 5 &&
                               CmpCaseInsensitive(request->args_[3], "PXAT")));
  const bool canonical_del =
      request->kind_ == CommandKind::kDel && request->args_.size() == 2;
  const bool canonical_flush_db =
      request->kind_ == CommandKind::kFlushDb && request->args_.size() == 2;
  if (!canonical_set && !canonical_del && !canonical_flush_db) {
    co_return absl::Status(
        absl::StatusCode::kInvalidArgument,
        "replication command is not a canonical SET, DEL, or FLUSHDB");
  }

  if (canonical_flush_db) {
    std::uint64_t epoch = 0;
    const auto* begin = request->args_[1].data();
    const auto* end = begin + request->args_[1].size();
    const auto parsed = std::from_chars(begin, end, epoch);
    if (parsed.ec != std::errc{} || parsed.ptr != end || epoch == 0) {
      co_return absl::InvalidArgumentError(
          "invalid FLUSHDB replication epoch");
    }
    co_return co_await g_storage->ApplyReplicatedFlushDb(request->db_id_,
                                                         epoch);
  }

  ReplyBuilder reply_builder;
  CommandReply reply = co_await ExecuteCommand(*request, reply_builder);
  if (reply.disk_value_.has_value() || reply.chunks_) {
    co_return absl::Status(absl::StatusCode::kInternal,
                           "replication write produced a streamed reply");
  }
  if (!reply.encoded_.empty() && reply.encoded_.front() == '-') {
    co_return absl::Status(absl::StatusCode::kFailedPrecondition,
                           std::string(reply.encoded_));
  }
  co_return absl::OkStatus();
}

}  // namespace keylane
