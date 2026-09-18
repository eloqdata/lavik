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

#include "lavik/command.h"

#include <sys/socket.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cctype>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/container/inlined_vector.h"
#include "absl/strings/str_cat.h"
#include "backup.h"
#include "blocking_wait.h"
#include "bycorf/io/storage.h"
#include "bycorf/runtime/cross_core.h"
#include "bycorf/runtime/cycle_clock.h"
#include "bycorf/runtime/worker.h"
#include "client_limit.h"
#include "cluster_command.h"
#include "cluster_gate.h"
#include "function_catalog.h"
#include "hash_command.h"
#include "lavik/cluster/authority.h"
#include "lavik/cluster/runtime.h"
#include "lavik/command_table.h"
#include "lavik/config.h"
#include "lavik/expiration.h"
#include "lavik/fault_injection.h"
#include "lavik/glob.h"
#include "lavik/memory.h"
#include "lavik/metrics.h"
#include "lavik/monitor.h"
#include "lavik/pubsub.h"
#include "lavik/random_sample.h"
#include "lavik/rdb.h"
#include "lavik/redis_parse.h"
#include "lavik/replication.h"
#include "lavik/replication_command.h"
#include "lavik/resp.h"
#include "lavik/session.h"
#include "lavik/slowlog.h"
#include "lavik/storage/engine.h"
#include "lavik/storage/format.h"
#include "lavik/tx/transaction.h"
#include "lavik/tx/tx_shard.h"
#include "lavik/version.h"
#include "list_command.h"
#include "lua_eval.h"
#include "set_command.h"
#include "sort_command.h"
#include "spdlog/spdlog.h"
#include "stream_command.h"
#include "string_command.h"
#include "zset_command.h"

namespace lavik {
using namespace bycorf;

namespace {

std::optional<CommandReply> RecheckClusterWriteAuthority(
    const CommandRequest& request, ReplyBuilder& reply_builder,
    cluster::AuthorityInFlightGuards* in_flights);

storage::StorageEngine* g_storage = nullptr;
ReplicationManager* g_replication = nullptr;
ClientLimit* g_client_limit = nullptr;
bool g_replica_read_only = false;
std::uint16_t g_server_port = 0;
unsigned g_server_threads = 0;
std::string g_server_bind_ip = "127.0.0.1";
std::string g_server_config_file;
std::chrono::steady_clock::time_point g_server_start;
// Cached closures share the worker VM's KEYS/ARGV globals. Keep Lua scripts
// serialized across redis.call() yields on a worker; ordinary commands are
// not subject to this gate.
thread_local bool g_lua_execution_active = false;

struct ClientConnectionRecord {
  enum class Type { kNormal, kReplica, kPubSub };

  std::uint64_t id_ = 0;
  int fd_ = -1;
  std::string address_;
  std::chrono::steady_clock::time_point connected_at_;
  std::uint64_t replication_session_id_ = 0;
  bool tls_ = false;
  Type type_ = Type::kNormal;
  std::string name_;
  std::string library_name_;
  std::string library_version_;
  RespVersion resp_version_ = RespVersion::k2;
  std::size_t subscriptions_ = 0;
  std::size_t pattern_subscriptions_ = 0;
  bool blocked_ = false;
  bool closing_ = false;
};

// Each element is touched only by its matching bycorf worker. CLIENT is a rare
// management command and visits the workers with cross-core messages; normal
// request processing needs neither a lock nor an atomic lookup.
std::array<std::vector<ClientConnectionRecord>, storage::kLogicalStorageShards>
    g_worker_clients;

bool CmpCaseInsensitive(std::string_view a, std::string_view b);
std::string_view AppendStorageError(ReplyBuilder& reply_builder,
                                    const absl::Status& status);
void NotifyRenamedValue(const CommandRequest& request, std::uint8_t db_id,
                        std::string_view key, storage::ValueType type);
void NotifyRenamedValue(const CommandRequest& request, std::string_view key,
                        storage::ValueType type);
Task<CommandReply> ExecuteClient(ConnectionContext& ctx,
                                 const CommandRequest& request,
                                 ReplyBuilder& reply_builder);

std::size_t SaturatingAdd(std::size_t left, std::size_t right) noexcept {
  return right > std::numeric_limits<std::size_t>::max() - left
             ? std::numeric_limits<std::size_t>::max()
             : left + right;
}

std::size_t RequestArgumentBytes(const CommandRequest& request) noexcept {
  const auto bytes = storage::ReplicationCommandStagingBytes(request.args_);
  return bytes.value_or(std::numeric_limits<std::size_t>::max());
}

std::size_t CanonicalCommandBytes(const CommandRequest& request) noexcept {
  std::size_t bytes = SaturatingAdd(8, request.args_.size() * 4);
  for (const std::string& argument : request.args_) {
    bytes = SaturatingAdd(bytes, argument.size());
  }
  return bytes;
}

bool ReplicationEventExceedsBacklog(std::size_t bytes) noexcept {
  if (bytes > kMaxNativeReplicationEventBytes) return true;
  if (g_replication == nullptr || g_storage == nullptr) return false;
  const std::size_t total_blocks =
      g_replication->backlog_size_bytes() / storage::kStorageBlockBytes;
  const std::size_t minimum_flow_blocks =
      total_blocks / g_storage->worker_count();
  const std::size_t per_block_payload =
      storage::kStorageBlockBytes - sizeof(storage::ReplicationFrameHeader);
  const std::size_t capacity =
      minimum_flow_blocks >
              std::numeric_limits<std::size_t>::max() / per_block_payload
          ? std::numeric_limits<std::size_t>::max()
          : minimum_flow_blocks * per_block_payload;
  return minimum_flow_blocks == 0 || bytes > capacity;
}

bool MayGrowMemory(const CommandRequest& request) noexcept {
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
    case CommandKind::kHReplace:
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
    case CommandKind::kRestore:
    case CommandKind::kSort:
      return true;
    case CommandKind::kXGroup:
      if (request.args_.size() > 1 &&
          (CmpCaseInsensitive(request.args_[1], "create") ||
           CmpCaseInsensitive(request.args_[1], "createconsumer")))
        return true;
      return false;
    case CommandKind::kLMove:
    case CommandKind::kRPopLPush:
    case CommandKind::kBLMove:
    case CommandKind::kBRPopLPush:
    case CommandKind::kSMove:
      return true;
    case CommandKind::kSDiffStore:
    case CommandKind::kSInterStore:
    case CommandKind::kSUnionStore:
      return request.args_.size() > 1;
    case CommandKind::kZDiffStore:
    case CommandKind::kZInterStore:
    case CommandKind::kZUnionStore:
    case CommandKind::kZRangeStore:
    case CommandKind::kGeoRadius:
    case CommandKind::kGeoRadiusByMember:
    case CommandKind::kGeoSearchStore:
      return true;
    case CommandKind::kMSet:
    case CommandKind::kMSetNx:
      return request.args_.size() > 1;
    default:
      return false;
  }
}

bool RejectForMemory(std::size_t additional_bytes) noexcept {
  if (!WouldExceedMemoryLimit(additional_bytes)) {
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

std::string EncodeSemanticNull(RespVersion version) {
  return version == RespVersion::k3 ? "_\r\n" : EncodeNullBulkString();
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

// GCC can diagnose Abseil's trivially-relocatable InlinedVector move as
// reading its inactive union member when an empty request is moved into
// StatusOr. The vector size remains zero and those bytes are never observed;
// keep the suppression scoped to the one construction path that instantiates
// that false positive so genuine uninitialized reads elsewhere stay visible.
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
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
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic pop
#endif

absl::StatusOr<ReplicaOfRequest> ParseReplicaOfRequest(
    std::span<const std::string> args) {
  if (args.size() != 3) {
    return absl::InvalidArgumentError(
        "wrong number of arguments for 'replicaof' command");
  }
  if (CmpCaseInsensitive(args[1], "NO") && CmpCaseInsensitive(args[2], "ONE")) {
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

    case CommandKind::kMonitor:
      reply.encoded_ = reply_builder.AppendSimpleString("OK");
      reply.start_monitoring_ = true;
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
      if (error != std::errc{} || parsed_end != end) {
        reply.encoded_ =
            reply_builder.AppendError("ERR DB index is out of range");
        return reply;
      }
      if (cluster::ClusterEnabled() && db_id != 0) {
        // Redis rejects every non-zero database in cluster mode
        // (db.c selectCommand); SELECT 0 stays a successful no-op.
        reply.encoded_ = reply_builder.AppendError(
            "ERR SELECT is not allowed in cluster mode");
        return reply;
      }
      if (db_id >= storage::kLogicalDatabaseCount) {
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

Task<CommandReply> ExecutePubSubCommand(ConnectionContext& context,
                                        const CommandRequest& request,
                                        ReplyBuilder& reply_builder) {
  const auto& args = request.args_;
  if (request.kind_ == CommandKind::kPubSub) {
    if (CmpCaseInsensitive(args[1], "channels")) {
      if (args.size() > 3) {
        co_return BuiltReply(reply_builder.AppendError(
            "ERR Unknown subcommand or wrong number of arguments for "
            "'channels'. Try PUBSUB HELP."));
      }
      std::optional<std::string> pattern;
      if (args.size() == 3) pattern = args[2];
      std::vector<std::string> channels =
          co_await PubSubChannels(std::move(pattern));
      reply_builder.AppendArrayHeader(channels.size());
      for (const std::string& channel : channels) {
        reply_builder.AppendBulkString(channel);
      }
      co_return BuiltReply(reply_builder.View());
    }
    if (CmpCaseInsensitive(args[1], "numsub")) {
      const std::span<const std::string> channels(args.data() + 2,
                                                  args.size() - 2);
      std::vector<std::uint64_t> counts = co_await PubSubNumSub(channels);
      reply_builder.AppendArrayHeader(channels.size() * 2);
      for (std::size_t index = 0; index < channels.size(); ++index) {
        reply_builder.AppendBulkString(channels[index]);
        reply_builder.AppendInteger(
            static_cast<long long>(std::min<std::uint64_t>(
                counts[index], std::numeric_limits<long long>::max())));
      }
      co_return BuiltReply(reply_builder.View());
    }
    if (CmpCaseInsensitive(args[1], "numpat") && args.size() == 2) {
      const std::uint64_t count = co_await PubSubNumPat();
      co_return BuiltReply(reply_builder.AppendInteger(
          static_cast<long long>(std::min<std::uint64_t>(
              count, std::numeric_limits<long long>::max()))));
    }
    if (CmpCaseInsensitive(args[1], "help") && args.size() == 2) {
      constexpr std::array<std::string_view, 10> help = {
          "PUBSUB <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
          "CHANNELS [<pattern>]",
          "    Return the currently active channels matching a <pattern> "
          "(default: '*').",
          "NUMPAT",
          "    Return the number of unique pattern subscriptions.",
          "NUMSUB [<channel> ...]",
          "    Return the number of subscribers for the specified channels, "
          "excluding",
          "    pattern subscriptions (default: no channels).",
          "HELP",
          "    Prints this help."};
      reply_builder.AppendArrayHeader(help.size());
      for (std::string_view line : help) reply_builder.AppendBulkString(line);
      co_return BuiltReply(reply_builder.View());
    }
    co_return BuiltReply(reply_builder.AppendError(
        "ERR Unknown subcommand or wrong number of arguments for '" + args[1] +
        "'. Try PUBSUB HELP."));
  }

  if (request.kind_ == CommandKind::kPublish) {
    cluster::AuthorityInFlightGuards cluster_in_flights;
    if (cluster::ClusterEnabled() && !request.replication_origin_ &&
        request.replication_capture_ == nullptr) {
      if (std::optional<CommandReply> rejected = RecheckClusterWriteAuthority(
              request, reply_builder, &cluster_in_flights);
          rejected.has_value()) {
        co_return std::move(*rejected);
      }
    }
    if (request.replication_capture_ != nullptr) {
      if (request.defer_pubsub_delivery_) {
        auto captured = co_await CapturePubSubPublication(args[1], args[2]);
        if (!captured.ok()) {
          co_return BuiltReply(
              reply_builder.AppendError(captured.status().message()));
        }
        CaptureReplicationCommand(request, request.args_);
        const std::uint64_t receivers = CapturedPubSubReceiverCount(*captured);
        request.replication_capture_->SetCapturedPubSubPublication(
            std::move(*captured));
        // Subscriber membership and the reply count belong to this command's
        // position in EXEC. Only physical delivery waits for replication to
        // commit, so a later SUBSCRIBE cannot receive this message.
        co_return BuiltReply(reply_builder.AppendInteger(
            static_cast<long long>(std::min<std::uint64_t>(
                receivers, std::numeric_limits<long long>::max()))));
      }
      CaptureReplicationCommand(request, request.args_);
    } else if (!request.replication_origin_ && g_storage != nullptr &&
               (g_replication == nullptr || !g_replication->is_replica())) {
      const std::uint16_t partition_id = storage::RedisSlot(args[1]);
      const unsigned source_worker = partition_id % g_storage->worker_count();
      std::vector<std::string> replication_args = request.args_;
      // Native full-sync identifies runtime-only commands before dispatching
      // them. Keep the replicated command name canonical even when the client
      // used mixed or lower case.
      replication_args[0] = "PUBLISH";
      storage::MutationPrecondition mutation_precondition =
          ClusterMutationPrecondition(request);
      absl::Status published = co_await bycorf::SubmitTaskTo(
          source_worker,
          [partition_id, args = std::move(replication_args),
           mutation_precondition = std::move(
               mutation_precondition)]() mutable -> Task<absl::Status> {
            co_return co_await g_storage->PublishEphemeralReplicationCommand(
                partition_id, std::move(args),
                std::move(mutation_precondition));
          });
      if (!published.ok()) {
        CommandReply reply = BuiltReply(reply_builder.AppendError(
            absl::StrCat("ERR ephemeral replication publish failed: ",
                         published.message())));
        if (IsClusterAuthorityChanged(published)) {
          co_return FinalizeClusterMutationReply(request, reply_builder,
                                                 std::move(reply));
        }
        co_return reply;
      }
    }
    const std::uint64_t receivers = co_await PublishChannel(args[1], args[2]);
    co_return BuiltReply(reply_builder.AppendInteger(
        static_cast<long long>(std::min<std::uint64_t>(
            receivers, std::numeric_limits<long long>::max()))));
  }

  if (context.pubsub_session_ == nullptr) {
    context.pubsub_session_ =
        RegisterPubSubSession(context.socket_fd_, context.resp_version());
  }
  const std::span<const std::string> channels(args.data() + 1, args.size() - 1);
  std::string encoded;
  switch (request.kind_) {
    case CommandKind::kSubscribe:
      encoded = SubscribeChannels(context.pubsub_session_, channels);
      break;
    case CommandKind::kUnsubscribe:
      encoded = UnsubscribeChannels(context.pubsub_session_, channels);
      break;
    case CommandKind::kPSubscribe:
      encoded = PSubscribePatterns(context.pubsub_session_, channels);
      break;
    case CommandKind::kPUnsubscribe:
      encoded = PUnsubscribePatterns(context.pubsub_session_, channels);
      break;
    default:
      break;
  }
  SetClientPubSubCounts(
      context.conn_id_, PubSubSubscriptionCount(context.pubsub_session_),
      PubSubPatternSubscriptionCount(context.pubsub_session_));
  co_return BuiltReply(reply_builder.AppendRaw(encoded));
}

Task<CommandReply> ExecuteReplicaOf(const CommandRequest& request,
                                    ReplyBuilder& reply_builder) {
  // Redis rejects REPLICAOF in cluster mode before even validating arguments
  // (replication.c replicaofCommand); the trailing period is verbatim.
  if (cluster::ClusterEnabled()) {
    co_return BuiltReply(reply_builder.AppendError(
        "ERR REPLICAOF not allowed in cluster mode."));
  }
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
      co_await g_replication->ApplyDirective(ReplicationDirective{
          .kind_ = ReplicationDirective::Kind::kSetUpstream,
          .upstream_ = std::move(upstream),
      });
  if (!configured.ok()) {
    co_return BuiltReply(
        reply_builder.AppendError(absl::StrCat("ERR ", configured.message())));
  }
  co_return BuiltReply(reply_builder.AppendSimpleString("OK"));
}

Task<CommandReply> ExecuteAddReplicaOf(const CommandRequest& request,
                                       ReplyBuilder& reply_builder) {
  // Same cluster-mode rejection as REPLICAOF: two topology sources must never
  // coexist on one node.
  if (cluster::ClusterEnabled()) {
    co_return BuiltReply(reply_builder.AppendError(
        "ERR REPLICAOF not allowed in cluster mode."));
  }
  auto parsed = ParseReplicaOfRequest(request.args_);
  if (!parsed.ok() || !parsed->host_.has_value()) {
    const std::string message = parsed.ok()
                                    ? "ADDREPLICAOF does not accept NO ONE"
                                    : std::string(parsed.status().message());
    co_return BuiltReply(
        reply_builder.AppendError(absl::StrCat("ERR ", message)));
  }
  if (g_replication == nullptr) {
    co_return BuiltReply(
        reply_builder.AppendError("ERR replication backend is unavailable"));
  }
  absl::Status configured =
      co_await g_replication->ApplyDirective(ReplicationDirective{
          .kind_ = ReplicationDirective::Kind::kAddUpstream,
          .upstream_ = ReplicaOfConfig{*parsed->host_, parsed->port_},
      });
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
  AppendClusterSlotsNode(reply_builder, replication.upstream_->host_,
                         replication.upstream_->port_,
                         replication.upstream_node_id_.value_or(std::string{}));
  if (advertise_local_replica) {
    AppendClusterSlotsNode(reply_builder, g_server_bind_ip, g_server_port,
                           replication.local_node_id_);
  }
  return BuiltReply(reply_builder.View());
}

Task<std::optional<std::string>> ReplicaMovedError(
    const ConnectionContext& ctx, const CommandRequest& request) {
  if (g_replication == nullptr || !g_replication->is_replica() ||
      !g_replication->redirects_clients_to_upstream() ||
      request.spec_ == nullptr || (request.spec_->flags_ & kCmdNoKeys) != 0) {
    co_return std::nullopt;
  }
  const bool write = (request.spec_->flags_ & kCmdWrite) != 0;
  if (!write && ctx.cluster_readonly_) {
    co_return std::nullopt;
  }
  absl::StatusOr<KeyIndexView> keys =
      DetermineKeys(*request.spec_, request.args_);
  if (!keys.ok() || keys->empty()) {
    co_return std::nullopt;
  }
  const std::optional<ReplicaOfConfig> upstream = g_replication->upstream();
  if (!upstream.has_value()) {
    co_return std::nullopt;
  }
  const std::uint16_t slot = storage::RedisSlot(request.args_[keys->first_]);
  co_return absl::StrCat("MOVED ", slot, " ", upstream->host_, ":",
                         upstream->port_);
}

// ---- Redis Cluster data-plane gate ----
//
// When cluster mode is enabled, admission is decided by the process-wide
// AuthorityGuard against the latest committed ServingState and its in-memory
// finite lease. The legacy replica-MOVED shim above never runs. Cluster
// mode forbids standalone REPLICAOF control but the replication manager may
// still have a Meta-authorized population source; that source never supplies
// client redirection authority.
//
// The replication LOADING gate (DispatchCommandImpl) and the cluster gate
// share the whitelist below but prove different facts. In cluster mode the
// former stays closed until the authorized population rebuild is complete;
// the latter independently checks topology authority plus the committed
// ServingState's storage/population readiness. Both must admit a data command.

// Commands served while the dataset is not ready. Verbatim mirror of the
// whitelist the standalone is_loading gate used before extraction; REPLICAOF/
// ADDREPLICAOF stay whitelisted so their cluster-mode rejection is produced by
// the command itself (Redis aligns the error text) instead of being masked by
// LOADING.
bool LoadingAllowedCommand(const CommandRequest& request) {
  if (request.kind_ == CommandKind::kFunction && request.args_.size() == 2) {
    // FUNCTION KILL must be able to release a function that itself keeps the
    // population transition from draining. STATS is the matching read-only
    // observation surface. No other FUNCTION subcommand is safe while the
    // dataset is fenced.
    return CmpCaseInsensitive(request.args_[1], "KILL") ||
           CmpCaseInsensitive(request.args_[1], "STATS");
  }
  switch (request.kind_) {
    case CommandKind::kPing:
    case CommandKind::kEcho:
    case CommandKind::kAuth:
    case CommandKind::kSelect:
    case CommandKind::kClient:
    case CommandKind::kReplicaOf:
    case CommandKind::kAddReplicaOf:
    case CommandKind::kConfig:
    case CommandKind::kInfo:
    case CommandKind::kRole:
    case CommandKind::kWait:
    case CommandKind::kCluster:
    case CommandKind::kCommand:
    case CommandKind::kReadOnly:
    case CommandKind::kReadWrite:
    case CommandKind::kMonitor:
    case CommandKind::kSlowLog:
    case CommandKind::kPublish:
    case CommandKind::kPubSub:
    case CommandKind::kPSubscribe:
    case CommandKind::kPUnsubscribe:
    case CommandKind::kSubscribe:
    case CommandKind::kUnsubscribe:
    case CommandKind::kQuit:
    case CommandKind::kReset:
    case CommandKind::kScript:
      return true;
    default:
      return false;
  }
}

// EVAL/EVALSHA/FCALL carry kCmdDynamicWrite: they may write and are treated as
// writes for admission (a read-only script still redirects to the primary,
// matching Redis); the *_ro forms carry kCmdReadOnly instead.
bool ClusterRequestIsWrite(const CommandRequest& request) {
  return request.spec_ != nullptr &&
         (request.spec_->flags_ &
          (kCmdWrite | kCmdMayReplicate | kCmdDynamicWrite)) != 0;
}

// Extracts the command's distinct Redis hash slots in first-occurrence order
// (Admit inspects the first key's slot before reporting cross-slot). A key
// extraction failure (e.g. malformed EVAL numkeys) means "no keys": the
// command is admitted locally and produces its own argument error, mirroring
// Redis getNodeByQuery returning myself for zero keys and matching the
// ReplicaMovedError precedent.
void PopulateClusterSlots(CommandRequest& request) {
  request.ClearClusterSlots();
  if (request.kind_ == CommandKind::kPublish && request.args_.size() >= 2) {
    request.AddClusterSlot(storage::RedisSlot(request.args_[1]));
    return;
  }
  if (request.spec_ == nullptr || (request.spec_->flags_ & kCmdNoKeys) != 0) {
    return;
  }
  const absl::StatusOr<KeyIndexView> keys =
      DetermineKeys(*request.spec_, request.args_);
  if (!keys.ok() || keys->empty()) return;
  if (keys->count() == 1) {
    // Source writes arrive with a route chosen before their owner hop. Reads
    // first discover their route here; retaining it avoids hashing the key a
    // second time when storage dispatch chooses the owner below.
    if (!request.HasRoutedPartitionFor(keys->first_)) {
      request.SetRoutedPartition(
          storage::RedisSlot(request.args_[keys->first_]), keys->first_);
    }
    request.AddClusterSlot(request.RoutedPartitionId());
    return;
  }
  for (std::uint32_t index = keys->first_; index <= keys->last_;
       index += keys->step_) {
    const std::uint16_t slot = storage::RedisSlot(request.args_[index]);
    request.AddClusterSlot(slot);
  }
}

// The MOVED target port follows the requesting connection's TLS state
// (Redis getNodeClientPort/shouldReturnTlsInfo): TLS connections get the
// target's TLS port, falling back to the plain port when it offers no TLS.
std::uint16_t ClusterDecisionClientPort(const cluster::Decision& decision,
                                        bool connection_tls) {
  if (connection_tls && decision.moved_tls_port_ != 0) {
    return decision.moved_tls_port_;
  }
  return decision.moved_port_;
}

// These commands mutate process-wide durable state but carry no key from
// which the cluster gate can derive an owner, lease, or in-flight drain cell.
// Cluster mode therefore rejects them: finite authority is always group-scoped
// and cannot authorize a process-wide mutation.
std::string_view UnscopedClusterMutation(const CommandRequest& request) {
  switch (request.kind_) {
    case CommandKind::kFlushDb:
      return "FLUSHDB";
    case CommandKind::kFlushAll:
      return "FLUSHALL";
    case CommandKind::kFunction:
      if (request.args_.size() < 2) return {};
      if (CmpCaseInsensitive(request.args_[1], "LOAD")) return "FUNCTION LOAD";
      if (CmpCaseInsensitive(request.args_[1], "DELETE")) {
        return "FUNCTION DELETE";
      }
      if (CmpCaseInsensitive(request.args_[1], "FLUSH")) {
        return "FUNCTION FLUSH";
      }
      if (CmpCaseInsensitive(request.args_[1], "RESTORE")) {
        return "FUNCTION RESTORE";
      }
      return {};
    default:
      return {};
  }
}

// Maps a non-serving admission decision to its wire reply.
// Returns true when the decision produced a terminal reply; false when it
// admits local execution. kCloseConnection yields an empty reply with the
// close flag: the outcome is undeterminable, so nothing is written.
bool EmitClusterDecision(const cluster::Decision& decision, bool connection_tls,
                         ReplyBuilder& reply_builder, CommandReply* reply) {
  switch (decision.kind_) {
    case cluster::Decision::Kind::kServe:
    case cluster::Decision::Kind::kServeStaleRead:
      return false;
    case cluster::Decision::Kind::kMoved:
      reply->encoded_ = AppendMovedError(
          reply_builder, decision.moved_slot_, decision.moved_host_,
          ClusterDecisionClientPort(decision, connection_tls));
      return true;
    case cluster::Decision::Kind::kCrossSlot:
      reply->encoded_ = AppendCrossSlotError(reply_builder);
      return true;
    case cluster::Decision::Kind::kClusterDownUnbound:
      reply->encoded_ = AppendClusterDownUnboundError(reply_builder);
      return true;
    case cluster::Decision::Kind::kLoading:
      // Cluster readiness comes from the published ServingState, not from an
      // upstream sync, so use Redis's plain loading text rather than the
      // replication-shaped message of the standalone gate.
      reply->encoded_ = reply_builder.AppendError(
          "LOADING Redis is loading the dataset in memory");
      return true;
    case cluster::Decision::Kind::kTryAgain:
      reply->encoded_ =
          reply_builder.AppendError("TRYAGAIN Failover in progress");
      return true;
    case cluster::Decision::Kind::kCloseConnection:
      reply->close_connection_ = true;
      return true;
  }
  return false;  // unreachable: every Kind is handled above
}

// Runs the cluster admission gate for one dispatched command. Returns true
// when the request was answered terminally; otherwise a write carries the
// guard-issued admission proof used by every owner-side re-check.
bool ClusterGateReject(ConnectionContext& ctx, CommandRequest& request,
                       ReplyBuilder& reply_builder, CommandReply* reply) {
  cluster::ClusterRuntime* runtime = cluster::GetClusterRuntime();
  PopulateClusterSlots(request);
  request.cluster_authority_admission_.reset();
  const bool is_write = ClusterRequestIsWrite(request);
  const cluster::RequestView view{
      .slots_ = request.ClusterSlots(),
      .is_write_ = is_write,
      .connection_readonly_ = ctx.cluster_readonly_,
      .loading_allowed_ = LoadingAllowedCommand(request) &&
                          request.kind_ != CommandKind::kPublish,
  };
  auto admission =
      runtime->authority_guard_.CaptureAndAdmit(view, cluster::LeaseClockNow());
  if (!EmitClusterDecision(admission.decision(), request.connection_tls_,
                           reply_builder, reply)) {
    // Reads need no owner-side re-check; rejected requests already have their
    // reply. Only admitted writes allocate a shared proof to retain across
    // worker hops and storage checks. The local admission owns any borrowed
    // MOVED host until EmitClusterDecision has copied it into the reply.
    if (is_write) {
      request.cluster_authority_admission_ =
          std::make_shared<const cluster::AuthorityAdmission>(
              std::move(admission));
    }
    return false;
  }
  return true;
}

// Owner-side authority re-check for non-transactional writes, called from
// ExecuteCommandBody after every
// suspending admission (publisher admission, DB gate, snapshot/order gates)
// and before the handler runs. Reads are intentionally not re-checked
// according to the stale-read policy. Returns the standard redirect/error
// reply when authority changed; std::nullopt when the write may proceed.
std::optional<CommandReply> RecheckClusterWriteAuthority(
    const CommandRequest& request, ReplyBuilder& reply_builder,
    cluster::AuthorityInFlightGuards* in_flights) {
  if (!cluster::ClusterEnabled() || request.replication_origin_ ||
      request.cluster_authority_admission_ == nullptr ||
      request.ClusterSlots().empty() || !ClusterRequestIsWrite(request)) {
    return std::nullopt;
  }
  cluster::ClusterRuntime* runtime = cluster::GetClusterRuntime();
  for (;;) {
    const std::shared_ptr<const cluster::AuthorityAdmission> admission =
        request.cluster_authority_admission_;
    // Requests execute on stable Bycorf workers, so the worker id is the exact
    // stripe identity required by the admitted ServingState.
    if (runtime->authority_guard_.RegisterAndRecheck(
            *admission, bycorf::ThisWorker().id_, cluster::LeaseClockNow(),
            in_flights) == cluster::RecheckResult::kOk) {
      return std::nullopt;
    }

    // Nothing has executed yet, so acquire a fresh complete proof. Writes are
    // never loading-whitelisted; a lost lease, session, readiness bit, or
    // owner therefore maps to the current honest redirect/error. A serve
    // result means only an unrelated generation changed, so re-arm and retry
    // the publication/registration handshake.
    const cluster::RequestView view{
        .slots_ = request.ClusterSlots(),
        .is_write_ = true,
        .connection_readonly_ = false,
        .loading_allowed_ = false,
    };
    auto fresh = std::make_shared<const cluster::AuthorityAdmission>(
        runtime->authority_guard_.CaptureAndAdmit(view,
                                                  cluster::LeaseClockNow()));
    CommandReply reply;
    if (EmitClusterDecision(fresh->decision(), request.connection_tls_,
                            reply_builder, &reply)) {
      return reply;
    }
    request.cluster_authority_admission_ = std::move(fresh);
  }
}

Task<CommandReply> ExecuteCluster(const CommandRequest& request,
                                  ReplyBuilder& reply_builder) {
  if (cluster::ClusterEnabled()) {
    // Cluster mode serves the real discovery surface (SLOTS/NODES/MYID/INFO/
    // KEYSLOT) from the committed ServingState; the legacy shim below fakes
    // full coverage from replication state for standalone mode only.
    co_return co_await ExecuteClusterModeCommand(request, reply_builder);
  }
  if (request.args_.size() >= 2 &&
      CmpCaseInsensitive(request.args_[1], "KEYSLOT")) {
    if (request.args_.size() != 3) {
      co_return BuiltReply(reply_builder.AppendError(
          "ERR wrong number of arguments for 'cluster' command"));
    }
    co_return BuiltReply(
        reply_builder.AppendInteger(storage::RedisSlot(request.args_[2])));
  }
  if (request.args_.size() != 2) {
    co_return BuiltReply(reply_builder.AppendError(
        "ERR wrong number of arguments for 'cluster' command"));
  }
  ReplicationStatus replication;
  if (g_replication != nullptr) replication = co_await g_replication->Observe();
  if (CmpCaseInsensitive(request.args_[1], "SLOTS")) {
    co_return BuildClusterSlotsReply(replication, reply_builder);
  }
  if (!CmpCaseInsensitive(request.args_[1], "NODES")) {
    co_return BuiltReply(reply_builder.AppendError(
        "ERR only CLUSTER KEYSLOT, CLUSTER NODES and CLUSTER SLOTS are "
        "supported"));
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
    const std::string master_id = replication.upstream_node_id_.value_or("-");
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
  count += (command.flags_ & kCmdAdmin) != 0 ? 1 : 0;
  count += (command.flags_ & kCmdSkipMonitor) != 0 ? 1 : 0;
  reply_builder.AppendSetHeader(count);
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
  if ((command.flags_ & kCmdAdmin) != 0) {
    reply_builder.AppendBulkString("admin");
  }
  if ((command.flags_ & kCmdSkipMonitor) != 0) {
    reply_builder.AppendBulkString("skip_monitor");
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
    reply_builder.AppendSetHeader(0);  // ACL categories
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
    for (std::uint16_t index = keys->first_;
         !keys->empty() && index <= keys->last_;
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
  co_return BuiltReply(reply_builder.AppendNull());
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

bool ParseSlowLogCount(std::string_view text, std::int64_t* value) {
  if (text.empty()) return false;
  const char* begin = text.data();
  const char* end = begin + text.size();
  const auto [parsed_end, error] = std::from_chars(begin, end, *value);
  return error == std::errc{} && parsed_end == end;
}

Task<CommandReply> ExecuteSlowLog(const CommandRequest& request,
                                  ReplyBuilder& reply_builder) {
  const auto& args = request.args_;
  const std::string_view subcommand = args[1];
  if (CmpCaseInsensitive(subcommand, "HELP") && args.size() == 2) {
    constexpr std::array<std::string_view, 12> help{
        "SLOWLOG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "GET [<count>]",
        "    Return the newest <count> entries (default: 10, -1 means all).",
        "    Entries are made of:",
        "    id, timestamp, time in microseconds, arguments array, client IP "
        "and port,",
        "    client name",
        "LEN",
        "    Return the length of the slow log.",
        "RESET",
        "    Reset the slow log.",
        "HELP",
        "    Print this help.",
    };
    reply_builder.AppendArrayHeader(help.size());
    for (std::string_view line : help) reply_builder.AppendBulkString(line);
    co_return BuiltReply(reply_builder.View());
  }
  if (CmpCaseInsensitive(subcommand, "LEN") && args.size() == 2) {
    const std::size_t length = co_await SlowLogLength();
    co_return BuiltReply(reply_builder.AppendInteger(static_cast<long long>(
        std::min<std::size_t>(length, std::numeric_limits<long long>::max()))));
  }
  if (CmpCaseInsensitive(subcommand, "RESET") && args.size() == 2) {
    const absl::Status reset = co_await ResetSlowLog();
    co_return reset.ok() ? BuiltReply(reply_builder.AppendSimpleString("OK"))
                         : BuiltReply(reply_builder.AppendError(
                               absl::StrCat("ERR ", reset.message())));
  }
  if (CmpCaseInsensitive(subcommand, "GET") && args.size() <= 3) {
    std::size_t count = 10;
    if (args.size() == 3) {
      std::int64_t parsed = 0;
      if (!ParseSlowLogCount(args[2], &parsed) || parsed < -1) {
        co_return BuiltReply(reply_builder.AppendError(
            "ERR count should be greater than or equal to -1"));
      }
      count = parsed == -1 ? std::numeric_limits<std::size_t>::max()
                           : static_cast<std::size_t>(parsed);
    }
    std::vector<SlowLogEntry> entries = co_await CollectSlowLog(count);
    reply_builder.AppendArrayHeader(entries.size());
    for (const SlowLogEntry& entry : entries) {
      reply_builder.AppendArrayHeader(6);
      reply_builder.AppendInteger(static_cast<long long>(entry.id_));
      reply_builder.AppendInteger(entry.unix_time_seconds_);
      reply_builder.AppendInteger(
          static_cast<long long>(std::min<std::uint64_t>(
              entry.duration_micros_, std::numeric_limits<long long>::max())));
      reply_builder.AppendArrayHeader(entry.args_.size());
      for (const std::string& argument : entry.args_) {
        reply_builder.AppendBulkString(argument);
      }
      reply_builder.AppendBulkString(entry.client_address_);
      reply_builder.AppendBulkString(entry.client_name_);
    }
    co_return BuiltReply(reply_builder.View());
  }
  co_return BuiltReply(reply_builder.AppendError(absl::StrCat(
      "ERR unknown subcommand '", subcommand, "'. Try SLOWLOG HELP.")));
}

constexpr std::string_view kSnapshotReadConcurrencyConfig =
    "replication-snapshot-read-concurrency";
constexpr std::string_view kSnapshotBatchSizeConfig =
    "replication-snapshot-batch-size";
constexpr std::string_view kReplicationBacklogSizeConfig = "repl-backlog-size";
constexpr std::string_view kReplicationBacklogBackpressureConfig =
    "replication-backlog-backpressure";
constexpr std::string_view kReplicationPublishQueueConfig =
    "replication-publish-queue-mb-per-worker";
constexpr std::string_view kReplicaPriorityConfig = "replica-priority";
constexpr std::string_view kDefragPausedConfig = "defrag-paused";
constexpr std::string_view kDefragMaxActiveConfig =
    "defrag-max-active-per-device";
constexpr std::string_view kDefragSleepConfig = "defrag-sleep-ms";
constexpr std::string_view kDefragRecordSleepConfig = "defrag-record-sleep-us";
constexpr std::string_view kTombRaiderModeConfig = "tomb-raider-mode";
constexpr std::string_view kTombRaiderIntervalConfig =
    "tomb-raider-interval-ms";
constexpr std::string_view kTombRaiderSleepConfig = "tomb-raider-sleep-ms";
constexpr std::string_view kTombRaiderDailyTimeConfig =
    "tomb-raider-daily-time";
constexpr std::string_view kTxCleanerCooldownConfig = "tx-cleaner-cooldown-ms";
constexpr std::string_view kActiveExpirationIntervalConfig =
    "active-expiration-interval-ms";
constexpr std::string_view kActiveExpirationMapStepsConfig =
    "active-expiration-map-steps-per-cycle";
constexpr std::string_view kActiveExpirationDeletesConfig =
    "active-expiration-deletes-per-cycle";
constexpr std::string_view kActiveExpirationIndexMaintenanceStepsConfig =
    "active-expiration-index-maintenance-steps-per-cycle";
constexpr std::string_view kShutdownCheckpointConfig = "shutdown-checkpoint";
constexpr std::string_view kForegroundBudgetConfig = "foreground-budget-us";
constexpr std::string_view kBackgroundBudgetConfig = "background-budget-us";
constexpr std::string_view kBackgroundWarrantConfig =
    "background-warrant-percent";
constexpr std::string_view kSpdkMaxCompletionsConfig =
    "spdk-max-completions-per-poll";
constexpr std::string_view kStreamNodeMaxEntriesConfig =
    "stream-node-max-entries";
constexpr std::string_view kSlowLogThresholdConfig = "slowlog-log-slower-than";
constexpr std::string_view kSlowLogMaxLenConfig = "slowlog-max-len";
constexpr std::string_view kLuaTimeLimitConfig = "lua-time-limit";
constexpr std::string_view kBusyReplyThresholdConfig = "busy-reply-threshold";
constexpr std::string_view kMaxClientsConfig = "maxclients";
constexpr std::string_view kClientQueryBufferLimitConfig =
    "client-query-buffer-limit";

enum class RuntimeConfigKey : std::uint8_t {
  kSnapshotReadConcurrency,
  kSnapshotBatchSize,
  kReplicationBacklogSize,
  kReplicationBacklogBackpressure,
  kReplicationPublishQueue,
  kReplicaPriority,
  kDefragPaused,
  kDefragMaxActive,
  kDefragSleep,
  kDefragRecordSleep,
  kTombRaiderMode,
  kTombRaiderInterval,
  kTombRaiderSleep,
  kTombRaiderDailyTime,
  kTxCleanerCooldown,
  kActiveExpirationInterval,
  kActiveExpirationMapSteps,
  kActiveExpirationDeletes,
  kActiveExpirationIndexMaintenanceSteps,
  kShutdownCheckpoint,
  kForegroundBudget,
  kBackgroundBudget,
  kBackgroundWarrant,
  kSpdkMaxCompletions,
  kStreamNodeMaxEntries,
  kSlowLogThreshold,
  kSlowLogMaxLen,
  kLuaTimeLimit,
  kMaxClients,
  kClientQueryBufferLimit,
};

struct RuntimeConfigDescriptor {
  std::string_view name_;
  RuntimeConfigKey key_;
};

// CONFIG command metadata only. Execution paths never consult this table;
// each subsystem reads its owning runtime state directly.
constexpr std::array kRuntimeConfigs{
    RuntimeConfigDescriptor{kSnapshotReadConcurrencyConfig,
                            RuntimeConfigKey::kSnapshotReadConcurrency},
    RuntimeConfigDescriptor{kSnapshotBatchSizeConfig,
                            RuntimeConfigKey::kSnapshotBatchSize},
    RuntimeConfigDescriptor{kReplicationBacklogSizeConfig,
                            RuntimeConfigKey::kReplicationBacklogSize},
    RuntimeConfigDescriptor{kReplicationBacklogBackpressureConfig,
                            RuntimeConfigKey::kReplicationBacklogBackpressure},
    RuntimeConfigDescriptor{kReplicationPublishQueueConfig,
                            RuntimeConfigKey::kReplicationPublishQueue},
    RuntimeConfigDescriptor{kReplicaPriorityConfig,
                            RuntimeConfigKey::kReplicaPriority},
    RuntimeConfigDescriptor{kDefragPausedConfig,
                            RuntimeConfigKey::kDefragPaused},
    RuntimeConfigDescriptor{kDefragMaxActiveConfig,
                            RuntimeConfigKey::kDefragMaxActive},
    RuntimeConfigDescriptor{kDefragSleepConfig, RuntimeConfigKey::kDefragSleep},
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
    RuntimeConfigDescriptor{kTxCleanerCooldownConfig,
                            RuntimeConfigKey::kTxCleanerCooldown},
    RuntimeConfigDescriptor{kActiveExpirationIntervalConfig,
                            RuntimeConfigKey::kActiveExpirationInterval},
    RuntimeConfigDescriptor{kActiveExpirationMapStepsConfig,
                            RuntimeConfigKey::kActiveExpirationMapSteps},
    RuntimeConfigDescriptor{kActiveExpirationDeletesConfig,
                            RuntimeConfigKey::kActiveExpirationDeletes},
    RuntimeConfigDescriptor{
        kActiveExpirationIndexMaintenanceStepsConfig,
        RuntimeConfigKey::kActiveExpirationIndexMaintenanceSteps},
    RuntimeConfigDescriptor{kShutdownCheckpointConfig,
                            RuntimeConfigKey::kShutdownCheckpoint},
    RuntimeConfigDescriptor{kForegroundBudgetConfig,
                            RuntimeConfigKey::kForegroundBudget},
    RuntimeConfigDescriptor{kBackgroundBudgetConfig,
                            RuntimeConfigKey::kBackgroundBudget},
    RuntimeConfigDescriptor{kBackgroundWarrantConfig,
                            RuntimeConfigKey::kBackgroundWarrant},
    RuntimeConfigDescriptor{kSpdkMaxCompletionsConfig,
                            RuntimeConfigKey::kSpdkMaxCompletions},
    RuntimeConfigDescriptor{kStreamNodeMaxEntriesConfig,
                            RuntimeConfigKey::kStreamNodeMaxEntries},
    RuntimeConfigDescriptor{kSlowLogThresholdConfig,
                            RuntimeConfigKey::kSlowLogThreshold},
    RuntimeConfigDescriptor{kSlowLogMaxLenConfig,
                            RuntimeConfigKey::kSlowLogMaxLen},
    RuntimeConfigDescriptor{kLuaTimeLimitConfig,
                            RuntimeConfigKey::kLuaTimeLimit},
    RuntimeConfigDescriptor{kBusyReplyThresholdConfig,
                            RuntimeConfigKey::kLuaTimeLimit},
    RuntimeConfigDescriptor{kMaxClientsConfig, RuntimeConfigKey::kMaxClients},
    RuntimeConfigDescriptor{kClientQueryBufferLimitConfig,
                            RuntimeConfigKey::kClientQueryBufferLimit},
};

absl::StatusOr<std::uint32_t> ParseDailySecond(std::string_view text);
std::string FormatDailySecond(std::uint32_t daily_second);
std::string_view TombRaiderModeName(storage::TombRaiderMode mode);

std::optional<storage::ActiveExpirationConfigKey> ActiveExpirationKey(
    RuntimeConfigKey key) {
  using Key = storage::ActiveExpirationConfigKey;
  switch (key) {
    case RuntimeConfigKey::kActiveExpirationInterval:
      return Key::kIntervalMs;
    case RuntimeConfigKey::kActiveExpirationMapSteps:
      return Key::kMapStepsPerCycle;
    case RuntimeConfigKey::kActiveExpirationDeletes:
      return Key::kDeletesPerCycle;
    case RuntimeConfigKey::kActiveExpirationIndexMaintenanceSteps:
      return Key::kIndexMaintenanceStepsPerCycle;
    default:
      return std::nullopt;
  }
}

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

Task<absl::Status> ConfigureAllWorkerSchedulers(RuntimeConfigKey key,
                                                unsigned value) {
  for (unsigned worker_id = 0; worker_id < g_storage->worker_count();
       ++worker_id) {
    absl::Status configured =
        co_await SubmitTaskTo(worker_id, [key, value]() -> Task<absl::Status> {
          bycorf::Worker* worker = bycorf::ThisWorker().self_;
          switch (key) {
            case RuntimeConfigKey::kForegroundBudget:
              co_return worker->SetForegroundBudgetUs(value);
            case RuntimeConfigKey::kBackgroundBudget:
              co_return worker->SetBackgroundBudgetUs(value);
            case RuntimeConfigKey::kBackgroundWarrant:
              co_return worker->SetBackgroundWarrantPercent(value);
            case RuntimeConfigKey::kSpdkMaxCompletions:
              worker->SetSpdkMaxCompletionsPerPoll(value);
              co_return absl::OkStatus();
            default:
              co_return absl::InvalidArgumentError(
                  "CONFIG parameter is not a worker scheduler setting");
          }
        });
    if (!configured.ok()) co_return configured;
  }
  co_return absl::OkStatus();
}

Task<CommandReply> ExecuteConfig(const CommandRequest& request,
                                 ReplyBuilder& reply_builder) {
  const auto& args = request.args_;
  if (CmpCaseInsensitive(args[1], "RESETSTAT") && args.size() == 2) {
    const absl::Status reset = co_await ResetCommandMetrics();
    co_return reset.ok() ? BuiltReply(reply_builder.AppendSimpleString("OK"))
                         : BuiltReply(reply_builder.AppendError(
                               absl::StrCat("ERR ", reset.message())));
  }
  if (CmpCaseInsensitive(args[1], "REWRITE") && args.size() == 2) {
    if (g_replication == nullptr) {
      co_return BuiltReply(
          reply_builder.AppendError("ERR replication backend is unavailable"));
    }
    const ReplicationStatus replication = co_await g_replication->Observe();
    if (replication.redis_sources_.size() > 1) {
      co_return BuiltReply(reply_builder.AppendError(
          "ERR CONFIG REWRITE cannot persist multiple Redis Cluster "
          "upstreams"));
    }
    const bool redis_upstream = !replication.redis_sources_.empty();
    const absl::Status rewritten = RewriteRedisConfigFile(
        g_server_config_file, replication.upstream_, redis_upstream,
        g_replication->replica_priority());
    co_return rewritten.ok()
        ? BuiltReply(reply_builder.AppendSimpleString("OK"))
        : BuiltReply(reply_builder.AppendError(
              absl::StrCat("ERR ", rewritten.message())));
  }
  if (CmpCaseInsensitive(args[1], "GET") && args.size() == 3) {
    const std::string pattern = NormalizeConfigPattern(args[2]);
    std::vector<const RuntimeConfigDescriptor*> matches;
    matches.reserve(kRuntimeConfigs.size());
    for (const RuntimeConfigDescriptor& config : kRuntimeConfigs) {
      if ((config.key_ == RuntimeConfigKey::kSnapshotReadConcurrency ||
           config.key_ == RuntimeConfigKey::kSnapshotBatchSize ||
           config.key_ == RuntimeConfigKey::kReplicationBacklogSize ||
           config.key_ == RuntimeConfigKey::kReplicationBacklogBackpressure ||
           config.key_ == RuntimeConfigKey::kReplicationPublishQueue ||
           config.key_ == RuntimeConfigKey::kReplicaPriority) &&
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
        case RuntimeConfigKey::kSnapshotBatchSize:
          return std::to_string(g_replication->snapshot_batch_size());
        case RuntimeConfigKey::kReplicationBacklogSize:
          return std::to_string(g_replication->backlog_size_bytes());
        case RuntimeConfigKey::kReplicationBacklogBackpressure:
          return g_replication->backlog_backpressure() ? "yes" : "no";
        case RuntimeConfigKey::kReplicationPublishQueue:
          return std::to_string(
              g_replication->publish_queue_bytes_per_worker() /
              (1024ULL * 1024));
        case RuntimeConfigKey::kReplicaPriority:
          return std::to_string(g_replication->replica_priority());
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
        case RuntimeConfigKey::kTxCleanerCooldown:
          return std::to_string(g_storage->TxCleanerCooldownMs());
        case RuntimeConfigKey::kActiveExpirationInterval:
        case RuntimeConfigKey::kActiveExpirationMapSteps:
        case RuntimeConfigKey::kActiveExpirationDeletes:
        case RuntimeConfigKey::kActiveExpirationIndexMaintenanceSteps:
          return std::to_string(g_storage->ActiveExpirationConfigValue(
              *ActiveExpirationKey(key)));
        case RuntimeConfigKey::kShutdownCheckpoint:
          return g_storage->ShutdownCheckpointEnabled() ? "yes" : "no";
        case RuntimeConfigKey::kForegroundBudget:
          return std::to_string(
              bycorf::ThisWorker().self_->foreground_budget_us());
        case RuntimeConfigKey::kBackgroundBudget:
          return std::to_string(
              bycorf::ThisWorker().self_->background_budget_us());
        case RuntimeConfigKey::kBackgroundWarrant:
          return std::to_string(
              bycorf::ThisWorker().self_->background_warrant_percent());
        case RuntimeConfigKey::kSpdkMaxCompletions:
          return std::to_string(
              bycorf::ThisWorker().self_->spdk_max_completions_per_poll());
        case RuntimeConfigKey::kStreamNodeMaxEntries:
          return std::to_string(StreamNodeMaxEntries());
        case RuntimeConfigKey::kSlowLogThreshold:
          return std::to_string(SlowLogThresholdMicros());
        case RuntimeConfigKey::kSlowLogMaxLen:
          return std::to_string(SlowLogMaxLen());
        case RuntimeConfigKey::kLuaTimeLimit:
          return std::to_string(LuaScriptBusyThresholdMs());
        case RuntimeConfigKey::kMaxClients:
          return std::to_string(g_client_limit->max_clients());
        case RuntimeConfigKey::kClientQueryBufferLimit:
          return std::to_string(g_client_limit->client_query_buffer_limit());
      }
      return {};
    };
    reply_builder.AppendMapHeader(matches.size());
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
      co_return BuiltReply(reply_builder.AppendError(
          absl::StrCat("ERR Unsupported CONFIG parameter: ", args[2])));
    }
    absl::Status configured;
    std::uint64_t value = 0;
    if (config->key_ == RuntimeConfigKey::kSnapshotReadConcurrency) {
      if (g_replication == nullptr) {
        configured =
            absl::FailedPreconditionError("replication backend is unavailable");
      } else if (!ParseUint64(args[3], &value) ||
                 value > std::numeric_limits<unsigned>::max()) {
        configured = absl::InvalidArgumentError(
            "value is not an integer or out of range");
      } else {
        configured =
            co_await g_replication->ApplyDirective(ReplicationDirective{
                .kind_ = ReplicationDirective::Kind::kSnapshotReadConcurrency,
                .upstream_ = std::nullopt,
                .value_ = value,
            });
      }
    } else if (config->key_ == RuntimeConfigKey::kSnapshotBatchSize) {
      if (g_replication == nullptr) {
        configured =
            absl::FailedPreconditionError("replication backend is unavailable");
      } else if (!ParseUint64(args[3], &value) || value == 0 ||
                 value > kMaxReplicationSnapshotBatchSize) {
        configured = absl::InvalidArgumentError(
            "value is not an integer or out of range");
      } else {
        configured =
            co_await g_replication->ApplyDirective(ReplicationDirective{
                .kind_ = ReplicationDirective::Kind::kSnapshotBatchSize,
                .upstream_ = std::nullopt,
                .value_ = value,
            });
      }
    } else if (config->key_ == RuntimeConfigKey::kReplicationBacklogSize) {
      if (g_replication == nullptr) {
        configured =
            absl::FailedPreconditionError("replication backend is unavailable");
      } else {
        auto bytes = ParseMemorySize(args[3]);
        if (!bytes.ok()) {
          configured = bytes.status();
        } else {
          configured =
              co_await g_replication->ApplyDirective(ReplicationDirective{
                  .kind_ = ReplicationDirective::Kind::kBacklogBytes,
                  .upstream_ = std::nullopt,
                  .value_ = *bytes,
              });
        }
      }
    } else if (config->key_ ==
               RuntimeConfigKey::kReplicationBacklogBackpressure) {
      const std::optional<bool> enabled = ParseConfigYesNo(args[3]);
      if (g_replication == nullptr) {
        configured =
            absl::FailedPreconditionError("replication backend is unavailable");
      } else if (!enabled.has_value()) {
        configured = absl::InvalidArgumentError("value must be 'yes' or 'no'");
      } else {
        configured =
            co_await g_replication->ApplyDirective(ReplicationDirective{
                .kind_ = ReplicationDirective::Kind::kBacklogBackpressure,
                .upstream_ = std::nullopt,
                .value_ = *enabled ? 1ULL : 0ULL,
            });
      }
    } else if (config->key_ == RuntimeConfigKey::kReplicationPublishQueue) {
      constexpr std::uint64_t kMiB = 1024ULL * 1024;
      if (g_replication == nullptr) {
        configured =
            absl::FailedPreconditionError("replication backend is unavailable");
      } else if (!ParseUint64(args[3], &value) || value == 0 ||
                 value > std::numeric_limits<std::size_t>::max() / kMiB) {
        configured = absl::InvalidArgumentError(
            "value is not a positive MiB integer or is out of range");
      } else {
        configured =
            co_await g_replication->ApplyDirective(ReplicationDirective{
                .kind_ = ReplicationDirective::Kind::kPublishQueueBytes,
                .upstream_ = std::nullopt,
                .value_ = value * kMiB,
            });
      }
    } else if (config->key_ == RuntimeConfigKey::kReplicaPriority) {
      if (g_replication == nullptr) {
        configured =
            absl::FailedPreconditionError("replication backend is unavailable");
      } else if (!ParseUint64(args[3], &value) ||
                 value > std::numeric_limits<unsigned>::max()) {
        configured = absl::InvalidArgumentError(
            "value is not an integer or out of range");
      } else {
        configured =
            co_await g_replication->ApplyDirective(ReplicationDirective{
                .kind_ = ReplicationDirective::Kind::kReplicaPriority,
                .upstream_ = std::nullopt,
                .value_ = value,
            });
      }
    } else if (config->key_ == RuntimeConfigKey::kMaxClients) {
      if (!ParseUint64(args[3], &value) || value == 0) {
        configured = absl::InvalidArgumentError(
            "value is not a positive integer or is out of range");
      } else {
        configured = g_client_limit->SetMaxClients(value);
      }
    } else if (config->key_ == RuntimeConfigKey::kClientQueryBufferLimit) {
      auto bytes = ParseClientQueryBufferLimit(args[3]);
      configured = bytes.ok()
                       ? g_client_limit->SetClientQueryBufferLimit(*bytes)
                       : bytes.status();
    } else if (config->key_ == RuntimeConfigKey::kDefragPaused) {
      const std::optional<bool> paused = ParseConfigYesNo(args[3]);
      if (!paused.has_value()) {
        configured = absl::InvalidArgumentError("value must be 'yes' or 'no'");
      } else {
        configured =
            co_await g_storage->ConfigureDefrag(storage::DefragConfigUpdate{
                .action_ = *paused ? storage::DefragConfigAction::kPause
                                   : storage::DefragConfigAction::kResume});
      }
    } else if (config->key_ == RuntimeConfigKey::kDefragMaxActive) {
      if (!ParseUint64(args[3], &value)) {
        configured = absl::InvalidArgumentError(
            "value is not an integer or out of range");
      } else {
        configured =
            co_await g_storage->ConfigureDefrag(storage::DefragConfigUpdate{
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
        configured =
            co_await g_storage->ConfigureDefrag(storage::DefragConfigUpdate{
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
    } else if (const auto expiration_key = ActiveExpirationKey(config->key_);
               expiration_key.has_value()) {
      if (!ParseUint64(args[3], &value)) {
        configured = absl::InvalidArgumentError(
            "value is not an integer or out of range");
      } else {
        configured =
            g_storage->ConfigureActiveExpiration(*expiration_key, value);
      }
    } else if (config->key_ == RuntimeConfigKey::kTxCleanerCooldown) {
      if (!ParseUint64(args[3], &value) ||
          value > std::numeric_limits<std::uint32_t>::max()) {
        configured = absl::InvalidArgumentError(
            "value is not an integer or out of range");
      } else {
        configured = g_storage->ConfigureTxCleanerCooldown(value);
      }
    } else if (config->key_ == RuntimeConfigKey::kShutdownCheckpoint) {
      const std::optional<bool> enabled = ParseConfigYesNo(args[3]);
      if (!enabled.has_value()) {
        configured = absl::InvalidArgumentError("value must be 'yes' or 'no'");
      } else {
        g_storage->ConfigureShutdownCheckpoint(*enabled);
      }
    } else if (config->key_ == RuntimeConfigKey::kForegroundBudget ||
               config->key_ == RuntimeConfigKey::kBackgroundBudget ||
               config->key_ == RuntimeConfigKey::kBackgroundWarrant ||
               config->key_ == RuntimeConfigKey::kSpdkMaxCompletions) {
      const bool allow_zero =
          config->key_ == RuntimeConfigKey::kSpdkMaxCompletions;
      if (!ParseUint64(args[3], &value) || (!allow_zero && value == 0) ||
          value > std::numeric_limits<unsigned>::max() ||
          (config->key_ == RuntimeConfigKey::kBackgroundWarrant &&
           value > 100)) {
        configured = absl::InvalidArgumentError(
            "value is not an integer or out of range");
      } else {
        configured = co_await ConfigureAllWorkerSchedulers(
            config->key_, static_cast<unsigned>(value));
      }
    } else if (config->key_ == RuntimeConfigKey::kStreamNodeMaxEntries) {
      if (!ParseUint64(args[3], &value)) {
        configured = absl::InvalidArgumentError(
            "value is not an integer or out of range");
      } else {
        configured = SetStreamNodeMaxEntries(value);
      }
    } else if (config->key_ == RuntimeConfigKey::kSlowLogThreshold) {
      std::int64_t threshold = 0;
      if (!ParseSlowLogCount(args[3], &threshold) || threshold < -1) {
        configured = absl::InvalidArgumentError(
            "value is not an integer or out of range");
      } else {
        configured = co_await ConfigureSlowLogThreshold(threshold);
      }
    } else if (config->key_ == RuntimeConfigKey::kSlowLogMaxLen) {
      if (!ParseUint64(args[3], &value) ||
          value > std::numeric_limits<std::size_t>::max()) {
        configured = absl::InvalidArgumentError(
            "value is not an integer or out of range");
      } else {
        configured =
            co_await ConfigureSlowLogMaxLen(static_cast<std::size_t>(value));
      }
    } else if (config->key_ == RuntimeConfigKey::kLuaTimeLimit) {
      if (!ParseUint64(args[3], &value)) {
        configured = absl::InvalidArgumentError(
            "value is not an integer or out of range");
      } else {
        SetLuaScriptBusyThresholdMs(value);
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
struct alignas(64) WorkerCommandGates {
  std::array<std::atomic<std::uint64_t>, storage::kLogicalDatabaseCount>
      db_states_{};
  std::atomic<std::uint64_t> snapshot_transaction_state_{0};
};

static_assert(alignof(WorkerCommandGates) == 64);
static_assert(sizeof(WorkerCommandGates) % 64 == 0);

std::array<WorkerCommandGates, storage::kLogicalStorageShards>
    g_worker_command_gates{};

class ReplicationTransactionOrderGate {
 private:
  struct Waiter {
    bycorf::Worker* worker_ = nullptr;
    std::coroutine_handle<> handle_{};
    Waiter* next_ = nullptr;
  };

 public:
  class Awaiter {
   public:
    Awaiter(ReplicationTransactionOrderGate* gate, bycorf::Worker* worker)
        : gate_(gate) {
      waiter_.worker_ = worker;
    }

    bool await_ready() const noexcept { return false; }
    bool await_suspend(std::coroutine_handle<> handle) noexcept {
      return gate_->AcquireOrQueue(&waiter_, handle);
    }
    void await_resume() const noexcept {}

   private:
    ReplicationTransactionOrderGate* gate_ = nullptr;
    Waiter waiter_;
  };

  Awaiter Acquire(bycorf::Worker& worker) noexcept {
    return Awaiter(this, &worker);
  }

  bool TryAcquire() noexcept {
    Lock();
    if (held_) {
      Unlock();
      return false;
    }
    held_ = true;
    Unlock();
    return true;
  }

  void Release() noexcept {
    Lock();
    Waiter* wake = waiters_head_;
    if (wake == nullptr) {
      held_ = false;
      Unlock();
      return;
    }
    waiters_head_ = wake->next_;
    if (waiters_head_ == nullptr) waiters_tail_ = nullptr;
    // Keep held_ set while handing ownership to the FIFO head. A newcomer
    // must queue behind it even if the resumed coroutine has not run yet.
    Unlock();

    const bycorf::CurrentWorker& current = bycorf::ThisWorker();
    if (wake->worker_->id() == current.id_) {
      wake->worker_->Enqueue(wake->handle_);
    } else {
      bycorf::PostNotification(
          current.cross_core_, wake->worker_->id(),
          bycorf::RemoteNotification{
              .context_ = wake->worker_,
              .value_ = static_cast<std::uint64_t>(
                  reinterpret_cast<std::uintptr_t>(wake->handle_.address())),
              .run_fn_ = &ReplicationTransactionOrderGate::ResumeRemote,
          });
    }
  }

 private:
  static void ResumeRemote(void* context, std::uint64_t value) noexcept {
    static_cast<bycorf::Worker*>(context)->Enqueue(
        std::coroutine_handle<>::from_address(
            reinterpret_cast<void*>(static_cast<std::uintptr_t>(value))));
  }

  bool AcquireOrQueue(Waiter* waiter, std::coroutine_handle<> handle) noexcept {
    waiter->handle_ = handle;
    waiter->next_ = nullptr;
    Lock();
    if (!held_) {
      held_ = true;
      Unlock();
      return false;
    }
    if (waiters_tail_ == nullptr) {
      waiters_head_ = waiter;
    } else {
      waiters_tail_->next_ = waiter;
    }
    waiters_tail_ = waiter;
    Unlock();
    return true;
  }

  void Lock() noexcept {
    while (lock_.test_and_set(std::memory_order_acquire)) {
#if defined(__x86_64__) || defined(__i386__)
      __builtin_ia32_pause();
#elif defined(__aarch64__) || defined(__arm__)
      asm volatile("yield" ::: "memory");
#else
      std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
    }
  }

  void Unlock() noexcept { lock_.clear(std::memory_order_release); }

  std::atomic_flag lock_ = ATOMIC_FLAG_INIT;
  bool held_ = false;
  Waiter* waiters_head_ = nullptr;
  Waiter* waiters_tail_ = nullptr;
};

ReplicationTransactionOrderGate g_replication_transaction_order;

unsigned DbGateWorkerCount() noexcept {
  constexpr unsigned kMaxDbGateWorkers = storage::kLogicalStorageShards;
  if (g_storage != nullptr) {
    return std::max(1U, std::min(g_storage->worker_count(), kMaxDbGateWorkers));
  }
  return std::max(1U, std::min(g_server_threads, kMaxDbGateWorkers));
}

std::atomic<std::uint64_t>& LocalDbGate(std::uint8_t db_id) noexcept {
  const unsigned worker = ThisWorker().id_;
  assert(worker < DbGateWorkerCount());
  assert(worker < storage::kLogicalStorageShards);
  return g_worker_command_gates[worker].db_states_[db_id];
}

void OpenDbGateWorker(std::uint8_t db_id, unsigned worker) noexcept {
  g_worker_command_gates[worker].db_states_[db_id].fetch_and(
      ~kDbGateClosed, std::memory_order_acq_rel);
}

bool DbGateHasActiveOperations(std::uint8_t db_id) noexcept {
  const unsigned workers = DbGateWorkerCount();
  for (unsigned worker = 0; worker < workers; ++worker) {
    if ((g_worker_command_gates[worker].db_states_[db_id].load(
             std::memory_order_acquire) &
         kDbGateCountMask) != 0) {
      return true;
    }
  }
  return false;
}

bool TryBeginDbOperation(std::uint8_t db_id) noexcept {
  auto& gate = LocalDbGate(db_id);
  std::uint64_t state = gate.load(std::memory_order_acquire);
  while ((state & kDbGateClosed) == 0) {
    if (gate.compare_exchange_weak(state, state + 1, std::memory_order_acq_rel,
                                   std::memory_order_acquire)) {
      return true;
    }
  }
  return false;
}

#if LAVIK_FAULTS_ENABLED
// InitStorage resolves this before worker threads start. Fault-enabled writes
// use the cached pointer instead of getenv or a function-local static guard;
// ordinary Release builds omit both the hook and its coroutine call sites.
const char* g_command_pause_before_db_admission = nullptr;

// Deterministic E2E hook for widening the role-change window after write
// admission. Changing the test-only environment variable at runtime is not
// supported.
Task<absl::Status> MaybePauseBeforeCommandDbAdmission() {
  const char* configured = g_command_pause_before_db_admission;
  if (configured == nullptr) co_return absl::OkStatus();
  std::uint64_t milliseconds = 0;
  const std::size_t length = std::strlen(configured);
  const auto parsed =
      std::from_chars(configured, configured + length, milliseconds);
  static std::atomic<bool> pause_used = false;
  if (parsed.ec != std::errc{} || parsed.ptr != configured + length ||
      milliseconds == 0 || milliseconds > 60000 ||
      pause_used.exchange(true, std::memory_order_acq_rel)) {
    co_return absl::OkStatus();
  }
  spdlog::info("client command admitted; pausing before database admission");
  co_return co_await bycorf::SleepFor(*ThisWorker().self_,
                                      std::chrono::milliseconds(milliseconds));
}
#endif

void EndDbOperation(std::uint8_t db_id) noexcept {
  LocalDbGate(db_id).fetch_sub(1, std::memory_order_acq_rel);
}

bool CloseDbGate(std::uint8_t db_id) noexcept {
  std::array<unsigned, storage::kLogicalStorageShards> closed{};
  std::size_t closed_count = 0;
  const unsigned workers = DbGateWorkerCount();
  for (unsigned worker = 0; worker < workers; ++worker) {
    auto& gate = g_worker_command_gates[worker].db_states_[db_id];
    std::uint64_t expected = gate.load(std::memory_order_acquire);
    while ((expected & kDbGateClosed) == 0) {
      if (gate.compare_exchange_weak(expected, expected | kDbGateClosed,
                                     std::memory_order_acq_rel,
                                     std::memory_order_acquire)) {
        closed[closed_count++] = worker;
        break;
      }
    }
    if ((expected & kDbGateClosed) != 0) {
      for (std::size_t i = 0; i < closed_count; ++i) {
        OpenDbGateWorker(db_id, closed[i]);
      }
      return false;
    }
  }
  return true;
}

void OpenDbGate(std::uint8_t db_id) noexcept {
  // Clear only the closed bit. After a completed drain the count bits are
  // zero anyway; on an early exit (today only worker shutdown) in-flight
  // operations still hold their counts, and zeroing those would let their
  // EndDbOperation underflow the gate into a permanently-closed value.
  const unsigned workers = DbGateWorkerCount();
  for (unsigned worker = 0; worker < workers; ++worker) {
    OpenDbGateWorker(db_id, worker);
  }
}

std::atomic<std::uint64_t>& LocalSnapshotTransactionGate() noexcept {
  const unsigned worker = ThisWorker().id_;
  assert(worker < DbGateWorkerCount());
  assert(worker < storage::kLogicalStorageShards);
  return g_worker_command_gates[worker].snapshot_transaction_state_;
}

void OpenSnapshotTransactionGateWorker(unsigned worker) noexcept {
  g_worker_command_gates[worker].snapshot_transaction_state_.fetch_and(
      ~kDbGateClosed, std::memory_order_acq_rel);
}

class SnapshotTransactionOperationGuard {
 public:
  SnapshotTransactionOperationGuard() = default;
  SnapshotTransactionOperationGuard(const SnapshotTransactionOperationGuard&) =
      delete;
  SnapshotTransactionOperationGuard& operator=(
      const SnapshotTransactionOperationGuard&) = delete;
  ~SnapshotTransactionOperationGuard() {
    if (active_) EndSnapshotTransaction();
  }

  void Activate() noexcept { active_ = true; }

 private:
  bool active_ = false;
};

class ReplicationTransactionOrderGuard {
 public:
  ReplicationTransactionOrderGuard() = default;
  ReplicationTransactionOrderGuard(const ReplicationTransactionOrderGuard&) =
      delete;
  ReplicationTransactionOrderGuard& operator=(
      const ReplicationTransactionOrderGuard&) = delete;
  ~ReplicationTransactionOrderGuard() { Release(); }

  void Activate() noexcept { active_.store(true, std::memory_order_release); }
  bool active() const noexcept {
    return active_.load(std::memory_order_acquire);
  }
  void Release() noexcept {
    if (!active_.exchange(false, std::memory_order_acq_rel)) return;
    EndReplicationTransactionOrder();
  }

 private:
  std::atomic<bool> active_{false};
};

// Joins coroutines spawned on this worker. They all complete back on that
// same thread, so the counter needs no atomics.
struct WorkerJoin {
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
      WorkerJoin* join_;
      bool await_ready() const { return join_->pending_ == 0; }
      void await_suspend(std::coroutine_handle<> handle) {
        join_->waiter_ = handle;
      }
      void await_resume() const {}
    };
    return Awaiter{this};
  }
};

// The participant that is this worker runs inline: dispatching to the thread
// we are already on would only buy a coroutine frame and a trip through that
// worker's own reply lane.
template <typename Step>
Task<absl::Status> RunJoinedStep(unsigned worker, Step step, WorkerJoin* join) {
  absl::Status status;
  // GCC 13 can reuse the coroutine-frame slot incorrectly when both arms of a
  // conditional expression suspend, so keep the two awaiters disjoint.
  if (worker == ThisWorker().id_) {
    status = co_await step();
  } else {
    status = co_await SubmitTaskTo(worker, std::move(step));
  }
  join->Complete(std::move(status));
  co_return absl::OkStatus();
}

// Precondition on `step_at`: a step touches only its own worker's state, so
// none waits on another and none depends on the order the others run in. That
// is what makes one round legal in place of `count` sequential round trips.
//
// Errors aggregate rather than short-circuit, because a step that never runs
// leaves its worker's state stranded — a held admission reservation, an
// un-erased undo journal — with no later chance to settle it.
//
// Settlement only. A loop that can block on another participant (publisher
// admission acquisition) must stay sequential and ordered; see there.
template <typename StepAt>
Task<absl::Status> ForEachParticipantParallel(std::size_t count,
                                              StepAt step_at) {
  if (count == 0) co_return absl::OkStatus();
  if (count == 1) {
    // A spawn and a join to make one call is a net loss, and single-key
    // writes reach these loops with exactly one participant.
    auto [worker, step] = step_at(std::size_t{0});
    if (worker == ThisWorker().id_) {
      co_return co_await step();
    }
    co_return co_await SubmitTaskTo(worker, std::move(step));
  }
  WorkerJoin join;
  join.pending_ = count;
  for (std::size_t index = 0; index < count; ++index) {
    auto [worker, step] = step_at(index);
    SpawnOnCurrentWorker(RunJoinedStep(worker, std::move(step), &join));
  }
  co_await join.Join();
  co_return std::move(join.error_);
}

struct ReplicationPublisherAdmission {
  struct WorkerToken {
    unsigned worker_ = 0;
    storage::ReplicationPublisherAdmission token_;
  };

  std::size_t logical_bytes_ = 0;
  std::vector<WorkerToken> worker_tokens_;
};

thread_local const ReplicationPublisherAdmission*
    g_active_replication_publisher_admission = nullptr;

class ActivePublisherAdmissionGuard {
 public:
  explicit ActivePublisherAdmissionGuard(
      const ReplicationPublisherAdmission* admission)
      : previous_(g_active_replication_publisher_admission) {
    g_active_replication_publisher_admission = admission;
  }
  ActivePublisherAdmissionGuard(const ActivePublisherAdmissionGuard&) = delete;
  ActivePublisherAdmissionGuard& operator=(
      const ActivePublisherAdmissionGuard&) = delete;
  ~ActivePublisherAdmissionGuard() {
    g_active_replication_publisher_admission = previous_;
  }

 private:
  const ReplicationPublisherAdmission* previous_;
};

std::size_t FullSyncReplacementAdmissionBytes(
    const CommandRequest& request) noexcept {
  if (request.spec_ == nullptr) return 0;
  const auto keys = DetermineKeys(*request.spec_, request.args_);
  if (!keys.ok() || keys->empty()) return 0;
  std::size_t bytes = 0;
  for (std::uint16_t index = keys->first_; index <= keys->last_;
       index = static_cast<std::uint16_t>(index + keys->step_)) {
    const std::size_t key_bytes = request.args_[index].size();
    const std::size_t identity =
        SaturatingAdd(storage::kFullSyncReplacementMetadataBytes,
                      key_bytes > std::numeric_limits<std::size_t>::max() / 2
                          ? std::numeric_limits<std::size_t>::max()
                          : key_bytes * 2);
    bytes = SaturatingAdd(bytes, identity);
    if (keys->last_ - index < keys->step_) break;
  }
  return bytes;
}

std::size_t ReplicationEventAdmissionBytes(
    const CommandRequest& request) noexcept {
  std::size_t bytes = CanonicalCommandBytes(request);
  if (request.spec_ == nullptr) return bytes;
  const auto keys = DetermineKeys(*request.spec_, request.args_);
  if (!keys.ok()) {
    return SaturatingAdd(bytes, RequestArgumentBytes(request));
  }
  for (std::uint16_t index = keys->first_;
       !keys->empty() && index <= keys->last_;
       index = static_cast<std::uint16_t>(index + keys->step_)) {
    // A committed command may append a final PERSIST/PEXPIREAT after-image.
    // Reserve for the duplicated key and its command envelope before the
    // mutation; the actual canonical event is usually smaller.
    bytes =
        SaturatingAdd(bytes, SaturatingAdd(request.args_[index].size(), 64));
    if (keys->last_ - index < keys->step_) break;
  }
  return bytes;
}

// Discharges the fan-out precondition: a release only decrements its own
// worker's admitted-bytes waterline and full-sync session credit. Releasing
// every participant even when one fails is the point — an early return used to
// leave the rest reserved for the life of the process.
//
// Not a coroutine on purpose: forwarding the task costs one frame where
// `co_return co_await` costs two, and a single-key write reaches this on the
// collapsed write hop. Callers must keep `admission` alive across the await.
Task<absl::Status> ReleaseReplicationPublisherAdmission(
    const ReplicationPublisherAdmission& admission) {
  return ForEachParticipantParallel(
      admission.worker_tokens_.size(), [&admission](std::size_t index) {
        const auto& worker_token = admission.worker_tokens_[index];
        return std::pair{
            worker_token.worker_,
            [token = worker_token.token_,
             bytes = admission.logical_bytes_]() -> Task<absl::Status> {
              g_storage->ReleaseReplicationPublisherAdmission(token, bytes);
              co_return absl::OkStatus();
            }};
      });
}

Task<absl::StatusOr<ReplicationPublisherAdmission>>
AcquireReplicationPublisherAdmission(std::size_t logical_bytes,
                                     const CommandRequest* request = nullptr,
                                     bool parallel_fanout = false) {
  ReplicationPublisherAdmission admission;
  if (request != nullptr) {
    logical_bytes = SaturatingAdd(logical_bytes,
                                  FullSyncReplacementAdmissionBytes(*request));
  }
  admission.logical_bytes_ = std::max<std::size_t>(logical_bytes, 1);
  struct WorkerScope {
    unsigned worker_ = 0;
    std::optional<storage::ReplicationPublisherTarget> target_;
  };
  std::vector<WorkerScope> scopes;
  if (request != nullptr && request->spec_ != nullptr &&
      (request->spec_->flags_ & kCmdGlobal) == 0) {
    const auto keys = DetermineKeys(*request->spec_, request->args_);
    if (keys.ok() && !keys->empty()) {
      if (keys->count() == 1) {
        const std::string& key = request->args_[keys->first_];
        const bool routed = request->HasRoutedPartitionFor(keys->first_);
        const std::uint16_t partition_id =
            routed ? request->RoutedPartitionId() : storage::RedisSlot(key);
        scopes.push_back(WorkerScope{
            .worker_ = partition_id % g_storage->worker_count(),
            .target_ =
                storage::ReplicationPublisherTarget{
                    .partition_id_ = partition_id,
                    .db_id_ = request->db_id_,
                },
        });
      } else {
        std::vector<bool> included(g_storage->worker_count());
        for (std::uint16_t index = keys->first_; index <= keys->last_;
             index = static_cast<std::uint16_t>(index + keys->step_)) {
          const unsigned worker = ShardForKey(request->args_[index]);
          if (!included[worker]) {
            included[worker] = true;
            // Multi-key commands reserve conservatively for every active
            // full-sync session on each actual participant worker. Outcome-
            // dependent destination DBs are resolved inside their handlers.
            scopes.push_back(
                WorkerScope{.worker_ = worker, .target_ = std::nullopt});
          }
          if (keys->last_ - index < keys->step_) break;
        }
      }
    }
  }
  if (scopes.empty()) {
    scopes.reserve(g_storage->worker_count());
    for (unsigned worker = 0; worker < g_storage->worker_count(); ++worker) {
      scopes.push_back(WorkerScope{.worker_ = worker, .target_ = std::nullopt});
    }
  }
  // Sequential and ordered by worker id, deliberately. Acquiring can block — a
  // FIFO ticket, then a wait on that worker's publish-queue capacity — and the
  // command holds every reservation it already took while it waits. One global
  // order is what stops two concurrent multi-key writes from each holding a
  // reservation the other waits on. Fanning this out would drop that order and
  // reintroduce the cycle; the release and undo loops around it are parallel
  // only because nothing in them can block on another participant.
  std::sort(scopes.begin(), scopes.end(),
            [](const WorkerScope& left, const WorkerScope& right) {
              return left.worker_ < right.worker_;
            });
  if (parallel_fanout && scopes.size() > 1) {
    // The caller owns the cross-flow transaction-order slot, so no second
    // multi-worker acquisition can hold a conflicting subset while this
    // round is suspended. Single-worker writers cannot form a reservation
    // cycle. Run the independent owner-local waits concurrently and join once.
    admission.worker_tokens_.resize(scopes.size());
    absl::Status acquired = co_await ForEachParticipantParallel(
        scopes.size(), [&scopes, &admission](std::size_t index) {
          const WorkerScope& scope = scopes[index];
          return std::pair{
              scope.worker_,
              [scope, index, &admission]() -> Task<absl::Status> {
                auto token =
                    co_await g_storage->AcquireReplicationPublisherAdmission(
                        admission.logical_bytes_, scope.target_);
                if (!token.ok()) co_return token.status();
                admission.worker_tokens_[index] =
                    ReplicationPublisherAdmission::WorkerToken{
                        .worker_ = scope.worker_,
                        .token_ = std::move(*token),
                };
                co_return absl::OkStatus();
              }};
        });
    if (!acquired.ok()) {
      ReplicationPublisherAdmission partial;
      partial.logical_bytes_ = admission.logical_bytes_;
      for (auto& token : admission.worker_tokens_) {
        // log_epoch_ and both vectors are empty only for an unfilled slot.
        if (token.token_.log_epoch_ == 0 &&
            token.token_.fullsync_session_ids_.empty() &&
            token.token_.fullsync_unstarted_guards_.empty()) {
          continue;
        }
        partial.worker_tokens_.push_back(std::move(token));
      }
      (void)co_await ReleaseReplicationPublisherAdmission(partial);
      co_return acquired;
    }
    co_return admission;
  }

  admission.worker_tokens_.reserve(scopes.size());
  for (const WorkerScope& scope : scopes) {
    const unsigned target_worker = scope.worker_;
    auto acquire = [bytes = admission.logical_bytes_,
                    target = scope.target_]() {
      return g_storage->AcquireReplicationPublisherAdmission(bytes, target);
    };
    std::optional<absl::StatusOr<storage::ReplicationPublisherAdmission>> token;
    if (target_worker == ThisWorker().id_) {
      token.emplace(co_await acquire());
    } else {
      token.emplace(co_await SubmitTaskTo(target_worker, acquire));
    }
    if (!token->ok()) {
      // worker_tokens_ holds exactly what was taken, so releasing the whole
      // admission gives back precisely those reservations.
      (void)co_await ReleaseReplicationPublisherAdmission(admission);
      co_return token->status();
    }
    admission.worker_tokens_.push_back(
        ReplicationPublisherAdmission::WorkerToken{
            .worker_ = target_worker,
            .token_ = std::move(**token),
        });
  }
  co_return admission;
}

Task<absl::Status> BeginReplicationTransactionOrder(
    ReplicationTransactionOrderGuard* guard) {
  co_await g_replication_transaction_order.Acquire(*ThisWorker().self_);
  guard->Activate();
  LAVIK_FAULT_INJECT(
      // Test-only hold for the order-gate e2e: once per process, keep the
      // freshly acquired gate for the configured span and log a marker the
      // fixture polls, so the test observes gate admission behaviour instead of
      // guessing with sleeps. The gate's real hold window (until participant
      // markers are queued) is otherwise too short to observe
      // deterministically.
      static std::atomic<bool> order_hold_claimed = false;
      const char* order_hold_text =
          std::getenv("LAVIK_REPLICATION_ORDER_HOLD_MS");
      bool expected_order_hold = false;
      if (order_hold_text != nullptr &&
          order_hold_claimed.compare_exchange_strong(
              expected_order_hold, true, std::memory_order_acq_rel)) {
        char* end = nullptr;
        const unsigned long hold_ms = std::strtoul(order_hold_text, &end, 10);
        if (end != order_hold_text && *end == '\0' && hold_ms != 0) {
          spdlog::warn(
              "LAVIK_REPLICATION_ORDER_HOLD_MS holding the replication order "
              "gate for {} ms",
              hold_ms);
          absl::Status held = co_await bycorf::SleepFor(
              *ThisWorker().self_, std::chrono::milliseconds(hold_ms));
          if (!held.ok()) {
            guard->Release();
            co_return held;
          }
        }
      });
  co_return absl::OkStatus();
}

Task<absl::Status> BeginSnapshotTransaction(
    SnapshotTransactionOperationGuard* guard) {
  while (!TryBeginSnapshotTransaction()) {
    absl::Status waited = co_await bycorf::SleepFor(
        *ThisWorker().self_, std::chrono::milliseconds(1));
    if (!waited.ok()) co_return waited;
  }
  guard->Activate();
  co_return absl::OkStatus();
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
  ~MultiDbOperationGuard() { Release(); }

  void Release() noexcept {
    for (const std::uint8_t db : dbs_) {
      EndDbOperation(db);
    }
    dbs_.clear();
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
  std::array<std::uint64_t, storage::kLogicalDatabaseCount> flushed_epochs{};
  ReplicationTransactionOrderGuard replication_order;
  if (!request.replication_origin_ && g_storage->ReplicationLogActive()) {
    // Acquire before closing any DB gate. A transaction takes this order gate
    // before entering its DB operations; reversing those acquisitions here
    // would deadlock a transaction waiting for FLUSH and FLUSH waiting for the
    // transaction's publication slot.
    detached = co_await BeginReplicationTransactionOrder(&replication_order);
    if (!detached.ok()) {
      co_return BuiltReply(
          reply_builder.AppendError(absl::StrCat("ERR ", detached.message())));
    }
  }
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
      while (DbGateHasActiveOperations(db_id)) {
        absl::Status waited = co_await bycorf::SleepFor(
            *ThisWorker().self_, std::chrono::milliseconds(1));
        if (!waited.ok()) {
          co_return BuiltReply(reply_builder.AppendError(
              absl::StrCat("ERR ", waited.message())));
        }
      }
    }

    if (!CommandWriteAdmissionIsCurrent(request)) {
      co_return BuiltReply(reply_builder.AppendError(
          "TRYAGAIN replication role changed; retry command"));
    }
    if (const char* error = CommandServingGenerationError(request);
        error != nullptr) [[unlikely]] {
      co_return BuiltReply(reply_builder.AppendError(error));
    }
    // FLUSH owns its database gates and therefore returns before the generic
    // command-body authority choke point. Recheck here after every gate wait
    // and retain any group guards through detach/publication.
    cluster::AuthorityInFlightGuards cluster_in_flights;
    if (std::optional<CommandReply> fenced = RecheckClusterWriteAuthority(
            request, reply_builder, &cluster_in_flights);
        fenced.has_value()) {
      co_return std::move(*fenced);
    }

    if (request.kind_ == CommandKind::kFlushAll) {
      detached = co_await g_storage->FlushAllDetach();
      if (detached.ok()) {
        for (std::uint8_t db_id = 0; db_id < storage::kLogicalDatabaseCount;
             ++db_id) {
          flushed_epochs[db_id] = g_storage->DbEpoch(db_id);
        }
      }
    } else {
      const std::uint8_t db_id = dbs.front();
      detached = co_await g_storage->FlushDbDetach(db_id);
      if (detached.ok()) flushed_epochs[db_id] = g_storage->DbEpoch(db_id);
    }

    if (detached.ok() && !request.replication_origin_ &&
        g_storage->ReplicationLogActive()) {
      // Cross-flow transactions and DB barriers share one source publication
      // order. Without this cold-path atomic gate, two concurrent publishers
      // could enqueue A->B on one flow and B->A on another, making the target
      // rendezvous cycle forever.
      if (request.kind_ == CommandKind::kFlushAll) {
        detached =
            co_await g_storage->PublishFlushAllReplication(flushed_epochs);
      } else {
        const std::uint8_t db_id = dbs.front();
        detached = co_await g_storage->PublishFlushDbReplication(
            db_id, flushed_epochs[db_id]);
      }
    }
  }
  // Publication is ordered and every DB gate is open again. Reclamation is
  // detached state cleanup and must not hold the cross-flow ordering slot.
  replication_order.Release();

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
  // COUNT is a work hint, not a latency promise. Partitions are interleaved
  // across workers, so allowing one command to walk an arbitrarily large
  // COUNT can turn into hundreds of serial cross-worker hops. Bound that
  // fan-out per response; the returned cursor still covers every partition.
  constexpr std::size_t kMaxPartitionsPerCall = 256;
  const std::size_t max_partitions_per_call =
      std::clamp<std::size_t>(parsed->count_, 64, kMaxPartitionsPerCall);
  static_assert(storage::kLogicalStorageShards ==
                (std::uint64_t{1} << kPartitionBits));

  const ScanOptions& options = *parsed;
  unsigned partition_id = static_cast<unsigned>(options.cursor_ >> kLocalBits);
  // ScanHashMap's cursor carries the bucket index in its low bits. Production
  // indexes use at most 32 bucket bits, so preserving it verbatim below the
  // 14-bit partition id leaves 18 spare bits without discarding scan state.
  std::uint64_t local_cursor = options.cursor_ & kPackedLocalMask;
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
      scanned = co_await bycorf::SubmitTaskTo(
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
      if ((batch.cursor_ & ~kPackedLocalMask) != 0) {
        co_return BuiltReply(
            reply_builder.AppendError("ERR local scan cursor overflow"));
      }
      const std::uint64_t cursor =
          (static_cast<std::uint64_t>(partition_id) << kLocalBits) |
          batch.cursor_;
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
  co_return co_await bycorf::SubmitTaskTo(
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
            co_await bycorf::Yield(*ThisWorker().self_);
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
  // The shared fault-injection pause makes the admission-to-exclusive-gate
  // generation race deterministic in fault-enabled integration coverage.
  LAVIK_FAULT_INJECT(
      absl::Status paused = co_await MaybePauseBeforeCommandDbAdmission();
      if (!paused.ok()) {
        co_return BuiltReply(reply_builder.AppendError(
            absl::StrCat("ERR database admission failed: ", paused.message())));
      });
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
  while (DbGateHasActiveOperations(db)) {
    absl::Status waited = co_await bycorf::SleepFor(
        *ThisWorker().self_, std::chrono::milliseconds(1));
    if (!waited.ok()) {
      co_return BuiltReply(
          reply_builder.AppendError(absl::StrCat("ERR ", waited.message())));
    }
  }
  // Closing the gate proves that a later population replacement cannot pass
  // us, but this request may have slept before winning that exclusivity. Bind
  // the scan to the generation admitted at dispatch before committing a RESP
  // array header that cannot subsequently be replaced with an error.
  if (const char* error = CommandServingGenerationError(request);
      error != nullptr) [[unlikely]] {
    co_return BuiltReply(reply_builder.AppendError(error));
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
  reply.chunks_ = std::make_unique<ReplyChunkSource>(
      [state]() { return NextKeysChunk(state); });
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
    long double extended_timeout = 0;
    if (ParseRedisLongDouble(text, &extended_timeout) &&
        extended_timeout * 1000.0L >
            static_cast<long double>(
                std::numeric_limits<std::int64_t>::max())) {
      return absl::InvalidArgumentError("timeout is out of range");
    }
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

// The snapshot and its accounting move together across workers. A cursor
// retains the selected tuple until all of its RESP fragments have been sent:
// choosing again at a chunk boundary would change the announced bulk value.
struct RandomStreamPayloadState {
  static constexpr std::size_t kChunkBytes = 1024 * 1024;
  RetainedMemoryCharge state_charge_;
  RetainedMemoryCharge chunk_charge_;
  RetainedMemoryCharge supplemental_snapshot_charge_;
  storage::HashResult snapshot_;
  NegativeRandomStreamOptions options_;
  std::uint64_t remaining_ = 0;
  std::uint64_t population_ = 0;
  enum class Part { kSelect, kHeader, kValue, kTerminator };
  Part part_ = Part::kSelect;
  std::size_t selected_ = 0;
  std::size_t tuple_field_ = 0;
  std::size_t part_offset_ = 0;
  std::array<char, 32> header_{};
  std::size_t header_bytes_ = 0;
};

absl::Status AdoptRandomStreamSnapshot(RandomStreamPayloadState& state,
                                       storage::HashResult snapshot) {
  const std::size_t width = state.options_.with_values_ ? 2 : 1;
  if (snapshot.length_ > std::numeric_limits<std::size_t>::max() / width ||
      snapshot.values_.size() != snapshot.length_ * width)
    return absl::InternalError(
        "random snapshot disagrees with collection length");
  std::size_t retained = sizeof(snapshot) + snapshot.values_.capacity() *
                                                sizeof(snapshot.values_[0]);
  for (const auto& value : snapshot.values_) {
    if (!value)
      return absl::InternalError("random snapshot contains a missing value");
    if (value->capacity() + 1 >
        std::numeric_limits<std::size_t>::max() - retained)
      return absl::ResourceExhaustedError("OOM random snapshot is too large");
    retained += value->capacity() + 1;
  }
  // Modern producers transfer their output charge. The fallback also covers
  // older compact producers, without charging the same buffers twice.
  if (retained > snapshot.retained_charge_.bytes()) {
    const auto additional = retained - snapshot.retained_charge_.bytes();
    auto admission = TryReserveMemory(additional);
    if (!admission) {
      RecordMemoryRejection();
      return absl::ResourceExhaustedError("OOM random snapshot retention");
    }
    state.supplemental_snapshot_charge_.Adopt(&*admission, additional);
  }
  // Account before constructing payloads. Allow an old returned chunk and
  // its successor to overlap while the socket writer advances its coroutine.
  constexpr auto bytes = 2 * (RandomStreamPayloadState::kChunkBytes + 1);
  auto admission = TryReserveMemory(bytes);
  if (!admission) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError("OOM random stream chunk retention");
  }
  state.chunk_charge_.Adopt(&*admission, bytes);
  state.population_ = snapshot.length_;
  state.snapshot_ = std::move(snapshot);
  return absl::OkStatus();
}

absl::StatusOr<std::string> EncodeRandomStreamChunk(
    RandomStreamPayloadState& state) {
  if (state.remaining_ == 0) return std::string();
  try {
    std::string payload;
    payload.reserve(RandomStreamPayloadState::kChunkBytes);
    const std::size_t width = state.options_.with_values_ ? 2 : 1;
    auto begin_bulk = [&] {
      state.header_[0] = '$';
      const auto size =
          state.snapshot_.values_[state.selected_ + state.tuple_field_]->size();
      auto encoded = std::to_chars(state.header_.data() + 1,
                                   state.header_.data() + 29, size);
      assert(encoded.ec == std::errc{});
      *encoded.ptr++ = '\r';
      *encoded.ptr++ = '\n';
      state.header_bytes_ = encoded.ptr - state.header_.data();
      state.part_ = RandomStreamPayloadState::Part::kHeader;
    };
    while (state.remaining_ != 0 &&
           payload.size() < RandomStreamPayloadState::kChunkBytes) {
      if (state.part_ == RandomStreamPayloadState::Part::kSelect) {
        state.selected_ = static_cast<std::size_t>(RandomRank(
                              state.population_, RandomSampleGenerator())) *
                          width;
        state.tuple_field_ = 0;
        begin_bulk();
      }
      std::string_view piece;
      switch (state.part_) {
        case RandomStreamPayloadState::Part::kHeader:
          piece = {state.header_.data(), state.header_bytes_};
          break;
        case RandomStreamPayloadState::Part::kValue:
          piece =
              *state.snapshot_.values_[state.selected_ + state.tuple_field_];
          break;
        case RandomStreamPayloadState::Part::kTerminator:
          piece = "\r\n";
          break;
        case RandomStreamPayloadState::Part::kSelect:
          std::terminate();  // begin_bulk always advances this state.
      }
      const auto copied =
          std::min(piece.size() - state.part_offset_,
                   RandomStreamPayloadState::kChunkBytes - payload.size());
      payload.append(piece.substr(state.part_offset_, copied));
      state.part_offset_ += copied;
      if (state.part_offset_ != piece.size()) continue;
      state.part_offset_ = 0;
      switch (state.part_) {
        case RandomStreamPayloadState::Part::kHeader:
          state.part_ = RandomStreamPayloadState::Part::kValue;
          break;
        case RandomStreamPayloadState::Part::kValue:
          state.part_ = RandomStreamPayloadState::Part::kTerminator;
          break;
        case RandomStreamPayloadState::Part::kTerminator:
          if (++state.tuple_field_ == width) {
            --state.remaining_;
            state.part_ = RandomStreamPayloadState::Part::kSelect;
          } else {
            begin_bulk();
          }
          break;
        case RandomStreamPayloadState::Part::kSelect:
          std::terminate();
      }
    }
    return payload;
  } catch (const std::bad_alloc&) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError("OOM random stream chunk");
  }
}

// Keep one immutable command-time view without retaining a DB gate or key
// lock across socket backpressure. Destroying the chunk source on cancellation
// also destroys the snapshot, its charges and any partially emitted tuple.
struct NegativeRandomStreamState : RandomStreamPayloadState {
  NegativeRandomStreamState(std::uint8_t db_id,
                            NegativeRandomStreamOptions stream_options,
                            std::string key, unsigned owner)
      : db_(db_id),
        key_(std::move(key)),
        owner_(owner),
        digest_(storage::ComputeDigest(key_)) {
    options_ = stream_options;
    remaining_ = stream_options.count_;
  }

  std::uint8_t db_ = 0;
  std::string key_;
  unsigned owner_ = 0;
  storage::Digest digest_{};
  std::uint64_t now_ms_ = 0;
};

Task<absl::StatusOr<std::uint64_t>> BeginNegativeRandomStream(
    const std::shared_ptr<NegativeRandomStreamState>& state) {
  auto begin = [state]() -> Task<absl::StatusOr<std::uint64_t>> {
    try {
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
            state->db_, state->key_, state->digest_,
            state->options_.with_values_, nullptr, state->now_ms_);
      } else if (state->options_.hash_) {
        snapshot = co_await g_storage->ExecuteHashLocked(
            state->db_, state->key_, state->digest_, operation, nullptr);
      } else {
        snapshot = co_await g_storage->ExecuteSetLocked(
            state->db_, state->key_, state->digest_, operation, nullptr);
      }
      if (!snapshot.ok()) co_return snapshot.status();
      if (snapshot->length_ == 0) co_return std::uint64_t{0};
      auto adopted = AdoptRandomStreamSnapshot(*state, std::move(*snapshot));
      if (!adopted.ok()) co_return adopted;
      co_return state->population_;
    } catch (const std::bad_alloc&) {
      RecordMemoryRejection();
      co_return absl::ResourceExhaustedError("OOM random stream snapshot");
    }
  };
  if (state->owner_ != ThisWorker().id_) {
    co_return co_await bycorf::SubmitTaskTo(state->owner_, std::move(begin));
  }
  co_return co_await begin();
}

Task<absl::StatusOr<std::string>> NextNegativeRandomChunk(
    std::shared_ptr<NegativeRandomStreamState> state) {
  co_return EncodeRandomStreamChunk(*state);
}

#if LAVIK_FAULTS_ENABLED
void MaybeFailRandomStreamBuild(const CommandRequest& request,
                                std::string_view stage) {
  if (LAVIK_FAULT_MATCHES("LAVIK_FAIL_RANDOM_STREAM_STAGE", stage))
    LAVIK_FAULT_BAD_ALLOC("LAVIK_FAIL_RANDOM_STREAM_KEY", request.args_[1]);
}
#endif

Task<CommandReply> ExecuteNegativeRandomStream(
    const CommandRequest& request, NegativeRandomStreamOptions options,
    ReplyBuilder& reply_builder) {
  try {
    const std::uint8_t db = request.db_id_;
    const unsigned owner = ShardForKey(request.args_[1]);
    const auto state_bytes =
        sizeof(NegativeRandomStreamState) + 64 + request.args_[1].size() + 1;
    auto admission = TryReserveMemory(state_bytes);
    if (!admission) {
      RecordMemoryRejection();
      co_return BuiltReply(AppendOomError(reply_builder));
    }
    auto state = std::make_shared<NegativeRandomStreamState>(
        db, std::move(options), request.args_[1], owner);
    state->state_charge_.Adopt(&*admission, state_bytes);
    if (!TryBeginDbOperation(db)) {
      co_return BuiltReply(
          AppendTryAgainError(reply_builder, "database flush is in progress"));
    }
    DbOperationGuard initial_db_guard(db);
    if (const char* error = CommandServingGenerationError(request);
        error != nullptr) [[unlikely]] {
      co_return BuiltReply(reply_builder.AppendError(error));
    }
    state->now_ms_ = RedisUnixTimeMillis();
    auto length = co_await BeginNegativeRandomStream(state);
    if (!length.ok()) {
      co_return BuiltReply(AppendStorageError(reply_builder, length.status()));
    }
    if (*length == 0) {
      co_return BuiltReply(reply_builder.AppendArrayHeader(0));
    }
    // The initial lookup is complete. Socket backpressure may retain the reply
    // for an arbitrary duration, so retain neither the DB gate nor the key
    // lock. Later batches sample only the immutable in-memory view.
    initial_db_guard.Release();

    std::uint64_t reply_elements = state->remaining_;
    if (state->options_.with_values_) reply_elements *= 2;
    LAVIK_FAULT_INJECT(MaybeFailRandomStreamBuild(request, "before-source"););
    auto chunks = std::make_unique<ReplyChunkSource>(
        [state]() { return NextNegativeRandomChunk(state); });
    CommandReply reply =
        BuiltReply(reply_builder.AppendArrayHeader(reply_elements));
    LAVIK_FAULT_INJECT(MaybeFailRandomStreamBuild(request, "after-header"););
    reply.chunks_ = std::move(chunks);
    co_return reply;
  } catch (const std::bad_alloc&) {
    RecordMemoryRejection();
    // This builder belongs to this request; no reply bytes have left this
    // function yet. Discard a partially built array before returning one error.
    reply_builder.Reset();
    co_return BuiltReply(AppendOomError(reply_builder));
  }
}

// EXEC must preserve the view observed at the command's position in the
// transaction, but it must not materialize a negative-count reply whose size
// is controlled by the client. Capture the compact collection once while the
// transaction owns its locks, then sample that immutable view in bounded
// chunks after EXEC has released every DB/key guard.
struct TransactionalRandomStreamState : RandomStreamPayloadState {};

struct OwnedStreamReply {
  std::string encoded_;
  ReplyChunkSource chunks_;
};

Task<absl::StatusOr<std::string>> NextTransactionalRandomChunk(
    std::shared_ptr<TransactionalRandomStreamState> state) {
  co_return EncodeRandomStreamChunk(*state);
}

Task<absl::StatusOr<OwnedStreamReply>>
PrepareTransactionalNegativeRandomStreamLocked(
    const CommandRequest& request, const storage::Digest& digest,
    storage::TxShardWrites* tx, NegativeRandomStreamOptions options) {
  try {
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

    constexpr auto state_bytes = sizeof(TransactionalRandomStreamState) + 64;
    auto admission = TryReserveMemory(state_bytes);
    if (!admission) {
      RecordMemoryRejection();
      co_return absl::ResourceExhaustedError("OOM transactional random stream");
    }
    auto state = std::make_shared<TransactionalRandomStreamState>();
    state->state_charge_.Adopt(&*admission, state_bytes);
    state->options_ = options;
    state->remaining_ = options.count_;
    auto adopted = AdoptRandomStreamSnapshot(*state, std::move(*snapshot));
    if (!adopted.ok()) co_return adopted;

    const std::uint64_t elements =
        options.with_values_ ? options.count_ * 2 : options.count_;
    OwnedStreamReply reply;
    LAVIK_FAULT_INJECT(MaybeFailRandomStreamBuild(request, "before-source"););
    reply.chunks_ = [state]() { return NextTransactionalRandomChunk(state); };
    reply.encoded_ = "*" + std::to_string(elements) + "\r\n";
    LAVIK_FAULT_INJECT(MaybeFailRandomStreamBuild(request, "after-header"););
    co_return reply;
  } catch (const std::bad_alloc&) {
    RecordMemoryRejection();
    // The owned reply is discarded, so no partial array reaches EXEC's builder.
    co_return absl::ResourceExhaustedError("OOM transactional random stream");
  }
}

std::string EncodeStorageError(const absl::Status& status) {
  if (status.message().starts_with("WRONGTYPE ")) {
    return EncodeError(status.message());
  }
  return EncodeError(absl::StrCat("ERR ", status.message()));
}

std::string_view AppendStorageError(ReplyBuilder& reply_builder,
                                    const absl::Status& status) {
  if (status.code() == absl::StatusCode::kResourceExhausted &&
      status.message().starts_with("OOM ")) {
    return AppendOomError(reply_builder);
  }
  return status.message().starts_with("WRONGTYPE ")
             ? reply_builder.AppendError(status.message())
             : reply_builder.AppendError("ERR ", status.message());
}

absl::StatusOr<storage::SetOptions> ParseSetOptions(
    const std::vector<std::string>& args) {
  storage::SetOptions options;
  // Plain SET is the dominant write path. Expiration parsing is the only
  // reason this routine needs wall time, so do not read the clock when there
  // are no options to interpret.
  if (args.size() == 3) return options;
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
  return ParseRedisExpirationDeadline(text, seconds, absolute, command,
                                      PastExpirationPolicy::kExpireImmediately);
}

long long ExpirationReplySeconds(std::uint64_t milliseconds) {
  const std::uint64_t rounded =
      milliseconds / 1000 + (milliseconds % 1000 >= 500 ? 1 : 0);
  return static_cast<long long>(rounded);
}

struct RestoreOptions {
  bool replace_ = false;
  bool absttl_ = false;
  std::uint64_t expire_at_ms_ = 0;
};

absl::StatusOr<RestoreOptions> ParseRestoreOptions(
    const std::vector<std::string>& args) {
  RestoreOptions options;
  for (std::size_t i = 4; i < args.size(); ++i) {
    if (CmpCaseInsensitive(args[i], "REPLACE")) {
      options.replace_ = true;
    } else if (CmpCaseInsensitive(args[i], "ABSTTL")) {
      options.absttl_ = true;
    } else if (CmpCaseInsensitive(args[i], "IDLETIME") ||
               CmpCaseInsensitive(args[i], "FREQ")) {
      return absl::UnimplementedError(
          "RESTORE IDLETIME and FREQ are not supported");
    } else {
      return absl::InvalidArgumentError("syntax error");
    }
  }

  return options;
}

absl::Status ParseRestoreTtl(std::string_view text, RestoreOptions* options) {
  std::int64_t ttl = 0;
  if (!ParseRedisInt64(text, &ttl) || ttl < 0) {
    return absl::InvalidArgumentError("Invalid TTL value, must be >= 0");
  }
  if (ttl == 0) return absl::OkStatus();
  const std::uint64_t unsigned_ttl = static_cast<std::uint64_t>(ttl);
  if (options->absttl_) {
    options->expire_at_ms_ = unsigned_ttl;
  } else {
    const std::uint64_t now_ms = RedisUnixTimeMillis();
    if (unsigned_ttl > std::numeric_limits<std::uint64_t>::max() - now_ms) {
      return absl::InvalidArgumentError("Invalid TTL value, must be >= 0");
    }
    options->expire_at_ms_ = now_ms + unsigned_ttl;
  }
  return absl::OkStatus();
}

std::string RestoreDecodeError(const absl::Status& status) {
  if (absl::IsResourceExhausted(status))
    return absl::StrCat("OOM ", status.message());
  if (status.message() == "DUMP payload version or checksum are wrong") {
    return absl::StrCat("ERR ", status.message());
  }
  return "ERR Bad data format";
}

struct PreparedRestoreValue {
  storage::RawValue raw_;
  std::optional<rdb::DumpReader> collection_;
  storage::ValueType value_type_ = storage::ValueType::kNone;
  std::uint64_t expire_at_ms_ = 0;
};

absl::StatusOr<PreparedRestoreValue> PrepareRestoreValue(
    std::string_view payload) {
  auto reader = rdb::DumpReader::Open(payload);
  if (!reader.ok()) return reader.status();
  PreparedRestoreValue value;
  if (!reader->collection()) {
    auto raw = reader->ReadRawValue();
    if (!raw.ok()) return raw.status();
    value.value_type_ = raw->value_type_;
    value.raw_ = std::move(*raw);
  } else {
    value.value_type_ = reader->value_type();
    // Validate every field/member, including duplicates across pages, before
    // replacing a live key. The second pass feeds the atomic ingest directly.
    for (;;) {
      auto page = reader->ReadCollectionPage();
      if (!page.ok()) return page.status();
      if (page->done_) break;
    }
    auto rewound = reader->Rewind();
    if (!rewound.ok()) return rewound;
    value.collection_.emplace(std::move(*reader));
  }
  return value;
}

Task<absl::StatusOr<storage::RestoreRawResult>> ApplyPreparedRestore(
    PreparedRestoreValue& value, std::uint8_t db, std::string_view key,
    bool replace, const storage::Digest* digest = nullptr,
    storage::TxShardWrites* tx = nullptr,
    storage::ReplicationCommandAppend* replication = nullptr,
    const storage::MutationPrecondition* mutation_precondition = nullptr) {
  if (!value.collection_) {
    value.raw_.expire_at_ms_ = value.expire_at_ms_;
    // if/else, not ?:, to keep the two co_awaits in separate full
    // expressions. GCC 13 can reuse the wrong coroutine-frame slot when both
    // arms of ?: contain co_await.
    if (digest != nullptr) {
      co_return co_await g_storage->RestoreRawValueLocked(
          db, key, *digest, value.raw_, replace, tx, replication,
          mutation_precondition);
    }
    co_return co_await g_storage->RestoreRawValue(
        db, key, value.raw_, replace, replication, mutation_precondition);
  }
  storage::CollectionPageReader next =
      [&value]() -> Task<absl::StatusOr<storage::CollectionPage>> {
    co_return value.collection_->ReadCollectionPage();
  };
  // Same GCC 13 double-co_await hazard as the raw branch above: keep the
  // suspensions in separate statements.
  if (digest != nullptr) {
    co_return co_await g_storage->RestoreCollectionValueLocked(
        db, key, *digest, value.value_type_, value.expire_at_ms_, replace,
        value.collection_->expected_items(), std::move(next), tx, replication,
        mutation_precondition);
  }
  co_return co_await g_storage->RestoreCollectionValue(
      db, key, value.value_type_, value.expire_at_ms_, replace,
      value.collection_->expected_items(), std::move(next), replication,
      mutation_precondition);
}

std::vector<std::string> CanonicalRestoreCommand(std::string_view key,
                                                 std::string_view payload,
                                                 std::uint64_t expire_at_ms) {
  if (expire_at_ms != 0 && expire_at_ms <= RedisUnixTimeMillis()) {
    return {"DEL", std::string(key)};
  }
  std::vector<std::string> result{
      "RESTORE", std::string(key),
      expire_at_ms == 0 ? "0" : std::to_string(expire_at_ms),
      std::string(payload), "REPLACE"};
  if (expire_at_ms != 0) result.emplace_back("ABSTTL");
  return result;
}

struct DumpReplyState {
  std::string payload_;
  std::size_t offset_ = 0;
};

struct PreparedDumpReply {
  std::string header_;
  ReplyChunkSource chunks_;
};

Task<absl::StatusOr<std::string>> NextDumpReplyChunk(
    std::shared_ptr<DumpReplyState> state) {
  if (state->offset_ == state->payload_.size()) co_return std::string();
  constexpr std::size_t kChunkBytes = 256 * 1024;
  const std::size_t size =
      std::min(kChunkBytes, state->payload_.size() - state->offset_);
  std::string chunk = state->payload_.substr(state->offset_, size);
  state->offset_ += size;
  if (state->offset_ == state->payload_.size()) chunk += "\r\n";
  co_return chunk;
}

PreparedDumpReply PrepareDumpReply(std::string payload) {
  const std::size_t size = payload.size();
  auto state = std::make_shared<DumpReplyState>();
  state->payload_ = std::move(payload);
  return PreparedDumpReply{
      .header_ = "$" + std::to_string(size) + "\r\n",
      .chunks_ = [state]() { return NextDumpReplyChunk(state); },
  };
}

Task<CommandReply> ExecuteStorageCommand(const CommandRequest& request,
                                         ReplyBuilder& reply_builder,
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
      auto value = co_await g_storage->Get(
          request.db_id_, args[1], read_trace,
          request.HasRoutedPartitionFor(1)
              ? std::optional<std::uint16_t>(request.RoutedPartitionId())
              : std::nullopt);
      if (!value.ok()) {
        if (value.status().code() == absl::StatusCode::kNotFound) {
          reply.encoded_ = reply_builder.AppendNull();
        } else {
          reply.encoded_ = AppendStorageError(reply_builder, value.status());
        }
      } else {
        reply.disk_value_ = std::move(*value);
      }
      co_return reply;
    }

    case CommandKind::kType: {
      auto metadata =
          co_await g_storage->ReadKeyMetadata(request.db_id_, args[1]);
      if (!metadata.ok()) {
        reply.encoded_ = AppendStorageError(reply_builder, metadata.status());
        co_return reply;
      }
      const auto& info = *metadata;
      reply.encoded_ = reply_builder.AppendSimpleString(
          info.exists_ ? ValueTypeName(info.value_type_) : "none");
      co_return reply;
    }

    case CommandKind::kDump: {
      auto value = co_await g_storage->ReadRawValue(request.db_id_, args[1]);
      if (!value.ok()) {
        reply.encoded_ =
            value.status().code() == absl::StatusCode::kNotFound
                ? reply_builder.AppendNull()
                : AppendStorageError(reply_builder, value.status());
        co_return reply;
      }
      auto payload = rdb::EncodeDump(*value);
      if (!payload.ok()) {
        reply.encoded_ = reply_builder.AppendError(
            absl::StrCat("ERR ", payload.status().message()));
        co_return reply;
      }
      PreparedDumpReply prepared = PrepareDumpReply(std::move(*payload));
      reply.encoded_ = reply_builder.AppendRaw(prepared.header_);
      reply.chunks_ =
          std::make_unique<ReplyChunkSource>(std::move(prepared.chunks_));
      co_return reply;
    }

    case CommandKind::kRestore: {
      auto options = ParseRestoreOptions(args);
      if (!options.ok()) {
        reply.encoded_ = reply_builder.AppendError(
            absl::StrCat("ERR ", options.status().message()));
        co_return reply;
      }
      if (!options->replace_ &&
          co_await g_storage->Exists(request.db_id_, args[1])) {
        reply.encoded_ = reply_builder.AppendError(
            "BUSYKEY Target key name already exists.");
        co_return reply;
      }
      const absl::Status ttl_status = ParseRestoreTtl(args[2], &*options);
      if (!ttl_status.ok()) {
        reply.encoded_ = reply_builder.AppendError(
            absl::StrCat("ERR ", ttl_status.message()));
        co_return reply;
      }
      auto value = PrepareRestoreValue(args[3]);
      if (!value.ok()) {
        reply.encoded_ =
            reply_builder.AppendError(RestoreDecodeError(value.status()));
        co_return reply;
      }
      value->expire_at_ms_ = options->expire_at_ms_;
      auto replication = PrepareReplicationCommand(
          request,
          CanonicalRestoreCommand(args[1], args[3], options->expire_at_ms_));
      const storage::MutationPrecondition mutation_precondition =
          ClusterMutationPrecondition(request);
      auto restored = co_await ApplyPreparedRestore(
          *value, request.db_id_, args[1], options->replace_, nullptr, nullptr,
          replication ? &*replication : nullptr, &mutation_precondition);
      if (!restored.ok()) {
        reply.encoded_ = AppendStorageError(reply_builder, restored.status());
      } else if (restored->busy_) {
        reply.encoded_ = reply_builder.AppendError(
            "BUSYKEY Target key name already exists.");
      } else {
        if (restored->changed_ && !restored->deleted_) {
          NotifyRenamedValue(request, args[1], value->value_type_);
        }
        reply.encoded_ = reply_builder.AppendSimpleString("OK");
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
      std::optional<storage::ReplicationCommandAppend> replication;
      if (!request.replication_origin_ && g_storage->ReplicationLogActive()) {
        const std::array<std::string_view, 3> copied_args{"SET", args[1],
                                                          args[2]};
        // The active publisher's fixed staging budget covers this journal
        // owner. Construct it before mutation; a physical allocation failure
        // is fatal rather than risking a missing replication event.
        replication.emplace();
        replication->args_.reserve(copied_args.size());
        for (std::string_view arg : copied_args) {
          replication->args_.emplace_back(arg);
        }
      }
      if (set_trace != nullptr) {
        set_trace->replication_ = replication.has_value();
      }
      const storage::MutationPrecondition mutation_precondition =
          ClusterMutationPrecondition(request);
      auto result = co_await g_storage->Set(
          request.db_id_, args[1], args[2], *options,
          replication ? &*replication : nullptr, set_trace,
          request.HasRoutedPartitionFor(1)
              ? std::optional<std::uint16_t>(request.RoutedPartitionId())
              : std::nullopt,
          &mutation_precondition);
      if (!result.ok()) {
        reply.encoded_ = AppendStorageError(reply_builder, result.status());
        co_return reply;
      }
      if (options->return_old_value_) {
        if (result->old_value_.has_value()) {
          reply.disk_value_ = std::move(*result->old_value_);
        } else {
          reply.encoded_ = reply_builder.AppendNull();
        }
      } else {
        // A routed SET executes on the key owner, while ReplyBuilder belongs
        // to the connection worker. The successful response is identical in
        // RESP2 and RESP3, so use immutable process storage instead of writing
        // five bytes into a remote worker's connection-private buffer.
        reply.encoded_ = result->applied_ ? std::string_view("+OK\r\n")
                                          : reply_builder.AppendNull();
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
    case CommandKind::kHReplace:
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
      auto metadata =
          co_await g_storage->ReadKeyMetadata(request.db_id_, args[1]);
      if (!metadata.ok()) {
        reply.encoded_ = AppendStorageError(reply_builder, metadata.status());
        co_return reply;
      }
      const auto& info = *metadata;
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
      auto replication = PrepareReplicationCommand(
          request, {"PEXPIREAT", args[1], std::to_string(*expire_at_ms)});
      const storage::MutationPrecondition mutation_precondition =
          ClusterMutationPrecondition(request);
      auto updated = co_await g_storage->UpdateExpiration(
          request.db_id_, args[1], *expire_at_ms, *condition,
          replication ? &*replication : nullptr, &mutation_precondition);
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
      auto replication = PrepareReplicationCommand(request);
      const storage::MutationPrecondition mutation_precondition =
          ClusterMutationPrecondition(request);
      auto updated = co_await g_storage->UpdateExpiration(
          request.db_id_, args[1], 0,
          storage::ExpirationCondition::kIfHasExpiration,
          replication ? &*replication : nullptr, &mutation_precondition);
      reply.encoded_ =
          updated.ok() ? reply_builder.AppendInteger(*updated ? 1 : 0)
                       : AppendStorageError(reply_builder, updated.status());
      co_return reply;
    }

    default:
      co_return ExecuteSimpleLocalCommand(request, reply_builder);
  }
}

Task<CommandReply> ExecuteRole(ReplyBuilder& reply_builder) {
  ReplicationStatus replication;
  if (g_replication != nullptr) replication = co_await g_replication->Observe();
  if (replication.role_ == ReplicationRole::kMaster) {
    reply_builder.AppendArrayHeader(3);
    reply_builder.AppendBulkString("master");
    reply_builder.AppendInteger(static_cast<long long>(
        std::min<std::uint64_t>(replication.master_repl_offset_,
                                std::numeric_limits<long long>::max())));
    reply_builder.AppendArrayHeader(replication.downstream_replicas_.size());
    for (const DownstreamReplicaStatus& replica :
         replication.downstream_replicas_) {
      reply_builder.AppendArrayHeader(3);
      reply_builder.AppendBulkString(replica.host_);
      reply_builder.AppendInteger(replica.port_);
      reply_builder.AppendInteger(
          static_cast<long long>(std::min<std::uint64_t>(
              replica.min_lsn_, std::numeric_limits<long long>::max())));
    }
    co_return BuiltReply(reply_builder.View());
  }

  reply_builder.AppendArrayHeader(5);
  reply_builder.AppendBulkString("slave");
  reply_builder.AppendBulkString(
      replication.upstream_.has_value() ? replication.upstream_->host_ : "");
  reply_builder.AppendInteger(
      replication.upstream_.has_value() ? replication.upstream_->port_ : 0);
  const std::string_view state =
      replication.role_ == ReplicationRole::kOnline    ? "connected"
      : replication.role_ == ReplicationRole::kSyncing ? "sync"
                                                       : "connecting";
  reply_builder.AppendBulkString(state);
  reply_builder.AppendInteger(static_cast<long long>(
      std::min<std::uint64_t>(replication.replica_repl_offset_,
                              std::numeric_limits<long long>::max())));
  co_return BuiltReply(reply_builder.View());
}

std::uint64_t CommandLatencyMicros(const CommandMetricTotals& command,
                                   double counter_frequency) {
  const long double micros = static_cast<long double>(command.latency_ticks_) *
                             1'000'000.0L / std::max(1.0, counter_frequency);
  if (micros >=
      static_cast<long double>(std::numeric_limits<std::uint64_t>::max())) {
    return std::numeric_limits<std::uint64_t>::max();
  }
  return static_cast<std::uint64_t>(micros);
}

std::string CommandUsecPerCall(const CommandMetricTotals& command,
                               double counter_frequency) {
  const long double hundredths =
      static_cast<long double>(command.latency_ticks_) * 100'000'000.0L /
      (std::max(1.0, counter_frequency) * command.calls_);
  const auto rounded = static_cast<std::uint64_t>(std::min<long double>(
      hundredths + 0.5L,
      static_cast<long double>(std::numeric_limits<std::uint64_t>::max())));
  const std::uint64_t fraction = rounded % 100;
  return absl::StrCat(rounded / 100, ".", fraction < 10 ? "0" : "", fraction);
}

// INFO: Redis-shaped sections built from what lavik actually tracks. The
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
  ReplicationStatus replication;
  if (g_replication != nullptr) replication = co_await g_replication->Observe();

  std::optional<WorkerMetricsSnapshot> runtime_metrics;
  if (wants("clients") || wants("stats") || wants("persistence") ||
      wants("commandstats")) {
    runtime_metrics = co_await CollectWorkerMetrics();
  }

  std::string info;
  if (wants("server")) {
    const auto uptime = std::chrono::duration_cast<std::chrono::seconds>(
                            std::chrono::steady_clock::now() - g_server_start)
                            .count();
    info += "# Server\r\n";
    info += "lavik_version:" + std::string(kVersion) + "\r\n";
    // Redis reports redis_mode in the Server section (server.c); cluster
    // clients and operators read it together with the # Cluster section.
    info += std::string("redis_mode:") +
            (cluster::ClusterEnabled() ? "cluster" : "standalone") + "\r\n";
    info += "process_id:" + std::to_string(::getpid()) + "\r\n";
    info += "run_id:" + replication.local_node_id_ + "\r\n";
    info += "tcp_port:" + std::to_string(g_server_port) + "\r\n";
    info += "worker_threads:" + std::to_string(g_server_threads) + "\r\n";
    info += "uptime_in_seconds:" + std::to_string(uptime) + "\r\n\r\n";
  }
  if (wants("clients")) {
    info += "# Clients\r\n";
    info += "connected_clients:" +
            std::to_string(runtime_metrics->connected_clients_) + "\r\n";
    info +=
        "maxclients:" + std::to_string(g_client_limit->max_clients()) + "\r\n";
    info +=
        "blocked_clients:" + std::to_string(runtime_metrics->blocked_clients_) +
        "\r\n\r\n";
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
    info += "maxmemory_clients:" +
            std::to_string(memory.client_buffer_limit_bytes_) + "\r\n";
    info += "client_buffered_request_bytes:" +
            std::to_string(memory.client_buffered_bytes_) + "\r\n";
    info += "fullsync_reserved_memory:" +
            std::to_string(memory.fullsync_reserved_bytes_) + "\r\n";
    info += "memory_admission_pending:" +
            std::to_string(memory.admission_pending_bytes_) + "\r\n";
    info += "maxmemory_policy:noeviction\r\n";
    info +=
        "number_of_cached_scripts:" + std::to_string(StoredLuaScriptCount()) +
        "\r\n";
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
  if (wants("persistence")) {
    info += "# Persistence\r\n";
    info += "rdb_changes_since_last_save:" +
            std::to_string(runtime_metrics->rdb_changes_since_last_save_) +
            "\r\n\r\n";
  }
  if (wants("stats")) {
    const storage::TombRaiderTotals raider = g_storage->TombRaiderStats();
    const storage::DefragTotals defrag = g_storage->DefragStats();
    const storage::TxCleanerTotals tx_cleaner = g_storage->TxCleanerStats();
    const storage::TxCommitBatchTotals tx_commit_batches =
        g_storage->TxCommitBatchStats();
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
    info += "tx_cleaner_rounds:" + std::to_string(tx_cleaner.rounds_) + "\r\n";
    info +=
        "tx_cleaner_failures:" + std::to_string(tx_cleaner.failures_) + "\r\n";
    info += "tx_cleaner_retired_generations:" +
            std::to_string(tx_cleaner.retired_generations_) + "\r\n";
    info += "tx_cleaner_retired_blocks:" +
            std::to_string(tx_cleaner.retired_blocks_) + "\r\n";
    info +=
        "tx_cleaner_cooldown_ms:" + std::to_string(tx_cleaner.cooldown_ms_) +
        "\r\n";
    info += std::string("tx_cleaner_running:") +
            (tx_cleaner.running_ ? "1\r\n\r\n" : "0\r\n\r\n");
    info += "tx_commit_batches:" + std::to_string(tx_commit_batches.batches_) +
            "\r\n";
    info += "tx_commit_batch_transactions:" +
            std::to_string(tx_commit_batches.transactions_) + "\r\n";
    info += "tx_commit_input_fences:" +
            std::to_string(tx_commit_batches.input_fences_) + "\r\n";
    info += "tx_commit_merged_fences:" +
            std::to_string(tx_commit_batches.merged_fences_) + "\r\n";
    info += "tx_commit_queue_depth:" +
            std::to_string(tx_commit_batches.queue_depth_) + "\r\n";
    info += "tx_commit_queue_peak:" +
            std::to_string(tx_commit_batches.queue_peak_) + "\r\n";
    info += "tx_commit_backpressure_waits:" +
            std::to_string(tx_commit_batches.backpressure_waits_) + "\r\n";
    info += "tx_commit_queue_high_watermark:" +
            std::to_string(tx_commit_batches.queue_high_watermark_) +
            "\r\n\r\n";
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
#if LAVIK_ENABLE_CROSS_CORE_HOP_COUNT
    std::uint64_t command_cross_core_hops = 0;
    for (unsigned worker = 0; worker < g_server_threads; ++worker) {
      command_cross_core_hops += co_await SubmitTo(
          worker, [] { return bycorf::LocalSubmitTaskCount(); });
    }
    info +=
        "command_cross_core_hops:" + std::to_string(command_cross_core_hops) +
        "\r\n\r\n";
#endif
  }
  if (wants("commandstats")) {
    info += "# Commandstats\r\n";
    for (std::size_t index = 0; index < runtime_metrics->commands_.size();
         ++index) {
      const CommandMetricTotals& command = runtime_metrics->commands_[index];
      if (command.calls_ == 0) continue;
      info += "cmdstat_" +
              std::string(CommandMetricName(static_cast<CommandKind>(index))) +
              ":calls=" + std::to_string(command.calls_) + ",usec=" +
              std::to_string(CommandLatencyMicros(
                  command, runtime_metrics->counter_frequency_)) +
              ",usec_per_call=" +
              CommandUsecPerCall(command, runtime_metrics->counter_frequency_) +
              ",rejected_calls=0,failed_calls=0\r\n";
    }
    info += "\r\n";
  }
  if (wants("replication")) {
    info += "# Replication\r\n";
    info +=
        "role:" +
        std::string(replication.role_ == ReplicationRole::kMaster ? "master"
                                                                  : "slave") +
        "\r\n";
    info += "lavik_replication_state:" +
            std::string(ReplicationRoleName(replication.role_)) + "\r\n";
    info += std::string("lavik_replication_failed_stopped:") +
            (replication.failed_stopped_ ? "1\r\n" : "0\r\n");
    info += "lavik_replication_role_epoch:" +
            std::to_string(replication.role_epoch_) + "\r\n";
    info += "lavik_replication_group_id:" + replication.group_id_ + "\r\n";
    info += "lavik_replication_boot_id:" + replication.boot_id_ + "\r\n";
    info += "lavik_replica_incarnation:" + replication.replica_incarnation_ +
            "\r\n";
    auto catalog_operation = co_await AcquireFunctionCatalogOperation();
    const storage::CatalogDurabilityToken catalog_token =
        GlobalFunctionCatalog().durability_token();
    catalog_operation.reset();
    info += "lavik_function_catalog_generation:" +
            std::to_string(catalog_token.catalog_generation_) + "\r\n";
    info += "lavik_function_catalog_crc64:" +
            std::to_string(catalog_token.dump_crc64_) + "\r\n";
    info += "master_replid:" +
            (replication.upstream_history_id_.has_value()
                 ? *replication.upstream_history_id_
                 : replication.local_history_id_) +
            "\r\n";
    info += "master_replid2:0000000000000000000000000000000000000000\r\n";
    info += "master_repl_offset:" +
            std::to_string(replication.role_ == ReplicationRole::kMaster
                               ? replication.master_repl_offset_
                               : replication.replica_repl_offset_) +
            "\r\n";
    info += "second_repl_offset:-1\r\n";
    if (replication.role_ == ReplicationRole::kMaster) {
      info += "connected_slaves:" +
              std::to_string(replication.downstream_replicas_.size()) + "\r\n";
      for (std::size_t index = 0;
           index < replication.downstream_replicas_.size(); ++index) {
        const DownstreamReplicaStatus& replica =
            replication.downstream_replicas_[index];
        info += "slave" + std::to_string(index) + ":ip=" + replica.host_ +
                ",port=" + std::to_string(replica.port_) +
                ",state=" + (replica.online_ ? "online" : "sync") +
                ",offset=" + std::to_string(replica.min_lsn_) + ",lag=0\r\n";
      }
    }
    if (replication.upstream_.has_value()) {
      info += "master_host:" + replication.upstream_->host_ + "\r\n";
      info += "master_port:" + std::to_string(replication.upstream_->port_) +
              "\r\n";
      const bool redis_links_up =
          replication.redis_sources_.empty() ||
          std::all_of(
              replication.redis_sources_.begin(),
              replication.redis_sources_.end(),
              [](const RedisSourceStatus& source) { return source.link_up_; });
      info += "master_link_status:" +
              std::string(replication.role_ == ReplicationRole::kOnline &&
                                  redis_links_up
                              ? "up\r\n"
                              : "down\r\n");
      info += "master_last_io_seconds_ago:" +
              std::to_string(replication.master_last_io_seconds_ago_) + "\r\n";
      info += "master_link_down_since_seconds:" +
              std::to_string(replication.master_link_down_since_seconds_) +
              "\r\n";
      info += "slave_repl_offset:" +
              std::to_string(replication.replica_repl_offset_) + "\r\n";
      info +=
          "slave_priority:" + std::to_string(replication.replica_priority_) +
          "\r\n";
      info += "replica_announced:1\r\n";
      info += "lavik_source_workers:" +
              std::to_string(replication.source_worker_count_) + "\r\n";
      info += "lavik_connected_flows:" +
              std::to_string(replication.connected_flows_) + "\r\n";
      if (!replication.redis_sources_.empty()) {
        info += std::string("lavik_redis_cluster:") +
                (replication.redis_cluster_ ? "1\r\n" : "0\r\n");
        info += std::string("lavik_redis_topology_fault:") +
                (replication.redis_topology_fault_ ? "1\r\n" : "0\r\n");
        info += "lavik_redis_sources:" +
                std::to_string(replication.redis_sources_.size()) + "\r\n";
        for (std::size_t index = 0; index < replication.redis_sources_.size();
             ++index) {
          const RedisSourceStatus& source = replication.redis_sources_[index];
          info += "lavik_redis_source" + std::to_string(index) +
                  ":node=" + source.node_id_ +
                  ",host=" + source.upstream_.host_ +
                  ",port=" + std::to_string(source.upstream_.port_) +
                  ",link=" + (source.link_up_ ? "up" : "down") +
                  ",offset=" + std::to_string(source.offset_) +
                  ",slots=" + source.slots_ + "\r\n";
        }
      }
      info += std::string("slave_read_only:") +
              (g_replication != nullptr && g_replication->replica_read_only()
                   ? "1\r\n"
                   : "0\r\n");
      info +=
          std::string("master_sync_in_progress:") +
          (replication.role_ == ReplicationRole::kOnline ? "0\r\n" : "1\r\n");
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
  if (wants("cluster")) {
    // Redis always emits the Cluster section (server.c:6219), standalone
    // included; clients key off cluster_enabled.
    info += "# Cluster\r\n";
    info += std::string("cluster_enabled:") +
            (cluster::ClusterEnabled() ? "1" : "0") + "\r\n\r\n";
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
          ? EncodeSemanticNull(request.resp_version_)
          : EncodeError(absl::StrCat("ERR ", value.status().message()));
    }

    case CommandKind::kType: {
      auto metadata =
          co_await g_storage->ReadKeyMetadataLocked(db_id, args[1], digest);
      if (!metadata.ok()) co_return EncodeStorageError(metadata.status());
      const auto& info = *metadata;
      co_return EncodeSimpleString(
          info.exists_ ? ValueTypeName(info.value_type_) : "none");
    }

    case CommandKind::kDump: {
      auto value =
          co_await g_storage->ReadRawValueLocked(db_id, args[1], digest);
      if (!value.ok()) {
        co_return value.status().code() == absl::StatusCode::kNotFound
            ? EncodeSemanticNull(request.resp_version_)
            : EncodeStorageError(value.status());
      }
      auto payload = rdb::EncodeDump(*value);
      if (!payload.ok()) {
        co_return EncodeError(absl::StrCat("ERR ", payload.status().message()));
      }
      PreparedDumpReply prepared = PrepareDumpReply(std::move(*payload));
      if (reply_chunks != nullptr) {
        *reply_chunks = std::move(prepared.chunks_);
      }
      co_return std::move(prepared.header_);
    }

    case CommandKind::kRestore: {
      MarkReplicationCommandHandled(request);
      auto options = ParseRestoreOptions(args);
      if (!options.ok()) {
        co_return EncodeError(absl::StrCat("ERR ", options.status().message()));
      }
      if (!options->replace_ &&
          co_await g_storage->ExistsLocked(db_id, args[1], digest)) {
        co_return EncodeError("BUSYKEY Target key name already exists.");
      }
      const absl::Status ttl_status = ParseRestoreTtl(args[2], &*options);
      if (!ttl_status.ok()) {
        co_return EncodeError(absl::StrCat("ERR ", ttl_status.message()));
      }
      auto value = PrepareRestoreValue(args[3]);
      if (!value.ok()) {
        co_return EncodeError(RestoreDecodeError(value.status()));
      }
      value->expire_at_ms_ = options->expire_at_ms_;
      auto restored = co_await ApplyPreparedRestore(
          *value, db_id, args[1], options->replace_, &digest, tx);
      if (!restored.ok()) co_return EncodeStorageError(restored.status());
      if (restored->busy_) {
        co_return EncodeError("BUSYKEY Target key name already exists.");
      }
      if (restored->changed_) {
        CaptureReplicationCommand(
            request,
            CanonicalRestoreCommand(args[1], args[3], options->expire_at_ms_));
        if (!restored->deleted_) {
          NotifyRenamedValue(request, db_id, args[1], value->value_type_);
        }
      }
      co_return EncodeSimpleString("OK");
    }

    case CommandKind::kSet: {
      MarkReplicationCommandHandled(request);
      auto options = ParseSetOptions(args);
      if (!options.ok()) {
        co_return EncodeError(absl::StrCat("ERR ", options.status().message()));
      }
      auto result = co_await g_storage->SetLocked(db_id, args[1], digest,
                                                  args[2], *options, tx);
      if (!result.ok()) {
        co_return EncodeStorageError(result.status());
      }
      if (result->applied_) {
        std::vector<std::string> canonical{"SET", args[1], args[2]};
        if (options->expire_at_ms_ != 0) {
          canonical.emplace_back("PXAT");
          canonical.push_back(std::to_string(options->expire_at_ms_));
        } else if (options->keep_ttl_) {
          canonical.emplace_back("KEEPTTL");
        }
        CaptureReplicationCommand(request, std::move(canonical));
      }
      if (options->return_old_value_) {
        if (result->old_value_.has_value()) {
          const auto bytes = result->old_value_->network_bytes();
          co_return std::string(reinterpret_cast<const char*>(bytes.data()),
                                bytes.size());
        }
        co_return EncodeSemanticNull(request.resp_version_);
      }
      co_return result->applied_ ? EncodeSimpleString("OK")
                                 : EncodeSemanticNull(request.resp_version_);
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
      ReplyBuilder string_reply_builder(request.resp_version_);
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
      ReplyBuilder bitmap_reply_builder(request.resp_version_);
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
      ReplyBuilder list_reply_builder(request.resp_version_);
      CommandReply reply = co_await ExecuteSingleListCommandLocked(
          request, digest, tx, list_reply_builder);
      co_return std::string(reply.encoded_);
    }

    case CommandKind::kHSet:
    case CommandKind::kHMSet:
    case CommandKind::kHReplace:
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
      ReplyBuilder hash_reply_builder(request.resp_version_);
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
      ReplyBuilder set_reply_builder(request.resp_version_);
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
      ReplyBuilder zset_reply_builder(request.resp_version_);
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
      ReplyBuilder stream_reply_builder(request.resp_version_);
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
      auto metadata =
          co_await g_storage->ReadKeyMetadataLocked(db_id, args[1], digest);
      if (!metadata.ok()) co_return EncodeStorageError(metadata.status());
      const auto& info = *metadata;
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
      MarkReplicationCommandHandled(request);
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
      if (updated.ok() && *updated) {
        CaptureReplicationCommand(
            request, {"PEXPIREAT", args[1], std::to_string(*expire_at_ms)});
      }
      co_return updated.ok() ? EncodeInteger(*updated ? 1 : 0)
                             : EncodeStorageError(updated.status());
    }

    case CommandKind::kPersist: {
      MarkReplicationCommandHandled(request);
      auto updated = co_await g_storage->UpdateExpirationLocked(
          db_id, args[1], digest, 0,
          storage::ExpirationCondition::kIfHasExpiration, tx);
      if (updated.ok() && *updated) {
        CaptureReplicationCommand(request, {"PERSIST", args[1]});
      }
      co_return updated.ok() ? EncodeInteger(*updated ? 1 : 0)
                             : EncodeStorageError(updated.status());
    }

    default:
      co_return EncodeError("ERR command is not allowed in transactions");
  }
}

// Shared context of one multi-key command's transaction. Shard callbacks
// write disjoint reply slots (MGET) or bump the shared counter (DEL/EXISTS)
// before the hop barrier; the coordinator assembles the reply afterwards.
struct MultiKeyContext {
  const CommandRequest* request_ = nullptr;
  // Single-key MSET/DEL use an untagged transaction callback, so they cannot
  // inherit the final authority check from a TxShardWrites receipt.
  storage::MutationPrecondition mutation_precondition_;
  std::vector<std::optional<std::string>>
      frames_;                      // MGET: encoded bulk per slot
  std::atomic<long long> hits_{0};  // DEL / EXISTS
  // Multi-key atomic write: per-worker receipts, non-empty only for tagged
  // writes (MSET / multi-key DEL). Each shard touches only its own slot.
  std::vector<storage::TxShardWrites> tx_writes_;
  // Set by the coordinator between the execute and finish hops of a tagged
  // multi-shard write: any shard failed, so every shard must undo.
  bool rollback_ = false;
  bool publish_fullsync_on_success_ = false;
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
  storage::TxShardWrites& shard = ctx->tx_writes_[bycorf::ThisWorker().id_];
  g_storage->PublishCommittedFullSyncEffects(&shard);
  co_return co_await g_storage->DiscardTxUndoLocal(txid);
}

using TwoPhaseCallback = Task<absl::Status> (*)(void*, const tx::ShardSlice&);

Task<absl::Status> ReleaseHeldKeys(void*, const tx::ShardSlice&);

struct TwoPhaseResult {
  absl::Status status_;
  std::uint64_t txid_ = 0;
  bool skipped_ = false;
};

template <typename Context>
Task<absl::Status> TwoPhaseFinishCallback(void* opaque, const tx::ShardSlice&) {
  auto* context = static_cast<Context*>(opaque);
  const std::uint64_t txid = context->writes_.front().txid_;
  if (context->rollback_) co_return co_await g_storage->RollbackTxLocal(txid);
  storage::TxShardWrites& shard = context->writes_[bycorf::ThisWorker().id_];
  g_storage->PublishCommittedFullSyncEffects(&shard);
  co_return co_await g_storage->DiscardTxUndoLocal(txid);
}

template <typename Context, typename ShouldSkip>
Task<TwoPhaseResult> ExecuteTwoPhaseWrite(
    tx::Transaction& transaction, const CommandRequest& request,
    Context* context, TwoPhaseCallback read_callback,
    TwoPhaseCallback write_callback, TwoPhaseCallback single_shard_callback,
    ShouldSkip should_skip) {
  absl::Status status = co_await transaction.Schedule();
  if (!status.ok()) co_return TwoPhaseResult{std::move(status)};

  const std::uint64_t txid = storage::StorageEngine::AllocateWriteTxid();
  context->writes_.resize(g_storage->worker_count());
  g_storage->InitializeTxWrites(txid, context->writes_,
                                ClusterMutationPrecondition(request));
  for (storage::TxShardWrites& writes : context->writes_) {
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
    // The release hop does not mutate; a fence landing after the write
    // decision must not turn lock cleanup into a spurious failure.
    transaction.SetShardValidator(nullptr, nullptr);
    absl::Status released =
        co_await transaction.Execute(&ReleaseHeldKeys, nullptr, true);
    disarm_undo();
    if (status.ok() && !released.ok()) status = std::move(released);
    const bool skipped = status.ok();
    co_return TwoPhaseResult{std::move(status), txid, skipped};
  }

  status = co_await transaction.Execute(write_callback, context, false);
  context->rollback_ = !status.ok();
  // The finish hop settles (publishes or rolls back) what the write hop did.
  // Validating it against a fresher fence would either block the settlement
  // or misreport an already-decided outcome, so the hook is cleared.
  transaction.SetShardValidator(nullptr, nullptr);
  absl::Status finished = co_await transaction.Execute(
      &TwoPhaseFinishCallback<Context>, context, true);
  if (status.ok() && !finished.ok()) status = std::move(finished);
  disarm_undo();
  co_return TwoPhaseResult{std::move(status), txid, false};
}

struct RenameContext {
  const CommandRequest* request_ = nullptr;
  storage::TransferValue source_;
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
      auto source = co_await g_storage->ReadValueForTransferLocked(
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
    storage::TxShardWrites* writes =
        &context->writes_[bycorf::ThisWorker().id_];
    if (key.arg_index_ == 1) {
      auto deleted = co_await g_storage->DeleteLocked(
          context->request_->db_id_, name, key.digest_, writes);
      if (!deleted.ok()) co_return deleted.status();
      if (!*deleted) co_return absl::NotFoundError("no such key");
    } else if (key.arg_index_ == 2) {
      absl::Status written = co_await g_storage->WriteValueForTransferLocked(
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
  auto source = co_await g_storage->ReadValueForTransferLocked(
      context->request_->db_id_, args[1], source_key->digest_);
  if (!source.ok()) co_return source.status();
  context->source_ = std::move(*source);
  context->destination_exists_ = co_await g_storage->ExistsLocked(
      context->request_->db_id_, args[2], destination_key->digest_);
  if (context->request_->kind_ == CommandKind::kRenameNx &&
      context->destination_exists_) {
    co_return absl::OkStatus();
  }

  storage::TxShardWrites& writes = context->writes_[bycorf::ThisWorker().id_];
  absl::Status written = co_await g_storage->WriteValueForTransferLocked(
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
  if (written.ok()) {
    g_storage->PublishCommittedFullSyncEffects(&writes);
  }
  // Keep each await in its own statement. GCC 13 can reuse the coroutine-frame
  // slot incorrectly when both arms of a conditional expression suspend.
  absl::Status finished;
  if (written.ok()) {
    finished = co_await g_storage->DiscardTxUndoLocal(writes.txid_);
  } else {
    finished = co_await g_storage->RollbackTxLocal(writes.txid_, &writes);
  }
  co_return written.ok() ? finished : (finished.ok() ? written : finished);
}

Task<absl::Status> ReleaseHeldKeys(void*, const tx::ShardSlice&) {
  co_return absl::OkStatus();
}

Task<absl::Status> PublishFullSyncEffectsCallback(void* opaque,
                                                  const tx::ShardSlice&) {
  auto* writes = static_cast<std::vector<storage::TxShardWrites>*>(opaque);
  g_storage->PublishCommittedFullSyncEffects(
      &(*writes)[bycorf::ThisWorker().id_]);
  co_return absl::OkStatus();
}

void NotifyRenamedValue(const CommandRequest& request, std::uint8_t db_id,
                        std::string_view key, storage::ValueType type) {
  if (request.blocking_notification_capture_ != nullptr) {
    request.blocking_notification_capture_->Record(db_id, std::string(key),
                                                   type);
    return;
  }
  if (type == storage::ValueType::kList) {
    NotifyListBlockingKey(db_id, key, request.blocking_wake_cascade_);
  } else if (type == storage::ValueType::kSortedSet) {
    NotifyZSetBlockingKey(db_id, key, request.blocking_wake_cascade_);
  } else if (type == storage::ValueType::kStream) {
    NotifyStreamBlockingKey(db_id, key, request.blocking_wake_cascade_);
  }
}

void NotifyRenamedValue(const CommandRequest& request, std::string_view key,
                        storage::ValueType type) {
  NotifyRenamedValue(request, request.db_id_, key, type);
}

Task<CommandReply> ExecuteRename(const CommandRequest& request,
                                 ReplyBuilder& reply_builder) {
  const auto& args = request.args_;
  const bool nx = request.kind_ == CommandKind::kRenameNx;
  if (args[1] == args[2]) {
    const unsigned owner = ShardForKey(args[1]);
    auto metadata = co_await SubmitTaskTo(
        owner, [db = request.db_id_, key = std::string(args[1])]() {
          return g_storage->ReadKeyMetadata(db, key);
        });
    if (!metadata.ok())
      co_return BuiltReply(
          AppendStorageError(reply_builder, metadata.status()));
    const auto& info = *metadata;
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
  ClusterShardValidatorContext cluster_validator;
  InstallClusterShardValidator(transaction, request, cluster_validator);
  ReplicationTransactionGuard replication(request, &transaction);
  if (!replication.status().ok()) {
    co_return BuiltReply(
        AppendStorageError(reply_builder, replication.status()));
  }
  RenameContext context;
  context.request_ = &request;
  TwoPhaseResult execution = co_await ExecuteTwoPhaseWrite(
      transaction, request, &context, &RenameReadCallback, &RenameWriteCallback,
      &RenameSingleShardCallback, [nx](const RenameContext& value) {
        return nx && value.destination_exists_;
      });
  absl::Status status = std::move(execution.status_);
  if (!status.ok()) {
    if (cluster_validator.tripped_.load(std::memory_order_relaxed)) {
      co_return ClusterValidatorFailureReply(transaction, cluster_validator,
                                             request.connection_tls_,
                                             reply_builder);
    }
    if (status.code() == absl::StatusCode::kNotFound) {
      co_return BuiltReply(reply_builder.AppendError("ERR no such key"));
    }
    co_return BuiltReply(AppendStorageError(reply_builder, status));
  }
  if (execution.skipped_) {
    co_return BuiltReply(reply_builder.AppendInteger(0));
  }

  replication.SetFinalExpirations(context.writes_);
  replication.Commit();
  if (!g_storage->EnqueueTxCommit(execution.txid_,
                                  std::move(context.writes_))) {
    co_await g_storage->WaitForTxCommitCapacity();
  }
  NotifyRenamedValue(request, args[2], context.source_.metadata_.value_type_);
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
  if (cluster::ClusterEnabled() && options.destination_db_ != 0) {
    // Cluster mode has no cross-database COPY (Redis db.c copyCommand). This
    // single choke point covers ExecuteCopy, the EXEC precompute, and the EXEC
    // sequential fallback.
    return absl::InvalidArgumentError(
        "Copying to another database is not allowed in cluster mode");
  }
  return options;
}

struct CopyContext {
  const CommandRequest* request_ = nullptr;
  CopyOptions options_;
  std::optional<storage::TransferValue> source_;
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
      auto source = co_await g_storage->ReadValueForTransferLocked(
          key.db_, name, key.digest_);
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
    storage::TxShardWrites* writes =
        &context->writes_[bycorf::ThisWorker().id_];
    absl::Status status = co_await g_storage->WriteValueForTransferLocked(
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
  storage::TxShardWrites& writes = context->writes_[bycorf::ThisWorker().id_];
  writes.collect_undo_ = false;
  if (status.ok()) {
    g_storage->PublishCommittedFullSyncEffects(&writes);
  }
  absl::Status finished;
  if (status.ok()) {
    finished = co_await g_storage->DiscardTxUndoLocal(writes.txid_);
  } else {
    finished = co_await g_storage->RollbackTxLocal(writes.txid_, &writes);
  }
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
        AppendTryAgainError(reply_builder, "database flush is in progress"));
  }
  if (!CommandWriteAdmissionIsCurrent(request)) {
    co_return BuiltReply(reply_builder.AppendError(
        "TRYAGAIN replication role changed; retry command"));
  }
  if (const char* error = CommandServingGenerationError(request);
      error != nullptr) [[unlikely]] {
    co_return BuiltReply(reply_builder.AppendError(error));
  }

  tx::Transaction transaction;
  transaction.AddKey(ShardForKey(args[1]), request.db_id_,
                     storage::ComputeDigest(args[1]), 1, tx::LockMode::kShared);
  transaction.AddKey(ShardForKey(args[2]), options->destination_db_,
                     storage::ComputeDigest(args[2]), 2,
                     tx::LockMode::kExclusive);
  transaction.Seal();
  ClusterShardValidatorContext cluster_validator;
  InstallClusterShardValidator(transaction, request, cluster_validator);
  ReplicationTransactionGuard replication(request, &transaction);
  if (!replication.status().ok()) {
    co_return BuiltReply(
        AppendStorageError(reply_builder, replication.status()));
  }
  CopyContext context;
  context.request_ = &request;
  context.options_ = *options;
  TwoPhaseResult execution = co_await ExecuteTwoPhaseWrite(
      transaction, request, &context, &CopyReadCallback, &CopyWriteCallback,
      &CopySingleShardCallback, [](const CopyContext& value) {
        return !value.source_.has_value() ||
               (value.destination_exists_ && !value.options_.replace_);
      });
  absl::Status status = std::move(execution.status_);
  if (!status.ok()) {
    if (cluster_validator.tripped_.load(std::memory_order_relaxed)) {
      co_return ClusterValidatorFailureReply(transaction, cluster_validator,
                                             request.connection_tls_,
                                             reply_builder);
    }
    co_return BuiltReply(AppendStorageError(reply_builder, status));
  }
  if (execution.skipped_ || !context.copied_) {
    co_return BuiltReply(reply_builder.AppendInteger(0));
  }

  replication.SetFinalExpirations(context.writes_);
  replication.Commit();
  if (!g_storage->EnqueueTxCommit(execution.txid_,
                                  std::move(context.writes_))) {
    co_await g_storage->WaitForTxCommitCapacity();
  }
  NotifyRenamedValue(request, options->destination_db_, args[2],
                     context.source_->metadata_.value_type_);
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
  return MSetNxWriteLocal(static_cast<MSetNxContext*>(opaque));
}

Task<absl::Status> MSetNxSingleShardCallback(void* opaque,
                                             const tx::ShardSlice& slice) {
  auto* context = static_cast<MSetNxContext*>(opaque);
  absl::Status checked = co_await MSetNxCheckCallback(opaque, slice);
  if (!checked.ok() || context->exists_.load(std::memory_order_relaxed)) {
    co_return checked;
  }
  absl::Status written = co_await MSetNxWriteLocal(context);
  storage::TxShardWrites& writes = context->writes_[bycorf::ThisWorker().id_];
  writes.collect_undo_ = false;
  if (written.ok()) {
    g_storage->PublishCommittedFullSyncEffects(&writes);
  }
  absl::Status finished;
  if (written.ok()) {
    finished = co_await g_storage->DiscardTxUndoLocal(writes.txid_);
  } else {
    finished = co_await g_storage->RollbackTxLocal(writes.txid_, &writes);
  }
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
  ClusterShardValidatorContext cluster_validator;
  InstallClusterShardValidator(transaction, request, cluster_validator);
  ReplicationTransactionGuard replication(request, &transaction);
  if (!replication.status().ok()) {
    co_return BuiltReply(
        AppendStorageError(reply_builder, replication.status()));
  }
  MSetNxContext context;
  context.request_ = &request;
  TwoPhaseResult execution = co_await ExecuteTwoPhaseWrite(
      transaction, request, &context, &MSetNxCheckCallback,
      &MSetNxWriteCallback, &MSetNxSingleShardCallback,
      [](const MSetNxContext& value) {
        return value.exists_.load(std::memory_order_relaxed);
      });
  absl::Status status = std::move(execution.status_);
  if (!status.ok()) {
    if (cluster_validator.tripped_.load(std::memory_order_relaxed)) {
      co_return ClusterValidatorFailureReply(transaction, cluster_validator,
                                             request.connection_tls_,
                                             reply_builder);
    }
    co_return BuiltReply(AppendStorageError(reply_builder, status));
  }
  if (execution.skipped_) {
    co_return BuiltReply(reply_builder.AppendInteger(0));
  }
  replication.SetFinalExpirations(context.writes_);
  replication.Commit();
  if (!g_storage->EnqueueTxCommit(execution.txid_,
                                  std::move(context.writes_))) {
    co_await g_storage->WaitForTxCommitCapacity();
  }
  co_return BuiltReply(reply_builder.AppendInteger(1));
}

Task<absl::Status> MultiKeyShardCallback(void* context,
                                         const tx::ShardSlice& slice) {
  auto* ctx = static_cast<MultiKeyContext*>(context);
  const auto& args = ctx->request_->args_;
  if (ctx->request_->kind_ == CommandKind::kMGet) {
    std::vector<storage::BatchGetRequest> reads;
    reads.reserve(slice.keys_.size());
    for (const tx::TxKey& key : slice.keys_) {
      reads.push_back(storage::BatchGetRequest{
          .key_ = args[key.arg_index_],
          .digest_ = key.digest_,
      });
    }
    auto values =
        co_await g_storage->BatchGetLocked(ctx->request_->db_id_, reads);
    for (std::size_t i = 0; i < values.size(); ++i) {
      if (!values[i].ok()) co_return values[i].status();
      if (values[i]->has_value()) {
        const std::size_t slot = slice.keys_[i].arg_index_ - 1;
        ctx->frames_[slot] = EncodeBulkString(**values[i]);
      }
    }
    co_return absl::OkStatus();
  }
  for (const tx::TxKey& key : slice.keys_) {
    const std::string& name = args[key.arg_index_];
    switch (ctx->request_->kind_) {
      case CommandKind::kMSet: {
        auto replication = ctx->tx_writes_.empty()
                               ? PrepareReplicationCommand(*ctx->request_)
                               : std::nullopt;
        auto result = co_await g_storage->SetLocked(
            ctx->request_->db_id_, name, key.digest_, args[key.arg_index_ + 1],
            {},
            ctx->tx_writes_.empty() ? nullptr
                                    : &ctx->tx_writes_[ThisWorker().id_],
            replication ? &*replication : nullptr, nullptr,
            &ctx->mutation_precondition_);
        if (!result.ok()) {
          if (!ctx->tx_writes_.empty()) {
            (void)co_await g_storage->RollbackTxLocal(
                ctx->tx_writes_.front().txid_);
          }
          co_return result.status();
        }
        break;
      }
      case CommandKind::kDel:
      case CommandKind::kUnlink: {
        std::optional<storage::ReplicationCommandAppend> replication;
        if (!ctx->request_->replication_origin_ && ctx->tx_writes_.empty() &&
            g_storage->ReplicationLogActive()) {
          replication.emplace();
          replication->args_ = {"DEL", name};
        }
        auto deleted = co_await g_storage->DeleteLocked(
            ctx->request_->db_id_, name, key.digest_,
            ctx->tx_writes_.empty() ? nullptr
                                    : &ctx->tx_writes_[ThisWorker().id_],
            replication ? &*replication : nullptr,
            &ctx->mutation_precondition_);
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
        auto metadata = co_await g_storage->ReadKeyMetadataLocked(
            ctx->request_->db_id_, name, key.digest_);
        if (!metadata.ok()) co_return metadata.status();
        if (metadata->exists_) {
          ctx->hits_.fetch_add(1, std::memory_order_relaxed);
        }
        break;
      }
    }
  }
  if (ctx->publish_fullsync_on_success_ && !ctx->tx_writes_.empty()) {
    g_storage->PublishCommittedFullSyncEffects(
        &ctx->tx_writes_[ThisWorker().id_]);
  }
  co_return absl::OkStatus();
}

// DEL / EXISTS / MSET / MGET run as one transaction: every key locked up
// front (across all owning shards), one hop where each shard works its
// slice, locks released when the hop completes.
void ReleaseReplicationOrderAfterMarkers(void* context) noexcept {
  static_cast<ReplicationTransactionOrderGuard*>(context)->Release();
}

Task<CommandReply> ExecuteMultiKey(
    const CommandRequest& request, ReplyBuilder& reply_builder,
    ReplicationTransactionOrderGuard* replication_order = nullptr) {
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
  ClusterShardValidatorContext cluster_validator;
  if (write) {
    InstallClusterShardValidator(txn, request, cluster_validator);
  }
  std::unique_ptr<ReplicationTransactionGuard> replication;
  if (keys->count() > 1) {
    replication = std::make_unique<ReplicationTransactionGuard>(request, &txn);
  }
  if (replication != nullptr && !replication->status().ok()) {
    if (replication_order != nullptr) replication_order->Release();
    co_return BuiltReply(
        AppendStorageError(reply_builder, replication->status()));
  }
  if (replication_order != nullptr) {
    if (replication != nullptr && replication->active()) {
      // Entry hooks run after the transaction owns its shard locks and before
      // the storage callbacks. Once every marker is queued, later transactions
      // cannot overtake this one on an overlapping flow, so storage IO no
      // longer needs to hold the global publication-order slot.
      replication->SetParticipantsEnteredHook(
          &ReleaseReplicationOrderAfterMarkers, replication_order);
    } else {
      // A one-key MSET has no cross-flow rendezvous to order.
      replication_order->Release();
    }
  }

  MultiKeyContext ctx;
  ctx.request_ = &request;
  ctx.mutation_precondition_ = ClusterMutationPrecondition(request);
  if (request.kind_ == CommandKind::kMGet) {
    ctx.frames_.resize(keys->count());
  }
  std::uint64_t write_txid = 0;
  if (write && keys->count() > 1) {
    write_txid = storage::StorageEngine::AllocateWriteTxid();
    ctx.tx_writes_.resize(g_storage->worker_count());
    g_storage->InitializeTxWrites(write_txid, ctx.tx_writes_,
                                  ClusterMutationPrecondition(request));
    for (auto& shard : ctx.tx_writes_) {
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
  ctx.publish_fullsync_on_success_ = write_txid != 0 && txn.single_shard();
  absl::Status status =
      co_await txn.Execute(&MultiKeyShardCallback, &ctx, !two_hop);
  if (two_hop) {
    // The finish hop settles the write hop's mutations; it must not be
    // fenced off by an authority change that the write hop already beat.
    txn.SetShardValidator(nullptr, nullptr);
    ctx.rollback_ = !status.ok();
    absl::Status finish =
        co_await txn.Execute(&MultiKeyFinishCallback, &ctx, true);
    if (!finish.ok() && status.ok()) {
      status = finish;
    }
  }
  if (!status.ok()) {
    if (cluster_validator.tripped_.load(std::memory_order_relaxed)) {
      co_return ClusterValidatorFailureReply(
          txn, cluster_validator, request.connection_tls_, reply_builder);
    }
    // Runtime state is already rolled back, and no commit record is ever
    // appended: recovery treats every record this write tagged as an aborted
    // prepare and drops it, so neither a reader nor a crash can observe half
    // of the command.
    co_return BuiltReply(AppendStorageError(reply_builder, status));
  }
  bool tx_commit_has_capacity = true;
  if (write_txid != 0) {
    // The two-hop path already discarded these journals in its finish hop;
    // the single-shard path has no finish hop. Settle both uniformly before
    // handing the receipts to the detached commit chain. Each shard erases
    // only its own journal.
    std::vector<unsigned> undo_owners;
    for (unsigned owner = 0; owner < ctx.tx_writes_.size(); ++owner) {
      storage::TxShardWrites& shard = ctx.tx_writes_[owner];
      if (!shard.collect_undo_) continue;
      shard.collect_undo_ = false;
      if (!shard.fences_.empty() || !shard.retirements_.empty()) {
        undo_owners.push_back(owner);
      }
    }
    absl::Status discarded = co_await ForEachParticipantParallel(
        undo_owners.size(), [&undo_owners, write_txid](std::size_t index) {
          return std::pair{undo_owners[index], [txid = write_txid] {
                             return g_storage->DiscardTxUndoLocal(txid);
                           }};
        });
    if (!discarded.ok()) {
      co_return BuiltReply(AppendStorageError(reply_builder, discarded));
    }
    if (replication != nullptr) {
      replication->SetFinalExpirations(ctx.tx_writes_);
    }
    tx_commit_has_capacity =
        g_storage->EnqueueTxCommit(write_txid, std::move(ctx.tx_writes_));
  }

  if (write && replication != nullptr) replication->Commit();
  if (!tx_commit_has_capacity) {
    co_await g_storage->WaitForTxCommitCapacity();
  }

  switch (request.kind_) {
    case CommandKind::kMSet:
      co_return BuiltReply(reply_builder.AppendSimpleString("OK"));
    case CommandKind::kMGet: {
      reply_builder.AppendArrayHeader(ctx.frames_.size());
      for (const auto& frame : ctx.frames_) {
        if (frame.has_value())
          reply_builder.AppendRaw(*frame);
        else
          reply_builder.AppendNull();
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
  // MSET must settle its command-local undo across all owners before EXEC
  // or a script can continue. A squashed run cannot roll back one command's
  // partial writes after a later command has already consumed them.
  return kind == CommandKind::kMSet || kind == CommandKind::kMSetNx ||
         kind == CommandKind::kLcs || kind == CommandKind::kBitOp;
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

bool IsLuaImmediateBlockingCommand(CommandKind kind) {
  // A script owns its declared-key transaction until it returns, so entering
  // the normal waiter path would deadlock the producer that could wake it.
  // Valkey instead gives these commands one immediate attempt. Keep this list
  // explicit: other kCmdMayBlock commands require their own script semantics.
  return kind == CommandKind::kBLPop || kind == CommandKind::kBRPop ||
         kind == CommandKind::kBLMove || kind == CommandKind::kBRPopLPush ||
         kind == CommandKind::kBZPopMin || kind == CommandKind::kBZPopMax ||
         kind == CommandKind::kXRead || kind == CommandKind::kXReadGroup;
}

bool LuaStreamReadHasBlockOption(const CommandRequest& command) {
  if (command.kind_ != CommandKind::kXRead &&
      command.kind_ != CommandKind::kXReadGroup) {
    return false;
  }
  const std::size_t option_begin =
      command.kind_ == CommandKind::kXReadGroup ? 4 : 1;
  for (std::size_t i = option_begin; i < command.args_.size(); ++i) {
    if (CmpCaseInsensitive(command.args_[i], "streams")) break;
    if (CmpCaseInsensitive(command.args_[i], "block")) return true;
  }
  return false;
}

bool IsExecSequentialSort(CommandKind kind) {
  return kind == CommandKind::kSort || kind == CommandKind::kSortRo;
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
  kSort,
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
  if (IsExecSequentialSort(kind)) return ExecSequentialFamily::kSort;
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

Task<std::string> ExecuteExecSequentialSort(
    const CommandRequest& command, const std::vector<ExecKey>& keys,
    std::vector<storage::TxShardWrites>& tx_writes,
    bool deterministic_set_order) {
  std::vector<SortExecKey> sort_keys;
  sort_keys.reserve(keys.size());
  for (const ExecKey& key : keys) {
    sort_keys.push_back(SortExecKey{
        .digest_ = key.digest_, .owner_ = key.owner_, .arg_ = key.arg_});
  }
  co_return co_await ExecuteSortCommandLocked(command, sort_keys, tx_writes,
                                              deterministic_set_order);
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

// `owners` is deduplicated by every caller, so no two steps land on one
// worker's journal. Arming happens only after every clear has succeeded, which
// is why a failure needs no unwinding.
Task<absl::Status> BeginExecCommandUndo(
    const std::vector<unsigned>& owners,
    std::vector<storage::TxShardWrites>& writes,
    std::vector<ExecWriteCheckpoint>* checkpoints) {
  checkpoints->clear();
  absl::Status discarded = co_await ForEachParticipantParallel(
      owners.size(), [&owners, &writes](std::size_t index) {
        const unsigned owner = owners[index];
        return std::pair{owner, [txid = writes[owner].txid_] {
                           return g_storage->DiscardTxUndoLocal(txid);
                         }};
      });
  if (!discarded.ok()) co_return discarded;
  checkpoints->reserve(owners.size());
  for (unsigned owner : owners) {
    checkpoints->push_back(ExecWriteCheckpoint{
        .owner_ = owner,
    });
    writes[owner].collect_undo_ = true;
  }
  co_return absl::OkStatus();
}

// The order sensitivity here is *within* a shard — reverse application order —
// and it lives inside RollbackTxLocal, so settling shards concurrently is
// safe: each unwinds only its own journal, into its own receipt slot.
Task<absl::Status> FinishExecCommandUndo(
    const std::vector<ExecWriteCheckpoint>& checkpoints,
    std::vector<storage::TxShardWrites>& writes, bool rollback) {
  // Compensation appends must not recursively enter the undo journal, and the
  // shards now run concurrently, so disarm all of them before any starts.
  for (const ExecWriteCheckpoint& checkpoint : checkpoints) {
    writes[checkpoint.owner_].collect_undo_ = false;
  }
  co_return co_await ForEachParticipantParallel(
      checkpoints.size(), [&checkpoints, &writes, rollback](std::size_t index) {
        storage::TxShardWrites& shard = writes[checkpoints[index].owner_];
        return std::pair{checkpoints[index].owner_,
                         [txid = shard.txid_, rollback, shard = &shard] {
                           return rollback
                                      ? g_storage->RollbackTxLocal(txid, shard)
                                      : g_storage->DiscardTxUndoLocal(txid);
                         }};
      });
}

Task<std::string> ExecuteExecSequentialRename(
    const CommandRequest& command, const std::vector<ExecKey>& keys,
    std::vector<storage::TxShardWrites>& tx_writes) {
  const auto& args = command.args_;
  const bool nx = command.kind_ == CommandKind::kRenameNx;
  if (nx) MarkReplicationCommandHandled(command);
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
    auto metadata = co_await SubmitTaskTo(
        source->owner_, [db = command.db_id_, key = std::string(args[1]),
                         digest = source->digest_]() {
          return g_storage->ReadKeyMetadataLocked(db, key, digest);
        });
    if (!metadata.ok()) co_return EncodeStorageError(metadata.status());
    const auto& info = *metadata;
    if (!info.exists_) co_return EncodeError("ERR no such key");
    co_return nx ? EncodeInteger(0) : EncodeSimpleString("OK");
  }
  if (destination == nullptr) {
    co_return EncodeError("ERR RENAME destination key is missing");
  }

  auto raw = co_await SubmitTaskTo(
      source->owner_, [db = command.db_id_, key = std::string(args[1]),
                       digest = source->digest_]() {
        return g_storage->ReadValueForTransferLocked(db, key, digest);
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
        return g_storage->WriteValueForTransferLocked(db, key, digest, *value,
                                                      writes);
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
  NotifyRenamedValue(command, args[2], raw->metadata_.value_type_);
  if (nx) {
    CaptureReplicationCommand(command, {"RENAME", args[1], args[2]});
  }
  co_return nx ? EncodeInteger(1) : EncodeSimpleString("OK");
}

Task<std::string> ExecuteExecSequentialCopy(
    const CommandRequest& command, const std::vector<ExecKey>& keys,
    std::vector<storage::TxShardWrites>& tx_writes) {
  MarkReplicationCommandHandled(command);
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
        return g_storage->ReadValueForTransferLocked(db, key, digest);
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
        return g_storage->WriteValueForTransferLocked(db, key, digest, *value,
                                                      writes);
      });
  if (!written.ok()) {
    absl::Status rolled_back =
        co_await FinishExecCommandUndo(checkpoints, tx_writes, true);
    co_return EncodeStorageError(rolled_back.ok() ? written : rolled_back);
  }
  absl::Status completed =
      co_await FinishExecCommandUndo(checkpoints, tx_writes, false);
  if (!completed.ok()) co_return EncodeStorageError(completed);
  NotifyRenamedValue(command, destination->db_, args[2],
                     raw->metadata_.value_type_);
  std::vector<std::string> canonical = args;
  if (!options->replace_) canonical.emplace_back("REPLACE");
  CaptureReplicationCommand(command, std::move(canonical));
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

  MarkReplicationCommandHandled(command);
  const auto& args = command.args_;
  const bool nx = command.kind_ == CommandKind::kMSetNx;
  auto find_key = [&](std::size_t argument) -> const ExecKey* {
    for (const ExecKey& key : keys) {
      if (key.arg_ == argument) return &key;
    }
    return nullptr;
  };
  if (nx) {
    for (std::size_t argument = 1; argument < args.size(); argument += 2) {
      const ExecKey* key = find_key(argument);
      if (key == nullptr) co_return EncodeError("ERR MSETNX key is missing");
      auto exists = [db = command.db_id_, name = std::string(args[argument]),
                     digest = key->digest_]() {
        return g_storage->ExistsLocked(db, name, digest);
      };
      bool present;
      if (key->owner_ == ThisWorker().id_) {
        present = co_await exists();
      } else {
        present = co_await SubmitTaskTo(key->owner_, exists);
      }
      if (present) co_return EncodeInteger(0);
    }
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
      co_return rolled_back.ok() ? EncodeError("ERR MSET key is missing")
                                 : EncodeStorageError(rolled_back);
    }
    auto write = [db = command.db_id_, name = std::string(args[argument]),
                  value = std::string(args[argument + 1]),
                  digest = key->digest_, writes = &tx_writes[key->owner_]]() {
      return g_storage->SetLocked(db, name, digest, value, {}, writes);
    };
    absl::StatusOr<storage::SetResult> result;
    if (key->owner_ == ThisWorker().id_) {
      result = co_await write();
    } else {
      result = co_await SubmitTaskTo(key->owner_, write);
    }
    if (!result.ok()) {
      absl::Status rolled_back =
          co_await FinishExecCommandUndo(checkpoints, tx_writes, true);
      co_return EncodeStorageError(rolled_back.ok() ? result.status()
                                                    : rolled_back);
    }
  }
  absl::Status completed =
      co_await FinishExecCommandUndo(checkpoints, tx_writes, false);
  if (!completed.ok()) co_return EncodeStorageError(completed);
  std::vector<std::string> canonical = args;
  canonical[0] = "MSET";
  CaptureReplicationCommand(command, std::move(canonical));
  co_return nx ? EncodeInteger(1) : EncodeSimpleString("OK");
}

Task<std::string> ExecuteExecSequentialSetMulti(
    const CommandRequest& command, const std::vector<ExecKey>& keys,
    std::vector<storage::TxShardWrites>& tx_writes) {
  std::vector<ExecWriteCheckpoint> checkpoints;
  bool undo_active = false;
  const auto oom = [] {
    return absl::ResourceExhaustedError(
        "OOM Set multi-key working set exceeds maxmemory");
  };
  const auto add_bytes = [](std::size_t* bytes, std::size_t count,
                            std::size_t width = 1) {
    if (count > (std::numeric_limits<std::size_t>::max() - *bytes) / width)
      return false;
    *bytes += count * width;
    return true;
  };
  const auto prepare_capture = [&](const std::vector<std::string>& first,
                                   const std::vector<std::string>& second,
                                   bool include_second) -> absl::Status {
    if (command.replication_capture_ == nullptr) return absl::OkStatus();
    std::size_t bytes = 0;
    for (const auto* effect : {&first, include_second ? &second : &first}) {
      if (!add_bytes(&bytes, effect->capacity(), sizeof(std::string)))
        return oom();
      for (const auto& argument : *effect) {
        if (!add_bytes(&bytes, argument.capacity() + 1)) return oom();
      }
      if (!include_second) break;
    }
    return command.replication_capture_->ReserveAdditionalCommands(
        include_second ? 2 : 1, bytes);
  };
  try {
    // These outlive the materialized input/output strings, including owner
    // hops. A failed command releases its scratch before attempting
    // command-local undo.
    std::vector<RetainedMemoryCharge> input_charges(command.args_.size());
    std::vector<RetainedMemoryCharge> vector_charges(command.args_.size());
    RetainedMemoryCharge aggregate_charge;
    RetainedMemoryCharge effect_charge;
    const auto& args = command.args_;
    auto find_key = [&](std::size_t argument) -> const ExecKey* {
      for (const ExecKey& key : keys) {
        if (key.arg_ == argument) return &key;
      }
      return nullptr;
    };
    auto run_set =
        [&](const ExecKey& key, storage::HashOperation operation,
            bool write) -> Task<absl::StatusOr<storage::HashResult>> {
      auto execute = [&]() -> Task<absl::StatusOr<storage::HashResult>> {
        auto result = co_await g_storage->ExecuteSetLocked(
            command.db_id_, args[key.arg_], key.digest_, operation,
            write ? &tx_writes[key.owner_] : nullptr);
        if (!result.ok()) co_return result.status();
        // Full kKeys can still use an uncharged legacy result. Establish the
        // retained owner before transferring its strings to the coordinator.
        if (operation.kind_ == storage::HashOperationKind::kKeys &&
            result->retained_charge_.bytes() == 0) {
          std::size_t bytes = 0;
          if (!add_bytes(&bytes, result->values_.capacity(),
                         sizeof(std::optional<std::string>)))
            co_return oom();
          for (const auto& member : result->values_) {
            if (member && !add_bytes(&bytes, member->capacity() + 1))
              co_return oom();
          }
          auto admitted = TryReserveMemory(bytes);
          if (!admitted) co_return oom();
          result->retained_charge_.Adopt(&*admitted, bytes);
        }
        co_return result;
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
      std::size_t bytes = 0;
      if (!add_bytes(&bytes, result->values_.size(), sizeof(std::string)))
        co_return oom();
      auto admitted = TryReserveMemory(bytes);
      if (!admitted) co_return oom();
      input_charges[argument] = std::move(result->retained_charge_);
      vector_charges[argument].Adopt(&*admitted, bytes);
      std::vector<std::string> members;
      members.reserve(result->values_.size());
      for (auto& member : result->values_) {
        members.push_back(std::move(*member));
      }
      co_return members;
    };

    if (command.kind_ == CommandKind::kSMove) {
      MarkReplicationCommandHandled(command);
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
      storage::HashOperation remove;
      remove.kind_ = storage::HashOperationKind::kDelete;
      remove.fields_.push_back(args[3]);
      storage::HashOperation add;
      add.kind_ = storage::HashOperationKind::kSet;
      add.fields_.push_back(args[3]);
      add.values_.push_back({});
      std::size_t effect_bytes = 4096;
      for (const auto& argument : args) {
        if (!add_bytes(&effect_bytes, argument.size() + 1, 4))
          co_return EncodeStorageError(oom());
      }
      auto effect_admission = TryReserveMemory(effect_bytes);
      if (!effect_admission) co_return EncodeStorageError(oom());
      effect_charge.Adopt(&*effect_admission, effect_bytes);
      std::vector<std::string> remove_effect{"SREM", args[1], args[3]};
      std::vector<std::string> add_effect{"SADD", args[2], args[3]};
      auto capture_ready = prepare_capture(remove_effect, add_effect, true);
      if (!capture_ready.ok()) co_return EncodeStorageError(capture_ready);
      absl::Status undo =
          co_await BeginExecCommandUndo(owners, tx_writes, &checkpoints);
      if (!undo.ok()) co_return EncodeStorageError(undo);
      undo_active = true;
      auto removed = co_await run_set(*source_key, std::move(remove), true);
      if (!removed.ok()) {
        absl::Status rolled_back =
            co_await FinishExecCommandUndo(checkpoints, tx_writes, true);
        undo_active = false;
        co_return EncodeStorageError(rolled_back.ok() ? removed.status()
                                                      : rolled_back);
      }
      auto added = co_await run_set(*destination, std::move(add), true);
      if (!added.ok()) {
        absl::Status rolled_back =
            co_await FinishExecCommandUndo(checkpoints, tx_writes, true);
        undo_active = false;
        co_return EncodeStorageError(rolled_back.ok() ? added.status()
                                                      : rolled_back);
      }
      absl::Status completed =
          co_await FinishExecCommandUndo(checkpoints, tx_writes, false);
      undo_active = false;
      if (!completed.ok()) co_return EncodeStorageError(completed);
      CaptureReplicationCommand(command, std::move(remove_effect));
      CaptureReplicationCommand(command, std::move(add_effect));
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
        const auto parsed = std::from_chars(
            text.data(), text.data() + text.size(), parsed_limit);
        if (parsed.ec != std::errc{} ||
            parsed.ptr != text.data() + text.size() || parsed_limit < 0) {
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
    std::size_t aggregate_bytes = 4096;
    for (const auto& source : sources) {
      if (!add_bytes(&aggregate_bytes, source.size(), 256))
        co_return EncodeStorageError(oom());
      for (const auto& member : source) {
        // The encoded EXEC/Lua reply is an owning string, so it can overlap the
        // hash table, ordered output and ReplyBuilder wire buffer.
        if (!add_bytes(&aggregate_bytes, member.capacity() + 1, 4))
          co_return EncodeStorageError(oom());
      }
    }
    auto aggregate_admission = TryReserveMemory(aggregate_bytes);
    if (!aggregate_admission) co_return EncodeStorageError(oom());
    aggregate_charge.Adopt(&*aggregate_admission, aggregate_bytes);
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
      MarkReplicationCommandHandled(command);
      const ExecKey* destination = find_key(1);
      if (destination == nullptr) {
        co_return EncodeError("ERR Set destination key is missing");
      }
      const std::vector<unsigned> owners = UniqueOwners({destination->owner_});
      // All STORE vectors and canonical copies are prepared while the original
      // destination is intact, not after DEL has entered the undo journal.
      storage::HashOperation add;
      add.kind_ = storage::HashOperationKind::kSet;
      add.fields_.reserve(output.size());
      add.values_.resize(output.size());
      for (const std::string& member : output) add.fields_.push_back(member);
      std::size_t effect_bytes = 4096;
      for (const auto& argument : args) {
        if (!add_bytes(&effect_bytes, argument.size() + 1, 4))
          co_return EncodeStorageError(oom());
      }
      for (const auto& member : output) {
        if (!add_bytes(&effect_bytes, member.size() + 1, 3) ||
            !add_bytes(&effect_bytes, 1, 128))
          co_return EncodeStorageError(oom());
      }
      auto effect_admission = TryReserveMemory(effect_bytes);
      if (!effect_admission) co_return EncodeStorageError(oom());
      effect_charge.Adopt(&*effect_admission, effect_bytes);
      std::vector<std::string> remove_effect{"DEL", args[1]};
      std::vector<std::string> canonical{"SADD", args[1]};
      if (!output.empty()) {
        std::vector<std::string> ordered(output.begin(), output.end());
        std::sort(ordered.begin(), ordered.end());
        canonical.insert(canonical.end(),
                         std::make_move_iterator(ordered.begin()),
                         std::make_move_iterator(ordered.end()));
      }
      auto capture_ready =
          prepare_capture(remove_effect, canonical, !output.empty());
      if (!capture_ready.ok()) co_return EncodeStorageError(capture_ready);
      absl::Status undo =
          co_await BeginExecCommandUndo(owners, tx_writes, &checkpoints);
      if (!undo.ok()) co_return EncodeStorageError(undo);
      undo_active = true;
      auto remove_destination = [&]() {
        return g_storage->DeleteLocked(command.db_id_, args[1],
                                       destination->digest_,
                                       &tx_writes[destination->owner_]);
      };
      absl::StatusOr<bool> deleted;
      if (destination->owner_ == ThisWorker().id_) {
        deleted = co_await remove_destination();
      } else {
        deleted =
            co_await SubmitTaskTo(destination->owner_, remove_destination);
      }
      if (!deleted.ok()) {
        absl::Status rolled_back =
            co_await FinishExecCommandUndo(checkpoints, tx_writes, true);
        undo_active = false;
        co_return EncodeStorageError(rolled_back.ok() ? deleted.status()
                                                      : rolled_back);
      }
      if (!output.empty()) {
        auto added = co_await run_set(*destination, std::move(add), true);
        if (!added.ok()) {
          absl::Status rolled_back =
              co_await FinishExecCommandUndo(checkpoints, tx_writes, true);
          undo_active = false;
          co_return EncodeStorageError(rolled_back.ok() ? added.status()
                                                        : rolled_back);
        }
      }
      absl::Status completed =
          co_await FinishExecCommandUndo(checkpoints, tx_writes, false);
      undo_active = false;
      if (!completed.ok()) co_return EncodeStorageError(completed);
      CaptureReplicationCommand(command, std::move(remove_effect));
      if (!output.empty()) {
        CaptureReplicationCommand(command, std::move(canonical));
      }
      co_return EncodeInteger(static_cast<long long>(output.size()));
    }

    std::vector<std::string> ordered(output.begin(), output.end());
    std::sort(ordered.begin(), ordered.end());
    ReplyBuilder builder(command.resp_version_);
    builder.AppendSetHeader(ordered.size());
    for (const std::string& member : ordered) builder.AppendBulkString(member);
    co_return std::string(builder.View());
  } catch (const std::bad_alloc&) {
    // Coroutine handlers cannot await; unwind owned scratch, then use the
    // same command-local compensation path as an ordinary storage OOM below.
  } catch (const std::length_error&) {
  }
  if (undo_active) {
    const auto restored =
        co_await FinishExecCommandUndo(checkpoints, tx_writes, true);
    if (!restored.ok()) co_return EncodeStorageError(restored);
  }
  co_return EncodeStorageError(oom());
}

Task<std::string> ExecuteExecSequentialListPop(
    const CommandRequest& command, const std::vector<ExecKey>& keys,
    std::vector<storage::TxShardWrites>& tx_writes) {
  MarkReplicationCommandHandled(command);
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
    ReplyBuilder builder(command.resp_version_);
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
    CaptureReplicationCommand(command,
                              {left ? "LPOP" : "RPOP", args[key.arg_],
                               std::to_string(result->values_.size())});
    co_return std::string(builder.View());
  }
  ReplyBuilder builder(command.resp_version_);
  co_return std::string(builder.AppendNullArray());
}

Task<std::string> ExecuteExecSequentialListMove(
    const CommandRequest& command, const std::vector<ExecKey>& keys,
    std::vector<storage::TxShardWrites>& tx_writes) {
  MarkReplicationCommandHandled(command);
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
    if (moved->values_.empty())
      co_return EncodeSemanticNull(command.resp_version_);
    const std::string& value = moved->values_.front();
    CaptureReplicationCommand(command,
                              {source_left ? "LPOP" : "RPOP", args[1]});
    CaptureReplicationCommand(
        command, {destination_left ? "LPUSH" : "RPUSH", args[2], value});
    NotifyListBlockingKey(command, args[1]);
    co_return EncodeBulkString(value);
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
    co_return EncodeSemanticNull(command.resp_version_);
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
  const std::string& value = popped->values_.front();
  CaptureReplicationCommand(command, {source_left ? "LPOP" : "RPOP", args[1]});
  CaptureReplicationCommand(
      command, {destination_left ? "LPUSH" : "RPUSH", args[2], value});
  NotifyListBlockingKey(command, args[1]);
  NotifyListBlockingKey(command, args[2]);
  co_return EncodeBulkString(value);
}

Task<std::string> ExecuteExecSequentialCommand(
    ExecSequentialFamily family, const CommandRequest& command,
    const std::vector<ExecKey>& keys,
    std::vector<storage::TxShardWrites>& tx_writes,
    bool script_context = false) {
  struct PreparedCaptureCleanup {
    ReplicationCommandCapture* capture;
    ~PreparedCaptureCleanup() {
      if (capture != nullptr) capture->ReleaseUnusedPreparation();
    }
  } capture_cleanup{family == ExecSequentialFamily::kSetMulti ||
                            family == ExecSequentialFamily::kZSetMulti
                        ? command.replication_capture_.get()
                        : nullptr};
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
    case ExecSequentialFamily::kSort:
      co_return co_await ExecuteExecSequentialSort(
          command, keys, tx_writes,
          /*deterministic_set_order=*/script_context);
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
        ReplyBuilder builder((*run.queued_)[i].resp_version_);
        builder.AppendArrayHeader(run.mget_[local].size());
        for (const auto& frame : run.mget_[local]) {
          if (frame.has_value())
            builder.AppendRaw(*frame);
          else
            builder.AppendNull();
        }
        (*run.replies_)[i] = std::move(builder).Release();
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

Task<absl::Status> ExecRunShardCallback(void* context,
                                        const tx::ShardSlice& slice);

bool IsDeclaredLuaKey(std::span<const std::string> keys,
                      std::string_view candidate) {
  return std::any_of(keys.begin(), keys.end(),
                     [&](const std::string& key) { return key == candidate; });
}

bool IsLuaReadOnlyKind(CommandKind kind) {
  return kind == CommandKind::kEvalRo || kind == CommandKind::kEvalShaRo ||
         kind == CommandKind::kFCallRo;
}

bool IsEvalSourceKind(CommandKind kind) {
  return kind == CommandKind::kEval || kind == CommandKind::kEvalRo;
}

std::string NormalizeEvalSha(std::string_view sha) {
  std::string normalized(sha);
  for (char& byte : normalized) {
    if (byte >= 'A' && byte <= 'Z') byte += 'a' - 'A';
  }
  return normalized;
}

void CollectLuaReplicationEffects(
    const CommandRequest& command, std::string_view reply,
    std::vector<CapturedReplicationCommand>* effects) {
  if (command.spec_ == nullptr || reply.empty() || reply.front() == '-' ||
      (command.spec_->flags_ & (kCmdWrite | kCmdMayReplicate)) == 0) {
    return;
  }
  CapturedReplicationEffects captured;
  if (command.replication_capture_ != nullptr) {
    captured = command.replication_capture_->Take();
  }
  if (!captured.handled_) {
    effects->push_back(
        CapturedReplicationCommand{command.db_id_, command.args_});
    return;
  }
  effects->insert(effects->end(),
                  std::make_move_iterator(captured.commands_.begin()),
                  std::make_move_iterator(captured.commands_.end()));
}

Task<CommandReply> ExecuteWait(ConnectionContext& ctx,
                               const CommandRequest& request,
                               ReplyBuilder& reply_builder, bool allow_blocking,
                               bool unresolved_write);

Task<std::string> ExecuteLuaRedisCall(
    const CommandRequest& eval_request, LuaRedisCall call,
    std::span<const std::string> declared_keys, tx::Transaction* transaction,
    std::vector<storage::TxShardWrites>* tx_writes,
    const std::shared_ptr<BlockingNotificationCapture>& notifications,
    std::vector<CapturedReplicationCommand>* effects,
    LuaExecution* lua_execution, ConnectionContext* connection) {
  RespCommand wire{.args_ = std::move(call.args_)};
  auto built = BuildCommandRequest(std::move(wire), eval_request.db_id_);
  if (!built.ok() || built->spec_ == nullptr) {
    const std::string name =
        built.ok() && !built->args_.empty() ? built->args_.front() : "";
    co_return EncodeError(
        absl::StrCat("ERR Unknown Redis command called from "
                     "script: ",
                     name));
  }
  CommandRequest command = std::move(*built);
  command.resp_version_ = lua_execution->resp_version();
  command.replication_origin_ = eval_request.replication_origin_;
  command.blocking_notification_capture_ = notifications;
  command.blocking_wake_cascade_ = eval_request.blocking_wake_cascade_;
  command.replication_capture_ = std::make_shared<ReplicationCommandCapture>();

  auto keys = DetermineKeys(*command.spec_, command.args_);
  if (!keys.ok()) {
    co_return EncodeError(absl::StrCat("ERR ", keys.status().message()));
  }
  if (command.kind_ == CommandKind::kWait) {
    if (command.args_.size() != 3) {
      co_return EncodeError("ERR wrong number of arguments for 'wait' command");
    }
    // The outer script owns its transaction and publishes only after Lua
    // returns. Waiting for a write already performed by this invocation would
    // deadlock that publication. Reuse the caller's pre-script watermark when
    // available, but force zero once the script has an unresolved write.
    ConnectionContext fallback;
    ConnectionContext& wait_context =
        connection != nullptr ? *connection : fallback;
    ReplyBuilder local_builder(RespVersion::k2);
    CommandReply local = co_await ExecuteWait(
        wait_context, command, local_builder, /*allow_blocking=*/false,
        /*unresolved_write=*/connection == nullptr || !effects->empty());
    co_return std::string(local.encoded_);
  }
  const std::uint32_t flags = command.spec_->flags_;
  const bool immediate_blocking = IsLuaImmediateBlockingCommand(command.kind_);
  if ((flags & (kCmdGlobal | kCmdAdmin | kCmdDynamicWrite)) != 0 ||
      ((flags & kCmdMayBlock) != 0 && !immediate_blocking) ||
      command.kind_ == CommandKind::kEval ||
      command.kind_ == CommandKind::kEvalSha ||
      command.kind_ == CommandKind::kEvalRo ||
      command.kind_ == CommandKind::kEvalShaRo ||
      command.kind_ == CommandKind::kFCall ||
      command.kind_ == CommandKind::kFCallRo ||
      command.kind_ == CommandKind::kScript ||
      command.kind_ == CommandKind::kFunction) {
    co_return EncodeError("ERR command is not allowed from script");
  }
  if (LuaStreamReadHasBlockOption(command)) {
    co_return EncodeError(
        absl::StrCat("ERR ", command.spec_->name_,
                     " command is not allowed with BLOCK option from scripts"));
  }
  if ((command.kind_ == CommandKind::kMSet ||
       command.kind_ == CommandKind::kMSetNx) &&
      command.args_.size() % 2 != 1) {
    co_return EncodeError(absl::StrCat("ERR wrong number of arguments for '",
                                       command.spec_->name_, "' command"));
  }
  const bool write = (flags & kCmdWrite) != 0;
  if ((IsLuaReadOnlyKind(eval_request.kind_) ||
       (lua_execution->function_flags() & kLuaFunctionNoWrites) != 0) &&
      (flags & (kCmdWrite | kCmdMayReplicate)) != 0) {
    co_return EncodeError(
        "ERR Write commands are not allowed from read-only scripts.");
  }
  const bool reject_writes = g_replication != nullptr
                                 ? g_replication->reject_writes()
                                 : g_replica_read_only;
  if (!command.replication_origin_ && write && reject_writes) {
    co_return EncodeError(
        "READONLY You can't write against a read only replica.");
  }
  const bool function_allows_oom =
      (lua_execution->function_flags() &
       (kLuaFunctionAllowOom | kLuaFunctionNoWrites)) != 0;
  if (write && !function_allows_oom && MayGrowMemory(command) &&
      RejectForMemory(0)) {
    co_return EncodeError(
        "OOM command not allowed when used memory > 'maxmemory'.");
  }

  if (keys->empty()) {
    if (command.kind_ != CommandKind::kPing &&
        command.kind_ != CommandKind::kEcho) {
      co_return EncodeError("ERR command is not allowed from script");
    }
    ReplyBuilder local_builder(RespVersion::k2);
    CommandReply local = ExecuteSimpleLocalCommand(command, local_builder);
    co_return std::string(local.encoded_);
  }

  std::vector<ExecKey> command_keys;
  command_keys.reserve(keys->count());
  std::uint16_t slot = 0;
  for (std::size_t index = keys->first_; index <= keys->last_;
       index += keys->step_) {
    if (!IsDeclaredLuaKey(declared_keys, command.args_[index])) {
      co_return EncodeError("ERR Script attempted to access an undeclared key");
    }
    command_keys.push_back(ExecKey{
        .digest_ = storage::ComputeDigest(command.args_[index]),
        .owner_ = static_cast<std::uint16_t>(ShardForKey(command.args_[index])),
        .arg_ = static_cast<std::uint16_t>(index),
        .slot_ = slot++,
        .mode_ = tx::LockMode::kExclusive,
        .db_ = command.db_id_,
    });
  }

  if (cluster::ClusterEnabled() && !command.replication_origin_) {
    // Cluster backstop on top of declared-key confinement: every accessed key
    // must stay within the slot set the script was admitted with. Redis raises
    // this same error from its script path (getNodeByQuery for scripts).
    for (const ExecKey& key : command_keys) {
      const std::uint16_t key_slot =
          storage::RedisSlot(command.args_[key.arg_]);
      const std::span<const std::uint16_t> admitted_slots =
          eval_request.ClusterSlots();
      if (std::find(admitted_slots.begin(), admitted_slots.end(), key_slot) ==
          admitted_slots.end()) {
        co_return EncodeError(
            "ERR Script attempted to access a non local key in a cluster "
            "node");
      }
    }
  }

  const bool routed_multi = (flags & (kCmdMultiShard | kCmdMovableKeys)) != 0 ||
                            command_keys.size() != 1;
  const ExecSequentialFamily sequential = ClassifyExecSequential(command.kind_);
  const bool batch_supported = command.kind_ == CommandKind::kMSet ||
                               command.kind_ == CommandKind::kMGet ||
                               command.kind_ == CommandKind::kDel ||
                               command.kind_ == CommandKind::kUnlink ||
                               command.kind_ == CommandKind::kExists ||
                               command.kind_ == CommandKind::kTouch;
  const bool unsafe_special_case =
      command.kind_ == CommandKind::kCopy ||
      command.kind_ == CommandKind::kGeoRadius ||
      command.kind_ == CommandKind::kGeoRadiusByMember ||
      command.kind_ == CommandKind::kGeoSearchStore;
  if (routed_multi &&
      (transaction == nullptr || unsafe_special_case ||
       (sequential == ExecSequentialFamily::kNone && !batch_supported))) {
    co_return EncodeError("ERR command is not supported from script");
  }
  if (write && !lua_execution->MarkWriteCommand()) {
    co_return EncodeError("ERR Script killed by user with SCRIPT KILL...");
  }

  // Choke point 2 for redis.call: re-check the script's captured authority
  // before dispatching a write. The single-key hop below re-checks again on
  // the owner worker (it bypasses tx::Transaction); the routed_multi path is
  // covered by the transaction's shard validator; the sequential families
  // mutate through SubmitTaskTo inside the already-armed transaction and rely
  // on this pre-dispatch check.
  if (write && !command.replication_origin_ &&
      eval_request.cluster_authority_admission_ != nullptr) {
    if (!RecheckClusterRequestAuthority(eval_request).ok()) {
      co_return EncodeError(
          "ERR Script attempted to access a non local key in a cluster node");
    }
  }

  std::string reply;
  if (routed_multi && sequential != ExecSequentialFamily::kNone) {
    reply = co_await ExecuteExecSequentialCommand(sequential, command,
                                                  command_keys, *tx_writes,
                                                  /*script_context=*/true);
    CollectLuaReplicationEffects(command, reply, effects);
    co_return reply;
  }
  if (routed_multi) {
    std::vector<CommandRequest> queued{command};
    std::vector<std::vector<ExecKey>> all_keys{std::move(command_keys)};
    std::vector<std::string> replies(1);
    std::vector<ReplyChunkSource> reply_chunks(1);
    ExecRunContext run;
    InitExecRun(run, queued, all_keys, replies, reply_chunks, 0, 1);
    run.tx_writes_ = tx_writes->data();
    absl::Status dispatched = co_await transaction->Execute(
        &ExecRunShardCallback, &run, /*release=*/false);
    if (!dispatched.ok()) {
      if (IsClusterAuthorityChanged(dispatched)) {
        co_return EncodeError(
            "ERR Script attempted to access a non local key in a cluster "
            "node");
      }
      co_return EncodeStorageError(dispatched);
    }
    AssembleRunReplies(run);
    if (reply_chunks.front()) {
      co_return EncodeError(
          "ERR streamed command replies are not allowed from script");
    }
    reply = std::move(replies.front());
    CollectLuaReplicationEffects(command, reply, effects);
    co_return reply;
  }

  const std::size_t key_index = command_keys.front().arg_;
  const storage::Digest digest = command_keys.front().digest_;
  const unsigned owner = command_keys.front().owner_;
  ReplyChunkSource chunks;
  absl::Status dispatched =
      co_await SubmitTaskTo(owner, [&]() -> Task<absl::Status> {
        // The per-call hop bypasses tx::Transaction, so re-check the
        // admission on the owner right before mutating.
        if (write && eval_request.cluster_authority_admission_ != nullptr &&
            !RecheckClusterRequestAuthority(eval_request).ok()) {
          co_return ClusterAuthorityChangedStatus();
        }
        storage::TxShardWrites* writes =
            tx_writes == nullptr ? nullptr : &(*tx_writes)[owner];
        if (command.kind_ == CommandKind::kDel ||
            command.kind_ == CommandKind::kUnlink) {
          auto deleted = co_await g_storage->DeleteLocked(
              command.db_id_, command.args_[key_index], digest, writes);
          if (!deleted.ok()) {
            reply = EncodeStorageError(deleted.status());
          } else {
            reply = EncodeInteger(*deleted ? 1 : 0);
          }
          co_return absl::OkStatus();
        }
        if (command.kind_ == CommandKind::kExists ||
            command.kind_ == CommandKind::kTouch) {
          auto metadata = co_await g_storage->ReadKeyMetadataLocked(
              command.db_id_, command.args_[key_index], digest);
          reply = metadata.ok() ? EncodeInteger(metadata->exists_ ? 1 : 0)
                                : EncodeStorageError(metadata.status());
          co_return absl::OkStatus();
        }
        reply = co_await RunSingleKeyLocked(command.db_id_, command, digest,
                                            writes, &chunks);
        co_return absl::OkStatus();
      });
  if (!dispatched.ok()) {
    if (IsClusterAuthorityChanged(dispatched)) {
      co_return EncodeError(
          "ERR Script attempted to access a non local key in a cluster node");
    }
    co_return EncodeStorageError(dispatched);
  }
  if (chunks) {
    co_return EncodeError(
        "ERR streamed command replies are not allowed from script");
  }
  CollectLuaReplicationEffects(command, reply, effects);
  co_return reply;
}

Task<bool> BeginLuaExecution() {
  while (g_lua_execution_active) {
    co_await Yield(*ThisWorker().self_);
  }
  g_lua_execution_active = true;
  co_return true;
}

class LuaExecutionGuard {
 public:
  LuaExecutionGuard() = default;
  LuaExecutionGuard(const LuaExecutionGuard&) = delete;
  LuaExecutionGuard& operator=(const LuaExecutionGuard&) = delete;
  ~LuaExecutionGuard() { g_lua_execution_active = false; }
};

Task<bool> CacheLuaScriptOnAllWorkers(const std::string& sha,
                                      std::string_view bytecode) {
  auto operation = co_await AcquireFunctionCatalogOperation();
  bytecode = StoreLuaScript(sha, bytecode);
  if (bytecode.empty()) co_return false;
  for (unsigned worker = 0; worker < g_storage->worker_count(); ++worker) {
    auto cache = [sha, bytecode] {
      return CacheLuaScriptLocally(sha, bytecode);
    };
    bool cached = false;
    if (worker == ThisWorker().id_) {
      cached = cache();
    } else {
      cached = co_await SubmitTo(worker, std::move(cache));
    }
    if (!cached) co_return false;
  }
  co_return true;
}

Task<bool> FlushLuaScriptCacheOnAllWorkers() {
  auto operation = co_await AcquireFunctionCatalogOperation();
  for (unsigned worker = 0; worker < g_storage->worker_count(); ++worker) {
    auto clear = [] {
      ClearLocalLuaScriptCache();
      return true;
    };
    if (worker == ThisWorker().id_) {
      clear();
    } else {
      (void)co_await SubmitTo(worker, std::move(clear));
    }
  }
  // Worker indexes hold string_views into the process-wide bodies. Release
  // the bodies only after every worker has crossed the clear barrier.
  ClearStoredLuaScripts();
  co_return true;
}

struct PreparedFunctionMutationPublication {
  storage::ReplicationPublisherAdmission admission_;
  storage::PreparedReplicationCommandPublication publication_;
};

Task<absl::StatusOr<std::optional<PreparedFunctionMutationPublication>>>
PrepareFunctionMutationPublication(const CommandRequest& request) {
  if (request.replication_capture_ != nullptr) {
    co_return std::nullopt;
  }
  if (request.replication_origin_ || g_storage == nullptr ||
      !g_storage->ReplicationLogActive() ||
      (g_replication != nullptr && g_replication->is_replica())) {
    co_return std::nullopt;
  }
  if (g_active_replication_publisher_admission == nullptr) {
    co_return absl::FailedPreconditionError(
        "Function mutation has no worker-zero publisher reservation");
  }
  const auto found = std::find_if(
      g_active_replication_publisher_admission->worker_tokens_.begin(),
      g_active_replication_publisher_admission->worker_tokens_.end(),
      [](const ReplicationPublisherAdmission::WorkerToken& token) {
        return token.worker_ == 0;
      });
  if (found == g_active_replication_publisher_admission->worker_tokens_.end()) {
    co_return absl::FailedPreconditionError(
        "Function mutation did not reserve the catalog flow");
  }
  storage::ReplicationPublisherAdmission admission = found->token_;
  std::vector<std::string> args = request.args_;
  if (ThisWorker().id_ == 0) {
    auto publication = g_storage->PrepareAdmittedReplicationCommand(
        admission, storage::ReplicationEventKind::kCatalogMutation, 0,
        std::move(args), std::vector<std::string>{});
    if (!publication.ok()) co_return publication.status();
    co_return PreparedFunctionMutationPublication{
        .admission_ = std::move(admission),
        .publication_ = std::move(*publication),
    };
  }
  co_return co_await SubmitTaskTo(
      0,
      [admission = std::move(admission), args = std::move(args)]() mutable
          -> Task<absl::StatusOr<
              std::optional<PreparedFunctionMutationPublication>>> {
        auto publication = g_storage->PrepareAdmittedReplicationCommand(
            admission, storage::ReplicationEventKind::kCatalogMutation, 0,
            std::move(args), std::vector<std::string>{});
        if (!publication.ok()) co_return publication.status();
        co_return PreparedFunctionMutationPublication{
            .admission_ = std::move(admission),
            .publication_ = std::move(*publication),
        };
      });
}

Task<absl::Status> PublishFunctionMutation(
    const CommandRequest& request,
    std::optional<PreparedFunctionMutationPublication> prepared) {
  if (request.replication_capture_ != nullptr) {
    CaptureReplicationCommand(request, request.args_);
    co_return absl::OkStatus();
  }
  if (!prepared.has_value()) co_return absl::OkStatus();
  if (ThisWorker().id_ == 0) {
    co_return g_storage->PublishPreparedReplicationCommand(
        prepared->admission_, std::move(prepared->publication_));
  }
  co_return co_await SubmitTaskTo(
      0, [prepared = std::move(*prepared)]() mutable -> Task<absl::Status> {
        co_return g_storage->PublishPreparedReplicationCommand(
            prepared.admission_, std::move(prepared.publication_));
      });
}

bool IsEvalCommand(const CommandRequest& request) {
  return request.kind_ == CommandKind::kEval ||
         request.kind_ == CommandKind::kEvalSha ||
         request.kind_ == CommandKind::kEvalRo ||
         request.kind_ == CommandKind::kEvalShaRo;
}

bool IsFCallCommand(const CommandRequest& request) {
  return request.kind_ == CommandKind::kFCall ||
         request.kind_ == CommandKind::kFCallRo;
}

bool IsLuaInvocationCommand(const CommandRequest& request) {
  return IsEvalCommand(request) || IsFCallCommand(request);
}

bool ExecCommandMayWrite(const CommandRequest& request) {
  return request.spec_ != nullptr &&
         (request.spec_->flags_ & (kCmdWrite | kCmdDynamicWrite)) != 0;
}

bool ExecCommandMayReplicate(const CommandRequest& request) {
  return request.spec_ != nullptr &&
         (request.spec_->flags_ &
          (kCmdWrite | kCmdMayReplicate | kCmdDynamicWrite)) != 0;
}

bool IsFunctionCatalogMutation(const CommandRequest& request) {
  if (request.kind_ != CommandKind::kFunction || request.args_.size() < 2) {
    return false;
  }
  const std::string_view subcommand = request.args_[1];
  return CmpCaseInsensitive(subcommand, "load") ||
         CmpCaseInsensitive(subcommand, "delete") ||
         CmpCaseInsensitive(subcommand, "flush") ||
         CmpCaseInsensitive(subcommand, "restore");
}

Task<std::string> ExecuteEvalWithTransaction(
    const CommandRequest& request, tx::Transaction* transaction,
    std::vector<storage::TxShardWrites>* tx_writes,
    const std::shared_ptr<BlockingNotificationCapture>& notifications,
    std::vector<CapturedReplicationCommand>* effects,
    ConnectionContext* connection) {
  auto key_view = DetermineKeys(*request.spec_, request.args_);
  if (!key_view.ok()) {
    co_return EncodeError(absl::StrCat("ERR ", key_view.status().message()));
  }

  const bool source_kind = IsEvalSourceKind(request.kind_);
  const bool function_kind = IsFCallCommand(request);
  std::unique_ptr<FunctionCatalogOperationGuard> function_catalog_guard;
  if (function_kind) {
    function_catalog_guard = co_await AcquireFunctionCatalogOperation();
  }
  std::string_view script;
  std::string sha;
  if (source_kind) {
    script = request.args_[1];
    sha = LuaScriptSha1(script);
  } else if (!function_kind) {
    // Redis accepts EVALSHA digests in either case. Keep this normalization
    // local to EVALSHA/EVALSHA_RO; SCRIPT EXISTS remains an exact lookup.
    sha = NormalizeEvalSha(request.args_[1]);
  }

  const std::size_t key_count = key_view->count();
  const std::size_t key_begin = key_count == 0 ? 3 : key_view->first_;
  const std::span<const std::string> declared_keys(
      request.args_.data() + key_begin, key_count);
  const std::size_t argv_begin = 3 + key_count;
  const std::span<const std::string> script_argv(
      request.args_.data() + argv_begin, request.args_.size() - argv_begin);
  (void)co_await BeginLuaExecution();
  LuaExecutionGuard lua_execution;
  const bool source_cached =
      source_kind && !sha.empty() && FindCachedLuaScript(sha).has_value();
  absl::StatusOr<std::unique_ptr<LuaExecution>> execution =
      function_kind
          ? LuaExecution::CreateFunction(request.args_[1], declared_keys,
                                         script_argv, request.resp_version_)
      : source_kind && !source_cached
          ? LuaExecution::Create(script, declared_keys, script_argv,
                                 request.resp_version_)
          : LuaExecution::CreateCached(sha, declared_keys, script_argv,
                                       request.resp_version_);
  if (!execution.ok()) {
    if (absl::IsNotFound(execution.status())) {
      if (function_kind) co_return EncodeError("ERR Function not found");
      co_return EncodeError("NOSCRIPT No matching script. Please use EVAL.");
    }
    co_return EncodeError(absl::StrCat("ERR Error compiling script: ",
                                       execution.status().message()));
  }
  if (function_kind && cluster::ClusterEnabled() &&
      ((*execution)->function_flags() & kLuaFunctionNoCluster) != 0) {
    // Redis refuses no-cluster functions on cluster nodes (script.c; the text
    // is verbatim and addReplyError prefixes "-ERR "). The check sits after
    // function resolution (the flags are only known then) and before any
    // execution. It precedes the *_ro write-flag check so FCALL_RO on a
    // no-cluster function reports the cluster reason first.
    co_return EncodeError(
        "ERR Can not run script on cluster, 'no-cluster' flag is set.");
  }
  if (request.kind_ == CommandKind::kFCallRo &&
      (((*execution)->function_flags() & kLuaFunctionNoWrites) == 0)) {
    co_return EncodeError(
        "ERR Can not execute a script with write flag using *_ro command.");
  }
  if (function_kind) {
    const std::uint64_t flags = (*execution)->function_flags();
    const bool reject_writes = g_replication != nullptr
                                   ? g_replication->reject_writes()
                                   : g_replica_read_only;
    if (!request.replication_origin_ && reject_writes &&
        (flags & kLuaFunctionNoWrites) == 0) {
      co_return EncodeError(
          "READONLY Can not run script with write flag on readonly replica");
    }
    if ((flags & (kLuaFunctionAllowOom | kLuaFunctionNoWrites)) == 0 &&
        RejectForMemory(0)) {
      co_return EncodeError(
          "OOM allow-oom flag is not set on the script, can not run it when "
          "used memory > 'maxmemory'");
    }
    // The callback is rooted on this coroutine's stack, and LuaExecution owns
    // the worker runtime that created the thread. Catalog replacement can
    // therefore retire that whole VM without invalidating the in-flight call.
    // Keep the catalog barrier only around lookup instead of serializing all
    // FCALL execution process-wide.
    function_catalog_guard.reset();
  }
  if (source_kind && !source_cached && !sha.empty()) {
    const bool cached =
        co_await CacheLuaScriptOnAllWorkers(sha, (*execution)->bytecode());
    if (!cached) {
      co_return EncodeError("ERR unable to cache compiled Lua script");
    }
  }

  LuaExecutionStep step =
      (*execution)
          ->Start(request.replication_origin_,
                  source_kind ? std::string_view(sha)
                              : std::string_view(request.args_[1]),
                  request.args_);
  for (;;) {
    if (step.scheduler_yield_) {
      co_await Yield(*ThisWorker().self_);
      step = (*execution)->ResumeAfterSchedulerYield();
      continue;
    }
    if (!step.call_.has_value()) break;
    std::string command_reply = co_await ExecuteLuaRedisCall(
        request, std::move(*step.call_), declared_keys, transaction, tx_writes,
        notifications, effects, execution->get(), connection);
    step = (*execution)->Resume(command_reply);
  }
  co_return std::move(step.reply_);
}

void StoreEvalReplicationEffects(
    const CommandRequest& request,
    std::vector<CapturedReplicationCommand> effects) {
  if (request.replication_capture_ == nullptr) return;
  MarkReplicationCommandHandled(request);
  for (CapturedReplicationCommand& effect : effects) {
    CaptureReplicationCommand(request, effect.db_id_, std::move(effect.args_));
  }
}

Task<CommandReply> ExecuteEval(const CommandRequest& request,
                               ReplyBuilder& reply_builder,
                               ConnectionContext* connection) {
  auto key_view = DetermineKeys(*request.spec_, request.args_);
  if (!key_view.ok()) {
    co_return BuiltReply(reply_builder.AppendError(
        absl::StrCat("ERR ", key_view.status().message())));
  }

  const std::size_t key_count = key_view->count();
  const bool read_only = IsLuaReadOnlyKind(request.kind_);
  const std::size_t key_begin = key_count == 0 ? 3 : key_view->first_;
  const std::span<const std::string> declared_keys(
      request.args_.data() + key_begin, key_count);
  auto notifications =
      std::make_shared<BlockingNotificationCapture>(g_storage->worker_count());
  std::vector<CapturedReplicationCommand> effects;
  std::vector<storage::TxShardWrites> tx_writes;
  std::optional<tx::Transaction> transaction;
  std::unique_ptr<ReplicationTransactionGuard> replication;
  // Owner-side authority re-check for the declared-key transaction (choke
  // point 2). The context is consulted from shard threads and must
  // outlive every hop of the transaction, so it lives at function scope.
  ClusterShardValidatorContext cluster_validator;

  if (key_count != 0) {
    transaction.emplace();
    for (std::size_t i = 0; i < key_count; ++i) {
      const std::string& key = declared_keys[i];
      transaction->AddKey(
          ShardForKey(key), request.db_id_, storage::ComputeDigest(key),
          static_cast<std::uint32_t>(i),
          read_only ? tx::LockMode::kShared : tx::LockMode::kExclusive);
    }
    transaction->Seal();
    if (!read_only) {
      InstallClusterShardValidator(*transaction, request, cluster_validator);
      replication =
          std::make_unique<ReplicationTransactionGuard>(request, &*transaction);
      if (!replication->status().ok()) {
        co_return BuiltReply(
            AppendStorageError(reply_builder, replication->status()));
      }
    }
    absl::Status scheduled = co_await transaction->Schedule();
    if (!scheduled.ok()) {
      co_return BuiltReply(
          reply_builder.AppendError(absl::StrCat("ERR ", scheduled.message())));
    }
    absl::Status armed = co_await transaction->Execute(
        &ArmOnlyShardCallback, nullptr, /*release=*/false);
    if (!armed.ok()) {
      transaction->SetShardValidator(nullptr, nullptr);
      (void)co_await transaction->Release();
      if (IsClusterAuthorityChanged(armed)) {
        co_return BuiltReply(reply_builder.AppendError(
            "ERR Script attempted to access a non local key in a cluster "
            "node"));
      }
      co_return BuiltReply(
          reply_builder.AppendError(absl::StrCat("ERR ", armed.message())));
    }
    if (!read_only) {
      const std::uint64_t txid = storage::StorageEngine::AllocateWriteTxid();
      tx_writes.resize(g_storage->worker_count());
      g_storage->InitializeTxWrites(txid, tx_writes,
                                    ClusterMutationPrecondition(request));
    }
  }

  std::string eval_reply = co_await ExecuteEvalWithTransaction(
      request, transaction.has_value() ? &*transaction : nullptr,
      tx_writes.empty() ? nullptr : &tx_writes, notifications, &effects,
      connection);

  if (transaction.has_value()) {
    if (!read_only) {
      // The script's mutations are complete once it returns; the publish and
      // release hops settle them and must not be fenced off retroactively.
      transaction->SetShardValidator(nullptr, nullptr);
      // redis.pcall can catch a command error, but cannot make a known failed
      // physical grouped transaction commit-safe. Reject before publishing
      // successful-looking effects or handing its receipt to the async queue.
      const auto valid = storage::StorageEngine::ValidateTxCommit(tx_writes);
      if (!valid.ok()) {
        (void)co_await transaction->Release();
        co_return BuiltReply(AppendStorageError(reply_builder, valid));
      }
      absl::Status published = co_await transaction->Execute(
          &PublishFullSyncEffectsCallback, &tx_writes, /*release=*/false);
      if (!published.ok()) {
        (void)co_await transaction->Release();
        co_return BuiltReply(reply_builder.AppendError(
            absl::StrCat("ERR ", published.message())));
      }
    }
    absl::Status released = co_await transaction->Release();
    if (!released.ok()) {
      co_return BuiltReply(
          reply_builder.AppendError(absl::StrCat("ERR ", released.message())));
    }
    if (!read_only && !effects.empty() && replication != nullptr) {
      replication->SetCommandArgs(
          EncodeReplicationCommandEffects(std::move(effects)));
      replication->SetFinalExpirations(tx_writes);
      replication->Commit();
    }
    if (!read_only) {
      const std::uint64_t txid = tx_writes.front().txid_;
      if (!g_storage->EnqueueTxCommit(txid, std::move(tx_writes))) {
        co_await g_storage->WaitForTxCommitCapacity();
      }
    }
  }

  absl::Status notified = co_await FlushBlockingNotifications(
      *notifications, request.blocking_wake_cascade_);
  if (!notified.ok()) {
    co_return BuiltReply(
        reply_builder.AppendError(absl::StrCat("ERR ", notified.message())));
  }
  co_return BuiltReply(reply_builder.AppendRaw(eval_reply));
}

Task<CommandReply> ExecuteScript(const CommandRequest& request,
                                 ReplyBuilder& reply_builder) {
  const std::string_view subcommand = request.args_[1];
  if (CmpCaseInsensitive(subcommand, "help") && request.args_.size() == 2) {
    constexpr std::array<std::string_view, 17> help = {
        "SCRIPT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "DEBUG (YES|SYNC|NO)",
        "    Set the debug mode for subsequent scripts executed.",
        "EXISTS <sha1> [<sha1> ...]",
        "    Return information about the existence of the scripts in the "
        "script cache.",
        "FLUSH [ASYNC|SYNC]",
        "    Flush the Lua scripts cache. Very dangerous on replicas.",
        "    When called without the optional mode argument, the behavior is "
        "determined by the",
        "    lazyfree-lazy-user-flush configuration directive. Valid modes "
        "are:",
        "    * ASYNC: Asynchronously flush the scripts cache.",
        "    * SYNC: Synchronously flush the scripts cache.",
        "KILL",
        "    Kill the currently executing Lua script.",
        "LOAD <script>",
        "    Load a script into the scripts cache without executing it.",
        "HELP",
        "    Print this help.",
    };
    reply_builder.AppendArrayHeader(help.size());
    for (std::string_view line : help) reply_builder.AppendSimpleString(line);
    co_return BuiltReply(reply_builder.View());
  }
  if (CmpCaseInsensitive(subcommand, "kill") && request.args_.size() == 2) {
    switch (RequestLuaScriptKill(false)) {
      case LuaScriptKillResult::kKilled:
        co_return BuiltReply(reply_builder.AppendSimpleString("OK"));
      case LuaScriptKillResult::kNotBusy:
        co_return BuiltReply(reply_builder.AppendError(
            "NOTBUSY No scripts in execution right now."));
      case LuaScriptKillResult::kUnkillableWrite:
        co_return BuiltReply(reply_builder.AppendError(
            "UNKILLABLE Sorry the script already executed write commands "
            "against the dataset. You can either wait the script termination "
            "or kill the server in a hard way using the SHUTDOWN NOSAVE "
            "command."));
      case LuaScriptKillResult::kUnkillableReplication:
        co_return BuiltReply(reply_builder.AppendError(
            "UNKILLABLE The busy script was sent by a master instance in the "
            "context of replication and cannot be killed."));
      case LuaScriptKillResult::kWrongInvocationKind:
        co_return BuiltReply(reply_builder.AppendError(
            "BUSY Redis is busy running a script. You can only call FUNCTION "
            "KILL or SHUTDOWN NOSAVE."));
    }
  }
  if (CmpCaseInsensitive(subcommand, "load")) {
    if (request.args_.size() != 3) {
      co_return BuiltReply(reply_builder.AppendError(
          "ERR wrong number of arguments for 'script|load' command"));
    }
    constexpr std::span<const std::string> empty;
    const std::string sha = LuaScriptSha1(request.args_[2]);
    if (sha.empty()) {
      co_return BuiltReply(
          reply_builder.AppendError("ERR unable to compute script SHA1"));
    }
    (void)co_await BeginLuaExecution();
    LuaExecutionGuard lua_execution;
    if (FindCachedLuaScript(sha).has_value()) {
      co_return BuiltReply(reply_builder.AppendBulkString(sha));
    }
    auto execution = LuaExecution::Create(request.args_[2], empty, empty);
    if (!execution.ok()) {
      co_return BuiltReply(reply_builder.AppendError(absl::StrCat(
          "ERR Error compiling script: ", execution.status().message())));
    }
    if (!(co_await CacheLuaScriptOnAllWorkers(sha, (*execution)->bytecode()))) {
      co_return BuiltReply(
          reply_builder.AppendError("ERR unable to cache compiled Lua script"));
    }
    co_return BuiltReply(reply_builder.AppendBulkString(sha));
  }

  if (CmpCaseInsensitive(subcommand, "exists")) {
    if (request.args_.size() < 3) {
      co_return BuiltReply(reply_builder.AppendError(
          "ERR wrong number of arguments for 'script|exists' command"));
    }
    reply_builder.AppendArrayHeader(request.args_.size() - 2);
    for (std::size_t i = 2; i < request.args_.size(); ++i) {
      reply_builder.AppendInteger(FindCachedLuaScript(request.args_[i]) ? 1
                                                                        : 0);
    }
    co_return BuiltReply(reply_builder.View());
  }

  if (CmpCaseInsensitive(subcommand, "flush")) {
    if (request.args_.size() > 3 ||
        (request.args_.size() == 3 &&
         !CmpCaseInsensitive(request.args_[2], "sync") &&
         !CmpCaseInsensitive(request.args_[2], "async"))) {
      co_return BuiltReply(reply_builder.AppendError(
          "ERR SCRIPT FLUSH only support SYNC|ASYNC option"));
    }
    (void)co_await FlushLuaScriptCacheOnAllWorkers();
    co_return BuiltReply(reply_builder.AppendSimpleString("OK"));
  }

  co_return BuiltReply(reply_builder.AppendError(
      absl::StrCat("ERR Unknown subcommand or wrong number of arguments for '",
                   subcommand, "'. Try SCRIPT HELP.")));
}

std::vector<std::string_view> LuaFunctionFlagNames(std::uint64_t flags) {
  std::vector<std::string_view> names;
  if ((flags & kLuaFunctionNoWrites) != 0) names.push_back("no-writes");
  if ((flags & kLuaFunctionAllowOom) != 0) names.push_back("allow-oom");
  if ((flags & kLuaFunctionAllowStale) != 0) names.push_back("allow-stale");
  if ((flags & kLuaFunctionNoCluster) != 0) names.push_back("no-cluster");
  if ((flags & kLuaFunctionAllowCrossSlotKeys) != 0) {
    names.push_back("allow-cross-slot-keys");
  }
  return names;
}

void AppendLuaFunctionFlags(ReplyBuilder& reply_builder, std::uint64_t flags) {
  const std::vector<std::string_view> names = LuaFunctionFlagNames(flags);
  reply_builder.AppendSetHeader(names.size());
  for (std::string_view name : names) reply_builder.AppendSimpleString(name);
}

bool FunctionMutationRejected(const CommandRequest& request) {
  if (request.replication_origin_) return false;
  return g_replication != nullptr ? g_replication->reject_writes()
                                  : g_replica_read_only;
}

Task<absl::Status> ApplyFunctionCatalogTarget(
    std::vector<LuaFunctionLibrary> target, const CommandRequest& request) {
  auto staged =
      co_await GlobalFunctionCatalog().StageCompleteCatalog(std::move(target));
  if (!staged.ok()) co_return staged.status();
  auto publication = co_await PrepareFunctionMutationPublication(request);
  if (!publication.ok()) {
    co_await GlobalFunctionCatalog().AbortStagedCatalog(&*staged);
    co_return publication.status();
  }
  auto durable =
      co_await GlobalFunctionCatalog().MakeStagedCatalogDurable(*staged);
  if (!durable.ok()) {
    co_await GlobalFunctionCatalog().AbortStagedCatalog(&*staged);
    co_return durable.status();
  }
  absl::Status installed = co_await GlobalFunctionCatalog().CommitStagedCatalog(
      std::move(*staged), *durable);
  if (!installed.ok()) co_return installed;
  absl::Status published =
      co_await PublishFunctionMutation(request, std::move(*publication));
  if (!published.ok()) {
    g_storage->FenceRequestServingUntilRestart();
    co_return absl::DataLossError(absl::StrCat(
        "durable Function catalog committed but replication publish failed: ",
        published.message()));
  }
  co_return absl::OkStatus();
}

CommandReply FunctionMutationError(ReplyBuilder& reply_builder,
                                   const absl::Status& status) {
  CommandReply reply = BuiltReply(reply_builder.AppendError(
      absl::StrCat("ERR Error registering functions: ", status.message())));
  // Unknown means the A/B root may or may not have committed; DataLoss means
  // the catalog committed but its reserved history event did not publish.
  // In both cases a retry on this connection could observe an unsafe answer.
  reply.close_connection_ = status.code() == absl::StatusCode::kUnknown ||
                            status.code() == absl::StatusCode::kDataLoss;
  return reply;
}

Task<CommandReply> ExecuteFunction(const CommandRequest& request,
                                   ReplyBuilder& reply_builder) {
  const std::string_view subcommand = request.args_[1];
  if (CmpCaseInsensitive(subcommand, "help") && request.args_.size() == 2) {
    constexpr std::string_view help[] = {
        "FUNCTION <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "LOAD [REPLACE] <FUNCTION CODE>",
        "    Create a new library with the given library name and code.",
        "DELETE <LIBRARY NAME>",
        "    Delete the given library.",
        "LIST [LIBRARYNAME PATTERN] [WITHCODE]",
        "    Return general information on all the libraries:",
        "    * Library name",
        "    * The engine used to run the Library",
        "    * Library description",
        "    * Functions list",
        "    * Library code (if WITHCODE is given)",
        "    It also possible to get only function that matches a pattern "
        "using LIBRARYNAME argument.",
        "STATS",
        "    Return information about the current function running:",
        "    * Function name",
        "    * Command used to run the function",
        "    * Duration in MS that the function is running",
        "    If no function is running, return nil",
        "    In addition, returns a list of available engines.",
        "KILL",
        "    Kill the current running function.",
        "FLUSH [ASYNC|SYNC]",
        "    Delete all the libraries.",
        "    When called without the optional mode argument, the behavior is "
        "determined by the",
        "    lazyfree-lazy-user-flush configuration directive. Valid modes "
        "are:",
        "    * ASYNC: Asynchronously flush the libraries.",
        "    * SYNC: Synchronously flush the libraries.",
        "DUMP",
        "    Return a serialized payload representing the current libraries, "
        "can be restored using FUNCTION RESTORE command",
        "RESTORE <PAYLOAD> [FLUSH|APPEND|REPLACE]",
        "    Restore the libraries represented by the given payload, it is "
        "possible to give a restore policy to",
        "    control how to handle existing libraries (default APPEND):",
        "    * FLUSH: delete all existing libraries.",
        "    * APPEND: appends the restored libraries to the existing "
        "libraries. On collision, abort.",
        "    * REPLACE: appends the restored libraries to the existing "
        "libraries, On collision, replace the old",
        "      libraries with the new libraries (notice that even on this "
        "option there is a chance of failure",
        "      in case of functions name collision with another library).",
        "HELP",
        "    Prints this help."};
    reply_builder.AppendArrayHeader(std::size(help));
    for (std::string_view line : help) reply_builder.AppendSimpleString(line);
    co_return BuiltReply(reply_builder.View());
  }

  if (CmpCaseInsensitive(subcommand, "load")) {
    bool replace = false;
    std::string_view code;
    if (request.args_.size() == 3) {
      code = request.args_[2];
    } else if (request.args_.size() == 4 &&
               CmpCaseInsensitive(request.args_[2], "replace")) {
      replace = true;
      code = request.args_[3];
    } else {
      co_return BuiltReply(reply_builder.AppendError(
          "ERR wrong number of arguments for 'function|load' command"));
    }
    if (FunctionMutationRejected(request)) {
      co_return BuiltReply(reply_builder.AppendError(
          "READONLY You can't write against a read only replica."));
    }
    auto operation = co_await AcquireFunctionCatalogOperation();
    std::vector<LuaFunctionLibrary> target = SnapshotLuaFunctionLibraries();
    const std::optional<std::string> name =
        LuaFunctionLibraryNameFromCode(code);
    if (name.has_value() && replace) {
      std::erase_if(target, [&](const LuaFunctionLibrary& library) {
        return library.name_ == *name;
      });
    }
    target.push_back(LuaFunctionLibrary{
        .name_ = name.value_or(""),
        .engine_ = "LUA",
        .code_ = std::string(code),
        .functions_ = {},
    });
    absl::Status applied =
        co_await ApplyFunctionCatalogTarget(std::move(target), request);
    if (!applied.ok()) {
      co_return FunctionMutationError(reply_builder, applied);
    }
    co_return BuiltReply(
        reply_builder.AppendBulkString(name.value_or(std::string{})));
  }

  if (CmpCaseInsensitive(subcommand, "delete")) {
    if (request.args_.size() != 3) {
      co_return BuiltReply(reply_builder.AppendError(
          "ERR wrong number of arguments for 'function|delete' command"));
    }
    if (FunctionMutationRejected(request)) {
      co_return BuiltReply(reply_builder.AppendError(
          "READONLY You can't write against a read only replica."));
    }
    auto operation = co_await AcquireFunctionCatalogOperation();
    std::vector<LuaFunctionLibrary> target = SnapshotLuaFunctionLibraries();
    const std::size_t erased =
        std::erase_if(target, [&](const LuaFunctionLibrary& library) {
          return library.name_ == request.args_[2];
        });
    if (erased == 0) {
      co_return BuiltReply(reply_builder.AppendError("ERR Library not found"));
    }
    absl::Status applied =
        co_await ApplyFunctionCatalogTarget(std::move(target), request);
    if (!applied.ok()) {
      co_return FunctionMutationError(reply_builder, applied);
    }
    co_return BuiltReply(reply_builder.AppendSimpleString("OK"));
  }

  if (CmpCaseInsensitive(subcommand, "flush")) {
    if (request.args_.size() > 3 ||
        (request.args_.size() == 3 &&
         !CmpCaseInsensitive(request.args_[2], "sync") &&
         !CmpCaseInsensitive(request.args_[2], "async"))) {
      co_return BuiltReply(reply_builder.AppendError(
          "ERR FUNCTION FLUSH only supports SYNC|ASYNC option"));
    }
    if (FunctionMutationRejected(request)) {
      co_return BuiltReply(reply_builder.AppendError(
          "READONLY You can't write against a read only replica."));
    }
    auto operation = co_await AcquireFunctionCatalogOperation();
    absl::Status applied = co_await ApplyFunctionCatalogTarget({}, request);
    if (!applied.ok()) {
      co_return FunctionMutationError(reply_builder, applied);
    }
    co_return BuiltReply(reply_builder.AppendSimpleString("OK"));
  }

  if (CmpCaseInsensitive(subcommand, "dump") && request.args_.size() == 2) {
    auto operation = co_await AcquireFunctionCatalogOperation();
    co_return BuiltReply(
        reply_builder.AppendBulkString(GlobalFunctionCatalog().SnapshotDump()));
  }

  if (CmpCaseInsensitive(subcommand, "restore")) {
    if (request.args_.size() < 3 || request.args_.size() > 4) {
      co_return BuiltReply(reply_builder.AppendError(
          "ERR wrong number of arguments for 'function|restore' command"));
    }
    enum class RestorePolicy { kAppend, kReplace, kFlush };
    RestorePolicy policy = RestorePolicy::kAppend;
    if (request.args_.size() == 4) {
      if (CmpCaseInsensitive(request.args_[3], "append")) {
        policy = RestorePolicy::kAppend;
      } else if (CmpCaseInsensitive(request.args_[3], "replace")) {
        policy = RestorePolicy::kReplace;
      } else if (CmpCaseInsensitive(request.args_[3], "flush")) {
        policy = RestorePolicy::kFlush;
      } else {
        co_return BuiltReply(reply_builder.AppendError(
            "ERR Wrong restore policy given, value should be either FLUSH, "
            "APPEND or REPLACE."));
      }
    }
    auto decoded = rdb::DecodeFunctionDump(request.args_[2]);
    if (!decoded.ok()) {
      co_return BuiltReply(reply_builder.AppendError(
          "ERR DUMP payload version or checksum are wrong"));
    }
    if (FunctionMutationRejected(request)) {
      co_return BuiltReply(reply_builder.AppendError(
          "READONLY You can't write against a read only replica."));
    }
    auto operation = co_await AcquireFunctionCatalogOperation();
    const std::vector<LuaFunctionLibrary> previous =
        SnapshotLuaFunctionLibraries();
    std::vector<LuaFunctionLibrary> target =
        policy == RestorePolicy::kFlush ? std::vector<LuaFunctionLibrary>{}
                                        : previous;
    for (const std::string& code : *decoded) {
      const std::optional<std::string> name =
          LuaFunctionLibraryNameFromCode(code);
      if (!name.has_value()) {
        co_return BuiltReply(reply_builder.AppendError(
            "ERR Error registering functions: Missing library metadata"));
      }
      const auto existing = std::find_if(
          target.begin(), target.end(),
          [&](const auto& library) { return library.name_ == *name; });
      if (existing != target.end()) {
        if (policy != RestorePolicy::kReplace) {
          co_return BuiltReply(reply_builder.AppendError(
              absl::StrCat("ERR Library ", *name, " already exists")));
        }
        target.erase(existing);
      }
      target.push_back(LuaFunctionLibrary{
          .name_ = *name, .engine_ = "LUA", .code_ = code, .functions_ = {}});
    }
    absl::Status applied =
        co_await ApplyFunctionCatalogTarget(std::move(target), request);
    if (!applied.ok()) {
      co_return FunctionMutationError(reply_builder, applied);
    }
    co_return BuiltReply(reply_builder.AppendSimpleString("OK"));
  }

  if (CmpCaseInsensitive(subcommand, "list")) {
    auto operation = co_await AcquireFunctionCatalogOperation();
    std::optional<std::string_view> pattern;
    bool with_code = false;
    for (std::size_t index = 2; index < request.args_.size(); ++index) {
      if (!with_code && CmpCaseInsensitive(request.args_[index], "withcode")) {
        with_code = true;
      } else if (!pattern.has_value() &&
                 CmpCaseInsensitive(request.args_[index], "libraryname") &&
                 index + 1 < request.args_.size()) {
        pattern = request.args_[++index];
      } else {
        co_return BuiltReply(reply_builder.AppendError(
            absl::StrCat("ERR Unknown argument ", request.args_[index])));
      }
    }
    std::vector<LuaFunctionLibrary> libraries = SnapshotLuaFunctionLibraries();
    const std::size_t count = static_cast<std::size_t>(std::count_if(
        libraries.begin(), libraries.end(), [&](const auto& library) {
          return !pattern.has_value() ||
                 RedisGlobMatch(*pattern, library.name_);
        }));
    reply_builder.AppendArrayHeader(count);
    for (const LuaFunctionLibrary& library : libraries) {
      if (pattern.has_value() && !RedisGlobMatch(*pattern, library.name_)) {
        continue;
      }
      reply_builder.AppendMapHeader(with_code ? 4 : 3);
      reply_builder.AppendBulkString("library_name");
      reply_builder.AppendBulkString(library.name_);
      reply_builder.AppendBulkString("engine");
      reply_builder.AppendBulkString(library.engine_);
      reply_builder.AppendBulkString("functions");
      reply_builder.AppendArrayHeader(library.functions_.size());
      for (const LuaFunctionInfo& function : library.functions_) {
        reply_builder.AppendMapHeader(3);
        reply_builder.AppendBulkString("name");
        reply_builder.AppendBulkString(function.name_);
        reply_builder.AppendBulkString("description");
        if (function.description_.has_value()) {
          reply_builder.AppendBulkString(*function.description_);
        } else {
          reply_builder.AppendNull();
        }
        reply_builder.AppendBulkString("flags");
        AppendLuaFunctionFlags(reply_builder, function.flags_);
      }
      if (with_code) {
        reply_builder.AppendBulkString("library_code");
        reply_builder.AppendBulkString(library.code_);
      }
    }
    co_return BuiltReply(reply_builder.View());
  }

  if (CmpCaseInsensitive(subcommand, "stats") && request.args_.size() == 2) {
    auto operation = co_await AcquireFunctionCatalogOperation();
    const std::optional<LuaRunningInvocation> running =
        SnapshotLuaRunningInvocation();
    if (running.has_value() && !running->is_function_) {
      co_return BuiltReply(reply_builder.AppendError(
          "BUSY Redis is busy running a script. You can only call SCRIPT KILL "
          "or SHUTDOWN NOSAVE."));
    }
    const std::vector<LuaFunctionLibrary> libraries =
        SnapshotLuaFunctionLibraries();
    std::size_t function_count = 0;
    for (const auto& library : libraries) {
      function_count += library.functions_.size();
    }
    reply_builder.AppendMapHeader(2);
    reply_builder.AppendBulkString("running_script");
    if (!running.has_value()) {
      reply_builder.AppendNull();
    } else {
      reply_builder.AppendMapHeader(3);
      reply_builder.AppendBulkString("name");
      reply_builder.AppendBulkString(running->name_);
      reply_builder.AppendBulkString("command");
      reply_builder.AppendArrayHeader(running->command_.size());
      for (const std::string& arg : running->command_) {
        reply_builder.AppendBulkString(arg);
      }
      reply_builder.AppendBulkString("duration_ms");
      reply_builder.AppendInteger(
          static_cast<long long>(running->duration_ms_));
    }
    reply_builder.AppendBulkString("engines");
    reply_builder.AppendMapHeader(1);
    reply_builder.AppendBulkString("LUA");
    reply_builder.AppendMapHeader(2);
    reply_builder.AppendBulkString("libraries_count");
    reply_builder.AppendInteger(static_cast<long long>(libraries.size()));
    reply_builder.AppendBulkString("functions_count");
    reply_builder.AppendInteger(static_cast<long long>(function_count));
    co_return BuiltReply(reply_builder.View());
  }

  if (CmpCaseInsensitive(subcommand, "kill") && request.args_.size() == 2) {
    switch (RequestLuaScriptKill(true)) {
      case LuaScriptKillResult::kKilled:
        co_return BuiltReply(reply_builder.AppendSimpleString("OK"));
      case LuaScriptKillResult::kNotBusy:
        co_return BuiltReply(reply_builder.AppendError(
            "NOTBUSY No scripts in execution right now."));
      case LuaScriptKillResult::kUnkillableWrite:
        co_return BuiltReply(reply_builder.AppendError(
            "UNKILLABLE Sorry the script already executed write commands "
            "against the dataset. You can either wait the script termination "
            "or kill the server in a hard way using the SHUTDOWN NOSAVE "
            "command."));
      case LuaScriptKillResult::kUnkillableReplication:
        co_return BuiltReply(reply_builder.AppendError(
            "UNKILLABLE The busy script was sent by a master instance in the "
            "context of replication and cannot be killed."));
      case LuaScriptKillResult::kWrongInvocationKind:
        co_return BuiltReply(
            reply_builder.AppendError("BUSY Redis is busy running a script. "
                                      "You can only call SCRIPT KILL "
                                      "or SHUTDOWN NOSAVE."));
    }
  }

  co_return BuiltReply(reply_builder.AppendError(
      absl::StrCat("ERR Unknown subcommand or wrong number of arguments for '",
                   subcommand, "'. Try FUNCTION HELP.")));
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
      std::vector<storage::BatchGetRequest> reads;
      std::vector<std::size_t> slots;
      reads.reserve(keys.size());
      slots.reserve(keys.size());
      for (const ExecKey& key : keys) {
        if (key.owner_ == self) {
          reads.push_back(storage::BatchGetRequest{
              .key_ = args[key.arg_],
              .digest_ = key.digest_,
          });
          slots.push_back(key.slot_);
        }
      }
      if (!reads.empty()) {
        auto values = co_await g_storage->BatchGetLocked(cmd.db_id_, reads);
        for (std::size_t read = 0; read < values.size(); ++read) {
          if (!values[read].ok()) {
            record_error(values[read].status());
          } else if (values[read]->has_value()) {
            ctx->mget_[local][slots[read]] = EncodeBulkString(**values[read]);
          }
        }
      }
      continue;
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
          auto metadata = co_await g_storage->ReadKeyMetadataLocked(
              cmd.db_id_, args[key.arg_], key.digest_);
          if (!metadata.ok()) {
            record_error(metadata.status());
            command_failed = true;
          } else if (metadata->exists_) {
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
    const std::vector<std::vector<ExecKey>>& cmd_keys,
    const std::vector<ConnectionContext::WatchedKey>& watched_keys) {
  std::vector<tx::KeyRef> refs;
  auto add = [&](std::uint8_t db, tx::LockFp fp, tx::LockMode mode) {
    bool merged = false;
    for (tx::KeyRef& ref : refs) {
      if (ref.fp_ == fp && ref.db_ == db) {
        if (mode == tx::LockMode::kExclusive) {
          ref.mode_ = tx::LockMode::kExclusive;
        }
        merged = true;
        break;
      }
    }
    if (!merged) {
      refs.push_back(tx::KeyRef{fp, mode, db});
    }
  };
  for (const auto& keys : cmd_keys) {
    for (const ExecKey& key : keys) {
      add(key.db_, tx::FingerprintOf(key.digest_), key.mode_);
    }
  }
  // WATCH validation and the queued body share one lock fence. Without these
  // read holds a concurrent write can dirty a watched-only key between the
  // check and EXEC's linearization point.
  for (const auto& watched : watched_keys) {
    add(watched.db_, watched.fp_, tx::LockMode::kShared);
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
  // WATCH is intercepted by connection-level dispatch, so it cannot rely on
  // ExecuteCommandBody's ordinary database admission despite carrying the
  // kCmdUsesDbGate classification. Hold the selected database across every
  // cross-worker registration and liveness read: a replacement that starts
  // later must dirty the completed registrations, while a replacement that
  // won first is detected before this request installs any new ones.
  LAVIK_FAULT_INJECT(
      absl::Status paused = co_await MaybePauseBeforeCommandDbAdmission();
      if (!paused.ok()) {
        co_return BuiltReply(reply_builder.AppendError(
            absl::StrCat("ERR database admission failed: ", paused.message())));
      });
  while (!TryBeginDbOperation(request.db_id_)) {
    absl::Status waited = co_await bycorf::SleepFor(
        *ThisWorker().self_, std::chrono::milliseconds(1));
    if (!waited.ok()) {
      co_return BuiltReply(reply_builder.AppendError(
          absl::StrCat("ERR database admission failed: ", waited.message())));
    }
  }
  DbOperationGuard db_guard(request.db_id_);
  if (const char* error = CommandServingGenerationError(request);
      error != nullptr) [[unlikely]] {
    co_return BuiltReply(reply_builder.AppendError(error));
  }
  for (std::size_t i = keys->first_; i <= keys->last_; i += keys->step_) {
    const std::uint8_t db = request.db_id_;
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
    const bool live = co_await bycorf::SubmitTaskTo(
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
        .serving_generation_ = request.serving_generation_,
        .serving_generation_valid_ =
            static_cast<bool>(request.serving_generation_valid_),
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
    if (watched.serving_generation_valid_ && g_replication != nullptr &&
        !g_replication->ServingGenerationMatches(watched.serving_generation_)) {
      co_return false;
    }
    const bool clean = co_await bycorf::SubmitTaskTo(
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

bool IsSentinelManagementCommand(const CommandRequest& command) {
  const auto& args = command.args_;
  if (command.kind_ == CommandKind::kReplicaOf) {
    return args.size() == 3;
  }
  if (command.kind_ == CommandKind::kConfig) {
    return args.size() == 2 && CmpCaseInsensitive(args[1], "REWRITE");
  }
  return command.kind_ == CommandKind::kClient && args.size() == 4 &&
         CmpCaseInsensitive(args[1], "KILL") &&
         CmpCaseInsensitive(args[2], "TYPE") &&
         (CmpCaseInsensitive(args[3], "NORMAL") ||
          CmpCaseInsensitive(args[3], "PUBSUB"));
}

// Redis Sentinel sends role change, config persistence, and client eviction as
// one MULTI/EXEC. Lavik preserves their order and per-command replies, but
// deliberately does not stop ordinary work on other workers between them; the
// role transition itself supplies the storage admission boundary.
Task<CommandReply> ExecuteSentinelManagementExec(
    ConnectionContext& ctx, std::vector<CommandRequest> queued,
    ReplyBuilder& reply_builder) {
  const bool clean =
      ctx.watched_.empty() || co_await CheckConnectionWatches(ctx);
  co_await DropWatches(ctx);
  if (!clean) {
    co_return BuiltReply(reply_builder.AppendNullArray());
  }

  std::vector<std::string> replies;
  replies.reserve(queued.size());
  for (const CommandRequest& command : queued) {
    ReplyBuilder local_builder(ctx.resp_version());
    CommandReply local;
    if (command.kind_ == CommandKind::kReplicaOf) {
      local = co_await ExecuteReplicaOf(command, local_builder);
    } else if (command.kind_ == CommandKind::kConfig) {
      local = co_await ExecuteConfig(command, local_builder);
    } else {
      local = co_await ExecuteClient(ctx, command, local_builder);
    }
    if (local.disk_value_.valid() || local.chunks_) {
      replies.push_back(
          EncodeError("ERR management command produced a streamed reply"));
    } else {
      replies.emplace_back(local.encoded_);
    }
  }

  reply_builder.AppendArrayHeader(replies.size());
  for (const std::string& reply : replies) reply_builder.AppendRaw(reply);
  co_return BuiltReply(reply_builder.View());
}

Task<CommandReply> ExecuteWait(ConnectionContext& ctx,
                               const CommandRequest& request,
                               ReplyBuilder& reply_builder, bool allow_blocking,
                               bool unresolved_write) {
  std::uint64_t required = 0;
  std::uint64_t timeout_ms = 0;
  if (!ParseUint64(request.args_[1], &required) ||
      !ParseUint64(request.args_[2], &timeout_ms)) {
    co_return BuiltReply(reply_builder.AppendError(
        "ERR value is not an integer or out of range"));
  }
  if (g_replication == nullptr || g_replication->is_replica()) {
    co_return BuiltReply(reply_builder.AppendInteger(0));
  }
  if (unresolved_write) {
    co_return BuiltReply(reply_builder.AppendInteger(0));
  }

  std::optional<std::chrono::steady_clock::time_point> deadline;
  if (timeout_ms != 0) {
    const auto now = std::chrono::steady_clock::now();
    const auto available =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::time_point::max() - now);
    const std::uint64_t clamped =
        std::min<std::uint64_t>(timeout_ms, available.count());
    deadline = now + std::chrono::milliseconds(clamped);
  }

  std::unique_ptr<BlockingWaitHandle> blocking_wait;

  for (;;) {
    std::uint64_t acknowledged = 0;
    if (ctx.native_replication_watermark_dirty_) {
      if (allow_blocking) {
        auto captured =
            co_await g_replication->CaptureNativeReplicationWatermark();
        if (!captured.ok()) {
          co_return BuiltReply(reply_builder.AppendError(
              absl::StrCat("ERR WAIT replication fence failed: ",
                           captured.status().message())));
        }
        if (captured->has_value()) {
          ctx.native_replication_watermark_ = std::move(**captured);
          ctx.native_replication_watermark_dirty_ = false;
          const auto count =
              co_await g_replication->CountAcknowledgedNativeReplicas(
                  *ctx.native_replication_watermark_);
          if (count.has_value()) acknowledged = *count;
        }
      }
      // A WAIT inside EXEC must never fence the publisher: the transaction's
      // own replication envelope is unresolved until EXEC commits. Returning
      // zero for an uncaptured newer write is conservative and nonblocking.
    } else if (ctx.native_replication_watermark_.has_value()) {
      const auto count =
          co_await g_replication->CountAcknowledgedNativeReplicas(
              *ctx.native_replication_watermark_);
      if (count.has_value()) {
        acknowledged = *count;
      } else {
        // LSNs are reusable after a history change. Rebase through a new
        // all-flow fence instead of comparing offsets from different domains.
        ctx.native_replication_watermark_dirty_ = true;
        continue;
      }
    } else {
      acknowledged = co_await g_replication->CountOnlineNativeReplicas();
    }

    if (blocking_wait != nullptr) {
      switch (BlockingWaitState(*blocking_wait)) {
        case BlockingWakeReason::kUnblockedError:
          co_return BuiltReply(reply_builder.AppendError(
              "UNBLOCKED client unblocked via CLIENT UNBLOCK"));
        case BlockingWakeReason::kCancelled:
          co_return BuiltReply(reply_builder.AppendError(
              "ERR WAIT interrupted: client connection closed"));
        case BlockingWakeReason::kTimeout:
          co_return BuiltReply(reply_builder.AppendInteger(
              static_cast<long long>(std::min<std::uint64_t>(
                  acknowledged, static_cast<std::uint64_t>(
                                    std::numeric_limits<long long>::max())))));
        case BlockingWakeReason::kWaiting:
        case BlockingWakeReason::kReady:
          break;
      }
    }

    if (acknowledged >= required || !allow_blocking ||
        (deadline.has_value() &&
         std::chrono::steady_clock::now() >= *deadline)) {
      co_return BuiltReply(reply_builder.AppendInteger(
          static_cast<long long>(std::min<std::uint64_t>(
              acknowledged, static_cast<std::uint64_t>(
                                std::numeric_limits<long long>::max())))));
    }

    if (blocking_wait == nullptr) {
      auto registered =
          co_await RegisterClientBlockingWait(ctx.conn_id_, deadline);
      if (!registered.ok()) {
        co_return BuiltReply(reply_builder.AppendError(absl::StrCat(
            "ERR WAIT registration failed: ", registered.status().message())));
      }
      blocking_wait = std::move(*registered);
      continue;
    }
    const auto now = std::chrono::steady_clock::now();
    auto sleep_for =
        std::chrono::steady_clock::duration(std::chrono::milliseconds(1));
    if (deadline.has_value()) {
      if (now >= *deadline) continue;
      sleep_for = std::min(sleep_for, *deadline - now);
    }
    // Native ACKs are updated on the source worker that owns each flow, while
    // the client coroutine may live on any worker. This short cooperative poll
    // avoids a cross-worker waiter registry and holds no DB or transaction
    // gate; if WAIT concurrency becomes material, ACK fan-out can replace it
    // without changing the watermark contract.
    absl::Status slept =
        co_await bycorf::SleepFor(*ThisWorker().self_, sleep_for);
    if (!slept.ok()) {
      co_return BuiltReply(reply_builder.AppendError(
          absl::StrCat("ERR WAIT interrupted: ", slept.message())));
    }
  }
}

Task<CommandReply> ExecuteExecBody(
    ConnectionContext& ctx, ReplyBuilder& reply_builder,
    std::optional<std::uint64_t> write_admission_role_epoch,
    const ReplicationPublisherAdmission* publisher_admission = nullptr,
    bool* publisher_admission_released = nullptr) {
  const RespVersion exec_reply_version = ctx.resp_version();
  std::vector<CommandRequest> queued = std::move(ctx.queued_);
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
    if (!clean) co_return BuiltReply(reply_builder.AppendNullArray());
    co_return BuiltReply(reply_builder.AppendArrayHeader(0));
  }
  if (IsSentinelManagementCommand(queued.front())) {
    co_return co_await ExecuteSentinelManagementExec(ctx, std::move(queued),
                                                     reply_builder);
  }

  const bool exec_may_grow_memory =
      std::any_of(queued.begin(), queued.end(), MayGrowMemory);
  if (exec_may_grow_memory && RejectForMemory(0)) {
    co_await DropWatches(ctx);
    co_return BuiltReply(AppendOomError(reply_builder));
  }

  const bool has_write =
      std::any_of(queued.begin(), queued.end(), ExecCommandMayWrite);
  const bool has_replicable =
      std::any_of(queued.begin(), queued.end(), ExecCommandMayReplicate);
  const bool has_cluster_mutation =
      has_write || std::any_of(queued.begin(), queued.end(), [](const auto& c) {
        return c.spec_ != nullptr && (c.spec_->flags_ & kCmdMayReplicate) != 0;
      });
  auto blocking_notifications =
      std::make_shared<BlockingNotificationCapture>(g_storage->worker_count());
  for (CommandRequest& command : queued) {
    command.blocking_notification_capture_ = blocking_notifications;
  }
  const bool source_write =
      has_write && std::any_of(queued.begin(), queued.end(),
                               [](const CommandRequest& command) {
                                 return !command.replication_origin_ &&
                                        ExecCommandMayWrite(command);
                               });
  const bool reject_writes = g_replication != nullptr
                                 ? g_replication->reject_writes()
                                 : g_replica_read_only;
  const bool source_declared_write =
      std::any_of(queued.begin(), queued.end(), [](const auto& command) {
        return !command.replication_origin_ && command.spec_ != nullptr &&
               (command.spec_->flags_ & kCmdWrite) != 0;
      });
  // Dynamic Lua commands can be read-only at runtime. Execute them so the
  // script guard can return a per-command READONLY error only if a write is
  // actually attempted, preserving EXEC's continue-on-error replies.
  if (source_declared_write && reject_writes) {
    co_await DropWatches(ctx);
    co_return BuiltReply(reply_builder.AppendError(
        "READONLY You can't write against a read only replica."));
  }
  const bool source_replicable =
      has_replicable && g_storage != nullptr &&
      g_storage->ReplicationLogActive() &&
      (g_replication == nullptr || !g_replication->is_replica()) &&
      std::any_of(queued.begin(), queued.end(),
                  [](const CommandRequest& command) {
                    return !command.replication_origin_ &&
                           ExecCommandMayReplicate(command);
                  });
  ReplicationTransactionOrderGuard replication_order_guard;
  if (source_replicable && g_storage != nullptr &&
      g_storage->ReplicationLogActive()) {
    absl::Status entered =
        co_await BeginReplicationTransactionOrder(&replication_order_guard);
    if (!entered.ok()) {
      co_await DropWatches(ctx);
      co_return BuiltReply(
          reply_builder.AppendError(absl::StrCat("ERR ", entered.message())));
    }
  }
  if (source_replicable && g_storage != nullptr &&
      g_storage->ReplicationLogActive()) {
    for (CommandRequest& command : queued) {
      if (ExecCommandMayReplicate(command)) {
        command.replication_capture_ =
            std::make_shared<ReplicationCommandCapture>();
        command.defer_pubsub_delivery_ = command.kind_ == CommandKind::kPublish;
      }
    }
  }
  SnapshotTransactionOperationGuard snapshot_transaction_guard;
  if (source_write && g_replication != nullptr && g_storage != nullptr &&
      g_storage->ReplicationLogActive()) {
    absl::Status entered =
        co_await BeginSnapshotTransaction(&snapshot_transaction_guard);
    if (!entered.ok()) {
      co_await DropWatches(ctx);
      co_return BuiltReply(
          reply_builder.AppendError(absl::StrCat("ERR ", entered.message())));
    }
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
    const bool write = ExecCommandMayWrite(cmd);
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
  for (const auto& watched : ctx.watched_) {
    if (std::find(dbs.begin(), dbs.end(), watched.db_) == dbs.end()) {
      dbs.push_back(watched.db_);
    }
  }

  // Cluster mode: the whole transaction must touch one slot that this node
  // still owns. Queue time gated each command against the snapshot it was
  // admitted under; EXEC re-evaluates the union against the current cache
  // because Meta may have moved the slot between queue and EXEC. The wire
  // behavior is the documented Redis semantics (no local redis-server was
  // available to verify against): a transaction whose queued keys span slots
  // fails as a whole with CROSSSLOT, and a slot now owned elsewhere redirects
  // the whole EXEC with MOVED.
  std::vector<std::uint16_t> exec_cluster_slots;
  std::shared_ptr<const cluster::AuthorityAdmission> exec_admission;
  cluster::AuthorityInFlightGuards exec_in_flights;
  if (cluster::ClusterEnabled() && !ctx.strict_replication_apply_) {
    for (std::size_t i = 0; i < queued.size(); ++i) {
      const CommandRequest& cmd = queued[i];
      if (cmd.spec_ == nullptr || !key_errors[i].empty()) {
        continue;
      }
      if ((cmd.spec_->flags_ & kCmdNoKeys) != 0) {
        // PUBLISH is protocol-level keyless, but its channel is routed through
        // the slot captured at queue-time admission. Re-admit that slot for
        // EXEC so a Meta transition between QUEUED and EXEC cannot publish
        // under obsolete owner authority.
        if (cmd.kind_ == CommandKind::kPublish) {
          for (const std::uint16_t slot : cmd.ClusterSlots()) {
            if (std::find(exec_cluster_slots.begin(), exec_cluster_slots.end(),
                          slot) == exec_cluster_slots.end()) {
              exec_cluster_slots.push_back(slot);
            }
          }
        }
        continue;
      }
      const absl::StatusOr<KeyIndexView> keys =
          DetermineKeys(*cmd.spec_, cmd.args_);
      if (!keys.ok() || keys->empty()) continue;
      for (std::uint32_t index = keys->first_; index <= keys->last_;
           index += keys->step_) {
        const std::uint16_t slot = storage::RedisSlot(cmd.args_[index]);
        if (std::find(exec_cluster_slots.begin(), exec_cluster_slots.end(),
                      slot) == exec_cluster_slots.end()) {
          exec_cluster_slots.push_back(slot);
        }
      }
    }
    if (exec_cluster_slots.size() > 1) {
      co_await DropWatches(ctx);
      co_return BuiltReply(AppendCrossSlotError(reply_builder));
    }
    if (!exec_cluster_slots.empty() && has_cluster_mutation) {
      cluster::ClusterRuntime* runtime = cluster::GetClusterRuntime();
      const cluster::RequestView view{
          .slots_ = exec_cluster_slots,
          .is_write_ = true,
          .connection_readonly_ = ctx.cluster_readonly_,
          // Writes are never loading-whitelisted; a not-ready snapshot must
          // answer LOADING rather than serve the write.
          .loading_allowed_ = false,
      };
      for (;;) {
        auto candidate = std::make_shared<const cluster::AuthorityAdmission>(
            runtime->authority_guard_.CaptureAndAdmit(
                view, cluster::LeaseClockNow()));
        CommandReply redirect;
        if (EmitClusterDecision(candidate->decision(),
                                queued.front().connection_tls_, reply_builder,
                                &redirect)) {
          co_await DropWatches(ctx);
          co_return redirect;
        }
        if (candidate->state() == nullptr) {
          // CaptureAndAdmit cannot serve a keyed write without a state. Keep
          // this defensive branch fail closed if that invariant regresses.
          co_await DropWatches(ctx);
          CommandReply reply;
          reply.close_connection_ = true;
          co_return reply;
        }
        if (runtime->authority_guard_.RegisterAndRecheck(
                *candidate, bycorf::ThisWorker().id_, cluster::LeaseClockNow(),
                &exec_in_flights) != cluster::RecheckResult::kOk) {
          continue;
        }
        exec_admission = std::move(candidate);
        // Align every queued command with the EXEC-time proof so per-command
        // re-checks (notably Lua redis.call) retain the same lease and anchor.
        for (CommandRequest& cmd : queued) {
          cmd.cluster_authority_admission_ = exec_admission;
        }
        break;
      }
    }
  }

  // Keep gate ownership separate from keyed transaction participation.
  // FUNCTION commands still belong to the selected database's serving
  // generation, but adding them to `dbs` would incorrectly change their
  // existing ephemeral replication path into a storage-backed transaction
  // with no shard owners.
  std::vector<std::uint8_t> gate_dbs = dbs;
  for (const CommandRequest& command : queued) {
    if (command.serving_generation_valid_ &&
        std::find(gate_dbs.begin(), gate_dbs.end(), command.db_id_) ==
            gate_dbs.end()) {
      gate_dbs.push_back(command.db_id_);
    }
  }

  std::vector<std::string> replies(queued.size());
  std::vector<ReplyChunkSource> reply_chunks(queued.size());
  std::optional<std::uint8_t> select_db;
  bool close_after_exec = false;
  bool captured_replication_published = false;
  auto finalize_exec_reply = [&](CommandReply reply) {
    reply.close_connection_ = reply.close_connection_ || close_after_exec;
    return FinalizeClusterMutationReply(queued.front(), reply_builder,
                                        std::move(reply));
  };
  auto run_keyless = [&](const CommandRequest& cmd) -> Task<std::string> {
    if (IsLuaInvocationCommand(cmd)) {
      std::vector<CapturedReplicationCommand> effects;
      std::string reply = co_await ExecuteEvalWithTransaction(
          cmd, nullptr, nullptr, blocking_notifications, &effects, &ctx);
      StoreEvalReplicationEffects(cmd, std::move(effects));
      co_return reply;
    }
    if (cmd.kind_ == CommandKind::kScript) {
      ReplyBuilder local_builder(ctx.resp_version());
      CommandReply local = co_await ExecuteScript(cmd, local_builder);
      co_return std::string(local.encoded_);
    }
    if (cmd.kind_ == CommandKind::kFunction) {
      ReplyBuilder local_builder(ctx.resp_version());
      CommandReply local = co_await ExecuteFunction(cmd, local_builder);
      close_after_exec = close_after_exec || local.close_connection_;
      co_return std::string(local.encoded_);
    }
    ReplyBuilder local_builder(ctx.resp_version());
    CommandReply local;
    if (cmd.kind_ == CommandKind::kHello) {
      if (ctx.hello_handler_ == nullptr) {
        co_return std::string(
            local_builder.AppendError("ERR HELLO is unavailable"));
      }
      co_return std::string(ctx.hello_handler_(ctx.hello_authenticator_,
                                               ctx.hello_replication_, ctx,
                                               cmd.args_, local_builder));
    } else if (cmd.kind_ == CommandKind::kMonitor) {
      // Valkey refuses streaming commands while EXEC is running with its
      // deny-blocking client state. Do not switch the connection from inside
      // an aggregate EXEC reply.
      local = BuiltReply(local_builder.AppendError(
          "ERR MONITOR isn't allowed for DENY BLOCKING client"));
    } else if (cmd.kind_ == CommandKind::kInfo) {
      local = co_await ExecuteInfo(cmd, local_builder);
    } else if (cmd.kind_ == CommandKind::kRole) {
      local = co_await ExecuteRole(local_builder);
    } else if (cmd.kind_ == CommandKind::kWait) {
      local = co_await ExecuteWait(ctx, cmd, local_builder,
                                   /*allow_blocking=*/false,
                                   /*unresolved_write=*/false);
    } else if (cmd.kind_ == CommandKind::kRandomKey) {
      local = co_await ExecuteRandomKey(cmd, local_builder);
    } else if (cmd.kind_ == CommandKind::kXGroup ||
               cmd.kind_ == CommandKind::kXInfo) {
      local = co_await ExecuteStreamCommand(cmd, local_builder);
    } else if (cmd.kind_ == CommandKind::kPublish ||
               cmd.kind_ == CommandKind::kPubSub ||
               cmd.kind_ == CommandKind::kPSubscribe ||
               cmd.kind_ == CommandKind::kPUnsubscribe ||
               cmd.kind_ == CommandKind::kSubscribe ||
               cmd.kind_ == CommandKind::kUnsubscribe) {
      local = co_await ExecutePubSubCommand(ctx, cmd, local_builder);
    } else {
      local = ExecuteSimpleLocalCommand(cmd, local_builder);
    }
    if (local.selected_db_.has_value()) {
      select_db = local.selected_db_;
    }
    co_return std::string(local.encoded_);
  };

  MultiDbOperationGuard db_guard;
  for (const std::uint8_t db : gate_dbs) {
    if (!db_guard.Add(db)) {
      co_await DropWatches(ctx);
      co_return finalize_exec_reply(BuiltReply(
          AppendTryAgainError(reply_builder, "database flush is in progress")));
    }
  }
  if (source_write && write_admission_role_epoch.has_value() &&
      g_replication != nullptr &&
      g_replication->role_epoch() != *write_admission_role_epoch) {
    co_await DropWatches(ctx);
    co_return finalize_exec_reply(BuiltReply(reply_builder.AppendError(
        "TRYAGAIN replication role changed; retry command")));
  }
  for (const CommandRequest& command : queued) {
    if (const char* error = CommandServingGenerationError(command);
        error != nullptr) [[unlikely]] {
      co_await DropWatches(ctx);
      co_return finalize_exec_reply(
          BuiltReply(reply_builder.AppendError(error)));
    }
  }

  if (!dbs.empty()) {
    // One write id for the whole EXEC: every record any of its commands
    // writes carries it, and one commit record at the end covers them all.
    // Read-only transactions collect no fences and append no commit.
    const std::uint64_t exec_txid = storage::StorageEngine::AllocateWriteTxid();
    std::vector<storage::TxShardWrites> tx_writes(g_storage->worker_count());
    const auto write_request =
        std::find_if(queued.begin(), queued.end(), ExecCommandMayWrite);
    g_storage->InitializeTxWrites(
        exec_txid, tx_writes,
        write_request == queued.end()
            ? storage::MutationPrecondition{}
            : ClusterMutationPrecondition(*write_request));

    // Every read and write owner participates in the replication barrier.
    // Any command that has not yet been reduced to an independent after-image
    // (currently COPY) still needs its shared source flows to reach this
    // position before the command is applied on the replica.
    std::vector<std::uint16_t> owners;
    for (const auto& keys : cmd_keys) {
      for (const ExecKey& key : keys) {
        if (std::find(owners.begin(), owners.end(), key.owner_) ==
            owners.end()) {
          owners.push_back(key.owner_);
        }
      }
    }
    for (const auto& watched : ctx.watched_) {
      if (std::find(owners.begin(), owners.end(), watched.owner_) ==
          owners.end()) {
        owners.push_back(watched.owner_);
      }
    }
    // Function mutations are keyless but their catalog history belongs to
    // flow zero. A mixed EXEC must include that flow in the same rendezvous as
    // its key owners or a later catalog mutation can overtake it there.
    const bool contains_catalog_replication_mutation =
        source_replicable &&
        std::any_of(queued.begin(), queued.end(), IsFunctionCatalogMutation);
    const bool needs_catalog_replication_participant =
        contains_catalog_replication_mutation &&
        std::find(owners.begin(), owners.end(), 0) == owners.end();

    const CommandRequest* replication_request = nullptr;
    if (source_replicable) {
      for (const CommandRequest& command : queued) {
        if (ExecCommandMayWrite(command)) {
          replication_request = &command;
          break;
        }
      }
    }
    std::vector<std::string> replication_args;
    if (replication_request != nullptr && !owners.empty()) {
      const std::size_t write_count =
          std::count_if(queued.begin(), queued.end(), ExecCommandMayReplicate);
      replication_args.reserve(2 + queued.size() * 3);
      replication_args.emplace_back(kReplicatedExecCommand);
      replication_args.push_back(std::to_string(write_count));
      for (const CommandRequest& command : queued) {
        if (!ExecCommandMayReplicate(command)) {
          continue;
        }
        replication_args.push_back(std::to_string(command.db_id_));
        replication_args.push_back(std::to_string(command.args_.size()));
        replication_args.insert(replication_args.end(), command.args_.begin(),
                                command.args_.end());
      }
    }
    auto resolved_replication_args = [&]() {
      std::vector<CapturedReplicationCommand> commands;
      for (std::size_t index = 0; index < queued.size(); ++index) {
        const CommandRequest& command = queued[index];
        CapturedReplicationEffects captured;
        if (command.replication_capture_ != nullptr) {
          captured = command.replication_capture_->Take();
        }
        if (captured.handled_) {
          commands.insert(commands.end(),
                          std::make_move_iterator(captured.commands_.begin()),
                          std::make_move_iterator(captured.commands_.end()));
        } else if (replies[index].empty() || replies[index].front() != '-') {
          if (command.spec_ != nullptr &&
              (command.spec_->flags_ & (kCmdWrite | kCmdMayReplicate)) != 0) {
            commands.push_back(
                CapturedReplicationCommand{command.db_id_, command.args_});
          }
        }
      }
      return EncodeReplicationCommandEffects(std::move(commands));
    };
    auto catalog_mutation_succeeded = [&]() {
      if (!contains_catalog_replication_mutation) return false;
      for (std::size_t index = 0; index < queued.size(); ++index) {
        if (IsFunctionCatalogMutation(queued[index]) &&
            !replies[index].empty() && replies[index].front() != '-') {
          return true;
        }
      }
      return false;
    };
    auto commit_replication =
        [&](ReplicationTransactionGuard* replication) -> absl::StatusOr<bool> {
      if (replication == nullptr) return false;
      const bool catalog_mutation_committed = catalog_mutation_succeeded();
      std::vector<std::string> resolved = resolved_replication_args();
      if (resolved.size() < 2 || resolved[1] == "0") {
        if (catalog_mutation_committed) {
          return absl::DataLossError(
              "durable Function catalog has no replication body");
        }
        return false;
      }
      replication->SetCommandArgs(std::move(resolved));
      replication->SetFinalExpirations(tx_writes);
      if (!replication->Commit()) {
        if (catalog_mutation_committed) {
          return absl::DataLossError(
              "durable Function catalog transaction could not be published");
        }
        return false;
      }
      captured_replication_published = true;
      return catalog_mutation_committed;
    };
    auto enter_catalog_replication_participant =
        [](ReplicationTransactionGuard* replication) -> Task<absl::Status> {
      if (ThisWorker().id_ == 0) {
        replication->EnterCurrentShard();
        co_return absl::OkStatus();
      }
      co_return co_await SubmitTaskTo(0, [replication] -> Task<absl::Status> {
        replication->EnterCurrentShard();
        co_return absl::OkStatus();
      });
    };
    auto fence_catalog_replication = [&]() -> Task<absl::Status> {
      const std::size_t participant_count =
          owners.size() + (needs_catalog_replication_participant ? 1 : 0);
      co_return co_await ForEachParticipantParallel(
          participant_count, [&](std::size_t index) {
            const unsigned worker = index < owners.size() ? owners[index] : 0;
            return std::pair{
                worker, []() -> Task<absl::Status> {
                  auto fence = co_await g_storage->FenceReplicationLog();
                  co_return fence.ok() ? absl::OkStatus() : fence.status();
                }};
          });
    };
    auto publish_committed_catalog_replication = [&]() -> Task<absl::Status> {
      if (publisher_admission == nullptr ||
          publisher_admission_released == nullptr) {
        co_return absl::FailedPreconditionError(
            "catalog transaction has no publisher admission");
      }
      if (*publisher_admission_released) {
        co_return absl::FailedPreconditionError(
            "catalog transaction publisher admission was already released");
      }
      LAVIK_FAULT_INJECT(
          if (const char* configured = std::getenv(
                  "LAVIK_EXEC_PAUSE_BEFORE_CATALOG_REPLICATION_FENCE_MS");
              configured != nullptr) {
            std::uint64_t pause_ms = 0;
            const std::size_t length = std::strlen(configured);
            const auto parsed =
                std::from_chars(configured, configured + length, pause_ms);
            static std::atomic<bool> pause_used = false;
            if (parsed.ec == std::errc{} && parsed.ptr == configured + length &&
                pause_ms != 0 && pause_ms <= 60000 &&
                !pause_used.exchange(true, std::memory_order_acq_rel)) {
              spdlog::info(
                  "catalog EXEC committed; pausing before replication fence");
              absl::Status paused = co_await bycorf::SleepFor(
                  *ThisWorker().self_, std::chrono::milliseconds(pause_ms));
              if (!paused.ok()) co_return paused;
            }
          });
      // Every after-image and participant marker is now enqueued. Release the
      // outer EXEC admission before waiting for the log fence: the fence must
      // cross the same admitted-bytes waterline, so retaining it here would
      // wait on this transaction's own reservation forever.
      absl::Status released =
          co_await ReleaseReplicationPublisherAdmission(*publisher_admission);
      *publisher_admission_released = true;
      if (!released.ok()) co_return released;
      co_return co_await fence_catalog_replication();
    };
    auto fail_catalog_replication =
        [&](absl::Status status) -> Task<CommandReply> {
      g_storage->FenceRequestServingUntilRestart();
      co_await DropWatches(ctx);
      CommandReply reply = BuiltReply(reply_builder.AppendError(absl::StrCat(
          "ERR durable Function catalog committed but EXEC replication "
          "failed: ",
          status.message())));
      reply.close_connection_ = true;
      co_return finalize_exec_reply(std::move(reply));
    };

    const bool contains_eval =
        std::any_of(queued.begin(), queued.end(), IsLuaInvocationCommand);
    if (owners.size() == 1 && !contains_eval) {
      std::unique_ptr<ReplicationTransactionGuard> replication;
      if (replication_request != nullptr) {
        std::vector<unsigned> participants{
            static_cast<unsigned>(owners.front())};
        if (needs_catalog_replication_participant) participants.push_back(0);
        replication = std::make_unique<ReplicationTransactionGuard>(
            *replication_request, std::move(participants),
            std::move(replication_args));
        if (!replication->status().ok()) {
          co_await DropWatches(ctx);
          co_return finalize_exec_reply(BuiltReply(
              AppendStorageError(reply_builder, replication->status())));
        }
        if (needs_catalog_replication_participant) {
          absl::Status entered =
              co_await enter_catalog_replication_participant(replication.get());
          if (!entered.ok()) {
            co_await DropWatches(ctx);
            co_return finalize_exec_reply(
                BuiltReply(AppendStorageError(reply_builder, entered)));
          }
        }
      }
      // Whole transaction on one shard: hop once, take the fast-path guard
      // over the union lock set, run every command inline.
      const std::vector<tx::KeyRef> refs =
          DedupExecLocks(cmd_keys, ctx.watched_);
      bool watch_aborted = false;
      absl::Status status =
          co_await SubmitTaskTo(owners.front(), [&]() -> Task<absl::Status> {
            auto guard = co_await tx::CurrentTxShard().AcquireKeys(
                std::span<const tx::KeyRef>(refs));
            if (replication != nullptr) replication->EnterCurrentShard();
            if (exec_admission != nullptr) {
              // Choke point 2 for the single-shard fast path (which never
              // builds a tx::Transaction): re-check the complete EXEC proof
              // after the key guard and before the first mutation.
              if (cluster::GetClusterRuntime()->authority_guard_.Recheck(
                      *exec_admission, cluster::LeaseClockNow()) !=
                  cluster::RecheckResult::kOk) {
                co_return ClusterAuthorityChangedStatus();
              }
            }
            if (!ctx.watched_.empty() &&
                !co_await CheckConnectionWatches(ctx)) {
              watch_aborted = true;
              co_return absl::OkStatus();
            }
            std::size_t i = 0;
            while (i < queued.size()) {
              CommandRequest& cmd = queued[i];
              cmd.resp_version_ = ctx.resp_version();
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
            const auto valid =
                storage::StorageEngine::ValidateTxCommit(tx_writes);
            if (!valid.ok()) co_return valid;
            g_storage->PublishCommittedFullSyncEffects(
                &tx_writes[bycorf::ThisWorker().id_]);
            co_return absl::OkStatus();
          });
      if (!status.ok()) {
        co_await DropWatches(ctx);
        if (IsClusterAuthorityChanged(status)) {
          // The fast-path re-check fires before any queued command runs, so
          // the redirect it maps to is always honest.
          co_return finalize_exec_reply(ClusterAuthorityChangedReply(
              exec_cluster_slots, queued.front().connection_tls_,
              reply_builder));
        }
        co_return finalize_exec_reply(BuiltReply(
            reply_builder.AppendError(absl::StrCat("ERR ", status.message()))));
      }
      if (watch_aborted) {
        co_await DropWatches(ctx);
        co_return finalize_exec_reply(
            BuiltReply(reply_builder.AppendNullArray()));
      }
      // Publish from the connection's coordinator worker after the shard hop
      // returns. EXEC is published from this same worker, preserving the
      // per-connection MULTI -> children -> EXEC order in every target lane.
      if (HasMonitorSessions()) [[unlikely]] {
        PublishExecMonitorCommands(ctx, queued);
      }
      absl::StatusOr<bool> committed_catalog = false;
      if (replication != nullptr) {
        committed_catalog = commit_replication(replication.get());
        if (!committed_catalog.ok()) {
          g_storage->FenceRequestServingUntilRestart();
        }
      }
      if (!g_storage->EnqueueTxCommit(exec_txid, std::move(tx_writes))) {
        co_await g_storage->WaitForTxCommitCapacity();
      }
      if (!committed_catalog.ok()) {
        co_return co_await fail_catalog_replication(committed_catalog.status());
      }
      if (*committed_catalog) {
        absl::Status published =
            co_await publish_committed_catalog_replication();
        if (!published.ok()) {
          co_return co_await fail_catalog_replication(std::move(published));
        }
      }
    } else {
      tx::Transaction txn;
      for (const auto& keys : cmd_keys) {
        for (const ExecKey& key : keys) {
          txn.AddKey(key.owner_, key.db_, key.digest_, key.arg_, key.mode_);
        }
      }
      for (const auto& watched : ctx.watched_) {
        bool covered = false;
        for (const auto& keys : cmd_keys) {
          covered =
              std::any_of(keys.begin(), keys.end(), [&](const ExecKey& key) {
                return key.db_ == watched.db_ &&
                       tx::FingerprintOf(key.digest_) == watched.fp_;
              });
          if (covered) break;
        }
        if (!covered) {
          txn.AddKey(watched.owner_, watched.db_, watched.digest_, 0,
                     tx::LockMode::kShared);
        }
      }
      txn.Seal();
      ClusterShardValidatorContext cluster_validator;
      if (exec_admission != nullptr) {
        cluster_validator.admission_ = exec_admission;
        txn.SetShardValidator(&ValidateClusterShardAuthority,
                              &cluster_validator);
      }
      std::unique_ptr<ReplicationTransactionGuard> replication;
      if (replication_request != nullptr) {
        replication = std::make_unique<ReplicationTransactionGuard>(
            *replication_request, &txn, std::move(replication_args),
            needs_catalog_replication_participant ? std::vector<unsigned>{0}
                                                  : std::vector<unsigned>{});
        if (!replication->status().ok()) {
          co_await DropWatches(ctx);
          co_return finalize_exec_reply(BuiltReply(
              AppendStorageError(reply_builder, replication->status())));
        }
        if (needs_catalog_replication_participant) {
          absl::Status entered =
              co_await enter_catalog_replication_participant(replication.get());
          if (!entered.ok()) {
            co_await DropWatches(ctx);
            co_return finalize_exec_reply(
                BuiltReply(AppendStorageError(reply_builder, entered)));
          }
        }
      }
      absl::Status scheduled = co_await txn.Schedule();
      if (!scheduled.ok()) {
        co_await DropWatches(ctx);
        co_return finalize_exec_reply(BuiltReply(reply_builder.AppendError(
            absl::StrCat("ERR ", scheduled.message()))));
      }
      // Acquire and retain every shard's holds before the coordinator runs
      // argument-ordered commands such as LMPOP one key at a time.
      absl::Status armed = co_await txn.Execute(&ArmOnlyShardCallback, nullptr,
                                                /*release=*/false);
      if (!armed.ok()) {
        (void)co_await txn.Release();
        co_await DropWatches(ctx);
        if (IsClusterAuthorityChanged(armed)) {
          // Nothing executed yet.
          co_return finalize_exec_reply(ClusterAuthorityChangedReply(
              exec_cluster_slots, queued.front().connection_tls_,
              reply_builder));
        }
        co_return finalize_exec_reply(BuiltReply(
            reply_builder.AppendError(absl::StrCat("ERR ", armed.message()))));
      }
      if (!ctx.watched_.empty()) {
        if (!co_await CheckConnectionWatches(ctx)) {
          (void)co_await txn.Release();
          co_await DropWatches(ctx);
          co_return finalize_exec_reply(
              BuiltReply(reply_builder.AppendNullArray()));
        }
      }
      if (HasMonitorSessions()) [[unlikely]] {
        PublishExecMonitorCommands(ctx, queued);
      }
      // Squashed execution: each hop covers a whole run of consecutive keyed
      // commands — every shard works its keys of every command in the run in
      // queue order. Keyless commands break runs, preserving their position
      // in the serial order.
      std::size_t i = 0;
      while (i < queued.size()) {
        CommandRequest& cmd = queued[i];
        cmd.resp_version_ = ctx.resp_version();
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
        if (IsLuaInvocationCommand(cmd)) {
          std::vector<CapturedReplicationCommand> effects;
          replies[i] = co_await ExecuteEvalWithTransaction(
              cmd, &txn, &tx_writes, blocking_notifications, &effects, &ctx);
          StoreEvalReplicationEffects(cmd, std::move(effects));
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
               !IsLuaInvocationCommand(queued[end]) &&
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
          if (IsClusterAuthorityChanged(hop)) {
            // A fence raced EXEC mid-flight: earlier runs may already have
            // committed, so the outcome is undeterminable. The Redis contract
            // for that is to close the connection without an error reply.
            (void)co_await txn.Release();
            co_await DropWatches(ctx);
            CommandReply reply;
            reply.close_connection_ = true;
            co_return reply;
          }
          for (std::size_t j = i; j < end; ++j) {
            replies[j] = EncodeStorageError(hop);
          }
        } else {
          AssembleRunReplies(run);
        }
        i = end;
      }
      // Every queued command ran; the publish/release hops settle their
      // effects and must not be fenced off retroactively.
      txn.SetShardValidator(nullptr, nullptr);
      const auto valid = storage::StorageEngine::ValidateTxCommit(tx_writes);
      if (!valid.ok()) {
        (void)co_await txn.Release();
        co_await DropWatches(ctx);
        co_return finalize_exec_reply(
            BuiltReply(AppendStorageError(reply_builder, valid)));
      }
      absl::Status published =
          co_await txn.Execute(&PublishFullSyncEffectsCallback, &tx_writes,
                               /*release=*/false);
      if (!published.ok()) {
        (void)co_await txn.Release();
        co_await DropWatches(ctx);
        co_return finalize_exec_reply(BuiltReply(reply_builder.AppendError(
            absl::StrCat("ERR ", published.message()))));
      }
      absl::Status released = co_await txn.Release();
      if (!released.ok()) {
        // Defensive: the no-op release hop cannot fail today. If it ever
        // can, the watches must still be consumed — EXEC ends them whatever
        // its outcome, and stale entries would falsely abort every later
        // EXEC on this connection.
        co_await DropWatches(ctx);
        co_return finalize_exec_reply(BuiltReply(reply_builder.AppendError(
            absl::StrCat("ERR ", released.message()))));
      }
      absl::StatusOr<bool> committed_catalog = false;
      if (replication != nullptr) {
        committed_catalog = commit_replication(replication.get());
        if (!committed_catalog.ok()) {
          g_storage->FenceRequestServingUntilRestart();
        }
      }
      if (!g_storage->EnqueueTxCommit(exec_txid, std::move(tx_writes))) {
        co_await g_storage->WaitForTxCommitCapacity();
      }
      if (!committed_catalog.ok()) {
        co_return co_await fail_catalog_replication(committed_catalog.status());
      }
      if (*committed_catalog) {
        absl::Status published =
            co_await publish_committed_catalog_replication();
        if (!published.ok()) {
          co_return co_await fail_catalog_replication(std::move(published));
        }
      }
    }
  } else {
    // Keyless-only transaction.
    if (!ctx.watched_.empty() && !co_await CheckConnectionWatches(ctx)) {
      co_await DropWatches(ctx);
      co_return finalize_exec_reply(
          BuiltReply(reply_builder.AppendNullArray()));
    }
    if (HasMonitorSessions()) [[unlikely]] {
      PublishExecMonitorCommands(ctx, queued);
    }
    for (std::size_t i = 0; i < queued.size(); ++i) {
      queued[i].resp_version_ = ctx.resp_version();
      if (!key_errors[i].empty()) {
        replies[i] = key_errors[i];
      } else {
        replies[i] = co_await run_keyless(queued[i]);
      }
    }
  }
  db_guard.Release();

  // A transaction with PUBLISH but no durable write has no storage shard on
  // which to place the ordinary transaction envelope. Publish its captured
  // effects once through the first channel's source flow instead. Read-only
  // children and SUBSCRIBE/UNSUBSCRIBE remain local connection state.
  if (source_replicable && (!has_write || dbs.empty()) &&
      g_storage != nullptr) {
    std::vector<CapturedReplicationCommand> commands;
    for (std::size_t index = 0; index < queued.size(); ++index) {
      const CommandRequest& command = queued[index];
      if (!replies[index].empty() && replies[index].front() == '-') continue;
      CapturedReplicationEffects captured;
      if (command.replication_capture_ != nullptr) {
        captured = command.replication_capture_->Take();
      }
      if (captured.handled_) {
        commands.insert(commands.end(),
                        std::make_move_iterator(captured.commands_.begin()),
                        std::make_move_iterator(captured.commands_.end()));
      } else if (command.spec_ != nullptr &&
                 (command.spec_->flags_ & kCmdMayReplicate) != 0) {
        commands.push_back(
            CapturedReplicationCommand{command.db_id_, command.args_});
      }
    }
    if (!commands.empty()) {
      const bool catalog_mutation =
          std::any_of(commands.begin(), commands.end(), [](const auto& item) {
            return !item.args_.empty() &&
                   CmpCaseInsensitive(item.args_.front(), "function");
          });
      const auto publish =
          std::find_if(commands.begin(), commands.end(), [](const auto& item) {
            return item.args_.size() > 1 &&
                   CmpCaseInsensitive(item.args_.front(), "publish");
          });
      const auto* active_admission = g_active_replication_publisher_admission;
      if ((!catalog_mutation && publish == commands.end()) ||
          active_admission == nullptr) {
        CommandReply reply = BuiltReply(reply_builder.AppendError(
            "ERR admitted EXEC replication token is missing"));
        reply.close_connection_ = true;
        co_await DropWatches(ctx);
        co_return reply;
      }
      const std::uint16_t partition_id =
          catalog_mutation ? 0 : storage::RedisSlot(publish->args_[1]);
      const unsigned source_worker = partition_id % g_storage->worker_count();
      std::optional<std::vector<std::string>> fullsync_projection;
      if (catalog_mutation) {
        std::vector<CapturedReplicationCommand> non_catalog_commands;
        non_catalog_commands.reserve(commands.size());
        for (const CapturedReplicationCommand& command : commands) {
          if (command.args_.empty() ||
              !CmpCaseInsensitive(command.args_.front(), "function")) {
            non_catalog_commands.push_back(command);
          }
        }
        // The final full-sync cut installs one authoritative complete catalog.
        // Retain PUBLISH or other non-catalog effects from a mixed EXEC, but
        // do not replay its Function children against the discarded catalog.
        fullsync_projection = non_catalog_commands.empty()
                                  ? std::vector<std::string>{}
                                  : EncodeReplicationCommandEffects(
                                        std::move(non_catalog_commands));
      }
      std::vector<std::string> effects =
          EncodeReplicationCommandEffects(std::move(commands));
      const auto token = std::find_if(active_admission->worker_tokens_.begin(),
                                      active_admission->worker_tokens_.end(),
                                      [source_worker](const auto& item) {
                                        return item.worker_ == source_worker;
                                      });
      if (token == active_admission->worker_tokens_.end()) {
        CommandReply reply = BuiltReply(reply_builder.AppendError(
            "ERR admitted EXEC replication token is missing"));
        reply.close_connection_ = true;
        co_await DropWatches(ctx);
        co_return reply;
      }
      storage::ReplicationPublisherAdmission storage_admission = token->token_;
      const storage::ReplicationEventKind event_kind =
          catalog_mutation ? storage::ReplicationEventKind::kCatalogMutation
                           : storage::ReplicationEventKind::kEphemeral;
      absl::Status published = co_await bycorf::SubmitTo(
          source_worker,
          [storage_admission = std::move(storage_admission), event_kind,
           partition_id, effects = std::move(effects),
           fullsync_projection = std::move(fullsync_projection),
           exec_admission]() mutable {
            LAVIK_FAULT_INJECT(
                static std::atomic<bool> reject_ephemeral_once = false;
                if (event_kind == storage::ReplicationEventKind::kEphemeral &&
                    std::getenv(
                        "LAVIK_EXEC_REJECT_EPHEMERAL_FINAL_RECHECK_ONCE") !=
                        nullptr &&
                    !reject_ephemeral_once.exchange(
                        true, std::memory_order_acq_rel)) {
                  return ClusterAuthorityChangedStatus();
                });
            if (exec_admission != nullptr &&
                cluster::GetClusterRuntime()
                        ->authority_guard_.RecheckAtMutation(
                            *exec_admission, cluster::LeaseClockNow()) !=
                    cluster::RecheckResult::kOk) {
              return ClusterAuthorityChangedStatus();
            }
            return g_storage->PublishLateAdmittedReplicationCommand(
                storage_admission, event_kind, partition_id, std::move(effects),
                std::move(fullsync_projection));
          });
      if (!published.ok()) {
        if (catalog_mutation) {
          g_storage->FenceRequestServingUntilRestart();
        }
        co_await DropWatches(ctx);
        CommandReply reply = BuiltReply(reply_builder.AppendError(absl::StrCat(
            "ERR EXEC replication failed: ", published.message())));
        reply.close_connection_ = true;
        co_return reply;
      }
      captured_replication_published = true;
    }
  }

  bool has_deferred_publish = false;
  for (std::size_t index = 0; index < queued.size(); ++index) {
    has_deferred_publish =
        has_deferred_publish ||
        (queued[index].defer_pubsub_delivery_ && !replies[index].empty() &&
         replies[index].front() != '-');
  }
  if (has_deferred_publish && !captured_replication_published) {
    co_await DropWatches(ctx);
    CommandReply reply = BuiltReply(reply_builder.AppendError(
        "ERR EXEC replication failed before captured PUBLISH delivery"));
    reply.close_connection_ = true;
    co_return finalize_exec_reply(std::move(reply));
  }

  // Captured source-side PUBLISH effects become subscriber-visible only after
  // the durable transaction or keyless late publication above has committed.
  // Each snapshot retains the subscription membership and RESP encoding from
  // the command's logical position in EXEC; later subscription changes cannot
  // reorder Pub/Sub semantics. The committed cut authorizes completion even
  // if the finite lease changes while cross-worker delivery is in flight.
  for (std::size_t index = 0; index < queued.size(); ++index) {
    const CommandRequest& command = queued[index];
    if (!command.defer_pubsub_delivery_ || replies[index].empty() ||
        replies[index].front() == '-') {
      continue;
    }
    assert(command.replication_capture_ != nullptr);
    auto publication =
        command.replication_capture_->TakeCapturedPubSubPublication();
    assert(publication != nullptr);
    (void)co_await DeliverCapturedPubSubPublication(std::move(publication));
  }

  absl::Status notified = co_await FlushBlockingNotifications(
      *blocking_notifications, ctx.blocking_wake_cascade_);
  if (!notified.ok()) {
    co_await DropWatches(ctx);
    CommandReply reply = BuiltReply(
        reply_builder.AppendError(absl::StrCat("ERR ", notified.message())));
    reply.close_connection_ = close_after_exec;
    co_return reply;
  }
  co_await DropWatches(ctx);
  if (ctx.strict_replication_apply_) {
    const auto failed = std::find_if(
        replies.begin(), replies.end(), [](const std::string& encoded) {
          return !encoded.empty() && encoded.front() == '-';
        });
    if (failed != replies.end()) {
      co_return finalize_exec_reply(
          BuiltReply(reply_builder.AppendRaw(*failed)));
    }
  }
  const bool streamed = std::any_of(
      reply_chunks.begin(), reply_chunks.end(),
      [](const ReplyChunkSource& source) { return static_cast<bool>(source); });
  const RespVersion connection_version = ctx.resp_version();
  reply_builder.SetVersion(exec_reply_version);
  reply_builder.AppendArrayHeader(replies.size());
  if (!streamed) {
    for (const std::string& reply : replies) {
      reply_builder.AppendRaw(reply);
    }
  }
  CommandReply reply = BuiltReply(reply_builder.View());
  reply.close_connection_ = close_after_exec;
  reply_builder.SetVersion(connection_version);
  // HELLO may have run on a key-owner worker while EXEC held that shard's
  // locks. Refresh the connection metadata on its owning worker after the
  // transaction returns here.
  SetClientRespVersion(ctx.conn_id_, connection_version);
  SetClientName(ctx.conn_id_, ctx.client_name_);
  if (streamed) {
    auto state = std::make_shared<ExecReplyStreamState>();
    state->replies_ = std::move(replies);
    state->chunks_ = std::move(reply_chunks);
    reply.chunks_ = std::make_unique<ReplyChunkSource>(
        [state]() { return NextExecReplyChunk(state); });
  }
  reply.selected_db_ = select_db;
  co_return reply;
}

Task<CommandReply> ExecuteExec(ConnectionContext& ctx,
                               ReplyBuilder& reply_builder) {
  std::optional<std::uint64_t> write_admission_role_epoch;
  if (g_replication != nullptr &&
      std::any_of(ctx.queued_.begin(), ctx.queued_.end(),
                  [](const CommandRequest& command) {
                    return !command.replication_origin_ &&
                           ExecCommandMayWrite(command);
                  })) {
    write_admission_role_epoch = g_replication->role_epoch();
  }
  std::size_t logical_bytes = 0;
  std::size_t event_bytes = 64;
  bool source_replicable = false;
  for (const CommandRequest& command : ctx.queued_) {
    if (!command.replication_origin_ && command.spec_ != nullptr &&
        ExecCommandMayReplicate(command) &&
        (g_replication == nullptr || !g_replication->is_replica())) {
      source_replicable = true;
      logical_bytes =
          SaturatingAdd(logical_bytes, RequestArgumentBytes(command));
      logical_bytes = SaturatingAdd(logical_bytes,
                                    FullSyncReplacementAdmissionBytes(command));
      event_bytes = SaturatingAdd(
          event_bytes,
          SaturatingAdd(ReplicationEventAdmissionBytes(command), 32));
    }
  }
  source_replicable = source_replicable && g_storage != nullptr &&
                      g_storage->ReplicationLogActive();
  if (!source_replicable) [[likely]] {
    co_return co_await ExecuteExecBody(ctx, reply_builder,
                                       write_admission_role_epoch);
  }
  if (ReplicationEventExceedsBacklog(event_bytes)) {
    ctx.ResetMulti();
    co_await DropWatches(ctx);
    co_return BuiltReply(reply_builder.AppendError(
        "ERR replication publisher admission failed: canonical EXEC event "
        "exceeds repl-backlog-size or the 1 GiB event limit"));
  }
  auto admission = co_await AcquireReplicationPublisherAdmission(logical_bytes);
  if (!admission.ok()) {
    ctx.ResetMulti();
    co_await DropWatches(ctx);
    if (absl::IsResourceExhausted(admission.status())) {
      co_return BuiltReply(AppendOomError(reply_builder));
    }
    co_return BuiltReply(reply_builder.AppendError(
        absl::StrCat("ERR replication publisher admission failed: ",
                     admission.status().message())));
  }
  ActivePublisherAdmissionGuard active_admission(&*admission);
  bool admission_released = false;
  CommandReply reply =
      co_await ExecuteExecBody(ctx, reply_builder, write_admission_role_epoch,
                               &*admission, &admission_released);
  if (!admission_released) {
    absl::Status released =
        co_await ReleaseReplicationPublisherAdmission(*admission);
    if (!released.ok()) {
      CommandReply failed = BuiltReply(reply_builder.AppendError(
          absl::StrCat("ERR replication publisher admission release failed: ",
                       released.message())));
      failed.close_connection_ = reply.close_connection_;
      co_return failed;
    }
  }
  co_return reply;
}

}  // namespace

Task<absl::Status> ReplaceLuaFunctionCatalog(
    const std::vector<std::string>& library_codes) {
  auto operation = co_await AcquireFunctionCatalogOperation();
  co_return co_await GlobalFunctionCatalog().ReplaceFromLibraryCodes(
      library_codes);
}

Task<absl::Status> ValidateLuaFunctionCatalog(
    const std::vector<std::string>& library_codes) {
  auto operation = co_await AcquireFunctionCatalogOperation();
  co_return co_await GlobalFunctionCatalog().ValidateLibraryCodes(
      library_codes);
}

bool TryBeginCommandDbOperation(std::uint8_t db_id) noexcept {
  return TryBeginDbOperation(db_id);
}

void EndCommandDbOperation(std::uint8_t db_id) noexcept {
  EndDbOperation(db_id);
}

bool CloseAllCommandDbGates() noexcept {
  std::uint8_t closed = 0;
  for (; closed < storage::kLogicalDatabaseCount; ++closed) {
    if (CloseDbGate(closed)) continue;
    while (closed != 0) OpenDbGate(--closed);
    return false;
  }
  return true;
}

void OpenAllCommandDbGates() noexcept {
  for (std::uint8_t db_id = 0; db_id < storage::kLogicalDatabaseCount;
       ++db_id) {
    OpenDbGate(db_id);
  }
}

bool CommandDbOperationsActive() noexcept {
  for (std::uint8_t db_id = 0; db_id < storage::kLogicalDatabaseCount;
       ++db_id) {
    if (DbGateHasActiveOperations(db_id)) return true;
  }
  return false;
}

bool TryBeginSnapshotTransaction() noexcept {
  auto& gate = LocalSnapshotTransactionGate();
  std::uint64_t state = gate.load(std::memory_order_acquire);
  while ((state & kDbGateClosed) == 0) {
    if (gate.compare_exchange_weak(state, state + 1, std::memory_order_acq_rel,
                                   std::memory_order_acquire)) {
      return true;
    }
  }
  return false;
}

void EndSnapshotTransaction() noexcept {
  LocalSnapshotTransactionGate().fetch_sub(1, std::memory_order_acq_rel);
}

bool CloseSnapshotTransactionGate() noexcept {
  std::array<unsigned, storage::kLogicalStorageShards> closed{};
  std::size_t closed_count = 0;
  const unsigned workers = DbGateWorkerCount();
  for (unsigned worker = 0; worker < workers; ++worker) {
    auto& gate = g_worker_command_gates[worker].snapshot_transaction_state_;
    std::uint64_t expected = gate.load(std::memory_order_acquire);
    while ((expected & kDbGateClosed) == 0) {
      if (gate.compare_exchange_weak(expected, expected | kDbGateClosed,
                                     std::memory_order_acq_rel,
                                     std::memory_order_acquire)) {
        closed[closed_count++] = worker;
        break;
      }
    }
    if ((expected & kDbGateClosed) != 0) {
      for (std::size_t i = 0; i < closed_count; ++i) {
        OpenSnapshotTransactionGateWorker(closed[i]);
      }
      return false;
    }
  }
  return true;
}

void OpenSnapshotTransactionGate() noexcept {
  const unsigned workers = DbGateWorkerCount();
  for (unsigned worker = 0; worker < workers; ++worker) {
    OpenSnapshotTransactionGateWorker(worker);
  }
}

bool SnapshotTransactionsActive() noexcept {
  const unsigned workers = DbGateWorkerCount();
  for (unsigned worker = 0; worker < workers; ++worker) {
    if ((g_worker_command_gates[worker].snapshot_transaction_state_.load(
             std::memory_order_acquire) &
         kDbGateCountMask) != 0) {
      return true;
    }
  }
  return false;
}

bool TryBeginReplicationTransactionOrder() noexcept {
  return g_replication_transaction_order.TryAcquire();
}

void EndReplicationTransactionOrder() noexcept {
  g_replication_transaction_order.Release();
}

bool RequestSpansMultipleShards(const CommandRequest& request) {
  if (request.spec_ == nullptr || g_storage == nullptr) return true;
  // Kinds without a proven-complete key view keep taking the gate; their
  // DetermineKeys view may miss participants (e.g. SORT BY/GET patterns,
  // GEORADIUS STORE destinations, ZUNIONSTORE's destination arg).
  if ((request.spec_->flags_ & kCmdKeyViewComplete) == 0) return true;
  const absl::StatusOr<KeyIndexView> keys =
      DetermineKeys(*request.spec_, request.args_);
  // Arity/syntax failures and keyless views keep today's behaviour: the
  // handler reports the error later, and admission stays conservative.
  if (!keys.ok() || keys->count() == 0) return true;
  const unsigned first = ShardForKey(request.args_[keys->first_]);
  for (std::size_t i = keys->first_ + keys->step_; i <= keys->last_;
       i += keys->step_) {
    if (ShardForKey(request.args_[i]) != first) return true;
  }
  return false;
}

void InitStorage(storage::StorageEngine* engine,
                 ReplicationManager* replication) {
  // Fault-enabled test processes set the immutable hook before workers launch.
  LAVIK_FAULT_INJECT(g_command_pause_before_db_admission = std::getenv(
                         "LAVIK_COMMAND_PAUSE_BEFORE_DB_ADMISSION_MS"););
  g_storage = engine;
  InitFunctionCatalog(engine);
  InitBlockingWaitStorage(engine);
  InitHashCommandStorage(engine);
  InitListCommandStorage(engine);
  InitSetCommandStorage(engine);
  InitSortCommandStorage(engine);
  InitStringCommandStorage(engine);
  InitStreamCommandStorage(engine);
  InitZSetCommandStorage(engine);
  g_replication = replication;
  g_replica_read_only = replication != nullptr && replication->is_replica();
}

void InitClientLimit(ClientLimit* limit) noexcept { g_client_limit = limit; }

absl::Status ReplicationCommandCapture::ReserveAdditionalCommands(
    std::size_t count, std::size_t payload_bytes) {
  std::lock_guard lock(mutex_);
  constexpr auto limit = std::numeric_limits<std::size_t>::max();
  constexpr auto overhead = sizeof(RetainedMemoryCharge) + 128;
  const auto owner = CurrentMemoryAccountingShard();
  if (prepared_charge_ && preparation_owner_ != owner)
    return absl::FailedPreconditionError(
        "replication capture preparation changed coordinator");
  const auto existing = prepared_charge_ ? prepared_charge_->bytes() : 0;
  if (count > commands_.max_size() - commands_.size() ||
      payload_bytes > limit - overhead ||
      count > (limit - overhead - payload_bytes) /
                  sizeof(CapturedReplicationCommand))
    return absl::ResourceExhaustedError(
        "OOM replication capture size overflow");
  const auto bytes =
      overhead + payload_bytes + count * sizeof(CapturedReplicationCommand);
  if (bytes > limit - existing)
    return absl::ResourceExhaustedError(
        "OOM replication capture size overflow");
  auto admission = TryReserveMemory(bytes);
  if (!admission) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError("OOM preparing replication capture");
  }
  try {
    auto charge = prepared_charge_ ? prepared_charge_
                                   : std::make_shared<RetainedMemoryCharge>();
    commands_.reserve(commands_.size() + count);
    if (prepared_charge_) {
      charge->Resize(existing + bytes);
      admission->Release();
    } else {
      charge->Adopt(&*admission, bytes);
    }
    preparation_owner_ = owner;
    prepared_charge_ = std::move(charge);
    return absl::OkStatus();
  } catch (const std::bad_alloc&) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError("OOM preparing replication capture");
  }
}

void ReplicationCommandCapture::ReleaseUnusedPreparation() {
  std::lock_guard lock(mutex_);
  if (!commands_.empty() || !prepared_charge_) return;
  // The queued request can outlive this failed command for the rest of EXEC.
  // Drop allocated capacity before the charge; preserve handled_ so an empty
  // canonical result cannot fall back to replaying the original command.
  std::vector<CapturedReplicationCommand>().swap(commands_);
  prepared_charge_.reset();
}

void ReplicationCommandCapture::MarkHandled() {
  std::lock_guard lock(mutex_);
  handled_ = true;
}

void ReplicationCommandCapture::Record(std::uint8_t db_id,
                                       std::vector<std::string> args) {
  if (args.empty()) return;
  std::lock_guard lock(mutex_);
  handled_ = true;
  CapturedReplicationCommand command{db_id, std::move(args)};
  command.retained_charge_ = prepared_charge_;
  commands_.push_back(std::move(command));
}

CapturedReplicationEffects ReplicationCommandCapture::Take() {
  std::lock_guard lock(mutex_);
  return CapturedReplicationEffects{handled_, std::move(commands_)};
}

void ReplicationCommandCapture::SetCapturedPubSubPublication(
    std::shared_ptr<CapturedPubSubPublication> publication) {
  std::lock_guard lock(mutex_);
  assert(pubsub_publication_ == nullptr);
  pubsub_publication_ = std::move(publication);
}

std::shared_ptr<CapturedPubSubPublication>
ReplicationCommandCapture::TakeCapturedPubSubPublication() {
  std::lock_guard lock(mutex_);
  return std::exchange(pubsub_publication_, nullptr);
}

void CaptureReplicationCommand(const CommandRequest& request,
                               std::vector<std::string> canonical_args) {
  CaptureReplicationCommand(request, request.db_id_, std::move(canonical_args));
}

void CaptureReplicationCommand(const CommandRequest& request,
                               std::uint8_t db_id,
                               std::vector<std::string> canonical_args) {
  if (request.replication_capture_ != nullptr) {
    request.replication_capture_->Record(db_id, std::move(canonical_args));
  }
}

void MarkReplicationCommandHandled(const CommandRequest& request) {
  if (request.replication_capture_ != nullptr) {
    request.replication_capture_->MarkHandled();
  }
}

std::vector<std::string> EncodeReplicationCommandEffects(
    std::vector<CapturedReplicationCommand> commands) {
  std::size_t argument_count = 2;
  for (const auto& command : commands) {
    argument_count += 2 + command.args_.size();
  }
  std::vector<std::string> args;
  args.reserve(argument_count);
  args.emplace_back(kReplicatedExecCommand);
  args.push_back(std::to_string(commands.size()));
  for (auto& command : commands) {
    args.push_back(std::to_string(command.db_id_));
    args.push_back(std::to_string(command.args_.size()));
    args.insert(args.end(), std::make_move_iterator(command.args_.begin()),
                std::make_move_iterator(command.args_.end()));
  }
  return args;
}

std::optional<storage::ReplicationCommandAppend> PrepareReplicationCommand(
    const CommandRequest& request, std::vector<std::string> canonical_args) {
  if (request.replication_origin_ || g_storage == nullptr ||
      !g_storage->ReplicationLogActive()) {
    return std::nullopt;
  }
  storage::ReplicationCommandAppend append;
  append.db_id_ = request.db_id_;
  append.args_ =
      canonical_args.empty() ? request.args_ : std::move(canonical_args);
  return append;
}

namespace {

std::atomic<std::uint64_t> g_next_replication_transaction_id{1};
}  // namespace

ReplicationTransactionGuard::ReplicationTransactionGuard(
    const CommandRequest& request, tx::Transaction* transaction,
    std::vector<std::string> canonical_args,
    std::vector<unsigned> additional_participants) {
  uncaught_exceptions_ = std::uncaught_exceptions();
  if (transaction == nullptr) return;
  std::vector<unsigned> participants = transaction->shard_ids();
  for (const unsigned participant : additional_participants) {
    if (std::find(participants.begin(), participants.end(), participant) ==
        participants.end()) {
      participants.push_back(participant);
    }
  }
  Initialize(request, std::move(participants), std::move(canonical_args));
  if (transaction_ != nullptr) {
    transaction->SetShardEntryHook(&ReplicationTransactionGuard::EnterShardHook,
                                   this);
  }
}

ReplicationTransactionGuard::ReplicationTransactionGuard(
    const CommandRequest& request, std::vector<unsigned> participants,
    std::vector<std::string> canonical_args) {
  uncaught_exceptions_ = std::uncaught_exceptions();
  Initialize(request, std::move(participants), std::move(canonical_args));
}

void ReplicationTransactionGuard::Initialize(
    const CommandRequest& request, std::vector<unsigned> participants,
    std::vector<std::string> canonical_args) {
  if (request.replication_origin_ || g_storage == nullptr ||
      !g_storage->ReplicationLogActive() || request.spec_ == nullptr ||
      (request.spec_->flags_ & (kCmdWrite | kCmdDynamicWrite)) == 0 ||
      participants.empty()) {
    return;
  }
  const std::uint64_t id =
      g_next_replication_transaction_id.fetch_add(1, std::memory_order_relaxed);
  if (id == 0) return;

  const auto& command_args =
      canonical_args.empty() ? request.args_ : canonical_args;
  const auto reservation_bytes =
      storage::ReplicationTransactionReservationBytes(
          participants.capacity(), participants.size(), command_args);
  if (!reservation_bytes.has_value()) {
    RecordMemoryRejection();
    status_ = absl::ResourceExhaustedError(
        "replication transaction allocation size overflow");
    return;
  }
  auto reservation = TryReserveMemory(*reservation_bytes);
  if (!reservation.has_value()) {
    RecordMemoryRejection();
    status_ = absl::ResourceExhaustedError(
        "OOM command not allowed when used memory > 'maxmemory'.");
    return;
  }
  try {
    auto transaction = std::make_shared<storage::ReplicationTransaction>();
    transaction->id_ = id;
    transaction->db_id_ = request.db_id_;
    transaction->participants_ = std::move(participants);
    // Rotate the one payload-bearing marker across the participant set. A
    // fixed flow would make that flow's bounded backlog the throughput limit
    // for otherwise balanced cross-flow transactions.
    transaction->payload_flow_ =
        transaction->participants_[id % transaction->participants_.size()];
    auto metadata =
        EncodeReplicationTransactionEnvelope(ReplicationTransactionEnvelope{
            .id_ = id,
            .payload_flow_ = transaction->payload_flow_,
            .participants_ = transaction->participants_,
        });
    if (!metadata.ok()) {
      status_ = metadata.status();
      return;
    }
    transaction->envelope_metadata_ = std::move(*metadata);
    transaction->command_args_.assign(command_args.begin(), command_args.end());
    const auto allocation_bytes =
        storage::ReplicationTransactionAllocationBytes(
            transaction->participants_.capacity(),
            std::span<const std::string>(&transaction->envelope_metadata_, 1),
            transaction->command_args_);
    if (!allocation_bytes.has_value() ||
        *allocation_bytes > reservation->bytes()) {
      RecordMemoryRejection();
      status_ = absl::ResourceExhaustedError(
          "replication transaction allocation exceeded reservation");
      return;
    }
    transaction->retained_charge_.Adopt(&*reservation, *allocation_bytes);
    transaction_ = std::move(transaction);
  } catch (const std::length_error&) {
    status_ =
        absl::ResourceExhaustedError("replication transaction is too large");
  }
}

ReplicationTransactionGuard::~ReplicationTransactionGuard() {
  if (transaction_ != nullptr) {
    storage::ReplicationTransactionResolution expected =
        storage::ReplicationTransactionResolution::kPending;
    // A normal command error explicitly returns through its handler and may
    // discard an uncommitted marker. An exception can escape after a storage
    // callback has made its mutation visible, so treating that case as an
    // ordinary discard could create an undetectable hole in replica history.
    const auto unresolved =
        std::uncaught_exceptions() > uncaught_exceptions_
            ? storage::ReplicationTransactionResolution::kInvalidate
            : storage::ReplicationTransactionResolution::kDiscard;
    transaction_->resolution_.compare_exchange_strong(
        expected, unresolved, std::memory_order_release,
        std::memory_order_relaxed);
  }
}

bool ReplicationTransactionGuard::Commit() noexcept {
  if (transaction_ != nullptr) {
    storage::ReplicationTransactionResolution expected =
        storage::ReplicationTransactionResolution::kPending;
    return transaction_->resolution_.compare_exchange_strong(
        expected, storage::ReplicationTransactionResolution::kPublish,
        std::memory_order_release, std::memory_order_relaxed);
  }
  return false;
}

absl::Status ReplicationTransactionGuard::TrySetCommandArgs(
    std::vector<std::string> canonical_args) noexcept {
  if (transaction_ == nullptr || canonical_args.empty() ||
      transaction_->resolution_.load(std::memory_order_acquire) !=
          storage::ReplicationTransactionResolution::kPending) {
    return absl::OkStatus();
  }
  if (transaction_->envelope_metadata_.empty()) {
    return absl::FailedPreconditionError(
        "replication transaction envelope is incomplete");
  }
  const auto target_bytes = storage::ReplicationTransactionAllocationBytes(
      transaction_->participants_.capacity(),
      std::span<const std::string>(&transaction_->envelope_metadata_, 1),
      canonical_args);
  if (!target_bytes.has_value()) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError(
        "replication transaction allocation size overflow");
  }
  const std::size_t current_bytes = transaction_->retained_charge_.bytes();
  std::optional<MemoryReservation> growth;
  if (*target_bytes > current_bytes) {
    growth = TryReserveMemory(*target_bytes - current_bytes);
    if (!growth.has_value()) {
      RecordMemoryRejection();
      return absl::ResourceExhaustedError(
          "OOM command not allowed when used memory > 'maxmemory'.");
    }
  }
  try {
    // Build a complete replacement before touching the shared payload. This
    // gives pre-mutation callers a strong failure boundary; publisher workers
    // continue seeing the old immutable body until the noexcept swap.
    std::vector<std::string> replacement;
    replacement.reserve(canonical_args.size());
    replacement.insert(replacement.end(),
                       std::make_move_iterator(canonical_args.begin()),
                       std::make_move_iterator(canonical_args.end()));
    transaction_->command_args_.swap(replacement);
    transaction_->retained_charge_.Resize(
        std::max(current_bytes, *target_bytes));
    return absl::OkStatus();
  } catch (const std::length_error&) {
    return absl::ResourceExhaustedError(
        "replication transaction payload is too large");
  } catch (const std::bad_alloc&) {
    // Pre-mutation callers must be able to abort without changing the shared
    // payload. In particular, a STORE's prepared effects cannot terminate the
    // process just because this noexcept boundary owns the final vector.
    RecordMemoryRejection();
    return absl::ResourceExhaustedError(
        "OOM preparing replication transaction payload");
  }
}

void ReplicationTransactionGuard::SetCommandArgs(
    std::vector<std::string> canonical_args) noexcept {
  if (!TrySetCommandArgs(std::move(canonical_args)).ok()) InvalidatePayload();
}

void ReplicationTransactionGuard::SetFinalExpirations(
    std::span<const storage::TxShardWrites> shard_writes) noexcept {
  if (transaction_ == nullptr ||
      transaction_->resolution_.load(std::memory_order_acquire) !=
          storage::ReplicationTransactionResolution::kPending) {
    return;
  }
  try {
    std::vector<storage::TxShardWrites::ExpirationEffect> final_effects;
    for (const auto& shard : shard_writes) {
      for (const auto& effect : shard.expiration_effects_) {
        auto found = std::find_if(
            final_effects.begin(), final_effects.end(), [&](const auto& prior) {
              return prior.db_id_ == effect.db_id_ && prior.key_ == effect.key_;
            });
        if (found == final_effects.end()) {
          final_effects.push_back(effect);
        } else {
          *found = effect;
        }
      }
    }
    if (final_effects.empty()) return;

    if (transaction_->command_args_.empty()) return;
    std::vector<std::string> command_args = transaction_->command_args_;
    command_args.reserve(command_args.size() + final_effects.size() * 4);
    for (const auto& effect : final_effects) {
      AppendReplicationExpirationEffect(&command_args, transaction_->db_id_,
                                        effect.db_id_, effect.key_,
                                        effect.exists_, effect.expire_at_ms_);
    }
    SetCommandArgs(std::move(command_args));
  } catch (const std::length_error&) {
    InvalidatePayload();
  } catch (const std::bad_alloc&) {
    // Expiry settlement happens after mutations. An incomplete replication
    // body must invalidate its envelope instead of publishing partial effects.
    RecordMemoryRejection();
    InvalidatePayload();
  }
}

void ReplicationTransactionGuard::InvalidatePayload() noexcept {
  if (transaction_ == nullptr) return;
  storage::ReplicationTransactionResolution expected =
      storage::ReplicationTransactionResolution::kPending;
  transaction_->resolution_.compare_exchange_strong(
      expected, storage::ReplicationTransactionResolution::kInvalidate,
      std::memory_order_release, std::memory_order_relaxed);
}

void ReplicationTransactionGuard::SetParticipantsEnteredHook(
    ParticipantsEnteredHook hook, void* context) noexcept {
  assert(entered_participants_.load(std::memory_order_relaxed) == 0);
  participants_entered_hook_ = hook;
  participants_entered_context_ = context;
}

void ReplicationTransactionGuard::EnterCurrentShard() noexcept {
  EnterShard(bycorf::ThisWorker().id_);
}

void ReplicationTransactionGuard::EnterShard(unsigned shard_id) noexcept {
  if (transaction_ == nullptr) return;
  const auto& participants = transaction_->participants_;
  if (std::find(participants.begin(), participants.end(), shard_id) ==
      participants.end()) {
    transaction_->resolution_.store(
        storage::ReplicationTransactionResolution::kDiscard,
        std::memory_order_release);
    return;
  }
  if (!g_storage->TryEnqueueReplicationTransaction(transaction_)) {
    storage::ReplicationTransactionResolution expected =
        storage::ReplicationTransactionResolution::kPending;
    transaction_->resolution_.compare_exchange_strong(
        expected, storage::ReplicationTransactionResolution::kDiscard,
        std::memory_order_release, std::memory_order_relaxed);
  }
  const unsigned entered =
      entered_participants_.fetch_add(1, std::memory_order_acq_rel) + 1;
  assert(entered <= participants.size());
  if (entered == participants.size() && participants_entered_hook_ != nullptr) {
    participants_entered_hook_(participants_entered_context_);
  }
}

void ReplicationTransactionGuard::EnterShardHook(void* context,
                                                 unsigned shard_id) {
  static_cast<ReplicationTransactionGuard*>(context)->EnterShard(shard_id);
}

void SetServerInfo(std::string bind_ip, std::uint16_t port,
                   unsigned thread_count, std::string config_file) {
  g_server_bind_ip = std::move(bind_ip);
  g_server_port = port;
  g_server_threads = thread_count;
  g_server_config_file = std::move(config_file);
  g_server_start = std::chrono::steady_clock::now();
  for (auto& clients : g_worker_clients) clients.clear();
}

void ConnectionOpened() noexcept { RecordConnectionOpened(); }

void ConnectionClosed() noexcept { RecordConnectionClosed(); }

void RegisterClientConnection(std::uint64_t id, int fd, std::string address,
                              bool tls, bool replica,
                              std::uint64_t replication_session_id) {
  const unsigned worker = bycorf::ThisWorker().id_;
  assert(worker < g_worker_clients.size());
  auto& clients = g_worker_clients[worker];
  const auto existing =
      std::find_if(clients.begin(), clients.end(),
                   [id](const auto& client) { return client.id_ == id; });
  assert(existing == clients.end());
  (void)existing;
  clients.push_back(ClientConnectionRecord{
      .id_ = id,
      .fd_ = fd,
      .address_ = std::move(address),
      .connected_at_ = std::chrono::steady_clock::now(),
      .replication_session_id_ = replication_session_id,
      .tls_ = tls,
      .type_ = replica ? ClientConnectionRecord::Type::kReplica
                       : ClientConnectionRecord::Type::kNormal,
      .name_ = {},
      .library_name_ = {},
      .library_version_ = {},
      .resp_version_ = RespVersion::k2,
      .subscriptions_ = 0,
      .pattern_subscriptions_ = 0,
      .blocked_ = false,
      .closing_ = false,
  });
}

void SetClientReplicationSession(
    std::uint64_t id, std::uint64_t replication_session_id) noexcept {
  const unsigned worker = bycorf::ThisWorker().id_;
  assert(worker < g_worker_clients.size());
  const auto found = std::find_if(
      g_worker_clients[worker].begin(), g_worker_clients[worker].end(),
      [id](const auto& client) { return client.id_ == id; });
  if (found != g_worker_clients[worker].end()) {
    found->replication_session_id_ = replication_session_id;
  }
}

void SetClientName(std::uint64_t id, std::string name) noexcept {
  const unsigned worker = bycorf::ThisWorker().id_;
  assert(worker < g_worker_clients.size());
  const auto found = std::find_if(
      g_worker_clients[worker].begin(), g_worker_clients[worker].end(),
      [id](const auto& client) { return client.id_ == id; });
  if (found != g_worker_clients[worker].end()) found->name_ = std::move(name);
}

void SetClientRespVersion(std::uint64_t id, RespVersion version) noexcept {
  const unsigned worker = bycorf::ThisWorker().id_;
  assert(worker < g_worker_clients.size());
  const auto found = std::find_if(
      g_worker_clients[worker].begin(), g_worker_clients[worker].end(),
      [id](const auto& client) { return client.id_ == id; });
  if (found != g_worker_clients[worker].end()) found->resp_version_ = version;
}

void SetClientPubSubCounts(std::uint64_t id, std::size_t subscriptions,
                           std::size_t pattern_subscriptions) noexcept {
  const unsigned worker = bycorf::ThisWorker().id_;
  assert(worker < g_worker_clients.size());
  const auto found = std::find_if(
      g_worker_clients[worker].begin(), g_worker_clients[worker].end(),
      [id](const auto& client) { return client.id_ == id; });
  if (found == g_worker_clients[worker].end() ||
      found->type_ == ClientConnectionRecord::Type::kReplica) {
    return;
  }
  found->subscriptions_ = subscriptions;
  found->pattern_subscriptions_ = pattern_subscriptions;
  found->type_ = subscriptions == 0 ? ClientConnectionRecord::Type::kNormal
                                    : ClientConnectionRecord::Type::kPubSub;
}

void SetClientBlocked(std::uint64_t id, bool blocked) noexcept {
  const unsigned worker = bycorf::ThisWorker().id_;
  assert(worker < g_worker_clients.size());
  const auto found = std::find_if(
      g_worker_clients[worker].begin(), g_worker_clients[worker].end(),
      [id](const auto& client) { return client.id_ == id; });
  if (found != g_worker_clients[worker].end()) found->blocked_ = blocked;
}

void UnregisterClientConnection(std::uint64_t id) noexcept {
  const unsigned worker = bycorf::ThisWorker().id_;
  assert(worker < g_worker_clients.size());
  std::erase_if(g_worker_clients[worker],
                [id](const auto& client) { return client.id_ == id; });
}

namespace {

struct ClientFilter {
  std::vector<std::uint64_t> ids_;
  std::optional<std::string> address_;
  std::optional<ClientConnectionRecord::Type> type_;
  bool skip_self_ = true;
};

bool ClientMatches(const ClientConnectionRecord& client,
                   const ClientFilter& filter,
                   std::uint64_t requester_id) noexcept {
  return !client.closing_ &&
         (!filter.skip_self_ || client.id_ != requester_id) &&
         (filter.ids_.empty() ||
          std::find(filter.ids_.begin(), filter.ids_.end(), client.id_) !=
              filter.ids_.end()) &&
         (!filter.address_.has_value() ||
          client.address_ == *filter.address_) &&
         (!filter.type_.has_value() || client.type_ == *filter.type_);
}

absl::StatusOr<ClientConnectionRecord::Type> ParseClientType(
    std::string_view value) {
  if (CmpCaseInsensitive(value, "normal")) {
    return ClientConnectionRecord::Type::kNormal;
  }
  if (CmpCaseInsensitive(value, "replica") ||
      CmpCaseInsensitive(value, "slave")) {
    return ClientConnectionRecord::Type::kReplica;
  }
  if (CmpCaseInsensitive(value, "pubsub")) {
    return ClientConnectionRecord::Type::kPubSub;
  }
  return absl::InvalidArgumentError(
      "CLIENT type must be NORMAL, REPLICA or PUBSUB");
}

const char* ClientFlags(const ClientConnectionRecord& client) noexcept {
  if (client.blocked_) return "b";
  if (client.type_ == ClientConnectionRecord::Type::kReplica) return "S";
  if (client.type_ == ClientConnectionRecord::Type::kPubSub) return "P";
  return "N";
}

std::string FormatClientInfo(const ClientConnectionRecord& client,
                             std::chrono::steady_clock::time_point now,
                             std::uint8_t db_id, std::string_view command) {
  const auto age = std::chrono::duration_cast<std::chrono::seconds>(
                       now - client.connected_at_)
                       .count();
  std::string listing;
  absl::StrAppend(
      &listing, "id=", client.id_, " addr=", client.address_,
      " fd=", client.fd_, " name=", client.name_, " age=", age,
      " idle=0 flags=", ClientFlags(client),
      " db=", static_cast<unsigned>(db_id),
      " sub=", client.subscriptions_ - client.pattern_subscriptions_,
      " psub=", client.pattern_subscriptions_, " ssub=0 multi=-1 qbuf=0 ",
      "qbuf-free=0 argv-mem=0 multi-mem=0 rbs=0 rbp=0 ",
      "obl=0 oll=0 omem=0 tot-mem=0 events=r cmd=", command,
      " user=default redir=-1 resp=",
      static_cast<unsigned>(client.resp_version_),
      " lib-name=", client.library_name_, " lib-ver=", client.library_version_,
      client.tls_ ? " tls=1" : "");
  return listing;
}

std::string_view AppendClientInfoReply(ReplyBuilder& reply_builder,
                                       std::string_view listing) {
  if (reply_builder.version() == RespVersion::k2) {
    return reply_builder.AppendBulkString(listing);
  }
  std::string encoded;
  absl::StrAppend(&encoded, "=", listing.size() + 4, "\r\ntxt:", listing,
                  "\r\n");
  return reply_builder.AppendRaw(encoded);
}

std::string_view AppendClientHelp(ReplyBuilder& reply_builder) {
  static constexpr std::string_view kHelp[] = {
      "CACHING (YES|NO)",
      "    Enable/disable tracking of the keys for next command in "
      "OPTIN/OPTOUT modes.",
      "GETREDIR",
      "    Return the client ID we are redirecting to when tracking is "
      "enabled.",
      "GETNAME",
      "    Return the name of the current connection.",
      "ID",
      "    Return the ID of the current connection.",
      "INFO",
      "    Return information about the current client connection.",
      "KILL <ip:port>",
      "    Kill connection made from <ip:port>.",
      "KILL <option> <value> [<option> <value> [...]]",
      "    Kill connections. Options are:",
      "    * ADDR (<ip:port>|<unixsocket>:0)",
      "      Kill connections made from the specified address",
      "    * LADDR (<ip:port>|<unixsocket>:0)",
      "      Kill connections made to specified local address",
      "    * TYPE (NORMAL|MASTER|REPLICA|PUBSUB)",
      "      Kill connections by type.",
      "    * USER <username>",
      "      Kill connections authenticated by <username>.",
      "    * SKIPME (YES|NO)",
      "      Skip killing current connection (default: yes).",
      "LIST [options ...]",
      "    Return information about client connections. Options:",
      "    * TYPE (NORMAL|MASTER|REPLICA|PUBSUB)",
      "      Return clients of specified type.",
      "UNPAUSE",
      "    Stop the current client pause, resuming traffic.",
      "PAUSE <timeout> [WRITE|ALL]",
      "    Suspend all, or just write, clients for <timeout> milliseconds.",
      "REPLY (ON|OFF|SKIP)",
      "    Control the replies sent to the current connection.",
      "SETNAME <name>",
      "    Assign the name <name> to the current connection.",
      "SETINFO <option> <value>",
      "    Set client meta attr. Options are:",
      "    * LIB-NAME: the client lib name.",
      "    * LIB-VER: the client lib version.",
      "UNBLOCK <clientid> [TIMEOUT|ERROR]",
      "    Unblock the specified blocked client.",
      "TRACKING (ON|OFF) [REDIRECT <id>] [BCAST] [PREFIX <prefix> [...]]",
      "         [OPTIN] [OPTOUT] [NOLOOP]",
      "    Control server assisted client side caching.",
      "TRACKINGINFO",
      "    Report tracking status for the current connection.",
      "NO-EVICT (ON|OFF)",
      "    Protect current client connection from eviction.",
      "NO-TOUCH (ON|OFF)",
      "    Will not touch LRU/LFU stats when this mode is on.",
  };
  reply_builder.AppendArrayHeader(std::size(kHelp) + 3);
  reply_builder.AppendSimpleString(
      "CLIENT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:");
  for (std::string_view line : kHelp) reply_builder.AppendSimpleString(line);
  reply_builder.AppendSimpleString("HELP");
  return reply_builder.AppendSimpleString("    Print this help.");
}

bool ValidClientAttribute(std::string_view value) noexcept {
  return std::all_of(value.begin(), value.end(), [](unsigned char byte) {
    return byte >= '!' && byte <= '~';
  });
}

Task<CommandReply> ExecuteClient(ConnectionContext& ctx,
                                 const CommandRequest& request,
                                 ReplyBuilder& reply_builder) {
  const auto& args = request.args_;
  if (CmpCaseInsensitive(args[1], "help")) {
    if (args.size() != 2) {
      co_return BuiltReply(reply_builder.AppendError(
          "ERR wrong number of arguments for 'client|help' command"));
    }
    co_return BuiltReply(AppendClientHelp(reply_builder));
  }
  if (CmpCaseInsensitive(args[1], "info")) {
    if (args.size() != 2) {
      co_return BuiltReply(reply_builder.AppendError(
          "ERR wrong number of arguments for 'client|info' command"));
    }
    const auto& clients = g_worker_clients[ThisWorker().id_];
    const auto found = std::find_if(
        clients.begin(), clients.end(),
        [id = ctx.conn_id_](const auto& client) { return client.id_ == id; });
    if (found == clients.end()) {
      co_return BuiltReply(
          reply_builder.AppendError("ERR current client is not registered"));
    }
    std::string listing =
        FormatClientInfo(*found, std::chrono::steady_clock::now(),
                         ctx.selected_db_, "client|info");
    listing.push_back('\n');
    co_return BuiltReply(AppendClientInfoReply(reply_builder, listing));
  }
  if (CmpCaseInsensitive(args[1], "setinfo")) {
    if (args.size() != 4) {
      co_return BuiltReply(reply_builder.AppendError(
          "ERR wrong number of arguments for 'client|setinfo' command"));
    }
    const bool library_name = CmpCaseInsensitive(args[2], "lib-name");
    const bool library_version = CmpCaseInsensitive(args[2], "lib-ver");
    if (!library_name && !library_version) {
      co_return BuiltReply(reply_builder.AppendError(
          absl::StrCat("ERR Unrecognized option '", args[2], "'")));
    }
    if (!ValidClientAttribute(args[3])) {
      co_return BuiltReply(reply_builder.AppendError(absl::StrCat(
          "ERR ", library_name ? "lib-name" : "lib-ver",
          " cannot contain spaces, newlines or special characters.")));
    }

    auto& clients = g_worker_clients[ThisWorker().id_];
    const auto found = std::find_if(
        clients.begin(), clients.end(),
        [id = ctx.conn_id_](const auto& client) { return client.id_ == id; });
    if (found == clients.end()) {
      co_return BuiltReply(
          reply_builder.AppendError("ERR current client is not registered"));
    }
    std::string& record_value =
        library_name ? found->library_name_ : found->library_version_;
    record_value = args[3];
    co_return BuiltReply(reply_builder.AppendSimpleString("OK"));
  }
  if (CmpCaseInsensitive(args[1], "id")) {
    if (args.size() != 2) {
      co_return BuiltReply(reply_builder.AppendError("ERR syntax error"));
    }
    co_return BuiltReply(reply_builder.AppendInteger(
        static_cast<long long>(std::min<std::uint64_t>(
            ctx.conn_id_, std::numeric_limits<long long>::max()))));
  }
  if (CmpCaseInsensitive(args[1], "setname")) {
    if (args.size() != 3) {
      co_return BuiltReply(reply_builder.AppendError("ERR syntax error"));
    }
    if (std::any_of(args[2].begin(), args[2].end(), [](unsigned char c) {
          return c == 0 || std::isspace(c);
        })) {
      co_return BuiltReply(
          reply_builder.AppendError("ERR Client names cannot contain spaces, "
                                    "newlines or special characters."));
    }
    ctx.client_name_ = args[2];
    SetClientName(ctx.conn_id_, ctx.client_name_);
    co_return BuiltReply(reply_builder.AppendSimpleString("OK"));
  }
  if (CmpCaseInsensitive(args[1], "getname")) {
    if (args.size() != 2) {
      co_return BuiltReply(reply_builder.AppendError("ERR syntax error"));
    }
    co_return BuiltReply(
        ctx.client_name_.empty()
            ? reply_builder.AppendNull()
            : reply_builder.AppendBulkString(ctx.client_name_));
  }
  if (CmpCaseInsensitive(args[1], "list")) {
    ClientFilter filter;
    filter.skip_self_ = false;
    bool saw_type = false;
    bool saw_id = false;
    for (std::size_t i = 2; i < args.size();) {
      if (CmpCaseInsensitive(args[i], "type")) {
        if (saw_type || i + 1 == args.size()) {
          co_return BuiltReply(reply_builder.AppendError("ERR syntax error"));
        }
        auto type = ParseClientType(args[i + 1]);
        if (!type.ok()) {
          co_return BuiltReply(reply_builder.AppendError(
              absl::StrCat("ERR ", type.status().message())));
        }
        filter.type_ = *type;
        saw_type = true;
        i += 2;
        continue;
      }
      if (!CmpCaseInsensitive(args[i], "id") || saw_id) {
        co_return BuiltReply(reply_builder.AppendError("ERR syntax error"));
      }
      saw_id = true;
      ++i;
      const std::size_t first_id = i;
      while (i < args.size() && !CmpCaseInsensitive(args[i], "type")) {
        std::uint64_t id = 0;
        if (!ParseUint64(args[i], &id)) {
          co_return BuiltReply(reply_builder.AppendError(
              "ERR value is not an integer or out of range"));
        }
        filter.ids_.push_back(id);
        ++i;
      }
      if (i == first_id) {
        co_return BuiltReply(reply_builder.AppendError("ERR syntax error"));
      }
    }

    std::vector<ClientConnectionRecord> clients;
    for (unsigned worker = 0; worker < g_server_threads; ++worker) {
      auto collect = [filter]() {
        std::vector<ClientConnectionRecord> matches;
        for (const auto& client : g_worker_clients[ThisWorker().id_]) {
          if (ClientMatches(client, filter, 0)) matches.push_back(client);
        }
        return matches;
      };
      auto local = worker == ThisWorker().id_
                       ? collect()
                       : co_await SubmitTo(worker, collect);
      std::move(local.begin(), local.end(), std::back_inserter(clients));
    }
    std::sort(clients.begin(), clients.end(),
              [](const auto& left, const auto& right) {
                return left.id_ < right.id_;
              });
    std::string listing;
    const auto now = std::chrono::steady_clock::now();
    for (const auto& client : clients) {
      absl::StrAppend(&listing, FormatClientInfo(client, now, 0, "client"),
                      "\n");
    }
    co_return BuiltReply(AppendClientInfoReply(reply_builder, listing));
  }

  if (CmpCaseInsensitive(args[1], "unblock")) {
    if (args.size() != 3 && args.size() != 4) {
      co_return BuiltReply(reply_builder.AppendError("ERR syntax error"));
    }
    std::uint64_t client_id = 0;
    if (!ParseUint64(args[2], &client_id)) {
      co_return BuiltReply(reply_builder.AppendError(
          "ERR value is not an integer or out of range"));
    }
    ClientUnblockMode mode = ClientUnblockMode::kTimeout;
    if (args.size() == 4) {
      if (CmpCaseInsensitive(args[3], "timeout")) {
        mode = ClientUnblockMode::kTimeout;
      } else if (CmpCaseInsensitive(args[3], "error")) {
        mode = ClientUnblockMode::kError;
      } else {
        co_return BuiltReply(reply_builder.AppendError(
            "ERR CLIENT UNBLOCK reason should be TIMEOUT or ERROR"));
      }
    }
    bool unblocked = false;
    for (unsigned worker = 0; worker < g_server_threads; ++worker) {
      auto unblock = [client_id, mode] {
        return UnblockClientOnCurrentWorker(client_id, mode);
      };
      unblocked = worker == ThisWorker().id_
                      ? unblock()
                      : co_await SubmitTo(worker, unblock);
      if (unblocked) break;
    }
    co_return BuiltReply(reply_builder.AppendInteger(unblocked ? 1 : 0));
  }

  if (!CmpCaseInsensitive(args[1], "kill")) {
    co_return BuiltReply(reply_builder.AppendError(
        "ERR unknown subcommand or wrong number of arguments for 'CLIENT'"));
  }

  ClientFilter filter;
  bool legacy_address = args.size() == 3;
  if (legacy_address) {
    filter.address_ = args[2];
  } else {
    if (args.size() < 4 || args.size() % 2 != 0) {
      co_return BuiltReply(reply_builder.AppendError("ERR syntax error"));
    }
    for (std::size_t i = 2; i < args.size(); i += 2) {
      if (CmpCaseInsensitive(args[i], "id")) {
        std::uint64_t id = 0;
        if (!ParseUint64(args[i + 1], &id)) {
          co_return BuiltReply(reply_builder.AppendError(
              "ERR client-id should be greater than 0"));
        }
        filter.ids_.push_back(id);
      } else if (CmpCaseInsensitive(args[i], "addr")) {
        filter.address_ = args[i + 1];
      } else if (CmpCaseInsensitive(args[i], "type")) {
        auto type = ParseClientType(args[i + 1]);
        if (!type.ok()) {
          co_return BuiltReply(reply_builder.AppendError(
              absl::StrCat("ERR ", type.status().message())));
        }
        filter.type_ = *type;
      } else if (CmpCaseInsensitive(args[i], "skipme")) {
        if (CmpCaseInsensitive(args[i + 1], "yes")) {
          filter.skip_self_ = true;
        } else if (CmpCaseInsensitive(args[i + 1], "no")) {
          filter.skip_self_ = false;
        } else {
          co_return BuiltReply(reply_builder.AppendError("ERR syntax error"));
        }
      } else {
        co_return BuiltReply(reply_builder.AppendError("ERR syntax error"));
      }
    }
  }

  std::vector<std::uint64_t> replication_sessions;
  std::uint64_t killed = 0;
  for (unsigned worker = 0; worker < g_server_threads; ++worker) {
    auto inspect = [filter, requester_id = ctx.conn_id_]() {
      std::pair<std::uint64_t, std::vector<std::uint64_t>> local;
      for (const auto& client : g_worker_clients[ThisWorker().id_]) {
        if (!ClientMatches(client, filter, requester_id)) continue;
        ++local.first;
        if (client.replication_session_id_ != 0) {
          local.second.push_back(client.replication_session_id_);
        }
      }
      return local;
    };
    auto inspected = worker == ThisWorker().id_
                         ? inspect()
                         : co_await SubmitTo(worker, inspect);
    killed += inspected.first;
    std::move(inspected.second.begin(), inspected.second.end(),
              std::back_inserter(replication_sessions));
  }
  std::sort(replication_sessions.begin(), replication_sessions.end());
  replication_sessions.erase(
      std::unique(replication_sessions.begin(), replication_sessions.end()),
      replication_sessions.end());

  std::vector<std::vector<ClientConnectionRecord>> targets(g_server_threads);
  // Selection is deliberately separate from shutdown. Closing a replica's
  // control socket causes its other flow sockets to tear down immediately;
  // expand an ID/ADDR hit to the complete physical session, then mark every
  // target before closing any of them.
  for (unsigned worker = 0; worker < g_server_threads; ++worker) {
    auto select = [filter, requester_id = ctx.conn_id_,
                   replication_sessions]() {
      std::vector<ClientConnectionRecord> local;
      for (auto& client : g_worker_clients[ThisWorker().id_]) {
        const bool direct = ClientMatches(client, filter, requester_id);
        const bool same_replication_session =
            !client.closing_ && client.replication_session_id_ != 0 &&
            std::binary_search(replication_sessions.begin(),
                               replication_sessions.end(),
                               client.replication_session_id_);
        if (!direct && !same_replication_session) continue;
        client.closing_ = true;
        local.push_back(client);
      }
      return local;
    };
    auto selected = worker == ThisWorker().id_
                        ? select()
                        : co_await SubmitTo(worker, select);
    targets[worker] = std::move(selected);
  }
  for (unsigned worker = 0; worker < targets.size(); ++worker) {
    if (targets[worker].empty()) continue;
    auto close = [selected = std::move(targets[worker])]() {
      for (const ClientConnectionRecord& target : selected) {
        const auto& clients = g_worker_clients[ThisWorker().id_];
        const auto current = std::find_if(
            clients.begin(), clients.end(), [&target](const auto& client) {
              return client.id_ == target.id_ && client.fd_ == target.fd_ &&
                     client.closing_;
            });
        if (current != clients.end()) (void)::shutdown(target.fd_, SHUT_RDWR);
      }
      return true;
    };
    (void)(worker == ThisWorker().id_ ? close()
                                      : co_await SubmitTo(worker, close));
  }
  if (legacy_address) {
    co_return killed != 0
        ? BuiltReply(reply_builder.AppendSimpleString("OK"))
        : BuiltReply(reply_builder.AppendError("ERR No such client"));
  }
  co_return BuiltReply(reply_builder.AppendInteger(static_cast<long long>(
      std::min<std::uint64_t>(killed, std::numeric_limits<long long>::max()))));
}

}  // namespace

// ---- Cluster owner-side re-check definitions (declared in cluster_gate.h) --
//
// These are lavik-scope (not file-local) because the per-type multi-key
// executors (set/zset/list/sort/string) inject the same validator into their
// own transactions. EmitClusterDecision stays file-local above; the functions
// below call it across the namespace boundary within this translation unit.

absl::Status ClusterAuthorityChangedStatus() {
  return absl::FailedPreconditionError("cluster authority changed");
}

bool IsClusterAuthorityChanged(const absl::Status& status) {
  return status.code() == absl::StatusCode::kFailedPrecondition &&
         status.message() == "cluster authority changed";
}

namespace {

absl::Status ValidateClusterStorageMutation(const void* opaque) {
  const auto* admission =
      static_cast<const cluster::AuthorityAdmission*>(opaque);
  if (admission != nullptr &&
      cluster::GetClusterRuntime()->authority_guard_.RecheckAtMutation(
          *admission, cluster::LeaseClockNow()) ==
          cluster::RecheckResult::kOk) {
    return absl::OkStatus();
  }
  return ClusterAuthorityChangedStatus();
}

}  // namespace

storage::MutationPrecondition ClusterMutationPrecondition(
    const CommandRequest& request) {
  if (!cluster::ClusterEnabled() || request.replication_origin_ ||
      request.cluster_authority_admission_ == nullptr ||
      request.ClusterSlots().empty() || !ClusterRequestIsWrite(request)) {
    return {};
  }
  return storage::MutationPrecondition(
      std::shared_ptr<const void>(request.cluster_authority_admission_),
      &ValidateClusterStorageMutation);
}

CommandReply FinalizeClusterMutationReply(const CommandRequest& request,
                                          ReplyBuilder& reply_builder,
                                          CommandReply reply) {
  const auto& admission = request.cluster_authority_admission_;
  if (admission == nullptr || !admission->final_recheck_failed()) return reply;
  if (!admission->mutation_started()) {
    // ReplyBuilder returns a view of its whole buffer, not only the most recent
    // frame. Discard the stale handler answer before constructing the fresh
    // redirect/LOADING response.
    reply_builder.Reset();
    return ClusterAuthorityChangedReply(admission->slots(),
                                        request.connection_tls_, reply_builder);
  }
  // At least one participant linearized before another failed its final
  // check. Its aggregate outcome cannot be represented as a retryable error.
  reply.encoded_ = {};
  reply.disk_value_ = storage::DiskValue{};
  reply.chunks_.reset();
  reply.close_connection_ = true;
  return reply;
}

CommandReply ClusterAuthorityChangedReply(std::span<const std::uint16_t> slots,
                                          bool connection_tls,
                                          ReplyBuilder& reply_builder) {
  CommandReply reply;
  const cluster::RequestView view{
      .slots_ = slots,
      .is_write_ = true,
      .connection_readonly_ = false,
      // Same rule as the dispatch gate: writes are never whitelisted, so a
      // snapshot that lost readiness maps to LOADING instead of serving.
      .loading_allowed_ = false,
  };
  cluster::ClusterRuntime* runtime = cluster::GetClusterRuntime();
  const cluster::AuthorityAdmission fresh =
      runtime->authority_guard_.CaptureAndAdmit(view, cluster::LeaseClockNow());
  if (!EmitClusterDecision(fresh.decision(), connection_tls, reply_builder,
                           &reply)) {
    // Authority became serveable again after the failed proof. We cannot
    // infer whether an earlier irreversible step ran, so never manufacture a
    // success or redirect from that race.
    reply.close_connection_ = true;
  }
  return reply;
}

absl::Status ValidateClusterShardAuthority(void* opaque, unsigned /*shard*/) {
  auto* context = static_cast<ClusterShardValidatorContext*>(opaque);
  if (context->admission_ != nullptr &&
      cluster::GetClusterRuntime()->authority_guard_.Recheck(
          *context->admission_, cluster::LeaseClockNow()) ==
          cluster::RecheckResult::kOk) {
    return absl::OkStatus();
  }
  context->tripped_.store(true, std::memory_order_relaxed);
  return ClusterAuthorityChangedStatus();
}

void InstallClusterShardValidator(tx::Transaction& transaction,
                                  const CommandRequest& request,
                                  ClusterShardValidatorContext& context) {
  if (!cluster::ClusterEnabled() || request.replication_origin_ ||
      request.cluster_authority_admission_ == nullptr ||
      request.ClusterSlots().empty()) {
    return;
  }
  context.admission_ = request.cluster_authority_admission_;
  transaction.SetShardValidator(&ValidateClusterShardAuthority, &context);
}

absl::Status RecheckClusterRequestAuthority(const CommandRequest& request) {
  if (!cluster::ClusterEnabled() || request.replication_origin_ ||
      request.cluster_authority_admission_ == nullptr ||
      request.ClusterSlots().empty()) {
    return absl::OkStatus();
  }
  if (cluster::GetClusterRuntime()->authority_guard_.Recheck(
          *request.cluster_authority_admission_, cluster::LeaseClockNow()) ==
      cluster::RecheckResult::kOk) {
    return absl::OkStatus();
  }
  return ClusterAuthorityChangedStatus();
}

CommandReply ClusterValidatorFailureReply(
    const tx::Transaction& transaction,
    const ClusterShardValidatorContext& context, bool connection_tls,
    ReplyBuilder& reply_builder) {
  if (!transaction.single_shard()) {
    CommandReply reply;
    reply.close_connection_ = true;
    return reply;
  }
  const std::span<const std::uint16_t> slots =
      context.admission_ == nullptr ? std::span<const std::uint16_t>{}
                                    : context.admission_->slots();
  return ClusterAuthorityChangedReply(slots, connection_tls, reply_builder);
}

const char* CommandServingGenerationError(
    const CommandRequest& request) noexcept {
  if (request.replication_origin_ || !request.serving_generation_valid_ ||
      g_replication == nullptr) {
    return nullptr;
  }
  if (g_replication->ServingGenerationMatches(request.serving_generation_)) {
    return nullptr;
  }
  return g_replication->is_loading()
             ? "LOADING Lavik is loading the dataset from the primary"
             : "TRYAGAIN Lavik dataset changed while the command was queued "
               "or blocked";
}

Task<CommandReply> DispatchCommandImpl(ConnectionContext& ctx,
                                       CommandRequest& request,
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
  const bool script_kill = kind == CommandKind::kScript &&
                           request.args_.size() == 2 &&
                           CmpCaseInsensitive(request.args_[1], "kill");
  const bool function_kill = kind == CommandKind::kFunction &&
                             request.args_.size() == 2 &&
                             CmpCaseInsensitive(request.args_[1], "kill");
  const bool function_stats = kind == CommandKind::kFunction &&
                              request.args_.size() == 2 &&
                              CmpCaseInsensitive(request.args_[1], "stats");
  if (!request.replication_origin_ && LuaScriptsBusy() && !script_kill &&
      !function_kill && !function_stats) {
    const std::optional<LuaRunningInvocation> running =
        SnapshotLuaRunningInvocation();
    co_return BuiltReply(reply_builder.AppendError(
        running.has_value() && running->is_function_
            ? "BUSY Redis is busy running a script. You can only call FUNCTION "
              "KILL or SHUTDOWN NOSAVE."
            : "BUSY Redis is busy running a script. You can only call SCRIPT "
              "KILL or SHUTDOWN NOSAVE."));
  }
  if (cluster::ClusterEnabled()) {
    const std::string_view unscoped_mutation = UnscopedClusterMutation(request);
    if (!unscoped_mutation.empty()) {
      // Meta authority is deliberately finite per group and can never prove a
      // process-wide durable mutation. Reject that policy before
      // the transient population LOADING gate so an unassigned/fenced node
      // cannot make the command appear potentially valid after recovery.
      if (ctx.in_multi_) ctx.multi_dirty_ = true;
      co_return BuiltReply(reply_builder.AppendError(absl::StrCat(
          "ERR ", unscoped_mutation, " is not allowed in cluster mode")));
    }
  }
  if (g_replication != nullptr && g_replication->is_loading()) [[unlikely]] {
    // The whitelist is shared verbatim with the cluster gate. In cluster mode
    // this is the target-side population fence; ClusterGateReject below still
    // performs the independent topology/authority admission.
    if (!LoadingAllowedCommand(request)) {
      co_return BuiltReply(reply_builder.AppendError(
          "LOADING Lavik is loading the dataset from the primary"));
    }
  }
  if (cluster::ClusterEnabled()) {
    // Cluster admission gate: redirect or refuse before any
    // execution, including at MULTI queue time so EXEC aborts dirty. The
    // admitted ServingState snapshot rides on the request for the owner-side
    // authority re-check.
    CommandReply cluster_reply;
    if (ClusterGateReject(ctx, request, reply_builder, &cluster_reply)) {
      if (ctx.in_multi_) ctx.multi_dirty_ = true;
      co_return cluster_reply;
    }
  } else if (std::optional<std::string> moved =
                 co_await ReplicaMovedError(ctx, request);
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
      case CommandKind::kQuit: {
        CommandReply reply = BuiltReply(reply_builder.AppendSimpleString("OK"));
        reply.close_connection_ = true;
        co_return reply;
      }
      case CommandKind::kReset:
        ctx.ResetMulti();
        co_await DropWatches(ctx);
        ctx.selected_db_ = 0;
        ctx.authenticated_ = !ctx.authentication_required_;
        ctx.cluster_readonly_ = false;
        ctx.SetRespVersion(RespVersion::k2);
        SetClientRespVersion(ctx.conn_id_, RespVersion::k2);
        ctx.client_name_.clear();
        SetClientName(ctx.conn_id_, {});
        ctx.native_replication_watermark_.reset();
        ctx.native_replication_watermark_dirty_ = false;
        co_return BuiltReply(reply_builder.AppendSimpleString("RESET"));
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
    const bool reject_writes = g_replication != nullptr
                                   ? g_replication->reject_writes()
                                   : g_replica_read_only;
    if (reject_writes && (spec.flags_ & kCmdWrite) != 0) {
      ctx.multi_dirty_ = true;
      co_return BuiltReply(reply_builder.AppendError(
          "READONLY You can't write against a read only replica."));
    }
    const bool sentinel_management = IsSentinelManagementCommand(request);
    const bool sentinel_batch =
        !ctx.queued_.empty() &&
        IsSentinelManagementCommand(ctx.queued_.front());
    if (sentinel_management || sentinel_batch) {
      if (!sentinel_management || (!ctx.queued_.empty() && !sentinel_batch)) {
        ctx.multi_dirty_ = true;
        co_return BuiltReply(reply_builder.AppendError(
            "ERR Sentinel management commands must be queued alone"));
      }
      request.db_id_ = ctx.multi_db_;
      request.blocking_wake_cascade_ = nullptr;
      ctx.queued_.push_back(std::move(request));
      co_return BuiltReply(reply_builder.AppendSimpleString("QUEUED"));
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
      ReplyBuilder local_builder(ctx.resp_version());
      CommandReply local = ExecuteSimpleLocalCommand(request, local_builder);
      if (!local.selected_db_.has_value()) {
        ctx.multi_dirty_ = true;
        co_return BuiltReply(reply_builder.AppendRaw(local.encoded_));
      }
      ctx.multi_db_ = *local.selected_db_;
    }
    if (MayGrowMemory(request) && RejectForMemory(0)) {
      ctx.multi_dirty_ = true;
      co_return BuiltReply(AppendOomError(reply_builder));
    }
    request.db_id_ = ctx.multi_db_;
    request.blocking_wake_cascade_ = nullptr;
    ctx.queued_.push_back(std::move(request));
    co_return BuiltReply(reply_builder.AppendSimpleString("QUEUED"));
  }

  // These are the observation/cancellation controls for an active function.
  // They must not wait behind the DB, snapshot, publisher, or cross-flow order
  // gates held by that same function, or FUNCTION KILL can never reach the
  // run-control flag needed to release those gates. Keep MULTI queue semantics
  // above, and retain the busy/loading/cluster policy checks already applied.
  if (function_kill || function_stats) {
    co_return co_await ExecuteFunction(request, reply_builder);
  }

  switch (kind) {
    case CommandKind::kHello:
      if (ctx.hello_handler_ == nullptr) {
        co_return BuiltReply(
            reply_builder.AppendError("ERR HELLO is unavailable"));
      }
      co_return BuiltReply(ctx.hello_handler_(ctx.hello_authenticator_,
                                              ctx.hello_replication_, ctx,
                                              request.args_, reply_builder));
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
    case CommandKind::kQuit: {
      CommandReply reply = BuiltReply(reply_builder.AppendSimpleString("OK"));
      reply.close_connection_ = true;
      co_return reply;
    }
    case CommandKind::kReset:
      ctx.ResetMulti();
      co_await DropWatches(ctx);
      ctx.selected_db_ = 0;
      ctx.authenticated_ = !ctx.authentication_required_;
      ctx.cluster_readonly_ = false;
      ctx.SetRespVersion(RespVersion::k2);
      SetClientRespVersion(ctx.conn_id_, RespVersion::k2);
      ctx.client_name_.clear();
      SetClientName(ctx.conn_id_, {});
      ctx.native_replication_watermark_.reset();
      ctx.native_replication_watermark_dirty_ = false;
      co_return BuiltReply(reply_builder.AppendSimpleString("RESET"));
    case CommandKind::kReadOnly:
      ctx.cluster_readonly_ = true;
      co_return BuiltReply(reply_builder.AppendSimpleString("OK"));
    case CommandKind::kReadWrite:
      ctx.cluster_readonly_ = false;
      co_return BuiltReply(reply_builder.AppendSimpleString("OK"));
    case CommandKind::kClient:
      co_return co_await ExecuteClient(ctx, request, reply_builder);
    case CommandKind::kWait:
      co_return co_await ExecuteWait(ctx, request, reply_builder,
                                     /*allow_blocking=*/true,
                                     /*unresolved_write=*/false);
    case CommandKind::kPublish:
    case CommandKind::kPubSub:
    case CommandKind::kPSubscribe:
    case CommandKind::kPUnsubscribe:
    case CommandKind::kSubscribe:
    case CommandKind::kUnsubscribe:
      co_return co_await ExecutePubSubCommand(ctx, request, reply_builder);
    default:
      break;
  }
  co_return co_await ExecuteCommand(request, reply_builder, ctx.conn_id_, &ctx);
}

Task<CommandReply> DispatchCommand(ConnectionContext& ctx,
                                   CommandRequest& request,
                                   ReplyBuilder& reply_builder) {
  // FLUSH, KEYS, and RDB backup commands acquire or close database gates in
  // their handlers instead of using the ordinary shared-admission path. They
  // still need a dispatch-time token to reject an old-population request after
  // it eventually wins and drains its exclusive cut.
  if (!request.replication_origin_ && request.spec_ != nullptr &&
      ((request.spec_->flags_ & kCmdUsesDbGate) != 0 ||
       request.kind_ == CommandKind::kFlushDb ||
       request.kind_ == CommandKind::kFlushAll ||
       request.kind_ == CommandKind::kKeys ||
       request.kind_ == CommandKind::kSave ||
       request.kind_ == CommandKind::kBgSave)) {
    request.serving_generation_ =
        g_replication != nullptr ? g_replication->CaptureServingGeneration()
                                 : 0;
    request.serving_generation_valid_ = true;
  }
  const CommandKind kind = request.kind_;
  const bool may_advance_replication_watermark = [&] {
    if (request.replication_origin_ || g_replication == nullptr ||
        g_replication->is_replica()) {
      return false;
    }
    if (kind == CommandKind::kExec && ctx.in_multi_) {
      return std::any_of(ctx.queued_.begin(), ctx.queued_.end(),
                         ExecCommandMayReplicate);
    }
    // A write queued by MULTI has not executed and must not affect WAIT until
    // EXEC succeeds. Other immediate commands can use the shared replication
    // classification, including dynamic Lua writes and PUBLISH.
    return !ctx.in_multi_ && ExecCommandMayReplicate(request);
  }();
  // Plain SET cannot make list, sorted-set, or stream waiters ready. Avoid an
  // atomic cascade object and its completion check on this high-volume path;
  // commands with broader effects retain the conservative cascade lifetime.
  std::optional<BlockingWakeCascade> cascade;
  if (kind != CommandKind::kSet) cascade.emplace();
  BlockingWakeCascade* previous_cascade = ctx.blocking_wake_cascade_;
  ctx.blocking_wake_cascade_ =
      cascade.has_value() ? &*cascade : previous_cascade;
  request.blocking_wake_cascade_ = ctx.blocking_wake_cascade_;
  struct RestoreCascade {
    ConnectionContext& ctx_;
    BlockingWakeCascade* previous_ = nullptr;
    ~RestoreCascade() { ctx_.blocking_wake_cascade_ = previous_; }
  } restore{ctx, previous_cascade};
  request.resp_version_ = ctx.resp_version();
  const bool may_block =
      request.spec_ != nullptr && (request.spec_->flags_ & kCmdMayBlock) != 0;
  const std::uint64_t started = bycorf::ReadCycleCounter();
  CommandReply reply =
      co_await DispatchCommandImpl(ctx, request, reply_builder);
  if (cascade.has_value() && !cascade->empty()) {
    (void)co_await DrainBlockingWakeCascade(*cascade);
  }
  if (may_advance_replication_watermark && !reply.encoded_.empty() &&
      reply.encoded_.front() != '-') {
    ctx.native_replication_watermark_dirty_ = true;
  }
  const std::uint64_t elapsed_ticks = bycorf::ReadCycleCounter() - started;
  RecordCommandMetric(kind, elapsed_ticks);
  MaybeRecordSlowCommand(request.args_, ctx.peer_address_, ctx.client_name_,
                         elapsed_ticks, may_block);
  co_return reply;
}

Task<absl::Status> ReleaseConnectionWatches(ConnectionContext& ctx) {
  return DropWatches(ctx);
}

Task<CommandReply> ExecuteCommandBody(
    const CommandRequest& request, ReplyBuilder& reply_builder,
    std::uint64_t client_id, ConnectionContext* connection,
    ReplicationTransactionOrderGuard* preacquired_order = nullptr,
    std::optional<bool> precomputed_spans_multiple_shards = std::nullopt) {
  const bool replication_origin = request.replication_origin_;
  const auto& args = request.args_;
  const std::uint32_t cmd_flags =
      request.spec_ != nullptr ? request.spec_->flags_ : 0u;
  const bool reject_writes = g_replication != nullptr
                                 ? g_replication->reject_writes()
                                 : g_replica_read_only;
  if (!replication_origin && reject_writes && (cmd_flags & kCmdWrite) != 0) {
    co_return BuiltReply(reply_builder.AppendError(
        "READONLY You can't write against a read only replica."));
  }
  if (!replication_origin && MayGrowMemory(request) && RejectForMemory(0)) {
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
  std::optional<DbOperationGuard> db_guard;
  if (uses_db && !manages_own_db_gate) {
    LAVIK_FAULT_INJECT(if (!replication_origin &&
                           (cmd_flags & (kCmdWrite | kCmdDynamicWrite)) != 0) {
      absl::Status paused = co_await MaybePauseBeforeCommandDbAdmission();
      if (!paused.ok()) {
        co_return BuiltReply(reply_builder.AppendError(
            absl::StrCat("ERR database admission failed: ", paused.message())));
      }
    });
    while (!TryBeginDbOperation(request.db_id_)) {
      absl::Status waited = co_await bycorf::SleepFor(
          *ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) {
        co_return BuiltReply(reply_builder.AppendError(
            absl::StrCat("ERR database admission failed: ", waited.message())));
      }
    }
    db_guard.emplace(request.db_id_);
    if (!CommandWriteAdmissionIsCurrent(request)) {
      co_return BuiltReply(reply_builder.AppendError(
          "TRYAGAIN replication role changed; retry command"));
    }
    if (const char* error = CommandServingGenerationError(request);
        error != nullptr) [[unlikely]] {
      co_return BuiltReply(reply_builder.AppendError(error));
    }
  }

  // Take the DB admission before the transaction gates. FULLSYNC_CUT closes
  // the transaction gates first and then the DB gates, so commands admitted
  // after either close suspend without holding a resource that the cut is
  // trying to drain. This is backpressure, not a transient client error.
  const bool snapshot_transaction =
      !replication_origin && g_replication != nullptr && g_storage != nullptr &&
      g_storage->ReplicationLogActive() && (cmd_flags & kCmdMultiShard) != 0 &&
      (cmd_flags & (kCmdWrite | kCmdDynamicWrite)) != 0 &&
      (cmd_flags & kCmdMayBlock) == 0;
  // The global order gate keeps cross-flow transactions in one source order
  // so overlapping flow subsets cannot rendezvous into an arrival/ACK cycle
  // on the replica; it only needs to cover the window until every
  // participant marker is queued (ExecuteMultiKey releases it from the
  // participants-entered hook). A request whose participants all land on one
  // shard publishes at most a single-flow envelope, and a single-flow marker
  // never joins a cross-flow rendezvous cycle, so it may skip the gate --
  // the same argument the one-key MSET fast path below already relies on.
  // FLUSH still holds the gate and publishes an all-flow control barrier; a
  // skipped single-shard write waits on no other flow, so it cannot form a
  // wait cycle with that barrier. The snapshot gate below is intentionally
  // unchanged: FULLSYNC_CUT close/drain semantics are the snapshot gate's
  // job, not this gate's.
  ReplicationTransactionOrderGuard local_replication_order;
  ReplicationTransactionOrderGuard* replication_order =
      preacquired_order != nullptr ? preacquired_order
                                   : &local_replication_order;
  // MSET must decide whether to preacquire the order gate before publisher
  // admission. Reuse that decision here instead of parsing and hashing its
  // complete key view a second time. Other command paths still decide lazily,
  // and the snapshot_transaction short circuit keeps standalone writes free
  // of this admission work.
  const bool spans_multiple_shards =
      snapshot_transaction && (precomputed_spans_multiple_shards.has_value()
                                   ? *precomputed_spans_multiple_shards
                                   : RequestSpansMultipleShards(request));
  if (spans_multiple_shards) {
    if (!replication_order->active()) {
      absl::Status entered =
          co_await BeginReplicationTransactionOrder(replication_order);
      if (!entered.ok()) {
        co_return BuiltReply(
            reply_builder.AppendError(absl::StrCat("ERR ", entered.message())));
      }
    }
  }
  SnapshotTransactionOperationGuard snapshot_transaction_guard;
  if (snapshot_transaction) {
    absl::Status entered =
        co_await BeginSnapshotTransaction(&snapshot_transaction_guard);
    if (!entered.ok()) {
      co_return BuiltReply(
          reply_builder.AppendError(absl::StrCat("ERR ", entered.message())));
    }
  }

  // Cluster owner-side authority re-check (choke point 1 of 2): the
  // admission decision was made at dispatch time against a snapshot that a
  // Meta transition may have fenced while this request suspended on the
  // admissions above. Nothing has executed yet, so a changed authority is
  // safely answered
  // with a fresh redirect. Transaction-based writes re-check per shard via the
  // tx validator hook (choke point 2). Blocking writes deliberately skip this
  // command-lifetime guard: their per-attempt path registers only while it is
  // touching storage, so an unbounded dormant wait cannot pin an old
  // assignment through a Meta fence.
  cluster::AuthorityInFlightGuards cluster_in_flights;
  if ((cmd_flags & kCmdMayBlock) == 0) {
    if (std::optional<CommandReply> fenced = RecheckClusterWriteAuthority(
            request, reply_builder, &cluster_in_flights);
        fenced.has_value()) {
      co_return std::move(*fenced);
    }
  }

  if (random_stream.has_value()) {
    co_return co_await ExecuteNegativeRandomStream(request, *random_stream,
                                                   reply_builder);
  }

  switch (request.kind_) {
    case CommandKind::kEval:
    case CommandKind::kEvalSha:
    case CommandKind::kEvalRo:
    case CommandKind::kEvalShaRo:
    case CommandKind::kFCall:
    case CommandKind::kFCallRo:
      co_return co_await ExecuteEval(request, reply_builder, connection);

    case CommandKind::kScript:
      co_return co_await ExecuteScript(request, reply_builder);

    case CommandKind::kFunction:
      co_return co_await ExecuteFunction(request, reply_builder);

    case CommandKind::kReplicaOf:
      co_return co_await ExecuteReplicaOf(request, reply_builder);

    case CommandKind::kAddReplicaOf:
      co_return co_await ExecuteAddReplicaOf(request, reply_builder);

    case CommandKind::kConfig:
      co_return co_await ExecuteConfig(request, reply_builder);

    case CommandKind::kSlowLog:
      co_return co_await ExecuteSlowLog(request, reply_builder);

    case CommandKind::kInfo:
      co_return co_await ExecuteInfo(request, reply_builder);

    case CommandKind::kRole:
      co_return co_await ExecuteRole(reply_builder);

    case CommandKind::kCluster:
      co_return co_await ExecuteCluster(request, reply_builder);

    case CommandKind::kCommand:
      co_return co_await ExecuteCommandIntrospection(request, reply_builder);

    case CommandKind::kKeys:
      co_return co_await ExecuteKeys(request, reply_builder);

    case CommandKind::kSave:
    case CommandKind::kBgSave:
    case CommandKind::kLastSave:
      co_return co_await ExecuteRdbBackupCommand(request, reply_builder);

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

    case CommandKind::kSort:
    case CommandKind::kSortRo:
      co_return co_await ExecuteSortCommand(request, reply_builder);

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
    case CommandKind::kMGet:
      co_return co_await ExecuteMultiKey(request, reply_builder);

    case CommandKind::kMSet:
      co_return co_await ExecuteMultiKey(request, reply_builder,
                                         replication_order);

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
      co_return co_await ExecuteBlockingListCommand(request, reply_builder,
                                                    client_id);

    case CommandKind::kBZMPop:
    case CommandKind::kBZPopMax:
    case CommandKind::kBZPopMin:
      co_return co_await ExecuteBlockingZSetCommand(request, reply_builder,
                                                    client_id);

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
      co_return co_await ExecuteStreamCommand(request, reply_builder,
                                              client_id);

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
    case CommandKind::kHReplace:
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
    case CommandKind::kDump:
    case CommandKind::kRestore:
    case CommandKind::kType:
      if (args.size() >= 2) {
        // Single-key source writes are already on this computed owner, while
        // cluster reads learned the same route during admission. Deriving the
        // worker from the retained slot is cheaper than hashing args[1] again.
        const bool routed = request.HasRoutedPartitionFor(1);
        const unsigned target =
            routed ? request.RoutedPartitionId() % g_storage->worker_count()
                   : ShardForKey(args[1]);
#if LAVIK_ENABLE_READ_LATENCY_TRACE
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
#if LAVIK_ENABLE_SET_LATENCY_TRACE
        if (request.kind_ == CommandKind::kSet) {
          // A source SET is already on the key owner by the time it gets
          // here, so remote_ reads false. Read route-out as unmeasured, not
          // as "no hop".
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

namespace {

// Publisher admission reserves worker-local state on the worker that appends
// this write to its replication log, which is the key owner -- the same worker
// the command body dispatches to. Resolving that owner out here leaves
// admission, body, and release on their existing "already on the target"
// inline paths, so the write pays one cross-core round trip instead of three.
std::optional<unsigned> SingleKeyWriteOwner(CommandRequest& request) {
  if (request.spec_ == nullptr || g_storage == nullptr) {
    return std::nullopt;
  }
  // Function-library mutations are process-global. Route every standalone
  // FUNCTION command through worker zero so mutation and replication order
  // are identical even when clients are accepted by different workers.
  if (request.kind_ == CommandKind::kFunction) return 0;
  if (request.replication_origin_) return std::nullopt;
  if ((request.spec_->flags_ & kCmdWrite) == 0 ||
      (request.spec_->flags_ & (kCmdGlobal | kCmdMultiShard)) != 0) {
    return std::nullopt;
  }
  // A refused write is answered where it arrived. A read-only replica should
  // not spend cross-core round trips producing READONLY errors.
  if (g_replication != nullptr ? g_replication->reject_writes()
                               : g_replica_read_only) {
    return std::nullopt;
  }
  // PopulateClusterSlots caches a route only after DetermineKeys proves the
  // request has exactly one key. For a static key spec, matching the cached
  // argument is therefore sufficient proof here and avoids repeating arity
  // and key-range resolution on every cluster write.
  if (request.HasRoutedPartitionFor(request.spec_->first_key_)) {
    return request.RoutedPartitionId() % g_storage->worker_count();
  }
  const absl::StatusOr<KeyIndexView> keys =
      DetermineKeys(*request.spec_, request.args_);
  // count() == 1 rather than !empty(): XGROUP HELP resolves to no key at all
  // and must keep the all-worker admission fan-out.
  if (!keys.ok() || keys->count() != 1) {
    request.ClearRoutedPartition();
    return std::nullopt;
  }
  // Cluster admission already computes the exact single-key route for both
  // reads and writes. Preserve that route when this helper rejects a read,
  // and reuse it for a write, so owner dispatch and storage lookup do not hash
  // the same key again. Rewritten requests are protected by the argument-index
  // identity check and recompute below.
  const std::uint16_t partition_id =
      request.HasRoutedPartitionFor(keys->first_)
          ? request.RoutedPartitionId()
          : storage::RedisSlot(request.args_[keys->first_]);
  request.SetRoutedPartition(partition_id, keys->first_);
  return partition_id % g_storage->worker_count();
}

// The acquire must stay ahead of the DB gate that ExecuteCommandBody takes.
// Admission suspends on the publish-queue capacity of the worker it runs on,
// and holding that same worker's DB gate across the wait would stall every
// FLUSHDB and FULLSYNC_CUT drain waiting for the gate counts to reach zero.
Task<CommandReply> ExecuteAdmittedWriteCommand(CommandRequest& request,
                                               ReplyBuilder& reply_builder,
                                               std::uint64_t client_id,
                                               ConnectionContext* connection) {
  if (ReplicationEventExceedsBacklog(ReplicationEventAdmissionBytes(request))) {
    co_return BuiltReply(reply_builder.AppendError(
        "ERR replication publisher admission failed: canonical event exceeds "
        "repl-backlog-size or the 1 GiB event limit"));
  }
  // Preacquire the order gate across publisher admission so a suspending
  // admission cannot invert the order of two wide MSETs. Single-shard MSETs
  // skip the gate for the same reason ExecuteCommandBody skips it: one
  // participant flow cannot join a cross-flow rendezvous cycle.
  std::optional<bool> mset_spans_multiple_shards;
  if (request.kind_ == CommandKind::kMSet && g_replication != nullptr) {
    mset_spans_multiple_shards = RequestSpansMultipleShards(request);
  }
  const bool ordered_mset = mset_spans_multiple_shards.value_or(false);
  ReplicationTransactionOrderGuard replication_order;
  if (ordered_mset) {
    absl::Status entered =
        co_await BeginReplicationTransactionOrder(&replication_order);
    if (!entered.ok()) {
      co_return BuiltReply(
          reply_builder.AppendError(absl::StrCat("ERR ", entered.message())));
    }
  }
  auto admission = co_await AcquireReplicationPublisherAdmission(
      RequestArgumentBytes(request), &request, ordered_mset);
  if (!admission.ok()) {
    if (absl::IsResourceExhausted(admission.status())) {
      co_return BuiltReply(AppendOomError(reply_builder));
    }
    co_return BuiltReply(reply_builder.AppendError(
        absl::StrCat("ERR replication publisher admission failed: ",
                     admission.status().message())));
  }
  ActivePublisherAdmissionGuard active_admission(&*admission);
  CommandReply reply = co_await ExecuteCommandBody(
      request, reply_builder, client_id, connection,
      ordered_mset ? &replication_order : nullptr, mset_spans_multiple_shards);
  absl::Status released =
      co_await ReleaseReplicationPublisherAdmission(*admission);
  if (!released.ok()) {
    reply = BuiltReply(reply_builder.AppendError(
        absl::StrCat("ERR replication publisher admission release failed: ",
                     released.message())));
  }
  co_return FinalizeClusterMutationReply(request, reply_builder,
                                         std::move(reply));
}

Task<CommandReply> ExecuteClusterFinalizedCommand(
    CommandRequest& request, ReplyBuilder& reply_builder,
    std::uint64_t client_id, ConnectionContext* connection) {
  CommandReply reply = co_await ExecuteCommandBody(request, reply_builder,
                                                   client_id, connection);
  co_return FinalizeClusterMutationReply(request, reply_builder,
                                         std::move(reply));
}

Task<CommandReply> ExecuteAdmittedCommand(CommandRequest& request,
                                          ReplyBuilder& reply_builder,
                                          std::uint64_t client_id,
                                          ConnectionContext* connection) {
  const bool source_write =
      !request.replication_origin_ && request.spec_ != nullptr &&
      (request.spec_->flags_ & (kCmdWrite | kCmdDynamicWrite)) != 0 &&
      g_storage != nullptr && g_storage->ReplicationLogActive();
  // Keep this wrapper non-coroutine. Reads, standalone requests, and
  // replica-applied writes need neither publisher admission nor final storage
  // outcome reconciliation, so they avoid a second coroutine frame.
  if (!source_write) [[likely]] {
    if (request.cluster_authority_admission_ != nullptr &&
        ClusterRequestIsWrite(request) && !request.replication_origin_) {
      return ExecuteClusterFinalizedCommand(request, reply_builder, client_id,
                                            connection);
    }
    return ExecuteCommandBody(request, reply_builder, client_id, connection);
  }
  return ExecuteAdmittedWriteCommand(request, reply_builder, client_id,
                                     connection);
}

}  // namespace

bool CommandWriteAdmissionIsCurrent(const CommandRequest& request) noexcept {
  return !request.write_admission_role_epoch_valid_ ||
         g_replication == nullptr ||
         g_replication->role_epoch() == request.write_admission_role_epoch_;
}

namespace {

Task<CommandReply> ExecuteCommandOnOwner(unsigned owner,
                                         CommandRequest& request,
                                         ReplyBuilder& reply_builder,
                                         std::uint64_t client_id,
                                         ConnectionContext* connection) {
  co_return co_await SubmitTaskTo(owner,
                                  [&request, &reply_builder, client_id,
                                   connection]() -> Task<CommandReply> {
                                    co_return co_await ExecuteAdmittedCommand(
                                        request, reply_builder, client_id,
                                        connection);
                                  });
}

}  // namespace

Task<CommandReply> ExecuteCommand(CommandRequest& request,
                                  ReplyBuilder& reply_builder,
                                  std::uint64_t client_id,
                                  ConnectionContext* connection) {
  if (!request.replication_origin_ && request.spec_ != nullptr &&
      (request.spec_->flags_ & (kCmdWrite | kCmdDynamicWrite)) != 0 &&
      g_replication != nullptr) {
    request.write_admission_role_epoch_ = g_replication->role_epoch();
    request.write_admission_role_epoch_valid_ = true;
  }
  const std::optional<unsigned> owner = SingleKeyWriteOwner(request);
  // Lua invocations carry a non-owning pointer to their connection's WAIT
  // watermark. Dynamic-write scripting commands never take the single-key
  // owner fast path, so that connection-owned state stays on its worker.
  assert(connection == nullptr || !IsLuaInvocationCommand(request) ||
         !owner.has_value());
  if (owner.has_value() && *owner != ThisWorker().id_) {
    return ExecuteCommandOnOwner(*owner, request, reply_builder, client_id,
                                 connection);
  }
  // This wrapper deliberately remains non-coroutine. The common local path
  // has no state that must survive suspension, while ExecuteCommandOnOwner
  // owns the cross-worker state for the uncommon routed-write path.
  return ExecuteAdmittedCommand(request, reply_builder, client_id, connection);
}

Task<absl::Status> ApplyReplicatedExec(const std::vector<std::string>& args) {
  if (args.size() < 2 || args[0] != kReplicatedExecCommand) {
    co_return absl::InvalidArgumentError("malformed replicated EXEC");
  }
  auto parse_size = [](std::string_view text, std::uint64_t* output) noexcept {
    const char* begin = text.data();
    const char* end = begin + text.size();
    const auto parsed = std::from_chars(begin, end, *output);
    return parsed.ec == std::errc{} && parsed.ptr == end;
  };
  std::uint64_t command_count = 0;
  if (!parse_size(args[1], &command_count) || command_count > args.size() - 2) {
    co_return absl::InvalidArgumentError("invalid replicated EXEC count");
  }
  ConnectionContext context;
  context.strict_replication_apply_ = true;
  context.queued_.reserve(static_cast<std::size_t>(command_count));
  std::size_t offset = 2;
  for (std::uint64_t index = 0; index < command_count; ++index) {
    if (offset + 2 > args.size()) {
      co_return absl::InvalidArgumentError("truncated replicated EXEC");
    }
    std::uint64_t db_id = 0;
    std::uint64_t argc = 0;
    if (!parse_size(args[offset++], &db_id) ||
        db_id >= storage::kLogicalDatabaseCount ||
        !parse_size(args[offset++], &argc) || argc == 0 ||
        argc > args.size() - offset) {
      co_return absl::InvalidArgumentError("invalid replicated EXEC command");
    }
    RespCommand wire;
    wire.args_.insert(wire.args_.end(), args.begin() + offset,
                      args.begin() + offset + static_cast<std::size_t>(argc));
    offset += static_cast<std::size_t>(argc);
    auto request =
        BuildCommandRequest(std::move(wire), static_cast<std::uint8_t>(db_id));
    if (!request.ok()) co_return request.status();
    if (request->spec_ == nullptr ||
        (request->spec_->flags_ & kCmdGlobal) != 0 ||
        request->kind_ == CommandKind::kMulti ||
        request->kind_ == CommandKind::kExec ||
        request->kind_ == CommandKind::kDiscard ||
        request->kind_ == CommandKind::kWatch ||
        request->kind_ == CommandKind::kUnwatch) {
      co_return absl::InvalidArgumentError(
          "unsupported command in replicated EXEC");
    }
    request->replication_origin_ = true;
    context.queued_.push_back(std::move(*request));
  }
  if (offset != args.size()) {
    co_return absl::InvalidArgumentError("trailing replicated EXEC data");
  }
  ReplyBuilder reply_builder;
  CommandReply reply = co_await ExecuteExec(context, reply_builder);
  if (!reply.encoded_.empty() && reply.encoded_.front() == '-') {
    co_return absl::FailedPreconditionError(std::string(reply.encoded_));
  }
  co_return absl::OkStatus();
}

Task<absl::Status> ApplyReplicatedCommand(const ReplicatedCommand& command) {
  if (!command.args_.empty() && command.args_[0] == kReplicatedExecCommand) {
    co_return co_await ApplyReplicatedExec(command.args_);
  }
  if (command.args_.empty()) {
    co_return absl::InvalidArgumentError("empty replicated command");
  }
  const bool flush_db = command.args_[0] == "FLUSHDB";
  const bool flush_all = command.args_[0] == "FLUSHALL";
  if (flush_db || flush_all) {
    const std::size_t expected_args =
        flush_db ? 3 : 2 + storage::kLogicalDatabaseCount;
    if (command.args_.size() != expected_args ||
        command.db_id_ >= storage::kLogicalDatabaseCount) {
      co_return absl::InvalidArgumentError(
          "malformed replicated database barrier");
    }
    auto parse_nonzero = [](std::string_view text,
                            std::uint64_t* value) noexcept {
      const char* begin = text.data();
      const char* end = begin + text.size();
      const auto parsed = std::from_chars(begin, end, *value);
      return parsed.ec == std::errc{} && parsed.ptr == end && *value != 0;
    };
    std::uint64_t barrier_id = 0;
    if (!parse_nonzero(command.args_[1], &barrier_id)) {
      co_return absl::InvalidArgumentError(
          "invalid replicated database barrier identity");
    }
    std::array<std::uint64_t, storage::kLogicalDatabaseCount> epochs{};
    if (flush_db) {
      if (!parse_nonzero(command.args_[2], &epochs[command.db_id_])) {
        co_return absl::InvalidArgumentError(
            "invalid replicated FLUSHDB epoch");
      }
    } else {
      for (std::uint8_t db_id = 0; db_id < storage::kLogicalDatabaseCount;
           ++db_id) {
        if (!parse_nonzero(command.args_[2 + db_id], &epochs[db_id])) {
          co_return absl::InvalidArgumentError(
              "invalid replicated FLUSHALL epoch vector");
        }
      }
    }

    while (!CloseAllCommandDbGates()) {
      absl::Status waited = co_await bycorf::SleepFor(
          *ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) co_return waited;
    }
    struct ReplicatedFlushGateGuard {
      ~ReplicatedFlushGateGuard() { OpenAllCommandDbGates(); }
    } reopen;
    while (CommandDbOperationsActive()) {
      absl::Status waited = co_await bycorf::SleepFor(
          *ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) co_return waited;
    }
    if (flush_db) {
      co_return co_await g_storage->ApplyReplicatedFlushDb(
          command.db_id_, epochs[command.db_id_]);
    }
    co_return co_await g_storage->ApplyReplicatedFlushAll(epochs);
  }

  RespCommand wire{.args_ = command.args_};
  auto request = BuildCommandRequest(std::move(wire), command.db_id_);
  if (!request.ok()) co_return request.status();
  request->replication_origin_ = true;
  if (request->kind_ == CommandKind::kPublish) {
    (void)co_await PublishChannel(request->args_[1], request->args_[2]);
    co_return absl::OkStatus();
  }
  bool replayable_write = request->kind_ == CommandKind::kFunction;
  if (request->spec_ != nullptr && (request->spec_->flags_ & kCmdWrite) != 0 &&
      (request->spec_->flags_ & (kCmdGlobal | kCmdMayBlock)) == 0) {
    auto keys = DetermineKeys(*request->spec_, request->args_);
    replayable_write = keys.ok() && keys->count() != 0;
  }
  if (!replayable_write) {
    co_return absl::Status(
        absl::StatusCode::kInvalidArgument,
        "replication command is not a replayable write or database barrier");
  }

  ReplyBuilder reply_builder;
  CommandReply reply = co_await ExecuteCommand(*request, reply_builder);
  if (reply.disk_value_.valid() || reply.chunks_) {
    co_return absl::Status(absl::StatusCode::kInternal,
                           "replication write produced a streamed reply");
  }
  if (!reply.encoded_.empty() && reply.encoded_.front() == '-') {
    co_return absl::Status(absl::StatusCode::kFailedPrecondition,
                           std::string(reply.encoded_));
  }
  co_return absl::OkStatus();
}

Task<absl::Status> ApplyRedisReplicatedCommand(
    const ReplicatedCommand& command) {
  if (command.args_.empty()) {
    co_return absl::InvalidArgumentError("empty Redis replication command");
  }
  RespCommand wire{.args_ = command.args_};
  auto request = BuildCommandRequest(std::move(wire), command.db_id_);
  if (!request.ok()) co_return request.status();
  request->replication_origin_ = true;

  // Redis includes PUBLISH in its replication stream even though it does not
  // mutate the keyspace. Deliver it locally without forwarding it again.
  if (request->kind_ == CommandKind::kPublish) {
    (void)co_await PublishChannel(request->args_[1], request->args_[2]);
    co_return absl::OkStatus();
  }

  bool replayable = request->kind_ == CommandKind::kFunction;
  if (request->spec_ != nullptr && (request->spec_->flags_ & kCmdWrite) != 0 &&
      (request->spec_->flags_ & kCmdMayBlock) == 0) {
    if (request->kind_ == CommandKind::kFlushDb ||
        request->kind_ == CommandKind::kFlushAll) {
      replayable = true;
    } else if ((request->spec_->flags_ & kCmdGlobal) == 0) {
      auto keys = DetermineKeys(*request->spec_, request->args_);
      replayable = keys.ok() && keys->count() != 0;
    }
  }
  if (!replayable) {
    co_return absl::InvalidArgumentError(
        "Redis replication command is not a replayable write");
  }

  ReplyBuilder reply_builder;
  CommandReply reply = co_await ExecuteCommand(*request, reply_builder);
  if (reply.disk_value_.valid() || reply.chunks_) {
    co_return absl::InternalError(
        "Redis replication write produced a streamed reply");
  }
  if (!reply.encoded_.empty() && reply.encoded_.front() == '-') {
    co_return absl::FailedPreconditionError(std::string(reply.encoded_));
  }
  co_return absl::OkStatus();
}

Task<absl::Status> ApplyRedisReplicatedTransaction(
    std::span<const ReplicatedCommand> commands) {
  if (commands.empty()) co_return absl::OkStatus();
  ConnectionContext context;
  context.strict_replication_apply_ = true;
  context.queued_.reserve(commands.size());
  for (const ReplicatedCommand& command : commands) {
    if (command.args_.empty() ||
        command.db_id_ >= storage::kLogicalDatabaseCount) {
      co_return absl::InvalidArgumentError(
          "malformed Redis replicated transaction command");
    }
    RespCommand wire{.args_ = command.args_};
    auto request = BuildCommandRequest(std::move(wire), command.db_id_);
    if (!request.ok()) co_return request.status();
    const bool function_mutation = request->kind_ == CommandKind::kFunction;
    if (request->spec_ == nullptr ||
        (!function_mutation &&
         (request->spec_->flags_ & (kCmdGlobal | kCmdMayBlock)) != 0) ||
        (!function_mutation &&
         (request->spec_->flags_ & (kCmdWrite | kCmdMayReplicate)) == 0) ||
        request->kind_ == CommandKind::kMulti ||
        request->kind_ == CommandKind::kExec ||
        request->kind_ == CommandKind::kDiscard ||
        request->kind_ == CommandKind::kWatch ||
        request->kind_ == CommandKind::kUnwatch ||
        request->kind_ == CommandKind::kSelect) {
      co_return absl::InvalidArgumentError(
          "unsupported command in Redis replicated transaction");
    }
    if ((request->spec_->flags_ & kCmdWrite) != 0) {
      auto keys = DetermineKeys(*request->spec_, request->args_);
      if (!keys.ok() || keys->count() == 0) {
        co_return absl::InvalidArgumentError(
            "Redis transaction command has no replayable key");
      }
    }
    request->replication_origin_ = true;
    context.queued_.push_back(std::move(*request));
  }

  ReplyBuilder reply_builder;
  CommandReply reply = co_await ExecuteExec(context, reply_builder);
  if (reply.disk_value_.valid() || reply.chunks_) {
    co_return absl::InternalError(
        "Redis replicated transaction produced a streamed reply");
  }
  if (!reply.encoded_.empty() && reply.encoded_.front() == '-') {
    co_return absl::FailedPreconditionError(std::string(reply.encoded_));
  }
  co_return absl::OkStatus();
}

}  // namespace lavik
