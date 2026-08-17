#include "keylane/metrics.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "absl/strings/str_cat.h"
#include "celer/net/http_service.h"
#include "celer/runtime/cross_core.h"
#include "celer/runtime/cycle_clock.h"
#include "celer/runtime/worker.h"
#include "keylane/memory.h"
#include "keylane/storage/engine.h"

namespace keylane {
namespace {

constexpr std::size_t ToIndex(CommandKind kind) noexcept {
  return static_cast<std::size_t>(kind);
}

// Written only by the owning worker. Alignment and the size assertion keep
// adjacent workers from sharing a cache line.
struct alignas(64) WorkerMetricsShard {
  std::array<CommandMetricTotals, kCommandKindCount> commands_{};
  std::uint64_t connected_clients_ = 0;
  std::uint64_t defrag_successes_ = 0;
  std::uint64_t defrag_resource_exhausted_ = 0;
  std::uint64_t defrag_failures_ = 0;
  std::uint64_t active_defrags_ = 0;
  std::uint64_t pending_defrags_ = 0;
};

static_assert(alignof(WorkerMetricsShard) == 64);
static_assert(sizeof(WorkerMetricsShard) % 64 == 0);

std::unique_ptr<WorkerMetricsShard[]> g_worker_metrics;
unsigned g_worker_metrics_count = 0;
std::array<std::uint64_t, kCommandLatencyBucketUpperUs.size()>
    g_latency_bucket_upper_ticks{};
double g_counter_frequency = 1.0;

}  // namespace

std::uint64_t WorkerMetricsSnapshot::TotalCalls() const noexcept {
  std::uint64_t total = 0;
  for (const CommandMetricTotals& command : commands_) {
    total += command.calls_;
  }
  return total;
}

void InitWorkerMetrics(unsigned worker_count) {
  g_worker_metrics = std::make_unique<WorkerMetricsShard[]>(worker_count);
  g_worker_metrics_count = worker_count;
  g_counter_frequency = std::max(1.0, celer::CycleCounterFrequency());
  for (std::size_t i = 0; i < kCommandLatencyBucketUpperUs.size(); ++i) {
    const long double ticks =
        static_cast<long double>(g_counter_frequency) *
        static_cast<long double>(kCommandLatencyBucketUpperUs[i]) /
        1'000'000.0L;
    g_latency_bucket_upper_ticks[i] =
        std::max<std::uint64_t>(1, static_cast<std::uint64_t>(ticks + 0.999L));
  }
}

void RecordConnectionOpened() noexcept {
  ++g_worker_metrics[celer::ThisWorker().id_].connected_clients_;
}

void RecordConnectionClosed() noexcept {
  --g_worker_metrics[celer::ThisWorker().id_].connected_clients_;
}

void RecordDefragMetric(DefragMetricResult result) noexcept {
  WorkerMetricsShard& shard = g_worker_metrics[celer::ThisWorker().id_];
  switch (result) {
    case DefragMetricResult::kSuccess:
      ++shard.defrag_successes_;
      break;
    case DefragMetricResult::kResourceExhausted:
      ++shard.defrag_resource_exhausted_;
      break;
    case DefragMetricResult::kFailure:
      ++shard.defrag_failures_;
      break;
  }
}

void SetDefragActive(bool active) noexcept {
  g_worker_metrics[celer::ThisWorker().id_].active_defrags_ = active;
}

void SetDefragPending(bool pending) noexcept {
  g_worker_metrics[celer::ThisWorker().id_].pending_defrags_ = pending;
}

void RecordCommandMetric(CommandKind kind,
                         std::uint64_t elapsed_ticks) noexcept {
  const unsigned worker = celer::ThisWorker().id_;
  std::size_t command_index = ToIndex(kind);
  if (worker >= g_worker_metrics_count) [[unlikely]] {
    return;
  }
  if (command_index >= kCommandKindCount) [[unlikely]] {
    command_index = ToIndex(CommandKind::kUnknown);
  }
  CommandMetricTotals& metric =
      g_worker_metrics[worker].commands_[command_index];
  ++metric.calls_;
  metric.latency_ticks_ += elapsed_ticks;
  const auto bucket =
      std::lower_bound(g_latency_bucket_upper_ticks.begin(),
                       g_latency_bucket_upper_ticks.end(), elapsed_ticks);
  ++metric.latency_bins_[static_cast<std::size_t>(
      bucket - g_latency_bucket_upper_ticks.begin())];
}

celer::Task<WorkerMetricsSnapshot> CollectWorkerMetrics() {
  WorkerMetricsSnapshot result;
  result.counter_frequency_ = g_counter_frequency;
  for (unsigned worker = 0; worker < g_worker_metrics_count; ++worker) {
    // Copy on the owner rather than reading its live cache lines remotely.
    const auto [shard, connections] =
        co_await celer::SubmitTo(worker, [worker] {
          return std::pair{g_worker_metrics[worker],
                           celer::ThisWorker().self_->ActiveConnectionCount()};
        });
    result.connections_ += connections;
    result.connected_clients_ += shard.connected_clients_;
    result.defrag_successes_ += shard.defrag_successes_;
    result.defrag_resource_exhausted_ += shard.defrag_resource_exhausted_;
    result.defrag_failures_ += shard.defrag_failures_;
    result.active_defrags_ += shard.active_defrags_;
    result.pending_defrags_ += shard.pending_defrags_;
    for (std::size_t command = 0; command < kCommandKindCount; ++command) {
      CommandMetricTotals& destination = result.commands_[command];
      const CommandMetricTotals& source = shard.commands_[command];
      destination.calls_ += source.calls_;
      destination.latency_ticks_ += source.latency_ticks_;
      for (std::size_t bucket = 0; bucket < source.latency_bins_.size();
           ++bucket) {
        destination.latency_bins_[bucket] += source.latency_bins_[bucket];
      }
    }
  }
  co_return result;
}

std::string_view CommandMetricName(CommandKind kind) noexcept {
  switch (kind) {
    case CommandKind::kPing:
      return "ping";
    case CommandKind::kEcho:
      return "echo";
    case CommandKind::kDbSize:
      return "dbsize";
    case CommandKind::kDel:
      return "del";
    case CommandKind::kUnlink:
      return "unlink";
    case CommandKind::kRename:
      return "rename";
    case CommandKind::kRenameNx:
      return "renamenx";
    case CommandKind::kCopy:
      return "copy";
    case CommandKind::kExists:
      return "exists";
    case CommandKind::kTouch:
      return "touch";
    case CommandKind::kRandomKey:
      return "randomkey";
    case CommandKind::kFlushDb:
      return "flushdb";
    case CommandKind::kFlushAll:
      return "flushall";
    case CommandKind::kGet:
      return "get";
    case CommandKind::kGetDel:
      return "getdel";
    case CommandKind::kGetEx:
      return "getex";
    case CommandKind::kGetRange:
      return "getrange";
    case CommandKind::kGetSet:
      return "getset";
    case CommandKind::kAppend:
      return "append";
    case CommandKind::kStrlen:
      return "strlen";
    case CommandKind::kIncr:
      return "incr";
    case CommandKind::kIncrBy:
      return "incrby";
    case CommandKind::kIncrByFloat:
      return "incrbyfloat";
    case CommandKind::kDecr:
      return "decr";
    case CommandKind::kDecrBy:
      return "decrby";
    case CommandKind::kSetEx:
      return "setex";
    case CommandKind::kPSetEx:
      return "psetex";
    case CommandKind::kSetNx:
      return "setnx";
    case CommandKind::kSetRange:
      return "setrange";
    case CommandKind::kSubstr:
      return "substr";
    case CommandKind::kLcs:
      return "lcs";
    case CommandKind::kExpire:
      return "expire";
    case CommandKind::kPExpire:
      return "pexpire";
    case CommandKind::kExpireAt:
      return "expireat";
    case CommandKind::kPExpireAt:
      return "pexpireat";
    case CommandKind::kPersist:
      return "persist";
    case CommandKind::kTtl:
      return "ttl";
    case CommandKind::kPttl:
      return "pttl";
    case CommandKind::kExpireTime:
      return "expiretime";
    case CommandKind::kPExpireTime:
      return "pexpiretime";
    case CommandKind::kScan:
      return "scan";
    case CommandKind::kType:
      return "type";
    case CommandKind::kSelect:
      return "select";
    case CommandKind::kSet:
      return "set";
    case CommandKind::kLPush:
      return "lpush";
    case CommandKind::kLPushX:
      return "lpushx";
    case CommandKind::kRPush:
      return "rpush";
    case CommandKind::kRPushX:
      return "rpushx";
    case CommandKind::kLPop:
      return "lpop";
    case CommandKind::kRPop:
      return "rpop";
    case CommandKind::kLLen:
      return "llen";
    case CommandKind::kLIndex:
      return "lindex";
    case CommandKind::kLRange:
      return "lrange";
    case CommandKind::kLSet:
      return "lset";
    case CommandKind::kLInsert:
      return "linsert";
    case CommandKind::kLRem:
      return "lrem";
    case CommandKind::kLTrim:
      return "ltrim";
    case CommandKind::kLPos:
      return "lpos";
    case CommandKind::kLMove:
      return "lmove";
    case CommandKind::kRPopLPush:
      return "rpoplpush";
    case CommandKind::kLMPop:
      return "lmpop";
    case CommandKind::kBLPop:
      return "blpop";
    case CommandKind::kBRPop:
      return "brpop";
    case CommandKind::kBLMove:
      return "blmove";
    case CommandKind::kBRPopLPush:
      return "brpoplpush";
    case CommandKind::kBLMPop:
      return "blmpop";
    case CommandKind::kHSet:
      return "hset";
    case CommandKind::kHMSet:
      return "hmset";
    case CommandKind::kHSetNx:
      return "hsetnx";
    case CommandKind::kHGet:
      return "hget";
    case CommandKind::kHMGet:
      return "hmget";
    case CommandKind::kHDel:
      return "hdel";
    case CommandKind::kHLen:
      return "hlen";
    case CommandKind::kHExists:
      return "hexists";
    case CommandKind::kHGetAll:
      return "hgetall";
    case CommandKind::kHKeys:
      return "hkeys";
    case CommandKind::kHVals:
      return "hvals";
    case CommandKind::kHStrlen:
      return "hstrlen";
    case CommandKind::kHIncrBy:
      return "hincrby";
    case CommandKind::kHIncrByFloat:
      return "hincrbyfloat";
    case CommandKind::kHRandField:
      return "hrandfield";
    case CommandKind::kHScan:
      return "hscan";
    case CommandKind::kSAdd:
      return "sadd";
    case CommandKind::kSCard:
      return "scard";
    case CommandKind::kSDiff:
      return "sdiff";
    case CommandKind::kSDiffStore:
      return "sdiffstore";
    case CommandKind::kSInter:
      return "sinter";
    case CommandKind::kSInterCard:
      return "sintercard";
    case CommandKind::kSInterStore:
      return "sinterstore";
    case CommandKind::kSIsMember:
      return "sismember";
    case CommandKind::kSMembers:
      return "smembers";
    case CommandKind::kSMIsMember:
      return "smismember";
    case CommandKind::kSMove:
      return "smove";
    case CommandKind::kSPop:
      return "spop";
    case CommandKind::kSRandMember:
      return "srandmember";
    case CommandKind::kSRem:
      return "srem";
    case CommandKind::kSScan:
      return "sscan";
    case CommandKind::kSUnion:
      return "sunion";
    case CommandKind::kSUnionStore:
      return "sunionstore";
    case CommandKind::kBZMPop:
      return "bzmpop";
    case CommandKind::kBZPopMax:
      return "bzpopmax";
    case CommandKind::kBZPopMin:
      return "bzpopmin";
    case CommandKind::kZAdd:
      return "zadd";
    case CommandKind::kZCard:
      return "zcard";
    case CommandKind::kZCount:
      return "zcount";
    case CommandKind::kZIncrBy:
      return "zincrby";
    case CommandKind::kZLexCount:
      return "zlexcount";
    case CommandKind::kZMPop:
      return "zmpop";
    case CommandKind::kZMScore:
      return "zmscore";
    case CommandKind::kZPopMax:
      return "zpopmax";
    case CommandKind::kZPopMin:
      return "zpopmin";
    case CommandKind::kZRandMember:
      return "zrandmember";
    case CommandKind::kZRange:
      return "zrange";
    case CommandKind::kZRangeStore:
      return "zrangestore";
    case CommandKind::kZRangeByLex:
      return "zrangebylex";
    case CommandKind::kZRangeByScore:
      return "zrangebyscore";
    case CommandKind::kZRank:
      return "zrank";
    case CommandKind::kZRem:
      return "zrem";
    case CommandKind::kZRemRangeByLex:
      return "zremrangebylex";
    case CommandKind::kZRemRangeByRank:
      return "zremrangebyrank";
    case CommandKind::kZRemRangeByScore:
      return "zremrangebyscore";
    case CommandKind::kZRevRange:
      return "zrevrange";
    case CommandKind::kZRevRangeByLex:
      return "zrevrangebylex";
    case CommandKind::kZRevRangeByScore:
      return "zrevrangebyscore";
    case CommandKind::kZRevRank:
      return "zrevrank";
    case CommandKind::kZScan:
      return "zscan";
    case CommandKind::kZScore:
      return "zscore";
    case CommandKind::kZDiff:
      return "zdiff";
    case CommandKind::kZDiffStore:
      return "zdiffstore";
    case CommandKind::kZInter:
      return "zinter";
    case CommandKind::kZInterCard:
      return "zintercard";
    case CommandKind::kZInterStore:
      return "zinterstore";
    case CommandKind::kZUnion:
      return "zunion";
    case CommandKind::kZUnionStore:
      return "zunionstore";
    case CommandKind::kGeoAdd:
      return "geoadd";
    case CommandKind::kGeoDist:
      return "geodist";
    case CommandKind::kGeoHash:
      return "geohash";
    case CommandKind::kGeoPos:
      return "geopos";
    case CommandKind::kGeoRadius:
      return "georadius";
    case CommandKind::kGeoRadiusRo:
      return "georadius_ro";
    case CommandKind::kGeoRadiusByMember:
      return "georadiusbymember";
    case CommandKind::kGeoRadiusByMemberRo:
      return "georadiusbymember_ro";
    case CommandKind::kGeoSearch:
      return "geosearch";
    case CommandKind::kGeoSearchStore:
      return "geosearchstore";
    case CommandKind::kXAdd:
      return "xadd";
    case CommandKind::kXDel:
      return "xdel";
    case CommandKind::kXLen:
      return "xlen";
    case CommandKind::kXRange:
      return "xrange";
    case CommandKind::kXRevRange:
      return "xrevrange";
    case CommandKind::kXTrim:
      return "xtrim";
    case CommandKind::kXSetId:
      return "xsetid";
    case CommandKind::kXGroup:
      return "xgroup";
    case CommandKind::kXAck:
      return "xack";
    case CommandKind::kXPending:
      return "xpending";
    case CommandKind::kXClaim:
      return "xclaim";
    case CommandKind::kXAutoClaim:
      return "xautoclaim";
    case CommandKind::kXInfo:
      return "xinfo";
    case CommandKind::kXRead:
      return "xread";
    case CommandKind::kXReadGroup:
      return "xreadgroup";
    case CommandKind::kMSet:
      return "mset";
    case CommandKind::kMSetNx:
      return "msetnx";
    case CommandKind::kMGet:
      return "mget";
    case CommandKind::kMulti:
      return "multi";
    case CommandKind::kExec:
      return "exec";
    case CommandKind::kDiscard:
      return "discard";
    case CommandKind::kWatch:
      return "watch";
    case CommandKind::kUnwatch:
      return "unwatch";
    case CommandKind::kInfo:
      return "info";
    case CommandKind::kKeys:
      return "keys";
    case CommandKind::kTombRaider:
      return "tombraider";
    case CommandKind::kDefrag:
      return "defrag";
    case CommandKind::kUnknown:
    case CommandKind::kCount:
      return "unknown";
  }
  return "unknown";
}

namespace {

void AppendEscapedLabel(std::string_view value, std::string* output) {
  for (const char c : value) {
    switch (c) {
      case '\\':
        output->append("\\\\");
        break;
      case '"':
        output->append("\\\"");
        break;
      case '\n':
        output->append("\\n");
        break;
      default:
        output->push_back(c);
        break;
    }
  }
}

std::string SecondsFromMicroseconds(std::uint64_t microseconds) {
  const std::uint64_t seconds = microseconds / 1'000'000;
  std::uint64_t remainder = microseconds % 1'000'000;
  if (remainder == 0) {
    return std::to_string(seconds);
  }
  std::string fraction = std::to_string(remainder + 1'000'000).substr(1);
  while (fraction.back() == '0') {
    fraction.pop_back();
  }
  return absl::StrCat(seconds, ".", fraction);
}

}  // namespace

celer::Task<absl::Status> RenderPrometheusMetrics(
    const storage::StorageEngine& storage, std::string* output_ptr) {
  RefreshMemoryDiagnostics();
  const WorkerMetricsSnapshot worker_metrics = co_await CollectWorkerMetrics();
  const storage::StorageMetricsSnapshot storage_metrics =
      co_await storage.CollectMetrics();
  const storage::DefragTotals defrag = storage.DefragStats();
  const MemoryStats memory_metrics = GetMemoryStats();
  const std::uint64_t current_memory = memory_metrics.used_bytes_;
  std::string& output = *output_ptr;
  output.clear();
  output.reserve(32 * 1024);

  absl::StrAppend(&output,
                  "# HELP keylane_commands_total Completed Redis commands.\n"
                  "# TYPE keylane_commands_total counter\n"
                  "keylane_commands_total ",
                  worker_metrics.TotalCalls(),
                  "\n"
                  "# HELP keylane_command_calls_total Completed Redis commands "
                  "by command.\n"
                  "# TYPE keylane_command_calls_total counter\n");
  for (std::size_t index = 0; index < worker_metrics.commands_.size();
       ++index) {
    const CommandMetricTotals& command = worker_metrics.commands_[index];
    if (command.calls_ == 0) {
      continue;
    }
    const std::string_view name =
        CommandMetricName(static_cast<CommandKind>(index));
    absl::StrAppend(&output, "keylane_command_calls_total{command=\"", name,
                    "\"} ", command.calls_, "\n");
  }

  output.append(
      "# HELP keylane_command_duration_seconds Redis command execution "
      "latency, excluding socket response writes.\n"
      "# TYPE keylane_command_duration_seconds histogram\n");
  for (std::size_t index = 0; index < worker_metrics.commands_.size();
       ++index) {
    const CommandMetricTotals& command = worker_metrics.commands_[index];
    if (command.calls_ == 0) {
      continue;
    }
    const std::string_view name =
        CommandMetricName(static_cast<CommandKind>(index));
    std::uint64_t cumulative = 0;
    for (std::size_t bucket = 0; bucket < kCommandLatencyBucketUpperUs.size();
         ++bucket) {
      cumulative += command.latency_bins_[bucket];
      absl::StrAppend(
          &output, "keylane_command_duration_seconds_bucket{command=\"", name,
          "\",le=\"",
          SecondsFromMicroseconds(kCommandLatencyBucketUpperUs[bucket]), "\"} ",
          cumulative, "\n");
    }
    cumulative += command.latency_bins_.back();
    const double latency_seconds = static_cast<double>(command.latency_ticks_) /
                                   worker_metrics.counter_frequency_;
    absl::StrAppend(&output,
                    "keylane_command_duration_seconds_bucket{command=\"", name,
                    "\",le=\"+Inf\"} ", cumulative, "\n",
                    "keylane_command_duration_seconds_sum{command=\"", name,
                    "\"} ", latency_seconds, "\n",
                    "keylane_command_duration_seconds_count{command=\"", name,
                    "\"} ", command.calls_, "\n");
  }

  absl::StrAppend(
      &output,
      "# HELP keylane_connections Current TCP connections, including Redis "
      "clients, metrics scrapes, and replication.\n"
      "# TYPE keylane_connections gauge\n"
      "keylane_connections ",
      worker_metrics.connections_, "\n",
      "# HELP keylane_connected_clients Current Redis client connections.\n"
      "# TYPE keylane_connected_clients gauge\n"
      "keylane_connected_clients ",
      worker_metrics.connected_clients_, "\n",
      "# HELP keylane_storage_defrag_runs_total Completed defrag attempts.\n"
      "# TYPE keylane_storage_defrag_runs_total counter\n"
      "keylane_storage_defrag_runs_total{result=\"success\"} ",
      worker_metrics.defrag_successes_, "\n",
      "keylane_storage_defrag_runs_total{result=\"resource_exhausted\"} ",
      worker_metrics.defrag_resource_exhausted_, "\n",
      "keylane_storage_defrag_runs_total{result=\"error\"} ",
      worker_metrics.defrag_failures_, "\n",
      "# HELP keylane_storage_defrag_active Currently running defrag jobs.\n"
      "# TYPE keylane_storage_defrag_active gauge\n"
      "keylane_storage_defrag_active ",
      worker_metrics.active_defrags_, "\n",
      "# HELP keylane_storage_defrag_pending Queued defrag jobs.\n"
      "# TYPE keylane_storage_defrag_pending gauge\n"
      "keylane_storage_defrag_pending ",
      worker_metrics.pending_defrags_, "\n",
      "# HELP keylane_storage_defrag_paused Whether relocation jobs are "
      "paused while candidates remain queued.\n"
      "# TYPE keylane_storage_defrag_paused gauge\n"
      "keylane_storage_defrag_paused ",
      defrag.paused_ ? 1 : 0, "\n",
      "# HELP keylane_storage_defrag_max_active_per_device Runtime maximum "
      "concurrent relocations per device.\n"
      "# TYPE keylane_storage_defrag_max_active_per_device gauge\n"
      "keylane_storage_defrag_max_active_per_device ",
      defrag.max_active_per_device_, "\n",
      "# HELP keylane_storage_defrag_block_sleep_seconds Runtime cooldown "
      "after each relocated block.\n"
      "# TYPE keylane_storage_defrag_block_sleep_seconds gauge\n"
      "keylane_storage_defrag_block_sleep_seconds ",
      static_cast<double>(defrag.block_sleep_ms_) / 1000.0, "\n",
      "# HELP keylane_storage_defrag_record_sleep_seconds Runtime pause "
      "after each record examined by defrag.\n"
      "# TYPE keylane_storage_defrag_record_sleep_seconds gauge\n"
      "keylane_storage_defrag_record_sleep_seconds ",
      static_cast<double>(defrag.record_sleep_us_) / 1'000'000.0, "\n",
      "# HELP keylane_memory_current_bytes Current process memory used for "
      "limit enforcement.\n"
      "# TYPE keylane_memory_current_bytes gauge\n"
      "keylane_memory_current_bytes ",
      current_memory, "\n",
      "# HELP keylane_memory_used_bytes Allocator usable bytes used for "
      "limit enforcement.\n"
      "# TYPE keylane_memory_used_bytes gauge\n"
      "keylane_memory_used_bytes ",
      memory_metrics.used_bytes_, "\n",
      "# HELP keylane_memory_rss_bytes Resident process memory.\n"
      "# TYPE keylane_memory_rss_bytes gauge\n"
      "keylane_memory_rss_bytes ",
      memory_metrics.rss_bytes_, "\n",
      "# HELP keylane_memory_committed_bytes Memory committed by the "
      "configured allocator.\n"
      "# TYPE keylane_memory_committed_bytes gauge\n"
      "keylane_memory_committed_bytes ",
      memory_metrics.committed_bytes_, "\n",
      "# HELP keylane_memory_reserved_bytes Address space reserved by the "
      "configured allocator.\n"
      "# TYPE keylane_memory_reserved_bytes gauge\n"
      "keylane_memory_reserved_bytes ",
      memory_metrics.reserved_bytes_, "\n",
      "# HELP keylane_memory_max_bytes Configured process memory limit.\n"
      "# TYPE keylane_memory_max_bytes gauge\n"
      "keylane_memory_max_bytes ",
      memory_metrics.max_bytes_, "\n",
      "# HELP keylane_memory_rejected_commands_total Commands rejected by "
      "the memory limit.\n"
      "# TYPE keylane_memory_rejected_commands_total counter\n"
      "keylane_memory_rejected_commands_total ",
      memory_metrics.rejected_commands_, "\n",
      "# HELP keylane_storage_capacity_bytes Usable data capacity.\n"
      "# TYPE keylane_storage_capacity_bytes gauge\n"
      "# HELP keylane_storage_available_bytes Space available to foreground "
      "writes after preserving the defrag reserve.\n"
      "# TYPE keylane_storage_available_bytes gauge\n"
      "# HELP keylane_filesystem_available_bytes Space available on the data "
      "file's filesystem.\n"
      "# TYPE keylane_filesystem_available_bytes gauge\n");
  for (const storage::StorageDeviceMetrics& device : storage_metrics.devices_) {
    std::string labels =
        absl::StrCat("device=\"", device.device_id_, "\",path=\"");
    AppendEscapedLabel(device.path_, &labels);
    labels.append("\"");
    absl::StrAppend(&output, "keylane_storage_capacity_bytes{", labels, "} ",
                    device.capacity_bytes_, "\n",
                    "keylane_storage_available_bytes{", labels, "} ",
                    device.available_bytes_, "\n");
    if (device.filesystem_available_bytes_.has_value()) {
      absl::StrAppend(&output, "keylane_filesystem_available_bytes{", labels,
                      "} ", *device.filesystem_available_bytes_, "\n");
    }
  }
  co_return absl::OkStatus();
}

std::unique_ptr<celer::Service> CreateMetricsService(
    std::uint16_t port, const storage::StorageEngine* storage) {
  auto service = std::make_unique<celer::HttpService>(port);
  service->RegisterGet(
      "/metrics",
      [storage](const celer::HttpRequest&,
                celer::HttpResponse* response) -> celer::Task<absl::Status> {
        response->content_type_ = "text/plain; version=0.0.4; charset=utf-8";
        co_return co_await RenderPrometheusMetrics(*storage, &response->body_);
      });
  return service;
}

}  // namespace keylane
