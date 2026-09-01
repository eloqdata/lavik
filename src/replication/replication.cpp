#include "keylane/replication.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <bitset>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "celer/net/connection.h"
#include "celer/net/tls.h"
#include "celer/runtime/concurrentqueue.h"
#include "celer/runtime/cross_core.h"
#include "celer/runtime/sync.h"
#include "celer/runtime/worker.h"
#include "keylane/command.h"
#include "keylane/command_table.h"
#include "keylane/memory.h"
#include "keylane/metrics.h"
#include "keylane/rdb.h"
#include "keylane/replication_command.h"
#include "keylane/resp.h"
#include "keylane/storage/engine.h"
#include "../redis/lua_eval.h"
#include "spdlog/spdlog.h"

namespace keylane {
namespace {

using celer::Connection;
using celer::Task;
using celer::TcpStream;
using storage::PartitionFullSyncBatch;
using storage::PartitionReplicationStart;
using storage::PartitionSnapshotBatch;
using storage::SnapshotRecord;

constexpr std::string_view kProtocolVersion = "1";
constexpr auto kHandshakeTimeout = std::chrono::seconds(10);

std::uint64_t SteadyNanos() noexcept {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

std::uint64_t SecondsSince(std::uint64_t started_nanos) noexcept {
  const std::uint64_t now = SteadyNanos();
  return now > started_nanos ? (now - started_nanos) / 1'000'000'000 : 0;
}
// A disk-backed full sync of a multi-terabyte dataset can legitimately run
// for hours. Only a flow that stops making protocol progress is timed out;
// there is deliberately no wall-clock limit on the whole synchronization.
constexpr auto kFullSyncStallTimeout = std::chrono::minutes(10);
constexpr auto kReconnectDelay = std::chrono::seconds(1);
constexpr auto kRedisTopologyPollInterval = std::chrono::seconds(2);
// Keep snapshot reads and captured writes on a small, symmetric scheduling
// quantum. Snapshot records are accumulated separately into transfer-sized
// frames, so this does not turn the wire protocol into 64-record packets.
constexpr std::size_t kFullSyncSchedulingItems = 64;
constexpr std::size_t kSnapshotKeysPerBatch = kFullSyncSchedulingItems;
constexpr std::size_t kOverrideRecordsPerBatch = 256;
// A continuously written tailing partition can keep the session FIFO
// permanently nonempty.  Snapshot scanning therefore consumes only a bounded
// number of commands at each interleave point.  Queue admission supplies
// backpressure when the target cannot keep up; the final cut closes admission
// and drains the remaining finite prefix completely.
constexpr std::size_t kFullSyncInterleaveCommands = kFullSyncSchedulingItems;
// Candidate epochs are independent of the source-side scan fence. Persist a
// group in one target fdatasync, then install/scan/handoff one source
// partition at a time. This keeps the one-partition memory bound without
// issuing one metadata durability round-trip for every empty partition.
constexpr std::size_t kFullSyncResetBatch = 64;
constexpr std::size_t kMaxDataFrame = 12U * 1024U * 1024U;
constexpr std::size_t kBacklogBatchBytes = storage::kReplicationTransferBytes;
constexpr std::size_t kBacklogBatchFrames = 128;
// A flow that finishes its partitions before its peers must keep publishing
// captured writes.  Use a small quantum there so all flows notice the final
// scanner promptly and reach the cut barrier with little queued work.
constexpr std::size_t kFullSyncReadyWaitCommands = kBacklogBatchFrames;
constexpr std::uint16_t kResetBatchAckPartition =
    std::numeric_limits<std::uint16_t>::max();
using RedisSlotSet = std::bitset<storage::kLogicalStorageShards>;

enum class DataFrameKind : std::uint8_t {
  kReset = 1,
  kRecords = 2,
  kAck = 3,
  kCommand = 4,
  kCursor = 5,
  kPartitionHandoff = 6,
  kFullSyncCut = 7,
  // Full-sync publish frames carry source partition metadata but use a
  // session-local contiguous ACK sequence independent of ONLINE flow LSNs.
  kFullSyncCommand = 8,
};

void PutU8(std::string& output, std::uint8_t value) {
  output.push_back(static_cast<char>(value));
}

void PutU16(std::string& output, std::uint16_t value) {
  PutU8(output, static_cast<std::uint8_t>(value));
  PutU8(output, static_cast<std::uint8_t>(value >> 8));
}

void PutU32(std::string& output, std::uint32_t value) {
  for (unsigned i = 0; i < 4; ++i) PutU8(output, value >> (i * 8));
}

void PutU64(std::string& output, std::uint64_t value) {
  for (unsigned i = 0; i < 8; ++i) PutU8(output, value >> (i * 8));
}

void PutString(std::string& output, std::string_view value) {
  output.append(value.data(), value.size());
}

absl::Status ReplicationMemoryExhausted(std::string_view operation) {
  RecordMemoryRejection();
  return absl::ResourceExhaustedError(
      absl::StrCat("insufficient memory for ", operation));
}

absl::Status ReserveReplicationString(std::string* output,
                                      std::size_t desired) {
  if (desired <= output->capacity()) return absl::OkStatus();
  std::size_t allocation_capacity = desired;
  if (output->capacity() <= std::numeric_limits<std::size_t>::max() / 2) {
    allocation_capacity =
        std::max(allocation_capacity, output->capacity() * 2);
  } else {
    return ReplicationMemoryExhausted("replication buffer");
  }
  if (allocation_capacity == std::numeric_limits<std::size_t>::max()) {
    return ReplicationMemoryExhausted("replication buffer");
  }
  try {
    output->reserve(allocation_capacity);
  } catch (const std::bad_alloc&) {
    return ReplicationMemoryExhausted("replication buffer allocation");
  } catch (const std::length_error&) {
    return absl::ResourceExhaustedError("replication buffer is too large");
  }
  return absl::OkStatus();
}

absl::Status AppendReplicationString(std::string* output,
                                     std::string_view value) {
  if (value.size() > std::numeric_limits<std::size_t>::max() - output->size()) {
    return ReplicationMemoryExhausted("replication buffer");
  }
  absl::Status reserved =
      ReserveReplicationString(output, output->size() + value.size());
  if (!reserved.ok()) return reserved;
  output->append(value);
  return absl::OkStatus();
}

class DataReader {
 public:
  explicit DataReader(std::string_view input) : input_(input) {}

  bool U8(std::uint8_t* value) {
    if (remaining() < 1) return false;
    *value = static_cast<std::uint8_t>(input_[position_++]);
    return true;
  }
  bool U16(std::uint16_t* value) {
    std::uint8_t lo = 0, hi = 0;
    if (!U8(&lo) || !U8(&hi)) return false;
    *value = static_cast<std::uint16_t>(lo | (hi << 8));
    return true;
  }
  bool U32(std::uint32_t* value) {
    std::uint32_t result = 0;
    for (unsigned i = 0; i < 4; ++i) {
      std::uint8_t part = 0;
      if (!U8(&part)) return false;
      result |= static_cast<std::uint32_t>(part) << (i * 8);
    }
    *value = result;
    return true;
  }
  bool U64(std::uint64_t* value) {
    std::uint64_t result = 0;
    for (unsigned i = 0; i < 8; ++i) {
      std::uint8_t part = 0;
      if (!U8(&part)) return false;
      result |= static_cast<std::uint64_t>(part) << (i * 8);
    }
    *value = result;
    return true;
  }
  absl::Status String(std::uint32_t size, std::string* value) {
    if (remaining() < size) {
      return absl::InvalidArgumentError(
          "malformed replication record payload");
    }
    absl::Status reserved = ReserveReplicationString(value, size);
    if (!reserved.ok()) return reserved;
    value->assign(input_.data() + position_, size);
    position_ += size;
    return absl::OkStatus();
  }
  std::size_t remaining() const { return input_.size() - position_; }

 private:
  std::string_view input_;
  std::size_t position_ = 0;
};

std::size_t EncodedRecordBytes(const SnapshotRecord& record) {
  return 1 + 1 + 1 + 8 + 8 + 8 + 8 + 4 + 4 + 4 + 4 + record.key_.size() +
         record.value_.size();
}

absl::Status EncodeRecords(std::uint16_t partition_id,
                           std::span<const SnapshotRecord> records,
                           std::string* output) {
  std::size_t bytes = 2 + 4;
  for (const SnapshotRecord& record : records) {
    if (record.key_.size() > std::numeric_limits<std::uint32_t>::max() ||
        record.value_.size() > std::numeric_limits<std::uint32_t>::max() ||
        bytes > kMaxDataFrame - EncodedRecordBytes(record)) {
      return absl::ResourceExhaustedError(
          "replication records exceed the frame limit");
    }
    bytes += EncodedRecordBytes(record);
  }
  output->clear();
  absl::Status reserved = ReserveReplicationString(output, bytes);
  if (!reserved.ok()) return reserved;
  PutU16(*output, partition_id);
  PutU32(*output, static_cast<std::uint32_t>(records.size()));
  for (const SnapshotRecord& record : records) {
    PutU8(*output, static_cast<std::uint8_t>(record.kind_));
    PutU8(*output, record.db_id_);
    PutU8(*output, static_cast<std::uint8_t>(record.value_type_));
    PutU64(*output, record.db_epoch_);
    PutU64(*output, record.mutation_sequence_);
    PutU64(*output, record.expire_at_ms_);
    PutU64(*output, record.logical_size_);
    PutU32(*output, record.chunk_index_);
    PutU32(*output, record.chunk_count_);
    PutU32(*output, static_cast<std::uint32_t>(record.key_.size()));
    PutU32(*output, static_cast<std::uint32_t>(record.value_.size()));
    PutString(*output, record.key_);
    PutString(*output, record.value_);
  }
  return absl::OkStatus();
}

// The returned records own every key and value copied from the frame. Catch at
// this ownership boundary so reserve, per-record strings, and vector growth
// share one failure path.
absl::StatusOr<std::pair<std::uint16_t, std::vector<SnapshotRecord>>>
DecodeRecords(std::string_view payload) try {
  DataReader reader(payload);
  std::uint16_t partition_id = 0;
  std::uint32_t count = 0;
  if (!reader.U16(&partition_id) || !reader.U32(&count) ||
      partition_id >= storage::kLogicalStorageShards || count > 65536) {
    return absl::InvalidArgumentError("malformed replication records frame");
  }
  std::vector<SnapshotRecord> records;
  records.reserve(count);
  for (std::uint32_t i = 0; i < count; ++i) {
    SnapshotRecord record;
    std::uint8_t kind = 0, value_type = 0;
    std::uint32_t key_size = 0, value_size = 0;
    if (!reader.U8(&kind) || !reader.U8(&record.db_id_) ||
        !reader.U8(&value_type) || !reader.U64(&record.db_epoch_) ||
        !reader.U64(&record.mutation_sequence_) ||
        !reader.U64(&record.expire_at_ms_) ||
        !reader.U64(&record.logical_size_) ||
        !reader.U32(&record.chunk_index_) ||
        !reader.U32(&record.chunk_count_) || !reader.U32(&key_size) ||
        !reader.U32(&value_size) ||
        kind < static_cast<std::uint8_t>(SnapshotRecord::Kind::kValue) ||
        kind > static_cast<std::uint8_t>(SnapshotRecord::Kind::kValueCommit) ||
        record.db_id_ >= storage::kLogicalDatabaseCount ||
        value_type > static_cast<std::uint8_t>(storage::ValueType::kStream)) {
      return absl::InvalidArgumentError("malformed replication record payload");
    }
    absl::Status key = reader.String(key_size, &record.key_);
    if (!key.ok()) return key;
    absl::Status value = reader.String(value_size, &record.value_);
    if (!value.ok()) return value;
    record.kind_ = static_cast<SnapshotRecord::Kind>(kind);
    record.value_type_ = static_cast<storage::ValueType>(value_type);
    records.push_back(std::move(record));
  }
  if (reader.remaining() != 0) {
    return absl::InvalidArgumentError("trailing replication record payload");
  }
  return std::make_pair(partition_id, std::move(records));
} catch (const std::bad_alloc&) {
  return ReplicationMemoryExhausted("replication record payload");
} catch (const std::length_error&) {
  return absl::ResourceExhaustedError(
      "replication record payload is too large");
}

bool EqualCaseInsensitive(std::string_view left,
                          std::string_view right) noexcept {
  if (left.size() != right.size()) return false;
  for (std::size_t index = 0; index < left.size(); ++index) {
    unsigned char a = static_cast<unsigned char>(left[index]);
    unsigned char b = static_cast<unsigned char>(right[index]);
    if (a >= 'A' && a <= 'Z') a += 'a' - 'A';
    if (b >= 'A' && b <= 'Z') b += 'a' - 'A';
    if (a != b) return false;
  }
  return true;
}

template <typename Integer>
bool ParseUnsigned(std::string_view text, Integer* result) noexcept {
  static_assert(std::is_unsigned_v<Integer>);
  if (text.empty()) return false;
  Integer parsed = 0;
  const char* begin = text.data();
  const char* end = begin + text.size();
  const auto [parsed_end, error] = std::from_chars(begin, end, parsed);
  if (error != std::errc{} || parsed_end != end) return false;
  *result = parsed;
  return true;
}

std::vector<std::string_view> SplitWords(std::string_view line) {
  std::vector<std::string_view> words;
  while (!line.empty()) {
    const std::size_t begin = line.find_first_not_of(' ');
    if (begin == std::string_view::npos) break;
    line.remove_prefix(begin);
    const std::size_t end = line.find(' ');
    words.push_back(line.substr(0, end));
    if (end == std::string_view::npos) break;
    line.remove_prefix(end + 1);
  }
  return words;
}

std::string EncodeRespCommand(std::span<const std::string> args) {
  std::string encoded = absl::StrCat("*", args.size(), "\r\n");
  for (const std::string& arg : args) {
    absl::StrAppend(&encoded, "$", arg.size(), "\r\n", arg, "\r\n");
  }
  return encoded;
}

Task<absl::Status> WriteText(TcpStream& stream, std::string_view text) {
  return stream.WriteAll(std::span<const std::byte>(
      reinterpret_cast<const std::byte*>(text.data()), text.size()));
}

absl::Status AppendDataFrame(std::string* output, DataFrameKind kind,
                             std::string_view payload) {
  if (payload.size() > kMaxDataFrame) {
    return absl::ResourceExhaustedError(
        "replication data frame exceeds configured limit");
  }
  PutU32(*output, static_cast<std::uint32_t>(payload.size() + 1));
  PutU8(*output, static_cast<std::uint8_t>(kind));
  output->append(payload);
  return absl::OkStatus();
}

Task<absl::Status> WriteDataFrame(TcpStream& stream, DataFrameKind kind,
                                  std::string_view payload) {
  std::string frame;
  if (payload.size() > std::numeric_limits<std::size_t>::max() - 5) {
    co_return ReplicationMemoryExhausted("replication frame");
  }
  absl::Status reserved =
      ReserveReplicationString(&frame, 5 + payload.size());
  if (!reserved.ok()) co_return reserved;
  absl::Status appended = AppendDataFrame(&frame, kind, payload);
  if (!appended.ok()) co_return appended;
  co_return co_await WriteText(stream, frame);
}

Task<absl::StatusOr<std::string>> ReadExact(TcpStream& stream,
                                            std::size_t size) {
  std::string result;
  absl::Status reserved = ReserveReplicationString(&result, size);
  if (!reserved.ok()) co_return reserved;
  result.resize(size);
  std::size_t offset = 0;
  while (offset < size) {
    auto read = co_await stream.ReadSome(std::span<std::byte>(
        reinterpret_cast<std::byte*>(result.data() + offset), size - offset));
    if (!read.ok()) co_return read.status();
    if (*read == 0) co_return absl::UnavailableError("replication peer closed");
    offset += *read;
  }
  co_return result;
}

Task<absl::StatusOr<std::pair<DataFrameKind, std::string>>> ReadDataFrame(
    TcpStream& stream) {
  auto header = co_await ReadExact(stream, 5);
  if (!header.ok()) co_return header.status();
  DataReader reader(*header);
  std::uint32_t length = 0;
  std::uint8_t kind = 0;
  if (!reader.U32(&length) || !reader.U8(&kind) || length == 0 ||
      length - 1 > kMaxDataFrame || kind < 1 ||
      kind > static_cast<std::uint8_t>(DataFrameKind::kFullSyncCommand)) {
    co_return absl::InvalidArgumentError("malformed replication frame header");
  }
  auto payload = co_await ReadExact(stream, length - 1);
  if (!payload.ok()) co_return payload.status();
  co_return std::make_pair(static_cast<DataFrameKind>(kind),
                           std::move(*payload));
}

Task<absl::Status> WriteFrameAndWaitAck(TcpStream& stream, DataFrameKind kind,
                                        std::string_view payload,
                                        std::uint16_t partition_id) {
  absl::Status sent = co_await WriteDataFrame(stream, kind, payload);
  if (!sent.ok()) co_return sent;
  auto ack = co_await ReadDataFrame(stream);
  if (!ack.ok()) co_return ack.status();
  if (ack->first != DataFrameKind::kAck) {
    co_return absl::InvalidArgumentError("replication frame ACK expected");
  }
  DataReader reader(ack->second);
  std::uint16_t acknowledged_partition = 0;
  if (!reader.U16(&acknowledged_partition) ||
      acknowledged_partition != partition_id || reader.remaining() != 8) {
    co_return absl::InvalidArgumentError("malformed replication frame ACK");
  }
  co_return absl::OkStatus();
}

Task<absl::Status> WaitFullSyncAck(TcpStream& stream,
                                   std::uint16_t partition_id,
                                   std::uint64_t fullsync_sequence) {
  auto ack = co_await ReadDataFrame(stream);
  if (!ack.ok()) co_return ack.status();
  if (ack->first != DataFrameKind::kAck) {
    co_return absl::InvalidArgumentError("full-sync frame ACK expected");
  }
  DataReader reader(ack->second);
  std::uint16_t acknowledged_partition = 0;
  std::uint64_t acknowledged_sequence = 0;
  if (!reader.U16(&acknowledged_partition) ||
      !reader.U64(&acknowledged_sequence) || reader.remaining() != 0 ||
      acknowledged_partition != partition_id ||
      acknowledged_sequence != fullsync_sequence) {
    co_return absl::InvalidArgumentError("malformed full-sync frame ACK");
  }
  co_return absl::OkStatus();
}

Task<absl::Status> WriteFullSyncFrameAndWaitAck(
    TcpStream& stream, DataFrameKind kind, std::string_view body,
    std::uint16_t partition_id, std::uint64_t fullsync_sequence) {
  std::string payload;
  payload.reserve(sizeof(fullsync_sequence) + body.size());
  PutU64(payload, fullsync_sequence);
  PutString(payload, body);
  absl::Status sent = co_await WriteDataFrame(stream, kind, payload);
  if (!sent.ok()) co_return sent;
  co_return co_await WaitFullSyncAck(stream, partition_id, fullsync_sequence);
}

Task<absl::StatusOr<std::string>> ReadLine(TcpStream& stream) {
  std::string pending;
  std::array<std::byte, 1> input{};
  while (pending.size() <= 64 * 1024) {
    const std::size_t line_end = pending.find("\r\n");
    if (line_end != std::string::npos) {
      std::string line = pending.substr(0, line_end);
      co_return line;
    }
    auto read = co_await stream.ReadSome(input);
    if (!read.ok()) co_return read.status();
    if (*read == 0) {
      co_return absl::UnavailableError("replication peer closed connection");
    }
    pending.append(reinterpret_cast<const char*>(input.data()), *read);
  }
  co_return absl::ResourceExhaustedError(
      "replication handshake line exceeds 64 KiB");
}

Task<absl::StatusOr<std::string>> ReadRedisBulkReply(TcpStream& stream) {
  auto header = co_await ReadLine(stream);
  if (!header.ok()) co_return header.status();
  if (header->empty()) {
    co_return absl::InvalidArgumentError("empty Redis reply");
  }
  if (header->front() == '-') {
    co_return absl::FailedPreconditionError(*header);
  }
  if (header->front() != '$') {
    co_return absl::InvalidArgumentError(
        absl::StrCat("Redis bulk reply expected: ", *header));
  }
  std::uint64_t length = 0;
  if (!ParseUnsigned(std::string_view(*header).substr(1), &length) ||
      length > 16ULL * 1024 * 1024) {
    co_return absl::InvalidArgumentError("invalid Redis bulk reply length");
  }
  auto body = co_await ReadExact(stream, static_cast<std::size_t>(length) + 2);
  if (!body.ok()) co_return body.status();
  if (!body->ends_with("\r\n")) {
    co_return absl::InvalidArgumentError("Redis bulk reply is not terminated");
  }
  body->resize(static_cast<std::size_t>(length));
  co_return std::move(*body);
}

struct RedisClusterMaster {
  std::string node_id_;
  ReplicaOfConfig endpoint_;
  RedisSlotSet slots_;
  bool myself_ = false;
};

struct RedisClusterTopology {
  std::vector<RedisClusterMaster> masters_;
  std::string self_id_;
};

bool HasCommaFlag(std::string_view flags, std::string_view wanted) {
  while (!flags.empty()) {
    const std::size_t comma = flags.find(',');
    if (flags.substr(0, comma) == wanted) return true;
    if (comma == std::string_view::npos) break;
    flags.remove_prefix(comma + 1);
  }
  return false;
}

absl::StatusOr<ReplicaOfConfig> ParseRedisClusterAddress(
    std::string_view address) {
  if (const std::size_t comma = address.find(',');
      comma != std::string_view::npos) {
    address = address.substr(0, comma);
  }
  if (const std::size_t bus = address.find('@');
      bus != std::string_view::npos) {
    address = address.substr(0, bus);
  }
  std::string_view host;
  std::string_view port_text;
  if (address.starts_with('[')) {
    const std::size_t close = address.find(']');
    if (close == std::string_view::npos || close + 1 >= address.size() ||
        address[close + 1] != ':') {
      return absl::InvalidArgumentError("invalid Redis Cluster IPv6 address");
    }
    host = address.substr(1, close - 1);
    port_text = address.substr(close + 2);
  } else {
    const std::size_t colon = address.rfind(':');
    if (colon == std::string_view::npos) {
      return absl::InvalidArgumentError("invalid Redis Cluster node address");
    }
    host = address.substr(0, colon);
    port_text = address.substr(colon + 1);
  }
  std::uint16_t port = 0;
  if (host.empty() || !ParseUnsigned(port_text, &port) || port == 0) {
    return absl::InvalidArgumentError("invalid Redis Cluster node endpoint");
  }
  return ReplicaOfConfig{std::string(host), port};
}

absl::StatusOr<RedisClusterTopology> ParseRedisClusterNodes(
    std::string_view body) {
  RedisClusterTopology topology;
  RedisSlotSet assigned;
  while (!body.empty()) {
    const std::size_t newline = body.find('\n');
    std::string_view line = body.substr(0, newline);
    if (!line.empty() && line.back() == '\r') line.remove_suffix(1);
    if (newline == std::string_view::npos) {
      body = {};
    } else {
      body.remove_prefix(newline + 1);
    }
    if (line.empty()) continue;
    const std::vector<std::string_view> fields = SplitWords(line);
    if (fields.size() < 8) {
      return absl::InvalidArgumentError("malformed CLUSTER NODES line");
    }
    const bool myself = HasCommaFlag(fields[2], "myself");
    const bool master = HasCommaFlag(fields[2], "master");
    if (myself) topology.self_id_ = std::string(fields[0]);
    if (!master || HasCommaFlag(fields[2], "fail") ||
        HasCommaFlag(fields[2], "fail?") ||
        HasCommaFlag(fields[2], "handshake") ||
        HasCommaFlag(fields[2], "noaddr")) {
      continue;
    }
    RedisClusterMaster entry;
    entry.node_id_ = std::string(fields[0]);
    entry.myself_ = myself;
    auto endpoint = ParseRedisClusterAddress(fields[1]);
    if (!endpoint.ok()) return endpoint.status();
    entry.endpoint_ = std::move(*endpoint);
    for (std::size_t i = 8; i < fields.size(); ++i) {
      const std::string_view token = fields[i];
      if (token.starts_with('[')) {
        return absl::FailedPreconditionError(
            "Redis Cluster is migrating or importing slots");
      }
      std::uint16_t first = 0;
      std::uint16_t last = 0;
      const std::size_t dash = token.find('-');
      if (dash == std::string_view::npos) {
        if (!ParseUnsigned(token, &first)) {
          return absl::InvalidArgumentError("invalid Redis Cluster slot");
        }
        last = first;
      } else if (!ParseUnsigned(token.substr(0, dash), &first) ||
                 !ParseUnsigned(token.substr(dash + 1), &last) ||
                 first > last) {
        return absl::InvalidArgumentError("invalid Redis Cluster slot range");
      }
      if (last >= storage::kLogicalStorageShards) {
        return absl::InvalidArgumentError("Redis Cluster slot is out of range");
      }
      for (std::uint32_t slot = first; slot <= last; ++slot) {
        if (assigned.test(slot)) {
          return absl::FailedPreconditionError(
              absl::StrCat("Redis Cluster masters overlap at slot ", slot));
        }
        assigned.set(slot);
        entry.slots_.set(slot);
      }
    }
    // Redis permits an empty master during scale-out. It is not a replication
    // source until it owns at least one slot.
    if (entry.slots_.any()) topology.masters_.push_back(std::move(entry));
  }
  if (topology.self_id_.empty()) {
    return absl::FailedPreconditionError(
        "CLUSTER NODES did not identify the connected node");
  }
  if (assigned.count() != storage::kLogicalStorageShards) {
    return absl::FailedPreconditionError(absl::StrCat(
        "Redis Cluster has incomplete slot coverage: ", assigned.count(), "/",
        storage::kLogicalStorageShards));
  }
  if (std::none_of(topology.masters_.begin(), topology.masters_.end(),
                   [&](const RedisClusterMaster& node) {
                     return node.node_id_ == topology.self_id_;
                   })) {
    return absl::FailedPreconditionError(
        "the connected Redis Cluster node is not a slot-owning master");
  }
  return topology;
}

std::string FormatRedisSlots(const RedisSlotSet& slots) {
  std::string result;
  for (std::size_t first = 0; first < slots.size();) {
    if (!slots.test(first)) {
      ++first;
      continue;
    }
    std::size_t last = first;
    while (last + 1 < slots.size() && slots.test(last + 1)) ++last;
    if (!result.empty()) result.push_back(',');
    absl::StrAppend(&result, first);
    if (last != first) absl::StrAppend(&result, "-", last);
    first = last + 1;
  }
  return result;
}

std::vector<std::uint16_t> RedisSlotsVector(const RedisSlotSet& slots) {
  std::vector<std::uint16_t> result;
  result.reserve(slots.count());
  for (std::size_t slot = 0; slot < slots.size(); ++slot) {
    if (slots.test(slot)) result.push_back(static_cast<std::uint16_t>(slot));
  }
  return result;
}

bool SameRedisSlotLayout(const RedisClusterTopology& left,
                         const RedisClusterTopology& right) {
  if (left.masters_.size() != right.masters_.size()) return false;
  for (const RedisClusterMaster& expected : left.masters_) {
    if (std::none_of(right.masters_.begin(), right.masters_.end(),
                     [&](const RedisClusterMaster& current) {
                       return current.slots_ == expected.slots_;
                     })) {
      return false;
    }
  }
  return true;
}

class TemporaryRedisRdb {
 public:
  TemporaryRedisRdb() = default;
  TemporaryRedisRdb(int fd, std::string path)
      : fd_(fd), path_(std::move(path)) {}
  TemporaryRedisRdb(const TemporaryRedisRdb&) = delete;
  TemporaryRedisRdb& operator=(const TemporaryRedisRdb&) = delete;
  TemporaryRedisRdb(TemporaryRedisRdb&& other) noexcept
      : fd_(std::exchange(other.fd_, -1)), path_(std::move(other.path_)) {}
  TemporaryRedisRdb& operator=(TemporaryRedisRdb&& other) noexcept {
    if (this == &other) return *this;
    Reset();
    fd_ = std::exchange(other.fd_, -1);
    path_ = std::move(other.path_);
    return *this;
  }
  ~TemporaryRedisRdb() { Reset(); }

  int fd() const noexcept { return fd_; }
  const std::string& path() const noexcept { return path_; }
  std::string ReleasePath() noexcept { return std::exchange(path_, {}); }
  absl::Status Close() {
    if (fd_ < 0) return absl::OkStatus();
    const int fd = std::exchange(fd_, -1);
    if (::close(fd) == 0) return absl::OkStatus();
    return absl::InternalError(absl::StrCat(
        "failed to close temporary Redis RDB: ", std::strerror(errno)));
  }

 private:
  void Reset() noexcept {
    if (fd_ >= 0) ::close(fd_);
    fd_ = -1;
    if (!path_.empty()) ::unlink(path_.c_str());
    path_.clear();
  }

  int fd_ = -1;
  std::string path_;
};

absl::Status WriteFileAll(int fd, std::span<const std::byte> bytes) {
  while (!bytes.empty()) {
    const ssize_t written = ::write(fd, bytes.data(), bytes.size());
    if (written < 0) {
      if (errno == EINTR) continue;
      return absl::InternalError(absl::StrCat(
          "failed to write temporary Redis RDB: ", std::strerror(errno)));
    }
    if (written == 0) {
      return absl::InternalError("short write to temporary Redis RDB");
    }
    bytes = bytes.subspan(static_cast<std::size_t>(written));
  }
  return absl::OkStatus();
}

Task<absl::StatusOr<std::string>> ReceiveRedisRdb(TcpStream& stream) {
  auto header = co_await ReadLine(stream);
  if (!header.ok()) co_return header.status();
  if (header->empty() || header->front() != '$' ||
      header->starts_with("$EOF:")) {
    co_return absl::InvalidArgumentError(absl::StrCat(
        "Redis PSYNC did not provide a length-delimited RDB: ", *header));
  }
  std::uint64_t length = 0;
  if (!ParseUnsigned(std::string_view(*header).substr(1), &length) ||
      length == 0 || length > std::numeric_limits<std::size_t>::max()) {
    co_return absl::InvalidArgumentError("invalid Redis RDB bulk length");
  }

  std::array<char, 64> path_template{};
  constexpr std::string_view prefix = "/tmp/keylane-redis-rdb-XXXXXX";
  std::copy(prefix.begin(), prefix.end(), path_template.begin());
  const int fd = ::mkstemp(path_template.data());
  if (fd < 0) {
    co_return absl::InternalError(absl::StrCat(
        "cannot create temporary Redis RDB: ", std::strerror(errno)));
  }
  (void)::fcntl(fd, F_SETFD, FD_CLOEXEC);
  TemporaryRedisRdb file(fd, path_template.data());

  std::array<std::byte, 256 * 1024> buffer{};
  std::uint64_t remaining = length;
  while (remaining != 0) {
    const std::size_t wanted = static_cast<std::size_t>(
        std::min<std::uint64_t>(remaining, buffer.size()));
    auto read =
        co_await stream.ReadSome(std::span<std::byte>(buffer).first(wanted));
    if (!read.ok()) co_return read.status();
    if (*read == 0) {
      co_return absl::UnavailableError(
          "Redis closed connection during RDB transfer");
    }
    absl::Status written = WriteFileAll(
        file.fd(), std::span<const std::byte>(buffer).first(*read));
    if (!written.ok()) co_return written;
    remaining -= *read;
  }
  // Redis replication uses a bulk-style length header but does not append the
  // RESP bulk string CRLF after the RDB payload. The next byte is already the
  // first byte of the incremental command stream.
  absl::Status closed = file.Close();
  if (!closed.ok()) co_return closed;
  co_return file.ReleasePath();
}

struct RedisWireCommand {
  RespCommand command_;
  std::uint64_t bytes_ = 0;
};

class RedisCommandStream {
 public:
  explicit RedisCommandStream(TcpStream* stream) : stream_(stream) {}

  Task<absl::StatusOr<RedisWireCommand>> Next() {
    constexpr std::size_t kMaxPendingBytes = 1ULL * 1024 * 1024 * 1024;
    std::uint64_t prefix_bytes = 0;
    std::array<std::byte, 64 * 1024> input{};
    while (true) {
      RespParseResult parsed = parser_.Parse(pending_);
      if (parsed.state_ == RespParseState::kOk) {
        RedisWireCommand result{.command_ = std::move(parsed.command_),
                                .bytes_ = prefix_bytes + parsed.consumed_};
        pending_.erase(0, parsed.consumed_);
        co_return result;
      }
      if (parsed.state_ == RespParseState::kError) {
        co_return parsed.status_;
      }
      if (parsed.consumed_ != 0) {
        prefix_bytes += parsed.consumed_;
        pending_.erase(0, parsed.consumed_);
      }
      if (pending_.size() >= kMaxPendingBytes) {
        co_return absl::ResourceExhaustedError(
            "Redis replication command exceeds 1 GiB");
      }
      const std::size_t wanted =
          std::min(input.size(), kMaxPendingBytes - pending_.size());
      auto read =
          co_await stream_->ReadSome(std::span<std::byte>(input).first(wanted));
      if (!read.ok()) co_return read.status();
      if (*read == 0) {
        co_return absl::UnavailableError("Redis replication connection closed");
      }
      pending_.append(reinterpret_cast<const char*>(input.data()), *read);
    }
  }

 private:
  TcpStream* stream_;
  RespCommandParser parser_;
  std::string pending_;
};

struct RedisExportAckState {
  std::atomic<bool> done_{false};
  std::atomic<bool> failed_{false};
};

Task<absl::Status> ConsumeRedisExportAcks(
    TcpStream* stream, std::shared_ptr<RedisExportAckState> state) {
  RedisCommandStream commands(stream);
  absl::Status status;
  while (status.ok()) {
    auto wire = co_await commands.Next();
    if (!wire.ok()) {
      status = wire.status();
      break;
    }
    const auto& args = wire->command_.args_;
    std::uint64_t offset = 0;
    if (args.size() != 3 || !EqualCaseInsensitive(args[0], "REPLCONF") ||
        !EqualCaseInsensitive(args[1], "ACK") ||
        !ParseUnsigned(args[2], &offset)) {
      status = absl::InvalidArgumentError(
          "unexpected command from Redis export replica");
      break;
    }
  }
  state->failed_.store(true, std::memory_order_release);
  state->done_.store(true, std::memory_order_release);
  (void)::shutdown(stream->NativeFd(), SHUT_RDWR);
  co_return status;
}

// Multiple storage workers encode the point-in-time image concurrently, but
// the Redis wire remains one ordered byte stream. Queue pressure suspends only
// these background scanners; it never holds command admission closed.
class RedisRdbStreamQueue
    : public std::enable_shared_from_this<RedisRdbStreamQueue> {
 public:
  RedisRdbStreamQueue(storage::StorageEngine* storage, std::uint64_t session_id)
      : storage_(storage), session_id_(session_id) {}

  void Start() {
    remaining_.store(storage_->worker_count(), std::memory_order_release);
    for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
      auto context = std::make_unique<std::shared_ptr<RedisRdbStreamQueue>>(
          shared_from_this());
      celer::PostNotification(
          celer::ThisWorker().cross_core_, worker,
          celer::RemoteNotification{
              .context_ = context.release(),
              .value_ = worker,
              .run_fn_ =
                  [](void* raw, std::uint64_t worker_id) noexcept {
                    std::unique_ptr<std::shared_ptr<RedisRdbStreamQueue>> queue(
                        static_cast<std::shared_ptr<RedisRdbStreamQueue>*>(
                            raw));
                    (*queue)->SpawnWorker(static_cast<unsigned>(worker_id));
                  },
          });
    }
  }

  bool TryPop(std::string* fragment) {
    if (!queue_.try_dequeue(*fragment)) return false;
    queued_bytes_.fetch_sub(fragment->size(), std::memory_order_acq_rel);
    return true;
  }

  bool done() const noexcept {
    return remaining_.load(std::memory_order_acquire) == 0;
  }
  bool failed() const noexcept {
    return failed_.load(std::memory_order_acquire);
  }
  void Abort(absl::Status status) {
    Fail(std::move(status));
    aborted_.store(true, std::memory_order_release);
  }
  absl::Status status() {
    absl::Status status;
    if (failures_.try_dequeue(status)) return status;
    return failed() ? absl::InternalError("Redis RDB stream failed")
                    : absl::OkStatus();
  }

 private:
  static Task<absl::Status> RunOwned(std::shared_ptr<RedisRdbStreamQueue> queue,
                                     unsigned worker_id) {
    // Keep this coroutine frame: it owns queue until ScanWorker completes.
    co_return co_await queue->ScanWorker(worker_id);
  }

  void SpawnWorker(unsigned worker_id) {
    celer::ThisWorker().self_->SpawnBackground(
        RunOwned(shared_from_this(), worker_id));
  }

  bool TryPush(std::string* fragment) {
    if (aborted_.load(std::memory_order_relaxed)) return false;
    const std::size_t bytes = fragment->size();
    std::size_t occupied = queued_bytes_.load(std::memory_order_acquire);
    for (;;) {
      // A single value may exceed the normal bound only as the sole item.
      if ((bytes > kMaximumQueuedBytes && occupied != 0) ||
          (bytes <= kMaximumQueuedBytes &&
           occupied > kMaximumQueuedBytes - bytes)) {
        return false;
      }
      if (queued_bytes_.compare_exchange_weak(occupied, occupied + bytes,
                                              std::memory_order_acq_rel,
                                              std::memory_order_acquire)) {
        break;
      }
    }
    if (aborted_.load(std::memory_order_acquire)) {
      queued_bytes_.fetch_sub(bytes, std::memory_order_acq_rel);
      return false;
    }
    if (!queue_.enqueue(std::move(*fragment))) {
      queued_bytes_.fetch_sub(bytes, std::memory_order_acq_rel);
      Fail(absl::ResourceExhaustedError(
          "failed to allocate Redis RDB stream queue entry"));
      aborted_.store(true, std::memory_order_release);
      return false;
    }
    return true;
  }

  Task<absl::Status> ScanWorker(unsigned worker_id) {
    (void)worker_id;
    absl::Status status;
    storage::RdbSnapshotCursor cursor;
    unsigned reads_since_yield = 0;
    while (status.ok() && !aborted_.load(std::memory_order_acquire)) {
      auto batch = co_await storage_->ReadRdbSnapshotBatch(
          session_id_, cursor, 1, 8ULL * 1024 * 1024);
      if (!batch.ok()) {
        status = batch.status();
        break;
      }
      cursor = batch->cursor_;
      for (storage::RdbSnapshotValue& value : batch->values_) {
        auto fragment =
            rdb::EncodeFileEntry(value.db_id_, value.key_, value.value_);
        std::string().swap(value.value_.encoded_);
        std::string().swap(value.key_);
        if (!fragment.ok()) {
          status = fragment.status();
          break;
        }
        while (!TryPush(&*fragment)) {
          if (aborted_.load(std::memory_order_acquire)) break;
          absl::Status yielded = co_await celer::SleepFor(
              *celer::ThisWorker().self_, std::chrono::milliseconds(1));
          if (!yielded.ok()) {
            status = yielded;
            break;
          }
        }
        if (!status.ok() || aborted_.load(std::memory_order_acquire)) break;
      }
      if (!status.ok() || batch->done_) break;
      if (++reads_since_yield == 64) {
        reads_since_yield = 0;
        co_await celer::Yield(*celer::ThisWorker().self_);
      }
    }
    absl::Status ended = co_await storage_->EndRdbSnapshot(session_id_);
    if (status.ok() && !aborted_.load(std::memory_order_acquire)) {
      status = std::move(ended);
    }
    if (!status.ok()) Fail(status);
    remaining_.fetch_sub(1, std::memory_order_acq_rel);
    co_return absl::OkStatus();
  }

  void Fail(absl::Status status) {
    if (status.ok()) return;
    bool expected = false;
    if (failed_.compare_exchange_strong(expected, true,
                                        std::memory_order_acq_rel,
                                        std::memory_order_acquire)) {
      (void)failures_.enqueue(std::move(status));
    }
  }

  static constexpr std::size_t kMaximumQueuedBytes = 64ULL * 1024 * 1024;
  storage::StorageEngine* storage_;
  std::uint64_t session_id_;
  moodycamel::ConcurrentQueue<std::string> queue_;
  std::atomic<std::size_t> queued_bytes_{0};
  moodycamel::ConcurrentQueue<absl::Status> failures_;
  std::atomic<unsigned> remaining_{0};
  std::atomic<bool> failed_{false};
  std::atomic<bool> aborted_{false};
};

struct RedisExportEvent {
  unsigned worker_ = 0;
  storage::ReplicationEventKind kind_ =
      storage::ReplicationEventKind::kMutation;
  storage::ReplicationLogCursor next_{};
  ReplicatedCommand command_;
};

constexpr std::string_view kRedisExportBacklogGapMessage =
    "Redis export cursor fell behind the online-write backlog";

struct RedisExportTransaction {
  std::uint64_t id_ = 0;
  std::vector<unsigned> participants_;
  unsigned payload_flow_ = 0;
  bool has_payload_ = false;
  ReplicatedCommand command_;
};

struct RedisExportBacklogState {
  enum class Phase : std::uint8_t {
    kFill,
    kMutation,
    kTransaction,
    kControl,
    kIdle,
  };
  explicit RedisExportBacklogState(
      std::vector<storage::ReplicationLogCursor> initial)
      : cursors_(std::move(initial)), heads_(cursors_.size()) {}

  std::vector<storage::ReplicationLogCursor> cursors_;
  std::vector<std::optional<RedisExportEvent>> heads_;
  unsigned next_worker_ = 0;
  std::uint8_t selected_db_ = 0;
  std::chrono::steady_clock::time_point last_write_ =
      std::chrono::steady_clock::now();
  Phase phase_ = Phase::kFill;
};

Task<absl::StatusOr<std::optional<RedisExportEvent>>> ReadLocalRedisExportEvent(
    storage::StorageEngine* storage, unsigned worker,
    storage::ReplicationLogCursor cursor) {
  std::string encoded;
  std::uint64_t lsn = 0;
  storage::ReplicationEventKind kind = storage::ReplicationEventKind::kMutation;
  std::uint32_t next_fragment = 0;
  while (true) {
    auto batch = co_await storage->ReadReplicationLog(
        cursor, storage::kReplicationTransferBytes, 1);
    if (!batch.ok()) {
      // Only an online-write backlog gap is classified as a slow Redis
      // replica. RDB producer pressure is handled separately by suspending the
      // snapshot scanner until the bounded MPSC queue has room.
      if (batch.status().code() == absl::StatusCode::kOutOfRange) {
        co_return absl::ResourceExhaustedError(kRedisExportBacklogGapMessage);
      }
      co_return batch.status();
    }
    if (batch->frames_.empty()) {
      if (!encoded.empty()) {
        co_return absl::InternalError(
            "Redis export observed a truncated replication event");
      }
      co_return std::optional<RedisExportEvent>{};
    }
    const storage::ReplicationLogFrame& frame = batch->frames_.front();
    const bool first =
        (frame.header_.flags_ &
         static_cast<std::uint8_t>(storage::ReplicationFrameFlag::kFirst)) != 0;
    const bool last =
        (frame.header_.flags_ &
         static_cast<std::uint8_t>(storage::ReplicationFrameFlag::kLast)) != 0;
    if (encoded.empty()) {
      if (!first || frame.header_.fragment_index_ != 0) {
        co_return absl::InternalError(
            "Redis export cursor did not start at an event boundary");
      }
      lsn = frame.header_.lsn_;
      kind = frame.header_.kind_;
    }
    if (frame.header_.lsn_ != lsn ||
        frame.header_.fragment_index_ != next_fragment ||
        frame.header_.kind_ != kind || (next_fragment != 0 && first)) {
      co_return absl::InternalError(
          "Redis export replication fragments are out of order");
    }
    absl::Status appended = AppendReplicationString(&encoded, frame.payload_);
    if (!appended.ok()) co_return appended;
    cursor = batch->next_;
    ++next_fragment;
    if (!last) continue;
    auto command = DecodeReplicationCommand(encoded);
    if (!command.ok()) co_return command.status();
    co_return std::optional<RedisExportEvent>(RedisExportEvent{
        .worker_ = worker,
        .kind_ = kind,
        .next_ = cursor,
        .command_ = std::move(*command),
    });
  }
}

absl::StatusOr<RedisExportTransaction> ParseRedisExportTransaction(
    const RedisExportEvent& event) {
  const auto& args = event.command_.args_;
  if (args.empty() || !IsReplicationTransactionEnvelope(args[0])) {
    return absl::InvalidArgumentError(
        "malformed Redis export transaction envelope");
  }
  auto metadata = DecodeReplicationTransactionEnvelope(args[0]);
  if (!metadata.ok()) return metadata.status();
  RedisExportTransaction transaction;
  transaction.id_ = metadata->id_;
  transaction.command_.db_id_ = event.command_.db_id_;
  transaction.payload_flow_ = metadata->payload_flow_;
  transaction.participants_ = std::move(metadata->participants_);
  transaction.has_payload_ = args.size() > 1;
  const bool event_is_payload = event.worker_ == transaction.payload_flow_;
  if (event_is_payload != transaction.has_payload_) {
    return absl::InvalidArgumentError(
        "Redis export transaction payload arrived on the wrong flow");
  }
  transaction.command_.args_.assign(args.begin() + 1, args.end());
  return transaction;
}

absl::StatusOr<std::string> EncodeRedisExportCommand(
    const ReplicatedCommand& command, bool transactional,
    std::uint8_t* selected_db) {
  if (command.args_.empty()) {
    return absl::InvalidArgumentError("empty Redis export command");
  }
  struct Child {
    std::uint8_t db_ = 0;
    std::vector<std::string> args_;
  };
  std::vector<Child> children;
  if (command.args_[0] == kReplicatedExecCommand) {
    if (command.args_.size() < 2) {
      return absl::InvalidArgumentError("truncated replicated EXEC");
    }
    unsigned count = 0;
    if (!ParseUnsigned(command.args_[1], &count) || count == 0) {
      return absl::InvalidArgumentError("invalid replicated EXEC count");
    }
    std::size_t position = 2;
    children.reserve(count);
    for (unsigned index = 0; index < count; ++index) {
      unsigned db = 0;
      unsigned argc = 0;
      if (position + 2 > command.args_.size() ||
          !ParseUnsigned(command.args_[position], &db) ||
          db >= storage::kLogicalDatabaseCount ||
          !ParseUnsigned(command.args_[position + 1], &argc) || argc == 0 ||
          position + 2 + argc > command.args_.size()) {
        return absl::InvalidArgumentError("malformed replicated EXEC child");
      }
      position += 2;
      Child child{.db_ = static_cast<std::uint8_t>(db), .args_ = {}};
      child.args_.assign(command.args_.begin() + position,
                         command.args_.begin() + position + argc);
      position += argc;
      children.push_back(std::move(child));
    }
    if (position != command.args_.size()) {
      return absl::InvalidArgumentError("trailing replicated EXEC arguments");
    }
    transactional = true;
  } else {
    children.push_back(Child{.db_ = command.db_id_, .args_ = command.args_});
  }

  std::string output;
  std::uint8_t current_db = *selected_db;
  if (current_db != children.front().db_) {
    output += EncodeRespCommand(std::vector<std::string>{
        "SELECT", std::to_string(children.front().db_)});
    current_db = children.front().db_;
  }
  if (transactional) {
    output += EncodeRespCommand(std::vector<std::string>{"MULTI"});
  }
  for (const Child& child : children) {
    if (current_db != child.db_) {
      output += EncodeRespCommand(
          std::vector<std::string>{"SELECT", std::to_string(child.db_)});
      current_db = child.db_;
    }
    output += EncodeRespCommand(child.args_);
  }
  if (transactional) {
    output += EncodeRespCommand(std::vector<std::string>{"EXEC"});
  }
  *selected_db = current_db;
  return output;
}

Task<absl::Status> FillRedisExportHeads(storage::StorageEngine* storage,
                                        RedisExportBacklogState* state) {
  for (unsigned worker = 0; worker < state->heads_.size(); ++worker) {
    if (state->heads_[worker].has_value()) continue;
    auto event = co_await celer::SubmitTaskTo(
        worker, [storage, worker, cursor = state->cursors_[worker]] {
          return ReadLocalRedisExportEvent(storage, worker, cursor);
        });
    if (!event.ok()) co_return event.status();
    if (event->has_value()) state->heads_[worker] = std::move(**event);
  }
  // Ready cross-worker barriers take priority over unrelated local mutations;
  // otherwise a continuously busy worker could starve a transaction forever.
  state->phase_ = RedisExportBacklogState::Phase::kTransaction;
  co_return absl::OkStatus();
}

Task<absl::Status> AdvanceRedisExportCursor(
    storage::StorageEngine* storage, bool backpressure, unsigned worker,
    std::uint64_t session_id, storage::ReplicationLogCursor cursor) {
  if (!backpressure) co_return absl::OkStatus();
  co_return co_await celer::SubmitTo(worker, [storage, session_id, cursor] {
    return storage->RetainReplicationLog(session_id, cursor.lsn_);
  });
}

Task<absl::Status> SendRedisExportMutation(TcpStream& stream,
                                           storage::StorageEngine* storage,
                                           bool backpressure,
                                           std::uint64_t session_id,
                                           RedisExportBacklogState* state) {
  const unsigned workers = state->heads_.size();
  for (unsigned offset = 0; offset < workers; ++offset) {
    const unsigned worker = (state->next_worker_ + offset) % workers;
    if (!state->heads_[worker].has_value() ||
        (state->heads_[worker]->kind_ !=
             storage::ReplicationEventKind::kMutation &&
         state->heads_[worker]->kind_ !=
             storage::ReplicationEventKind::kEphemeral)) {
      continue;
    }
    if (!state->heads_[worker]->command_.args_.empty() &&
        IsReplicationTransactionEnvelope(
            state->heads_[worker]->command_.args_[0])) {
      co_return absl::InternalError(
          "transaction envelope was journaled as a mutation");
    }
    auto encoded = EncodeRedisExportCommand(state->heads_[worker]->command_,
                                            false, &state->selected_db_);
    if (!encoded.ok()) co_return encoded.status();
    absl::Status sent = co_await WriteText(stream, *encoded);
    if (!sent.ok()) co_return sent;
    state->last_write_ = std::chrono::steady_clock::now();
    state->cursors_[worker] = state->heads_[worker]->next_;
    state->heads_[worker].reset();
    state->next_worker_ = (worker + 1) % workers;
    absl::Status advanced = co_await AdvanceRedisExportCursor(
        storage, backpressure, worker, session_id, state->cursors_[worker]);
    if (!advanced.ok()) co_return advanced;
    state->phase_ = RedisExportBacklogState::Phase::kFill;
    co_return absl::OkStatus();
  }
  state->phase_ = RedisExportBacklogState::Phase::kIdle;
  co_return absl::NotFoundError("no Redis export mutation is ready");
}

Task<absl::Status> SendRedisExportTransaction(TcpStream& stream,
                                              storage::StorageEngine* storage,
                                              bool backpressure,
                                              std::uint64_t session_id,
                                              RedisExportBacklogState* state) {
  const unsigned workers = state->heads_.size();
  for (unsigned offset = 0; offset < workers; ++offset) {
    const unsigned worker = (state->next_worker_ + offset) % workers;
    if (!state->heads_[worker].has_value() ||
        state->heads_[worker]->kind_ !=
            storage::ReplicationEventKind::kTransaction) {
      continue;
    }
    auto transaction = ParseRedisExportTransaction(*state->heads_[worker]);
    if (!transaction.ok()) co_return transaction.status();
    bool ready = true;
    std::vector<std::uint8_t> included(workers, 0);
    std::optional<ReplicatedCommand> payload;
    for (unsigned participant : transaction->participants_) {
      if (participant >= workers || included[participant]) {
        co_return absl::InvalidArgumentError(
            "invalid Redis export transaction participant set");
      }
      included[participant] = 1;
      if (!state->heads_[participant].has_value() ||
          state->heads_[participant]->kind_ !=
              storage::ReplicationEventKind::kTransaction) {
        ready = false;
        continue;
      }
      auto peer = ParseRedisExportTransaction(*state->heads_[participant]);
      if (!peer.ok()) co_return peer.status();
      if (peer->id_ != transaction->id_) {
        ready = false;
        continue;
      }
      if (peer->participants_ != transaction->participants_ ||
          peer->payload_flow_ != transaction->payload_flow_ ||
          peer->command_.db_id_ != transaction->command_.db_id_) {
        co_return absl::InvalidArgumentError(
            "conflicting Redis export transaction envelope");
      }
      if (peer->has_payload_) {
        if (payload.has_value()) {
          co_return absl::InvalidArgumentError(
              "duplicate Redis export transaction payload");
        }
        payload = peer->command_;
      }
    }
    if (!included[worker]) {
      co_return absl::InvalidArgumentError(
          "transaction does not name its source worker");
    }
    if (!ready) continue;
    if (!payload.has_value()) {
      co_return absl::InvalidArgumentError(
          "Redis export transaction payload is missing");
    }
    auto encoded =
        EncodeRedisExportCommand(*payload, true, &state->selected_db_);
    if (!encoded.ok()) co_return encoded.status();
    absl::Status sent = co_await WriteText(stream, *encoded);
    if (!sent.ok()) co_return sent;
    state->last_write_ = std::chrono::steady_clock::now();
    for (unsigned participant : transaction->participants_) {
      state->cursors_[participant] = state->heads_[participant]->next_;
      state->heads_[participant].reset();
      absl::Status advanced = co_await AdvanceRedisExportCursor(
          storage, backpressure, participant, session_id,
          state->cursors_[participant]);
      if (!advanced.ok()) co_return advanced;
    }
    state->next_worker_ = (worker + 1) % workers;
    state->phase_ = RedisExportBacklogState::Phase::kFill;
    co_return absl::OkStatus();
  }
  state->phase_ = RedisExportBacklogState::Phase::kControl;
  co_return absl::NotFoundError("no Redis export transaction is ready");
}

Task<absl::Status> SendRedisExportControl(TcpStream& stream,
                                          storage::StorageEngine* storage,
                                          bool backpressure,
                                          std::uint64_t session_id,
                                          RedisExportBacklogState* state) {
  const unsigned workers = state->heads_.size();
  for (unsigned worker = 0; worker < workers; ++worker) {
    if (!state->heads_[worker].has_value() ||
        state->heads_[worker]->kind_ !=
            storage::ReplicationEventKind::kControl) {
      continue;
    }
    const auto& args = state->heads_[worker]->command_.args_;
    if (args.size() < 2 || (args[0] != "FLUSHDB" && args[0] != "FLUSHALL")) {
      co_return absl::InvalidArgumentError(
          "unsupported Redis export control event");
    }
    bool ready = true;
    for (unsigned peer = 0; peer < workers; ++peer) {
      if (!state->heads_[peer].has_value() ||
          state->heads_[peer]->kind_ !=
              storage::ReplicationEventKind::kControl ||
          state->heads_[peer]->command_.args_.size() < 2 ||
          state->heads_[peer]->command_.args_[0] != args[0] ||
          state->heads_[peer]->command_.args_[1] != args[1]) {
        ready = false;
        break;
      }
    }
    if (!ready) continue;
    ReplicatedCommand control{.db_id_ = state->heads_[worker]->command_.db_id_,
                              .args_ = {args[0]}};
    auto encoded =
        EncodeRedisExportCommand(control, false, &state->selected_db_);
    if (!encoded.ok()) co_return encoded.status();
    absl::Status sent = co_await WriteText(stream, *encoded);
    if (!sent.ok()) co_return sent;
    state->last_write_ = std::chrono::steady_clock::now();
    for (unsigned peer = 0; peer < workers; ++peer) {
      state->cursors_[peer] = state->heads_[peer]->next_;
      state->heads_[peer].reset();
      absl::Status advanced = co_await AdvanceRedisExportCursor(
          storage, backpressure, peer, session_id, state->cursors_[peer]);
      if (!advanced.ok()) co_return advanced;
    }
    state->phase_ = RedisExportBacklogState::Phase::kFill;
    co_return absl::OkStatus();
  }
  state->phase_ = RedisExportBacklogState::Phase::kMutation;
  co_return absl::NotFoundError("no Redis export control is ready");
}

Task<absl::Status> PingRedisExportBacklog(TcpStream& stream,
                                          RedisExportBacklogState* state) {
  absl::Status status = co_await WriteText(stream, "*1\r\n$4\r\nPING\r\n");
  if (status.ok()) state->last_write_ = std::chrono::steady_clock::now();
  if (status.ok()) state->phase_ = RedisExportBacklogState::Phase::kFill;
  co_return status;
}

Task<absl::Status> SleepRedisExportBacklog(RedisExportBacklogState* state) {
  absl::Status status = co_await celer::SleepFor(*celer::ThisWorker().self_,
                                                 std::chrono::milliseconds(1));
  if (status.ok()) state->phase_ = RedisExportBacklogState::Phase::kFill;
  co_return status;
}

Task<absl::Status> IdleRedisExportBacklog(TcpStream& stream,
                                          RedisExportBacklogState* state) {
  if (std::chrono::steady_clock::now() - state->last_write_ >=
      std::chrono::seconds(1)) {
    return PingRedisExportBacklog(stream, state);
  }
  return SleepRedisExportBacklog(state);
}

Task<absl::Status> DispatchRedisExportBacklogPhase(
    TcpStream& stream, storage::StorageEngine* storage, bool backpressure,
    std::uint64_t session_id, RedisExportBacklogState* state) {
  switch (state->phase_) {
    case RedisExportBacklogState::Phase::kFill:
      return FillRedisExportHeads(storage, state);
    case RedisExportBacklogState::Phase::kMutation:
      return SendRedisExportMutation(stream, storage, backpressure, session_id,
                                     state);
    case RedisExportBacklogState::Phase::kTransaction:
      return SendRedisExportTransaction(stream, storage, backpressure,
                                        session_id, state);
    case RedisExportBacklogState::Phase::kControl:
      return SendRedisExportControl(stream, storage, backpressure, session_id,
                                    state);
    case RedisExportBacklogState::Phase::kIdle:
      return IdleRedisExportBacklog(stream, state);
  }
  return []() -> Task<absl::Status> {
    co_return absl::InternalError("invalid Redis export backlog phase");
  }();
}

Task<absl::Status> RunRedisExportBacklogLoop(TcpStream& stream,
                                             storage::StorageEngine* storage,
                                             bool backpressure,
                                             std::uint64_t session_id,
                                             RedisExportBacklogState* state) {
  while (stream.IsOpen()) {
    absl::Status iteration = co_await DispatchRedisExportBacklogPhase(
        stream, storage, backpressure, session_id, state);
    if (!iteration.ok() && iteration.code() != absl::StatusCode::kNotFound) {
      co_return iteration;
    }
  }
  co_return absl::UnavailableError("Redis export connection closed");
}

struct RedisPsyncReply {
  bool full_ = false;
  std::optional<std::string> replid_;
  std::uint64_t offset_ = 0;
};

bool IsReplicationId(std::string_view value);

absl::StatusOr<RedisPsyncReply> ParseRedisPsyncReply(std::string_view line) {
  const std::vector<std::string_view> words = SplitWords(line);
  if (words.empty()) {
    return absl::InvalidArgumentError("empty Redis PSYNC response");
  }
  if (words[0] == "+FULLRESYNC") {
    RedisPsyncReply reply;
    reply.full_ = true;
    if (words.size() != 3 || !IsReplicationId(words[1]) ||
        !ParseUnsigned(words[2], &reply.offset_)) {
      return absl::InvalidArgumentError(
          "invalid FULLRESYNC response from Redis");
    }
    reply.replid_ = std::string(words[1]);
    return reply;
  }
  if (words[0] == "+CONTINUE") {
    RedisPsyncReply reply;
    if (words.size() == 2) {
      if (!IsReplicationId(words[1])) {
        return absl::InvalidArgumentError(
            "invalid replid in Redis CONTINUE response");
      }
      reply.replid_ = std::string(words[1]);
    } else if (words.size() != 1) {
      return absl::InvalidArgumentError("invalid CONTINUE response from Redis");
    }
    return reply;
  }
  return absl::FailedPreconditionError(
      absl::StrCat("Redis PSYNC failed: ", line));
}

Task<absl::Status> AuthenticateUpstream(TcpStream& stream,
                                        std::string_view username,
                                        std::string_view password) {
  if (password.empty()) co_return absl::OkStatus();
  std::vector<std::string> args{"AUTH"};
  if (username != "default") args.emplace_back(username);
  args.emplace_back(password);
  absl::Status sent = co_await WriteText(stream, EncodeRespCommand(args));
  if (!sent.ok()) co_return sent;
  auto response = co_await ReadLine(stream);
  if (!response.ok()) co_return response.status();
  if (*response != "+OK") {
    co_return absl::PermissionDeniedError(
        absl::StrCat("replication AUTH failed: ", *response));
  }
  co_return absl::OkStatus();
}

Task<absl::Status> WaitForClose(TcpStream& stream) {
  std::array<std::byte, 1024> input{};
  while (stream.IsOpen()) {
    auto read = co_await stream.ReadSome(input);
    if (!read.ok()) {
      if (read.status().code() == absl::StatusCode::kFailedPrecondition ||
          read.status().code() == absl::StatusCode::kCancelled) {
        co_return absl::OkStatus();
      }
      co_return read.status();
    }
    if (*read == 0) break;
    // Items 1/2 establish connection ownership only. Data frames are added by
    // the snapshot/backlog slice; bytes before then are a protocol violation.
    co_return absl::InvalidArgumentError(
        "unexpected bytes on idle replication connection");
  }
  co_return absl::OkStatus();
}

absl::Status ConfigureConnectedFd(int fd) {
  int one = 1;
  if (::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) != 0) {
    return absl::InternalError("setsockopt(TCP_NODELAY) failed");
  }
  const int flags = ::fcntl(fd, F_GETFL, 0);
  if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0) {
    return absl::InternalError("fcntl(O_NONBLOCK) failed");
  }
  return absl::OkStatus();
}

Task<absl::StatusOr<TcpStream>> ConnectTcp(
    std::string_view host, std::uint16_t port,
    const std::shared_ptr<celer::TlsContext>& tls_context) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  addrinfo* addresses = nullptr;
  const std::string service = std::to_string(port);
  const int resolved = ::getaddrinfo(std::string(host).c_str(), service.c_str(),
                                     &hints, &addresses);
  if (resolved != 0) {
    co_return absl::UnavailableError(absl::StrCat(
        "cannot resolve replication upstream: ", ::gai_strerror(resolved)));
  }

  int connected_fd = -1;
  for (addrinfo* address = addresses; address != nullptr;
       address = address->ai_next) {
    const int fd =
        ::socket(address->ai_family, address->ai_socktype | SOCK_CLOEXEC,
                 address->ai_protocol);
    if (fd < 0) continue;
    // Cold-path connect. Once registered, all reads and writes use io_uring.
    if (::connect(fd, address->ai_addr, address->ai_addrlen) == 0) {
      connected_fd = fd;
      break;
    }
    ::close(fd);
  }
  ::freeaddrinfo(addresses);
  if (connected_fd < 0) {
    co_return absl::UnavailableError("replication connect failed");
  }

  absl::Status configured = ConfigureConnectedFd(connected_fd);
  if (!configured.ok()) {
    ::close(connected_fd);
    co_return configured;
  }
  Connection connection;
  connection.worker_ = celer::ThisWorker().self_;
  connection.file_.fd_ = connected_fd;
  connection.closed_ = false;
  Connection* registered =
      celer::ThisWorker().self_->AddConnection(std::move(connection));
  if (registered == nullptr) {
    ::close(connected_fd);
    co_return absl::InternalError("failed to register replication connection");
  }
  TcpStream stream(registered);
  if (tls_context != nullptr) {
    absl::Status started = co_await stream.StartTls(tls_context, false, host);
    if (!started.ok()) {
      stream.Close().IgnoreError();
      co_return started;
    }
  }
  co_return stream;
}

std::string NewReplicationId() {
  std::random_device random;
  std::mt19937_64 generator((static_cast<std::uint64_t>(random()) << 32) ^
                            random() ^ static_cast<std::uint64_t>(::getpid()));
  constexpr char digits[] = "0123456789abcdef";
  std::string result(40, '0');
  for (char& value : result) value = digits[generator() & 0xfU];
  return result;
}

bool IsReplicationId(std::string_view value) {
  return value.size() == 40 &&
         std::all_of(value.begin(), value.end(), [](unsigned char digit) {
           return (digit >= '0' && digit <= '9') ||
                  (digit >= 'a' && digit <= 'f');
         });
}

std::string PeerHost(int fd) {
  sockaddr_storage address{};
  socklen_t size = sizeof(address);
  if (::getpeername(fd, reinterpret_cast<sockaddr*>(&address), &size) != 0) {
    return {};
  }
  char host[NI_MAXHOST]{};
  if (::getnameinfo(reinterpret_cast<const sockaddr*>(&address), size, host,
                    sizeof(host), nullptr, 0, NI_NUMERICHOST) != 0) {
    return {};
  }
  return host;
}

class SocketSet {
 public:
  bool Add(int fd) {
    std::lock_guard lock(mutex_);
    if (cancelled_) {
      ::shutdown(fd, SHUT_RDWR);
      return false;
    }
    fds_.push_back(fd);
    return true;
  }

  void Remove(int fd) {
    std::lock_guard lock(mutex_);
    const auto found = std::find(fds_.begin(), fds_.end(), fd);
    if (found != fds_.end()) fds_.erase(found);
  }

  void Cancel() {
    std::lock_guard lock(mutex_);
    if (cancelled_) return;
    cancelled_ = true;
    for (int fd : fds_) ::shutdown(fd, SHUT_RDWR);
  }

  bool cancelled() const {
    std::lock_guard lock(mutex_);
    return cancelled_;
  }

 private:
  mutable std::mutex mutex_;
  std::vector<int> fds_;
  bool cancelled_ = false;
};

struct ReplicaCursorState {
  explicit ReplicaCursorState(unsigned count) : cursors_(count) {
    for (auto& cursor : cursors_) {
      cursor = {.lsn_ = 1, .fragment_index_ = 0};
    }
    total_lsn_.store(count, std::memory_order_relaxed);
  }

  storage::ReplicationLogCursor Load(unsigned flow_id) const noexcept {
    if (flow_id >= cursors_.size()) return {};
    return cursors_[flow_id];
  }

  void Store(unsigned flow_id, std::uint64_t lsn,
             std::uint32_t fragment) noexcept {
    if (flow_id >= cursors_.size()) return;
    const std::uint64_t previous = cursors_[flow_id].lsn_;
    cursors_[flow_id] = {.lsn_ = lsn, .fragment_index_ = fragment};
    if (lsn >= previous) {
      total_lsn_.fetch_add(lsn - previous, std::memory_order_relaxed);
    } else {
      total_lsn_.fetch_sub(previous - lsn, std::memory_order_relaxed);
    }
  }

  std::size_t size() const noexcept { return cursors_.size(); }
  std::uint64_t total_lsn() const noexcept {
    return total_lsn_.load(std::memory_order_relaxed);
  }

  std::vector<storage::ReplicationLogCursor> cursors_;
  std::atomic<std::uint64_t> total_lsn_{0};
};

// Transaction apply is detached from flow staging, so both participant ACK
// tasks and later dependency tasks may begin waiting after completion. This
// one-shot latch closes that race and resumes each coroutine on its owner
// worker without polling or blocking a runtime thread.
class ReplicaCompletionLatch {
  struct Waiter {
    celer::Worker* worker_ = nullptr;
    std::coroutine_handle<> handle_{};
    Waiter* next_ = nullptr;
  };

 public:
  class Awaiter {
   public:
    Awaiter(ReplicaCompletionLatch* latch, celer::Worker* worker) noexcept
        : latch_(latch) {
      waiter_.worker_ = worker;
    }

    bool await_ready() const noexcept {
      return latch_->resolution() !=
             storage::ReplicationTransactionResolution::kPending;
    }
    bool await_suspend(std::coroutine_handle<> awaiting) noexcept {
      return latch_->Register(&waiter_, awaiting);
    }
    storage::ReplicationTransactionResolution await_resume() const noexcept {
      return latch_->resolution();
    }

   private:
    ReplicaCompletionLatch* latch_ = nullptr;
    Waiter waiter_;
  };

  Awaiter Wait(celer::Worker& worker) noexcept {
    return Awaiter(this, &worker);
  }

  storage::ReplicationTransactionResolution resolution() const noexcept {
    return resolution_.load(std::memory_order_acquire);
  }

  bool ResolveOnce(
      storage::ReplicationTransactionResolution resolution) noexcept {
    if (resolution == storage::ReplicationTransactionResolution::kPending) {
      return false;
    }
    Lock();
    storage::ReplicationTransactionResolution expected =
        storage::ReplicationTransactionResolution::kPending;
    if (!resolution_.compare_exchange_strong(expected, resolution,
                                             std::memory_order_release,
                                             std::memory_order_relaxed)) {
      Unlock();
      return false;
    }
    Waiter* wake = waiters_head_;
    waiters_head_ = nullptr;
    waiters_tail_ = nullptr;
    Unlock();

    const celer::CurrentWorker& current = celer::ThisWorker();
    while (wake != nullptr) {
      Waiter* waiter = wake;
      wake = wake->next_;
      if (waiter->worker_->id() == current.id_) {
        waiter->worker_->Enqueue(waiter->handle_);
      } else {
        celer::PostNotification(
            current.cross_core_, waiter->worker_->id(),
            celer::RemoteNotification{
                .context_ = waiter->worker_,
                .value_ = static_cast<std::uint64_t>(
                    reinterpret_cast<std::uintptr_t>(
                        waiter->handle_.address())),
                .run_fn_ = &ReplicaCompletionLatch::ResumeRemote,
            });
      }
    }
    return true;
  }

 private:
  static void ResumeRemote(void* context, std::uint64_t value) noexcept {
    static_cast<celer::Worker*>(context)->Enqueue(
        std::coroutine_handle<>::from_address(
            reinterpret_cast<void*>(static_cast<std::uintptr_t>(value))));
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

  bool Register(Waiter* waiter,
                std::coroutine_handle<> awaiting) noexcept {
    Lock();
    if (resolution_.load(std::memory_order_acquire) !=
        storage::ReplicationTransactionResolution::kPending) {
      Unlock();
      return false;
    }
    waiter->handle_ = awaiting;
    waiter->next_ = nullptr;
    if (waiters_tail_ == nullptr) {
      waiters_head_ = waiter;
    } else {
      waiters_tail_->next_ = waiter;
    }
    waiters_tail_ = waiter;
    Unlock();
    return true;
  }

  std::atomic<storage::ReplicationTransactionResolution> resolution_{
      storage::ReplicationTransactionResolution::kPending};
  std::atomic_flag lock_ = ATOMIC_FLAG_INIT;
  Waiter* waiters_head_ = nullptr;
  Waiter* waiters_tail_ = nullptr;
};

struct ReplicaTransactionArrival {
  std::uint64_t id_ = 0;
  std::uint8_t db_id_ = 0;
  std::vector<unsigned> participants_;
  std::vector<std::string> command_args_;
  // One predecessor per distinct participant-flow tail is sufficient to
  // preserve the source partial order. Transactions with disjoint flow sets
  // deliberately have no dependency and may apply concurrently.
  std::vector<std::shared_ptr<ReplicaTransactionArrival>> predecessors_;
  unsigned payload_flow_ = 0;
  bool payload_arrived_ = false;
  std::vector<bool> arrived_;
  std::vector<std::uint64_t> lsns_;
  std::size_t arrival_count_ = 0;
  bool applying_ = false;
  absl::Status status_ =
      absl::UnknownError("replicated transaction has not completed");
  // This cross-worker latch is producer-resolved rather than arrival-counted:
  // flow staging may register later transactions before ACK consumers wait on
  // earlier ones, and dependency tasks also need to observe completion.
  ReplicaCompletionLatch completion_;
};

struct PreparedReplicaTransactionArrival {
  std::uint64_t id_ = 0;
  std::uint8_t db_id_ = 0;
  std::vector<unsigned> participants_;
  std::vector<std::string> command_args_;
  std::shared_ptr<ReplicaTransactionArrival> predecessor_;
  unsigned payload_flow_ = 0;
  unsigned flow_id_ = 0;
  std::uint64_t lsn_ = 0;
  bool has_payload_ = false;
};

struct ReplicaControlArrival {
  explicit ReplicaControlArrival(unsigned flow_count)
      : completion_(flow_count) {}

  ReplicatedCommand command_;
  std::vector<bool> arrived_;
  std::vector<std::uint64_t> lsns_;
  std::size_t arrival_count_ = 0;
  std::size_t departure_count_ = 0;
  bool applying_ = false;
  absl::Status status_ =
      absl::UnknownError("replicated control barrier has not completed");
  celer::CoroutineBarrier completion_;
};

struct ReplicaTransactionOwner {
  // This table is initialized before flow startup and thereafter touched only
  // by the worker whose id indexes the owner vector. Cross-worker flow
  // coroutines submit registrations to that worker instead of sharing a lock.
  absl::flat_hash_map<std::uint64_t, std::shared_ptr<ReplicaTransactionArrival>>
      transactions_;
};

struct ReplicaSession {
  std::uint64_t session_id_ = 0;
  unsigned source_worker_count_ = 0;
  std::shared_ptr<ReplicaCursorState> cursors_;
  std::unique_ptr<celer::CoroutineBarrier> fullsync_cut_;
  std::unique_ptr<celer::CoroutineBarrier> root_swap_complete_;
  // Flow coroutines are detached onto their owner workers. Track their whole
  // lifetime, including connect/handshake and storage apply, so a failed
  // session cannot start a replacement while old flows are still mutating
  // replica storage or holding network buffers.
  std::atomic<unsigned> active_flows_{0};
  std::atomic<unsigned> active_transaction_applies_{0};
  std::atomic<unsigned> connected_flows_{0};
  SocketSet sockets_;
  std::atomic<bool> cancelled_{false};
  std::vector<std::unique_ptr<ReplicaTransactionOwner>> transaction_owners_;
  std::mutex control_mutex_;
  absl::flat_hash_map<std::uint64_t, std::shared_ptr<ReplicaControlArrival>>
      controls_;

  void Cancel() {
    if (cancelled_.exchange(true, std::memory_order_acq_rel)) return;
    sockets_.Cancel();
    std::vector<std::shared_ptr<ReplicaControlArrival>> controls;
    {
      std::lock_guard lock(control_mutex_);
      controls.reserve(controls_.size());
      for (auto& [_, arrival] : controls_) {
        controls.push_back(std::move(arrival));
      }
      controls_.clear();
    }
    const absl::Status cancelled =
        absl::CancelledError("replication session cancelled");
    for (const auto& arrival : controls) {
      arrival->completion_.Abort(cancelled);
    }
    if (fullsync_cut_ != nullptr) fullsync_cut_->Abort(cancelled);
    if (root_swap_complete_ != nullptr) root_swap_complete_->Abort(cancelled);
  }

  bool cancelled() const noexcept {
    return cancelled_.load(std::memory_order_acquire);
  }
};

class ReplicaFlowActivityGuard {
 public:
  explicit ReplicaFlowActivityGuard(std::atomic<unsigned>* active)
      : active_(active) {}
  ReplicaFlowActivityGuard(const ReplicaFlowActivityGuard&) = delete;
  ReplicaFlowActivityGuard& operator=(const ReplicaFlowActivityGuard&) = delete;
  ~ReplicaFlowActivityGuard() {
    active_->fetch_sub(1, std::memory_order_acq_rel);
  }

 private:
  std::atomic<unsigned>* active_;
};

enum class ReplicationPhase : std::uint8_t {
  kConnecting,
  kReset,
  kSnapshot,
  kOverrideCatchup,
  kBacklog,
  kReady,
  kFailed,
};

std::string_view ReplicationPhaseName(ReplicationPhase phase) noexcept {
  switch (phase) {
    case ReplicationPhase::kConnecting:
      return "connecting";
    case ReplicationPhase::kReset:
      return "reset";
    case ReplicationPhase::kSnapshot:
      return "snapshot";
    case ReplicationPhase::kOverrideCatchup:
      return "override_catchup";
    case ReplicationPhase::kBacklog:
      return "backlog";
    case ReplicationPhase::kReady:
      return "ready";
    case ReplicationPhase::kFailed:
      return "failed";
  }
  return "unknown";
}

struct ReplicaFlowProgress {
  std::atomic<ReplicationPhase> phase_{ReplicationPhase::kConnecting};
  // The next backlog frame required by this replica. Keeping a next cursor,
  // rather than the last ACK itself, makes the value directly usable by both
  // partial resync and shared-backlog retention.
  std::atomic<std::uint64_t> lsn_{1};
  std::atomic<std::uint32_t> fragment_index_{0};
  std::atomic<std::uint16_t> current_partition_{0};
  std::atomic<std::uint64_t> partition_sequence_{0};
  std::atomic<std::uint64_t> activity_generation_{1};
};

struct ReplicaFlowProgressSnapshot {
  ReplicationPhase phase_ = ReplicationPhase::kConnecting;
  std::uint64_t lsn_ = 1;
  std::uint32_t fragment_index_ = 0;
  std::uint16_t current_partition_ = 0;
  std::uint64_t partition_sequence_ = 0;
};

class ReplicationConnectionMetricGuard {
 public:
  explicit ReplicationConnectionMetricGuard(ReplicationConnectionKind kind)
      : kind_(kind) {
    RecordReplicationConnectionOpened(kind_);
  }
  ReplicationConnectionMetricGuard(const ReplicationConnectionMetricGuard&) =
      delete;
  ReplicationConnectionMetricGuard& operator=(
      const ReplicationConnectionMetricGuard&) = delete;
  ~ReplicationConnectionMetricGuard() {
    RecordReplicationConnectionClosed(kind_);
  }

 private:
  ReplicationConnectionKind kind_;
};

struct MasterSession {
  MasterSession(std::uint64_t id, unsigned worker_count, std::string node_id,
                std::string host, std::uint16_t port, bool allow_continue)
      : id_(id),
        node_id_(std::move(node_id)),
        host_(std::move(host)),
        port_(port),
        allow_continue_(allow_continue),
        flow_fds_(worker_count, -1),
        flows_(worker_count),
        flow_resume_possible_(worker_count, -1),
        snapshot_ready_(worker_count),
        snapshot_gate_closed_(worker_count),
        snapshot_fenced_(worker_count),
        snapshot_capture_stopped_(worker_count) {}

  bool SetControl(int fd) {
    std::lock_guard lock(mutex_);
    if (cancelled_.load(std::memory_order_acquire) || control_fd_ >= 0) {
      return false;
    }
    control_fd_ = fd;
    return true;
  }

  bool SetFlow(unsigned flow_id, int fd) {
    std::lock_guard lock(mutex_);
    if (cancelled_.load(std::memory_order_acquire) ||
        flow_id >= flow_fds_.size() || flow_fds_[flow_id] >= 0) {
      return false;
    }
    flow_fds_[flow_id] = fd;
    connected_flows_.fetch_add(1, std::memory_order_release);
    return true;
  }

  void ClearFlow(unsigned flow_id, int fd) {
    std::lock_guard lock(mutex_);
    if (flow_id < flow_fds_.size() && flow_fds_[flow_id] == fd) {
      flow_fds_[flow_id] = -1;
      connected_flows_.fetch_sub(1, std::memory_order_acq_rel);
    }
  }

  bool SetFlowResumePossible(unsigned flow_id, bool possible) {
    std::lock_guard lock(mutex_);
    if (cancelled_.load(std::memory_order_acquire) ||
        flow_id >= flow_resume_possible_.size() ||
        flow_resume_possible_[flow_id] != -1) {
      return false;
    }
    flow_resume_possible_[flow_id] = possible ? 1 : 0;
    ++flow_modes_registered_;
    all_flows_resume_possible_ &= possible;
    return true;
  }

  std::optional<bool> ContinueMode() const {
    std::lock_guard lock(mutex_);
    if (cancelled_.load(std::memory_order_acquire) ||
        flow_modes_registered_ != flow_resume_possible_.size()) {
      return std::nullopt;
    }
    return all_flows_resume_possible_;
  }

  celer::CoroutineBarrier::Awaiter WaitSnapshotReady() {
    return snapshot_ready_.Wait(*celer::ThisWorker().self_);
  }

  void MarkSnapshotScanComplete() {
    snapshot_scans_complete_.fetch_add(1, std::memory_order_acq_rel);
  }

  bool AllSnapshotScansComplete() const {
    return snapshot_scans_complete_.load(std::memory_order_acquire) ==
           flows_.size();
  }

  celer::CoroutineBarrier::Awaiter WaitSnapshotGateClosed() {
    return snapshot_gate_closed_.Wait(*celer::ThisWorker().self_);
  }

  celer::CoroutineBarrier::Awaiter WaitSnapshotFenced() {
    return snapshot_fenced_.Wait(*celer::ThisWorker().self_);
  }

  celer::CoroutineBarrier::Awaiter WaitSnapshotCaptureStopped() {
    return snapshot_capture_stopped_.Wait(*celer::ThisWorker().self_);
  }

  void AbortSnapshotCut(const absl::Status& status) {
    snapshot_ready_.Abort(status);
    snapshot_gate_closed_.Abort(status);
    snapshot_fenced_.Abort(status);
    snapshot_capture_stopped_.Abort(status);
  }

  unsigned connected_flows() const {
    return connected_flows_.load(std::memory_order_acquire);
  }

  void SetProgress(unsigned flow_id, ReplicationPhase phase, std::uint64_t lsn,
                   std::uint32_t fragment_index, std::uint16_t partition_id,
                   std::uint64_t partition_sequence) {
    if (cancelled_.load(std::memory_order_acquire) ||
        flow_id >= flows_.size()) {
      return;
    }
    ReplicaFlowProgress& progress = flows_[flow_id];
    progress.lsn_.store(lsn, std::memory_order_relaxed);
    progress.fragment_index_.store(fragment_index, std::memory_order_relaxed);
    progress.current_partition_.store(partition_id, std::memory_order_relaxed);
    progress.partition_sequence_.store(partition_sequence,
                                       std::memory_order_relaxed);
    progress.phase_.store(phase, std::memory_order_release);
    progress.activity_generation_.fetch_add(1, std::memory_order_release);
  }

  void SetBacklogCursor(unsigned flow_id, ReplicationPhase phase,
                        storage::ReplicationLogCursor cursor) {
    if (cancelled_.load(std::memory_order_acquire) ||
        flow_id >= flows_.size()) {
      return;
    }
    ReplicaFlowProgress& progress = flows_[flow_id];
    progress.lsn_.store(cursor.lsn_, std::memory_order_relaxed);
    progress.fragment_index_.store(cursor.fragment_index_,
                                   std::memory_order_relaxed);
    progress.phase_.store(phase, std::memory_order_release);
    progress.activity_generation_.fetch_add(1, std::memory_order_release);
  }

  void TouchProgress(unsigned flow_id) {
    if (cancelled_.load(std::memory_order_acquire) ||
        flow_id >= flows_.size()) {
      return;
    }
    flows_[flow_id].activity_generation_.fetch_add(1,
                                                   std::memory_order_release);
  }

  std::uint64_t ProgressGeneration(unsigned flow_id) const {
    return flow_id < flows_.size() ? flows_[flow_id].activity_generation_.load(
                                         std::memory_order_acquire)
                                   : 0;
  }

  void MarkFailed(unsigned flow_id) {
    if (flow_id < flows_.size()) {
      flows_[flow_id].phase_.store(ReplicationPhase::kFailed,
                                   std::memory_order_release);
      flows_[flow_id].activity_generation_.fetch_add(1,
                                                     std::memory_order_release);
    }
  }

  bool all_flows_ready() const {
    if (cancelled_.load(std::memory_order_acquire) ||
        connected_flows_.load(std::memory_order_acquire) != flows_.size()) {
      return false;
    }
    return std::all_of(flows_.begin(), flows_.end(), [](const auto& flow) {
      return flow.phase_.load(std::memory_order_acquire) ==
             ReplicationPhase::kReady;
    });
  }

  std::optional<std::uint64_t> RetainedLsn(unsigned flow_id) const {
    if (cancelled_.load(std::memory_order_acquire) ||
        flow_id >= flows_.size()) {
      return std::nullopt;
    }
    const ReplicationPhase phase =
        flows_[flow_id].phase_.load(std::memory_order_acquire);
    // A full-sync flow is protected by its own per-session publish FIFO and
    // is restarted on disconnect. It must not pin the reconnect backlog from
    // the beginning of a multi-hour scan. Only the ONLINE/backlog phases own
    // a resumable shared-history cursor.
    if (phase != ReplicationPhase::kBacklog &&
        phase != ReplicationPhase::kReady) {
      return std::nullopt;
    }
    return flows_[flow_id].lsn_.load(std::memory_order_acquire);
  }

  std::optional<ReplicaFlowProgressSnapshot> Progress(unsigned flow_id) const {
    if (flow_id >= flows_.size()) return std::nullopt;
    const ReplicaFlowProgress& progress = flows_[flow_id];
    return ReplicaFlowProgressSnapshot{
        .phase_ = progress.phase_.load(std::memory_order_acquire),
        .lsn_ = progress.lsn_.load(std::memory_order_relaxed),
        .fragment_index_ =
            progress.fragment_index_.load(std::memory_order_relaxed),
        .current_partition_ =
            progress.current_partition_.load(std::memory_order_relaxed),
        .partition_sequence_ =
            progress.partition_sequence_.load(std::memory_order_relaxed),
    };
  }

  unsigned worker_count() const noexcept {
    return static_cast<unsigned>(flow_fds_.size());
  }

  std::uint64_t min_lsn() const noexcept {
    std::uint64_t result = std::numeric_limits<std::uint64_t>::max();
    for (const ReplicaFlowProgress& flow : flows_) {
      result = std::min(result, flow.lsn_.load(std::memory_order_acquire));
    }
    return result == std::numeric_limits<std::uint64_t>::max() ? 0 : result;
  }

  void MarkOnline() noexcept { online_.store(true, std::memory_order_release); }

  bool online() const noexcept {
    return online_.load(std::memory_order_acquire) && !cancelled();
  }

  void Cancel() {
    if (cancelled_.exchange(true, std::memory_order_acq_rel)) return;
    {
      std::lock_guard lock(mutex_);
      online_.store(false, std::memory_order_release);
      if (control_fd_ >= 0) ::shutdown(control_fd_, SHUT_RDWR);
      for (int fd : flow_fds_) {
        if (fd >= 0) ::shutdown(fd, SHUT_RDWR);
      }
    }
    AbortSnapshotCut(
        absl::CancelledError("replication session snapshot cut cancelled"));
  }

  bool cancelled() const { return cancelled_.load(std::memory_order_acquire); }

  std::uint64_t id_ = 0;
  const std::string node_id_;
  const std::string host_;
  const std::uint16_t port_ = 0;
  const bool allow_continue_ = false;

 private:
  mutable std::mutex mutex_;
  int control_fd_ = -1;
  std::vector<int> flow_fds_;
  std::vector<ReplicaFlowProgress> flows_;
  std::vector<std::int8_t> flow_resume_possible_;
  std::size_t flow_modes_registered_ = 0;
  bool all_flows_resume_possible_ = true;
  celer::CoroutineBarrier snapshot_ready_;
  celer::CoroutineBarrier snapshot_gate_closed_;
  celer::CoroutineBarrier snapshot_fenced_;
  celer::CoroutineBarrier snapshot_capture_stopped_;
  std::atomic<unsigned> snapshot_scans_complete_{0};
  std::atomic<unsigned> connected_flows_{0};
  std::atomic<bool> online_{false};
  std::atomic<bool> cancelled_{false};
};

struct RedisSource {
  ReplicaOfConfig upstream_;
  std::string node_id_;
  RedisSlotSet slots_;
  std::shared_ptr<ReplicaSession> session_;
  std::optional<std::string> replid_;
  std::atomic<std::uint64_t> offset_{0};
  std::uint64_t role_epoch_ = 0;
  std::atomic<bool> dataset_valid_{false};
  std::atomic<bool> link_up_{false};
  std::atomic<std::uint64_t> link_state_changed_nanos_{SteadyNanos()};
  std::atomic<bool> syncing_{false};
  bool coordinator_started_ = false;
};

enum class UpstreamProtocol : std::uint8_t { kNative, kRedis };

struct UpstreamDiscovery {
  UpstreamProtocol protocol_ = UpstreamProtocol::kNative;
  bool redis_cluster_ = false;
  std::optional<RedisClusterTopology> topology_;
  std::optional<RedisClusterMaster> self_;
};

}  // namespace

class ReplicationManager::Impl {
 public:
  Impl(storage::StorageEngine* storage,
       std::optional<ReplicaOfConfig> initial_upstream,
       const ReplicationOptions& options)
      : storage_(storage),
        upstream_(std::move(initial_upstream)),
        cursor_state_(
            std::make_shared<ReplicaCursorState>(storage->worker_count())),
        replica_priority_(options.replica_priority_),
        node_id_(NewReplicationId()),
        history_id_(NewReplicationId()),
        listen_port_(options.listen_port_),
        tls_context_(options.use_tls_ ? options.tls_context_ : nullptr),
        masteruser_(options.masteruser_),
        masterauth_(options.masterauth_),
        redis_psync_(options.redis_psync_),
        redis_export_backpressure_(options.redis_export_backpressure_) {
    const std::size_t minimum_blocks = storage_->worker_count();
    const std::size_t configured_blocks =
        options.backlog_size_bytes_ / storage::kStorageBlockBytes;
    backlog_size_bytes_.store(std::max(minimum_blocks, configured_blocks) *
                                  storage::kStorageBlockBytes,
                              std::memory_order_relaxed);
    publish_queue_bytes_per_worker_.store(
        options.publish_queue_bytes_per_worker_, std::memory_order_relaxed);
    snapshot_batch_size_.store(options.snapshot_batch_size_,
                               std::memory_order_relaxed);
    if (upstream_.has_value()) {
      StoreRole(ReplicationRole::kConnecting, std::memory_order_relaxed);
      role_epoch_.store(1, std::memory_order_relaxed);
      if (options.redis_psync_) {
        auto source = std::make_shared<RedisSource>();
        source->upstream_ = *upstream_;
        source->role_epoch_ = 1;
        redis_sources_.push_back(std::move(source));
      } else {
        initial_protocol_probe_pending_ = true;
      }
    }
  }

  void StorageReady(celer::Worker& worker) {
    ready_workers_.fetch_add(1, std::memory_order_acq_rel);
    if (worker.id() == 0 && !ready_waiter_started_) {
      ready_waiter_started_ = true;
      worker.Spawn(WaitUntilStorageReady());
    }
  }

  Task<absl::Status> SetUpstream(std::optional<ReplicaOfConfig> upstream) {
    if (celer::ThisWorker().id_ != 0) {
      co_return co_await celer::SubmitTaskTo(
          0, [this, upstream = std::move(upstream)]() mutable {
            return SetUpstream(std::move(upstream));
          });
    }
    if (upstream.has_value() &&
        (upstream->host_.empty() || upstream->port_ == 0)) {
      co_return absl::InvalidArgumentError("invalid replication upstream");
    }
    if (upstream.has_value() && upstream->port_ == listen_port_ &&
        (upstream->host_ == "127.0.0.1" || upstream->host_ == "::1" ||
         EqualCaseInsensitive(upstream->host_, "localhost"))) {
      co_return absl::InvalidArgumentError(
          "replication upstream resolves to this server");
    }

    std::optional<UpstreamDiscovery> discovery;
    if (upstream.has_value()) {
      auto probed = co_await ProbeUpstream(*upstream);
      if (probed.ok()) {
        discovery = std::move(*probed);
      } else if (probed.status().code() != absl::StatusCode::kUnavailable) {
        co_return probed.status();
      }
    }

    while (!CloseAllCommandDbGates()) {
      absl::Status waited = co_await celer::SleepFor(
          *celer::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) co_return waited;
    }
    struct RoleGateGuard {
      ~RoleGateGuard() { OpenAllCommandDbGates(); }
    } role_gate;
    while (CommandDbOperationsActive()) {
      absl::Status waited = co_await celer::SleepFor(
          *celer::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) co_return waited;
    }

    if (upstream.has_value()) {
      absl::Status quiesced = co_await storage_->QuiesceExpiration();
      if (!quiesced.ok()) co_return quiesced;
      storage_->SetExpirationAuthority(false);
      storage_->ResumeExpiration();
    }

    std::vector<std::shared_ptr<ReplicaSession>> cancelled;
    bool start_upstream = false;
    bool discard_incomplete_root = false;
    {
      std::lock_guard lock(state_mutex_);
      const bool had_upstream = upstream_.has_value();
      bool dataset_valid =
          native_dataset_valid_.load(std::memory_order_acquire);
      for (const auto& source : redis_sources_) {
        dataset_valid =
            dataset_valid ||
            source->dataset_valid_.load(std::memory_order_acquire);
      }
      // A lost transport changes ONLINE to CONNECTING, but the last committed
      // replica root remains a valid promotion candidate. Only an explicit
      // source switch whose replacement full sync has not reached its cut
      // publishes an empty dataset on REPLICAOF NO ONE.
      discard_incomplete_root =
          !upstream.has_value() && had_upstream && !dataset_valid;
      upstream_ = upstream;
      if (upstream.has_value()) {
        native_dataset_valid_.store(false, std::memory_order_release);
      }
      if (active_replica_session_ != nullptr) {
        cancelled.push_back(std::move(active_replica_session_));
      }
      for (const auto& source : redis_sources_) {
        if (source->session_ != nullptr) cancelled.push_back(source->session_);
      }
      redis_sources_.clear();
      expected_redis_topology_.reset();
      redis_cluster_ = false;
      redis_topology_fault_ = false;
      redis_topology_monitor_started_ = false;
      initial_protocol_probe_pending_ =
          upstream.has_value() && !discovery.has_value();
      replica_session_id_ = 0;
      source_worker_count_ = 0;
      upstream_node_id_.reset();
      upstream_history_id_.reset();
      // An explicit topology change is not an automatic reconnect. Local
      // writes may have occurred while promoted or while following another
      // source, so none of the old per-flow cursors are safe for CONTINUE.
      cursor_state_.reset();
      const std::uint64_t next_epoch =
          role_epoch_.fetch_add(1, std::memory_order_acq_rel) + 1;
      const bool redis = discovery.has_value() &&
                         discovery->protocol_ == UpstreamProtocol::kRedis;
      redis_psync_.store(redis, std::memory_order_release);
      if (redis) {
        auto source = std::make_shared<RedisSource>();
        source->upstream_ = *upstream;
        source->role_epoch_ = next_epoch;
        redis_cluster_ = discovery->redis_cluster_;
        if (redis_cluster_) {
          expected_redis_topology_ = std::move(discovery->topology_);
          source->node_id_ = discovery->self_->node_id_;
          source->slots_ = discovery->self_->slots_;
        } else {
          source->node_id_ = "standalone";
          source->slots_.set();
        }
        redis_sources_.push_back(std::move(source));
      }
      StoreRole(upstream_.has_value() ? ReplicationRole::kConnecting
                                      : ReplicationRole::kMaster,
                std::memory_order_release);
      start_upstream = upstream_.has_value();
    }
    for (const auto& session : cancelled) session->Cancel();
    for (const auto& session : cancelled) {
      absl::Status stopped = co_await CancelAndWaitForReplicaFlows(session);
      if (!stopped.ok()) co_return stopped;
      if (session->session_id_ != 0) {
        absl::Status discarded =
            co_await storage_->AbortReplicaRoot(session->session_id_);
        if (!discarded.ok()) co_return discarded;
      }
    }
    if (discard_incomplete_root) {
      absl::Status cleared = co_await storage_->FlushAllDetach();
      if (!cleared.ok()) {
        storage_->SetReplicaLoading(false);
        storage_->SetExpirationAuthority(true);
        co_return cleared;
      }
    }
    if (!start_upstream) {
      storage_->SetReplicaLoading(false);
      storage_->SetExpirationAuthority(true);
    }
    if (start_upstream && StorageIsReady()) StartCoordinator();
    co_return absl::OkStatus();
  }

  Task<absl::Status> AddUpstream(ReplicaOfConfig upstream) {
    if (celer::ThisWorker().id_ != 0) {
      co_return co_await celer::SubmitTaskTo(
          0, [this, upstream = std::move(upstream)]() mutable {
            return AddUpstream(std::move(upstream));
          });
    }
    if (!redis_psync_.load(std::memory_order_acquire) || !redis_cluster_ ||
        !expected_redis_topology_.has_value()) {
      co_return absl::FailedPreconditionError(
          "ADDREPLICAOF requires an active Redis Cluster upstream");
    }
    if (redis_topology_fault_) {
      co_return absl::FailedPreconditionError(
          "Redis Cluster topology is faulted; use REPLICAOF to rebuild it");
    }
    auto discovery = co_await DiscoverRedis(upstream);
    if (!discovery.ok()) co_return discovery.status();
    if (!discovery->redis_cluster_ || !discovery->topology_.has_value() ||
        !discovery->self_.has_value()) {
      co_return absl::FailedPreconditionError(
          "ADDREPLICAOF source is not a Redis Cluster master");
    }
    std::shared_ptr<RedisSource> source;
    {
      std::lock_guard lock(state_mutex_);
      if (!redis_psync_.load(std::memory_order_relaxed) || !redis_cluster_ ||
          !expected_redis_topology_.has_value() || redis_topology_fault_) {
        co_return absl::FailedPreconditionError(
            "Redis Cluster replication changed while adding the source");
      }
      if (!SameRedisSlotLayout(*expected_redis_topology_,
                               *discovery->topology_)) {
        co_return absl::FailedPreconditionError(
            "Redis Cluster slot topology changed; reconfigure with "
            "REPLICAOF");
      }
      for (const auto& current : redis_sources_) {
        const RedisSlotSet overlap = current->slots_ & discovery->self_->slots_;
        if (overlap.any()) {
          std::size_t slot = 0;
          while (!overlap.test(slot)) ++slot;
          co_return absl::AlreadyExistsError(
              absl::StrCat("Redis replication slot overlap at slot ", slot));
        }
      }
      source = std::make_shared<RedisSource>();
      source->upstream_ = std::move(upstream);
      source->node_id_ = discovery->self_->node_id_;
      source->slots_ = discovery->self_->slots_;
      source->role_epoch_ = role_epoch_.load(std::memory_order_relaxed);
      redis_sources_.push_back(source);
    }
    if (StorageIsReady()) StartRedisCoordinator(source);
    RefreshRedisRole();
    co_return absl::OkStatus();
  }

  ReplicationStatus status() const {
    ReplicationStatus result;
    result.role_ = role_.load(std::memory_order_acquire);
    result.role_epoch_ = role_epoch_.load(std::memory_order_acquire);
    result.local_node_id_ = node_id_;
    {
      std::lock_guard lock(state_mutex_);
      result.upstream_ = upstream_;
      result.upstream_node_id_ = upstream_node_id_;
      result.upstream_history_id_ = upstream_history_id_;
      result.session_id_ = replica_session_id_;
      result.source_worker_count_ = source_worker_count_;
      if (active_replica_session_ != nullptr) {
        result.connected_flows_ =
            active_replica_session_->connected_flows_.load(
                std::memory_order_acquire);
      }
      if (redis_psync_.load(std::memory_order_relaxed)) {
        result.source_worker_count_ =
            static_cast<unsigned>(redis_sources_.size());
        result.connected_flows_ = 0;
        for (const auto& source : redis_sources_) {
          if (source->session_ != nullptr) {
            result.connected_flows_ += source->session_->connected_flows_.load(
                std::memory_order_acquire);
          }
        }
      }
      result.redis_cluster_ = redis_cluster_;
      result.redis_topology_fault_ = redis_topology_fault_;
      result.redis_sources_.reserve(redis_sources_.size());
      for (const auto& source : redis_sources_) {
        result.redis_sources_.push_back(RedisSourceStatus{
            .upstream_ = source->upstream_,
            .node_id_ = source->node_id_,
            .slots_ = FormatRedisSlots(source->slots_),
            .replid_ = source->replid_,
            .offset_ = source->offset_.load(std::memory_order_acquire),
            .link_up_ = source->link_up_.load(std::memory_order_acquire),
            .dataset_valid_ =
                source->dataset_valid_.load(std::memory_order_acquire),
        });
        result.replica_repl_offset_ +=
            source->offset_.load(std::memory_order_acquire);
      }
      if (result.redis_sources_.empty() && cursor_state_ != nullptr) {
        result.replica_repl_offset_ = cursor_state_->total_lsn();
      }
      result.replica_priority_ =
          replica_priority_.load(std::memory_order_acquire);
      if (result.upstream_.has_value()) {
        std::uint64_t down_seconds = 0;
        if (!redis_sources_.empty()) {
          for (const auto& source : redis_sources_) {
            if (!source->link_up_.load(std::memory_order_acquire)) {
              down_seconds = std::max(
                  down_seconds,
                  SecondsSince(source->link_state_changed_nanos_.load(
                      std::memory_order_acquire)));
            }
          }
        } else if (result.role_ != ReplicationRole::kOnline) {
          down_seconds = SecondsSince(
              link_state_changed_nanos_.load(std::memory_order_acquire));
        }
        result.master_link_down_since_seconds_ = down_seconds;
        result.master_last_io_seconds_ago_ = down_seconds;
      }
    }
    {
      std::lock_guard lock(master_mutex_);
      result.local_history_id_ = history_id_;
      result.downstream_replicas_.reserve(master_sessions_.size());
      for (const auto& [session_id, session] : master_sessions_) {
        (void)session_id;
        if (session->node_id_.empty() || session->host_.empty() ||
            session->port_ == 0 || session->cancelled()) {
          continue;
        }
        result.downstream_replicas_.push_back(DownstreamReplicaStatus{
            .node_id_ = session->node_id_,
            .host_ = session->host_,
            .port_ = session->port_,
            .online_ = session->online(),
            .min_lsn_ = session->min_lsn(),
        });
        result.master_repl_offset_ =
            std::max(result.master_repl_offset_, session->min_lsn());
      }
    }
    std::sort(result.downstream_replicas_.begin(),
              result.downstream_replicas_.end(),
              [](const auto& left, const auto& right) {
                return left.node_id_ < right.node_id_;
              });
    return result;
  }

  void StoreRole(ReplicationRole next, std::memory_order order) noexcept {
    const ReplicationRole previous = role_.exchange(next, order);
    if (next == ReplicationRole::kOnline ||
        previous == ReplicationRole::kOnline ||
        previous == ReplicationRole::kMaster) {
      link_state_changed_nanos_.store(SteadyNanos(),
                                      std::memory_order_release);
    }
  }

  void StoreRedisLink(const std::shared_ptr<RedisSource>& source,
                      bool up) noexcept {
    if (source->link_up_.exchange(up, std::memory_order_acq_rel) != up) {
      source->link_state_changed_nanos_.store(SteadyNanos(),
                                              std::memory_order_release);
    }
  }

  bool is_replica() const noexcept {
    return role_.load(std::memory_order_acquire) != ReplicationRole::kMaster;
  }

  bool is_redis_follower() const noexcept {
    return redis_psync_.load(std::memory_order_acquire);
  }

  bool is_loading() const noexcept {
    const ReplicationRole role = role_.load(std::memory_order_acquire);
    return role == ReplicationRole::kConnecting ||
           role == ReplicationRole::kSyncing;
  }

  absl::Status SetSnapshotReadConcurrency(unsigned concurrency) noexcept {
    if (concurrency == 0 ||
        concurrency > kMaxReplicationSnapshotReadConcurrency) {
      return absl::InvalidArgumentError(absl::StrCat(
          "replication snapshot read concurrency must be between 1 and ",
          kMaxReplicationSnapshotReadConcurrency));
    }
    snapshot_read_concurrency_.store(concurrency, std::memory_order_release);
    return absl::OkStatus();
  }

  unsigned snapshot_read_concurrency() const noexcept {
    return snapshot_read_concurrency_.load(std::memory_order_acquire);
  }

  absl::Status SetSnapshotBatchSize(std::size_t count) noexcept {
    if (count == 0 || count > kMaxReplicationSnapshotBatchSize) {
      return absl::InvalidArgumentError(
          absl::StrCat("replication snapshot batch size must be between 1 and ",
                       kMaxReplicationSnapshotBatchSize));
    }
    snapshot_batch_size_.store(count, std::memory_order_release);
    return absl::OkStatus();
  }

  std::size_t snapshot_batch_size() const noexcept {
    return snapshot_batch_size_.load(std::memory_order_acquire);
  }

  absl::Status SetReplicaPriority(unsigned priority) noexcept {
    replica_priority_.store(priority, std::memory_order_release);
    return absl::OkStatus();
  }

  unsigned replica_priority() const noexcept {
    return replica_priority_.load(std::memory_order_acquire);
  }

  std::size_t BacklogCapacityForFlow(unsigned flow_id,
                                     std::size_t global_bytes) const noexcept {
    const std::size_t total_blocks = global_bytes / storage::kStorageBlockBytes;
    const std::size_t workers = storage_->worker_count();
    const std::size_t blocks =
        total_blocks / workers + (flow_id < total_blocks % workers ? 1 : 0);
    return blocks * storage::kStorageBlockBytes;
  }

  Task<absl::Status> SetBacklogSizeBytes(std::size_t bytes) {
    if (celer::ThisWorker().id_ != 0) {
      co_return co_await celer::SubmitTaskTo(
          0, [this, bytes]() { return SetBacklogSizeBytes(bytes); });
    }
    const std::size_t blocks = bytes / storage::kStorageBlockBytes;
    if (blocks < storage_->worker_count()) {
      co_return absl::InvalidArgumentError(
          "repl-backlog-size must provide at least one 8 MiB block per "
          "worker");
    }
    const std::size_t effective = blocks * storage::kStorageBlockBytes;
    const std::uint64_t max_memory = GetMemoryStats().max_bytes_;
    if (max_memory != 0 && effective > max_memory) {
      co_return absl::InvalidArgumentError(
          "repl-backlog-size cannot exceed maxmemory");
    }
    for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
      const std::size_t flow_capacity =
          BacklogCapacityForFlow(worker, effective);
      absl::Status configured;
      if (worker == 0) {
        configured =
            co_await storage_->SetReplicationLogCapacity(flow_capacity);
      } else {
        configured =
            co_await celer::SubmitTaskTo(worker, [this, flow_capacity]() {
              return storage_->SetReplicationLogCapacity(flow_capacity);
            });
      }
      if (!configured.ok()) co_return configured;
    }
    backlog_size_bytes_.store(effective, std::memory_order_release);
    co_return absl::OkStatus();
  }

  std::size_t backlog_size_bytes() const noexcept {
    return backlog_size_bytes_.load(std::memory_order_acquire);
  }

  Task<absl::Status> SetPublishQueueBytesPerWorker(std::size_t bytes) {
    if (celer::ThisWorker().id_ != 0) {
      co_return co_await celer::SubmitTaskTo(
          0, [this, bytes]() { return SetPublishQueueBytesPerWorker(bytes); });
    }
    if (bytes == 0) {
      co_return absl::InvalidArgumentError(
          "replication publish queue capacity must be nonzero");
    }
    for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
      absl::Status configured;
      if (worker == 0) {
        configured =
            co_await storage_->SetReplicationPublishQueueCapacity(bytes);
      } else {
        configured = co_await celer::SubmitTaskTo(worker, [this, bytes]() {
          return storage_->SetReplicationPublishQueueCapacity(bytes);
        });
      }
      if (!configured.ok()) co_return configured;
    }
    publish_queue_bytes_per_worker_.store(bytes, std::memory_order_release);
    co_return absl::OkStatus();
  }

  std::size_t publish_queue_bytes_per_worker() const noexcept {
    return publish_queue_bytes_per_worker_.load(std::memory_order_acquire);
  }

  Task<absl::Status> ServeNativeConnection(TcpStream& stream,
                                           std::vector<std::string> args,
                                           std::uint64_t client_id,
                                           std::string client_address,
                                           bool tls) {
    if (redis_psync_.load(std::memory_order_acquire)) {
      co_return absl::FailedPreconditionError(
          "a redis-replicaof follower cannot serve downstream replicas");
    }
    // Accepted Redis sockets are normally optimized for batched replies, but
    // replication is an ACK-driven stream whose frame header and payload are
    // written separately. Without TCP_NODELAY on the source endpoint, Nagle
    // can hold every payload behind the tiny header until the peer's delayed
    // ACK fires (about 40 ms per sparse snapshot partition on Linux).
    absl::Status accepted_config = ConfigureConnectedFd(stream.NativeFd());
    if (!accepted_config.ok()) co_return accepted_config;
    unsigned owner = 0;
    std::uint64_t replication_session_id = 0;
    if (EqualCaseInsensitive(args.front(), "KLFLOW")) {
      unsigned flow_id = 0;
      if (args.size() != 6 ||
          !ParseUnsigned(args[2], &replication_session_id) ||
          replication_session_id == 0 || !ParseUnsigned(args[3], &flow_id)) {
        co_return absl::InvalidArgumentError("invalid KLFLOW handshake");
      }
      // flow_id belongs to the upstream worker set. Multiple upstream flows
      // may share one local worker when the worker counts differ.
      owner = flow_id % storage_->worker_count();
    }
    if (owner == celer::ThisWorker().id_) {
      RegisterClientConnection(client_id, stream.NativeFd(),
                               std::move(client_address), tls, true,
                               replication_session_id);
      absl::Status status = co_await ServeOwnedNativeConnection(
          stream, std::move(args), client_id);
      UnregisterClientConnection(client_id);
      co_return status;
    }

    absl::Status paused = co_await stream.PauseRead();
    if (!paused.ok()) co_return paused;
    std::shared_ptr<celer::TlsState> tls_state = stream.TakeTlsState();
    const int duplicate = ::fcntl(stream.NativeFd(), F_DUPFD_CLOEXEC, 0);
    if (duplicate < 0) {
      co_return absl::InternalError(
          "failed to duplicate replication connection for worker adoption");
    }
    absl::Status configured = ConfigureConnectedFd(duplicate);
    if (!configured.ok()) {
      ::close(duplicate);
      co_return configured;
    }
    // Do not close the original Connection from inside TcpService::Serve().
    // Its outer RunSession coroutine still owns and inspects that object after
    // Serve returns.  Returning lets RunSession close the original descriptor
    // safely; the duplicated descriptor has already transferred the byte
    // stream to the destination worker.
    co_return co_await celer::SubmitTo(
        owner, [this, duplicate, tls_state = std::move(tls_state),
                args = std::move(args), client_id,
                client_address = std::move(client_address), tls,
                replication_session_id]() mutable {
          Connection connection;
          connection.worker_ = celer::ThisWorker().self_;
          connection.file_.fd_ = duplicate;
          connection.closed_ = false;
          if (tls_state != nullptr) {
            connection.recv_mode_ = celer::RecvMode::kOneShot;
            connection.tls_state_ = std::move(tls_state);
          }
          Connection* registered =
              celer::ThisWorker().self_->AddConnection(std::move(connection));
          if (registered == nullptr) {
            ::close(duplicate);
            return absl::InternalError(
                "failed to adopt replication connection");
          }
          celer::ThisWorker().self_->Spawn(RunAdoptedConnection(
              registered, std::move(args), client_id, std::move(client_address),
              tls, replication_session_id));
          return absl::OkStatus();
        });
  }

  Task<absl::Status> ServeRedisExportConnection(TcpStream& stream,
                                                std::vector<std::string> args,
                                                std::uint64_t client_id,
                                                std::string client_address,
                                                bool tls, bool eof_capable) {
    if (args.size() != 3 || !EqualCaseInsensitive(args[0], "PSYNC")) {
      co_return absl::InvalidArgumentError("invalid Redis PSYNC handshake");
    }
    if (!eof_capable) {
      absl::Status sent = co_await WriteText(
          stream, "-ERR diskless PSYNC requires REPLCONF capa eof\r\n");
      co_return sent.ok()
          ? absl::FailedPreconditionError(
                "Redis replica did not advertise EOF capability")
          : sent;
    }
    if (is_replica()) {
      absl::Status sent = co_await WriteText(
          stream, "-ERR detach this Keylane replica before Redis export\r\n");
      co_return sent.ok() ? absl::FailedPreconditionError(
                                "a Keylane replica cannot export Redis PSYNC")
                          : sent;
    }
    bool expected = false;
    if (!redis_export_active_.compare_exchange_strong(
            expected, true, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      absl::Status sent = co_await WriteText(
          stream, "-ERR only one Redis PSYNC export is supported\r\n");
      co_return sent.ok()
          ? absl::AlreadyExistsError("a Redis PSYNC export is already active")
          : sent;
    }
    struct ActiveGuard {
      std::atomic<bool>* active_;
      ~ActiveGuard() { active_->store(false, std::memory_order_release); }
    } active_guard{&redis_export_active_};

    absl::Status configured = ConfigureConnectedFd(stream.NativeFd());
    if (!configured.ok()) co_return configured;
    const std::uint64_t session_id =
        next_master_session_id_.fetch_add(1, std::memory_order_relaxed);
    SetClientReplicationSession(client_id, session_id);
    RegisterClientConnection(client_id, stream.NativeFd(),
                             std::move(client_address), tls, true, session_id);
    struct ClientGuard {
      std::uint64_t id_;
      ~ClientGuard() { UnregisterClientConnection(id_); }
    } client_guard{client_id};

    absl::Status status = co_await ResetInvalidReplicationHistory();
    if (!status.ok()) co_return status;
    for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
      const std::size_t flow_capacity = BacklogCapacityForFlow(
          worker, backlog_size_bytes_.load(std::memory_order_acquire));
      status = co_await celer::SubmitTaskTo(
          worker, [this, session_id, flow_capacity]() -> Task<absl::Status> {
            co_return co_await storage_->EnableReplicationLog(session_id,
                                                              flow_capacity);
          });
      if (!status.ok()) co_return status;
    }

    while (!CloseAllCommandDbGates()) {
      status = co_await celer::SleepFor(*celer::ThisWorker().self_,
                                        std::chrono::milliseconds(1));
      if (!status.ok()) co_return status;
    }
    bool gates_open = false;
    struct GateGuard {
      bool* open_;
      ~GateGuard() {
        if (!*open_) OpenAllCommandDbGates();
      }
    } gate_guard{&gates_open};
    while (CommandDbOperationsActive()) {
      status = co_await celer::SleepFor(*celer::ThisWorker().self_,
                                        std::chrono::milliseconds(1));
      if (!status.ok()) co_return status;
    }

    const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
    const std::uint64_t snapshot_time_ms =
        now > 0 ? static_cast<std::uint64_t>(now) : 1;
    unsigned snapshots_begun = 0;
    for (; snapshots_begun < storage_->worker_count(); ++snapshots_begun) {
      status = co_await celer::SubmitTo(
          snapshots_begun, [this, session_id, snapshot_time_ms] {
            return storage_->BeginRdbSnapshot(session_id, snapshot_time_ms);
          });
      if (!status.ok()) break;
    }
    if (!status.ok()) {
      for (unsigned worker = 0; worker < snapshots_begun; ++worker) {
        (void)co_await celer::SubmitTaskTo(worker, [this, session_id] {
          return storage_->EndRdbSnapshot(session_id);
        });
      }
      co_return status;
    }

    std::vector<storage::ReplicationLogCursor> cursors(
        storage_->worker_count());
    for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
      auto fenced = co_await celer::SubmitTaskTo(
          worker, [this]() { return storage_->FenceReplicationLog(); });
      if (!fenced.ok()) {
        status = fenced.status();
        break;
      }
      cursors[worker] =
          storage::ReplicationLogCursor{.lsn_ = *fenced, .fragment_index_ = 0};
      if (redis_export_backpressure_.load(std::memory_order_acquire)) {
        status = co_await celer::SubmitTo(
            worker, [this, session_id, cursor = cursors[worker]] {
              return storage_->RetainReplicationLog(session_id, cursor.lsn_);
            });
        if (!status.ok()) break;
      }
    }
    if (!status.ok()) {
      for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
        (void)co_await celer::SubmitTaskTo(worker, [this, session_id] {
          return storage_->EndRdbSnapshot(session_id);
        });
        (void)co_await celer::SubmitTo(worker, [this, session_id] {
          storage_->ReleaseReplicationLogRetention(session_id);
          return true;
        });
      }
      co_return status;
    }
    const std::vector<LuaFunctionLibrary> function_libraries =
        SnapshotLuaFunctionLibraries();
    OpenAllCommandDbGates();
    gates_open = true;

    const std::string eof_token = NewReplicationId();
    auto rdb_queue =
        std::make_shared<RedisRdbStreamQueue>(storage_, session_id);
    // Start all storage workers even if the socket fails immediately: every
    // producer owns the matching EndRdbSnapshot cleanup.
    rdb_queue->Start();
    status = co_await WriteText(
        stream, absl::StrCat("+FULLRESYNC ", node_id_, " 0\r\n$EOF:", eof_token,
                             "\r\n"));
    if (status.ok()) {
      // RDB v10 is accepted by Redis 7.0 and later. Keylane's value opcodes
      // do not require the v11 metadata additions used by backup files.
      rdb::StreamEncoder encoder(10);
      status = co_await WriteText(stream, encoder.Header());
      if (status.ok()) {
        for (const LuaFunctionLibrary& library : function_libraries) {
          const std::string fragment =
              rdb::EncodeFunctionLibraryEntry(library.code_);
          status = co_await WriteText(stream, fragment);
          if (!status.ok()) break;
          encoder.Account(fragment);
        }
      }
      if (status.ok()) {
        while (status.ok()) {
          std::string fragment;
          if (rdb_queue->TryPop(&fragment)) {
            status = co_await WriteText(stream, fragment);
            if (status.ok()) encoder.Account(fragment);
            continue;
          }
          if (rdb_queue->done()) break;
          if (rdb_queue->failed()) {
            status = rdb_queue->status();
            break;
          }
          status = co_await celer::SleepFor(*celer::ThisWorker().self_,
                                            std::chrono::milliseconds(1));
        }
        if (status.ok() && rdb_queue->failed()) status = rdb_queue->status();
        if (status.ok()) {
          const std::string trailer = encoder.Finish();
          status = co_await WriteText(stream, trailer);
        }
        if (status.ok()) status = co_await WriteText(stream, eof_token);
      }
    }
    if (!status.ok() && !rdb_queue->done()) rdb_queue->Abort(status);
    while (!rdb_queue->done()) {
      absl::Status waited = co_await celer::SleepFor(
          *celer::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) break;
    }
    std::shared_ptr<RedisExportAckState> ack_state;
    if (status.ok()) {
      spdlog::info("Redis PSYNC export {} completed diskless RDB cut",
                   session_id);
      ack_state = std::make_shared<RedisExportAckState>();
      celer::ThisWorker().self_->Spawn(
          ConsumeRedisExportAcks(&stream, ack_state));
      auto backlog_state =
          std::make_shared<RedisExportBacklogState>(std::move(cursors));
      status = co_await RunRedisExportBacklog(stream, session_id,
                                              backlog_state.get());
    }
    if (ack_state != nullptr &&
        !ack_state->done_.load(std::memory_order_acquire)) {
      (void)::shutdown(stream.NativeFd(), SHUT_RDWR);
      while (!ack_state->done_.load(std::memory_order_acquire)) {
        absl::Status waited = co_await celer::SleepFor(
            *celer::ThisWorker().self_, std::chrono::milliseconds(1));
        if (!waited.ok()) break;
      }
    }
    for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
      (void)co_await celer::SubmitTo(worker, [this, session_id] {
        storage_->ReleaseReplicationLogRetention(session_id);
        return true;
      });
    }
    if (status.code() == absl::StatusCode::kResourceExhausted &&
        status.message() == kRedisExportBacklogGapMessage) {
      spdlog::warn(
          "Redis PSYNC export {} failed: Redis replica fell behind online "
          "writes; disconnecting and requiring a new full sync",
          session_id);
    } else if (!status.ok()) {
      spdlog::warn("Redis PSYNC export {} ended: {}", session_id,
                   status.message());
    }
    co_return status;
  }

 private:
  Task<absl::Status> RunRedisExportBacklog(TcpStream& stream,
                                           std::uint64_t session_id,
                                           RedisExportBacklogState* state) {
    return RunRedisExportBacklogLoop(
        stream, storage_,
        redis_export_backpressure_.load(std::memory_order_acquire), session_id,
        state);
  }

  bool StorageIsReady() const noexcept {
    return ready_workers_.load(std::memory_order_acquire) ==
           storage_->worker_count();
  }

  Task<absl::StatusOr<std::optional<RedisClusterTopology>>>
  QueryRedisClusterTopology(const ReplicaOfConfig& upstream) {
    auto connected =
        co_await ConnectTcp(upstream.host_, upstream.port_, tls_context_);
    if (!connected.ok()) co_return connected.status();
    TcpStream stream = std::move(*connected);
    absl::Status status =
        co_await AuthenticateUpstream(stream, masteruser_, masterauth_);
    if (!status.ok()) {
      stream.Close().IgnoreError();
      co_return status;
    }
    const std::vector<std::string> command{"CLUSTER", "NODES"};
    status = co_await WriteText(stream, EncodeRespCommand(command));
    if (!status.ok()) {
      stream.Close().IgnoreError();
      co_return status;
    }
    auto body = co_await ReadRedisBulkReply(stream);
    stream.Close().IgnoreError();
    if (!body.ok()) {
      const std::string message(body.status().message());
      if (body.status().code() == absl::StatusCode::kFailedPrecondition &&
          message.find("cluster support disabled") != std::string::npos) {
        co_return std::optional<RedisClusterTopology>{};
      }
      co_return body.status();
    }
    auto topology = ParseRedisClusterNodes(*body);
    if (!topology.ok()) co_return topology.status();
    co_return std::optional<RedisClusterTopology>(std::move(*topology));
  }

  Task<absl::StatusOr<UpstreamDiscovery>> DiscoverRedis(
      const ReplicaOfConfig& upstream) {
    auto topology = co_await QueryRedisClusterTopology(upstream);
    if (!topology.ok()) co_return topology.status();
    UpstreamDiscovery result;
    result.protocol_ = UpstreamProtocol::kRedis;
    if (!topology->has_value()) co_return result;
    result.redis_cluster_ = true;
    result.topology_ = std::move(**topology);
    for (const RedisClusterMaster& master : result.topology_->masters_) {
      if (master.node_id_ == result.topology_->self_id_) {
        result.self_ = master;
        break;
      }
    }
    if (!result.self_.has_value()) {
      co_return absl::FailedPreconditionError(
          "connected Redis node is not a slot-owning cluster master");
    }
    co_return result;
  }

  Task<absl::StatusOr<UpstreamDiscovery>> ProbeUpstream(
      const ReplicaOfConfig& upstream) {
    auto connected =
        co_await ConnectTcp(upstream.host_, upstream.port_, tls_context_);
    if (!connected.ok()) co_return connected.status();
    TcpStream stream = std::move(*connected);
    absl::Status status =
        co_await AuthenticateUpstream(stream, masteruser_, masterauth_);
    if (!status.ok()) {
      stream.Close().IgnoreError();
      co_return status;
    }
    const std::vector<std::string> sync_args{
        "KLPSYNC", std::string(kProtocolVersion), "?", "?"};
    status = co_await WriteText(stream, EncodeRespCommand(sync_args));
    if (!status.ok()) {
      stream.Close().IgnoreError();
      co_return status;
    }
    auto response = co_await ReadLine(stream);
    stream.Close().IgnoreError();
    if (!response.ok()) co_return response.status();
    if (response->starts_with("+KLFULLRESYNC ")) {
      UpstreamDiscovery result;
      result.protocol_ = UpstreamProtocol::kNative;
      co_return result;
    }
    if (response->starts_with('-') &&
        response->find("unknown command") != std::string::npos &&
        (response->find("KLPSYNC") != std::string::npos ||
         response->find("klpsync") != std::string::npos)) {
      co_return co_await DiscoverRedis(upstream);
    }
    co_return absl::FailedPreconditionError(
        absl::StrCat("upstream rejected Keylane protocol probe: ", *response));
  }

  Task<absl::Status> WaitUntilStorageReady() {
    while (!StorageIsReady()) {
      absl::Status slept = co_await celer::SleepFor(
          *celer::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!slept.ok()) co_return slept;
    }
    StartCoordinator();
    co_return absl::OkStatus();
  }

  void StartCoordinator() {
    if (celer::ThisWorker().id_ != 0 || !StorageIsReady()) return;
    if (initial_protocol_probe_pending_) {
      if (coordinator_started_) return;
      coordinator_started_ = true;
      celer::ThisWorker().self_->SpawnRoot(ProbeInitialUpstream());
      return;
    }
    if (redis_psync_.load(std::memory_order_acquire)) {
      std::vector<std::shared_ptr<RedisSource>> sources;
      {
        std::lock_guard lock(state_mutex_);
        sources = redis_sources_;
      }
      spdlog::info("starting {} Redis replication coordinator(s)",
                   sources.size());
      for (const auto& source : sources) StartRedisCoordinator(source);
      if (redis_cluster_) StartRedisTopologyMonitor();
      return;
    }
    if (coordinator_started_) return;
    {
      std::lock_guard lock(state_mutex_);
      if (!upstream_.has_value()) return;
    }
    coordinator_started_ = true;
    celer::ThisWorker().self_->Spawn(Coordinator());
  }

  Task<absl::Status> ProbeInitialUpstream() {
    while (true) {
      ReplicaOfConfig upstream;
      std::uint64_t role_epoch = 0;
      {
        std::lock_guard lock(state_mutex_);
        if (!upstream_.has_value() || !initial_protocol_probe_pending_) {
          coordinator_started_ = false;
          co_return absl::OkStatus();
        }
        upstream = *upstream_;
        role_epoch = role_epoch_.load(std::memory_order_relaxed);
      }
      auto discovery = co_await ProbeUpstream(upstream);
      if (!discovery.ok()) {
        spdlog::warn("replication protocol probe for {}:{} failed: {}",
                     upstream.host_, upstream.port_,
                     discovery.status().message());
        absl::Status slept = co_await celer::SleepFor(
            *celer::ThisWorker().self_, kReconnectDelay);
        if (!slept.ok()) {
          coordinator_started_ = false;
          co_return slept;
        }
        continue;
      }
      {
        std::lock_guard lock(state_mutex_);
        if (!upstream_.has_value() || *upstream_ != upstream ||
            role_epoch_.load(std::memory_order_relaxed) != role_epoch ||
            !initial_protocol_probe_pending_) {
          coordinator_started_ = false;
          co_return absl::CancelledError("initial upstream was replaced");
        }
        initial_protocol_probe_pending_ = false;
        if (discovery->protocol_ == UpstreamProtocol::kRedis) {
          redis_psync_.store(true, std::memory_order_release);
          redis_cluster_ = discovery->redis_cluster_;
          auto source = std::make_shared<RedisSource>();
          source->upstream_ = upstream;
          source->role_epoch_ = role_epoch;
          if (redis_cluster_) {
            expected_redis_topology_ = std::move(discovery->topology_);
            source->node_id_ = discovery->self_->node_id_;
            source->slots_ = discovery->self_->slots_;
          } else {
            source->node_id_ = "standalone";
            source->slots_.set();
          }
          redis_sources_.push_back(std::move(source));
        }
      }
      coordinator_started_ = false;
      StartCoordinator();
      co_return absl::OkStatus();
    }
  }

  bool RedisSourceRegistered(const std::shared_ptr<RedisSource>& source) const {
    return std::find(redis_sources_.begin(), redis_sources_.end(), source) !=
           redis_sources_.end();
  }

  void RefreshRedisRole() {
    bool ready = false;
    bool syncing = false;
    {
      std::lock_guard lock(state_mutex_);
      if (!redis_psync_.load(std::memory_order_relaxed)) return;
      RedisSlotSet registered;
      bool valid = !redis_sources_.empty() && !redis_topology_fault_;
      for (const auto& source : redis_sources_) {
        registered |= source->slots_;
        valid = valid && source->dataset_valid_.load(std::memory_order_acquire);
        syncing = syncing || source->syncing_.load(std::memory_order_acquire);
      }
      const bool complete =
          !redis_cluster_ ||
          (expected_redis_topology_.has_value() &&
           registered.count() == storage::kLogicalStorageShards &&
           redis_sources_.size() == expected_redis_topology_->masters_.size());
      ready = valid && complete;
    }
    StoreRole(ready ? ReplicationRole::kOnline
                    : (syncing ? ReplicationRole::kSyncing
                               : ReplicationRole::kConnecting),
              std::memory_order_release);
  }

  void StartRedisTopologyMonitor() {
    if (redis_topology_monitor_started_ || !redis_cluster_) return;
    redis_topology_monitor_started_ = true;
    const std::uint64_t epoch = role_epoch_.load(std::memory_order_acquire);
    celer::ThisWorker().self_->SpawnRoot(RedisTopologyMonitor(epoch));
  }

  void FaultRedisTopology(std::string_view reason) {
    std::vector<std::shared_ptr<ReplicaSession>> sessions;
    {
      std::lock_guard lock(state_mutex_);
      if (redis_topology_fault_) return;
      redis_topology_fault_ = true;
      for (const auto& source : redis_sources_) {
        StoreRedisLink(source, false);
        source->syncing_ = false;
        if (source->session_ != nullptr) sessions.push_back(source->session_);
      }
    }
    for (const auto& session : sessions) session->Cancel();
    spdlog::error(
        "Redis Cluster topology changed; replication stopped and Keylane "
        "entered LOADING: {}",
        reason);
    RefreshRedisRole();
  }

  void ApplyStableRedisTopology(RedisClusterTopology topology) {
    std::vector<std::shared_ptr<ReplicaSession>> replaced;
    {
      std::lock_guard lock(state_mutex_);
      for (const auto& source : redis_sources_) {
        auto current =
            std::find_if(topology.masters_.begin(), topology.masters_.end(),
                         [&](const RedisClusterMaster& master) {
                           return master.slots_ == source->slots_;
                         });
        if (current == topology.masters_.end()) continue;
        if (source->node_id_ == current->node_id_ &&
            source->upstream_ == current->endpoint_) {
          continue;
        }
        spdlog::warn(
            "Redis Cluster master for slots {} changed from {} {}:{} to {} "
            "{}:{}",
            FormatRedisSlots(source->slots_), source->node_id_,
            source->upstream_.host_, source->upstream_.port_, current->node_id_,
            current->endpoint_.host_, current->endpoint_.port_);
        source->node_id_ = current->node_id_;
        source->upstream_ = current->endpoint_;
        StoreRedisLink(source, false);
        if (source->session_ != nullptr) replaced.push_back(source->session_);
      }
      expected_redis_topology_ = std::move(topology);
      if (!redis_sources_.empty())
        upstream_ = redis_sources_.front()->upstream_;
    }
    for (const auto& session : replaced) session->Cancel();
  }

  Task<absl::Status> RedisTopologyMonitor(std::uint64_t role_epoch) {
    unsigned incompatible_observations = 0;
    while (true) {
      absl::Status slept = co_await celer::SleepFor(*celer::ThisWorker().self_,
                                                    kRedisTopologyPollInterval);
      if (!slept.ok()) co_return slept;
      std::vector<ReplicaOfConfig> endpoints;
      {
        std::lock_guard lock(state_mutex_);
        if (!redis_psync_.load(std::memory_order_relaxed) || !redis_cluster_ ||
            redis_topology_fault_ ||
            role_epoch_.load(std::memory_order_relaxed) != role_epoch) {
          if (role_epoch_.load(std::memory_order_relaxed) == role_epoch) {
            redis_topology_monitor_started_ = false;
          }
          co_return absl::OkStatus();
        }
        endpoints.reserve(redis_sources_.size());
        for (const auto& source : redis_sources_) {
          endpoints.push_back(source->upstream_);
        }
      }

      absl::StatusOr<std::optional<RedisClusterTopology>> observed =
          absl::UnavailableError("no Redis Cluster source was reachable");
      for (const ReplicaOfConfig& endpoint : endpoints) {
        observed = co_await QueryRedisClusterTopology(endpoint);
        if (observed.ok() ||
            observed.status().code() != absl::StatusCode::kUnavailable) {
          break;
        }
      }
      if (!observed.ok() &&
          observed.status().code() == absl::StatusCode::kUnavailable) {
        spdlog::warn("Redis Cluster topology check unavailable: {}",
                     observed.status().message());
        continue;
      }

      bool compatible = false;
      if (observed.ok() && observed->has_value()) {
        std::lock_guard lock(state_mutex_);
        compatible = expected_redis_topology_.has_value() &&
                     SameRedisSlotLayout(*expected_redis_topology_, **observed);
      }
      if (compatible) {
        incompatible_observations = 0;
        ApplyStableRedisTopology(std::move(**observed));
        continue;
      }

      ++incompatible_observations;
      const std::string reason =
          observed.ok()
              ? "source no longer reports a complete Redis Cluster topology"
              : std::string(observed.status().message());
      if (incompatible_observations < 2) {
        spdlog::warn("Redis Cluster topology change awaiting confirmation: {}",
                     reason);
        continue;
      }
      FaultRedisTopology(reason);
      redis_topology_monitor_started_ = false;
      co_return absl::FailedPreconditionError(reason);
    }
  }

  void StartRedisCoordinator(const std::shared_ptr<RedisSource>& source) {
    if (source->coordinator_started_) return;
    source->coordinator_started_ = true;
    spdlog::info("starting Redis replication coordinator for {}:{}",
                 source->upstream_.host_, source->upstream_.port_);
    celer::ThisWorker().self_->SpawnRoot(RedisCoordinator(source));
  }

  Task<absl::Status> RedisCoordinator(std::shared_ptr<RedisSource> source) {
    if (source->node_id_.empty()) {
      auto discovery = co_await DiscoverRedis(source->upstream_);
      if (!discovery.ok()) {
        spdlog::warn("failed to discover Redis source {}:{}: {}",
                     source->upstream_.host_, source->upstream_.port_,
                     discovery.status().message());
        source->coordinator_started_ = false;
        RefreshRedisRole();
        co_return discovery.status();
      }
      {
        std::lock_guard lock(state_mutex_);
        if (!RedisSourceRegistered(source) ||
            source->role_epoch_ !=
                role_epoch_.load(std::memory_order_relaxed)) {
          source->coordinator_started_ = false;
          co_return absl::CancelledError("Redis source was replaced");
        }
        redis_cluster_ = discovery->redis_cluster_;
        if (redis_cluster_) {
          expected_redis_topology_ = std::move(discovery->topology_);
          source->node_id_ = discovery->self_->node_id_;
          source->slots_ = discovery->self_->slots_;
        } else {
          source->node_id_ = "standalone";
          source->slots_.set();
        }
      }
      RefreshRedisRole();
      if (redis_cluster_) StartRedisTopologyMonitor();
    }

    while (true) {
      auto session = std::make_shared<ReplicaSession>();
      {
        std::lock_guard lock(state_mutex_);
        if (!redis_psync_.load(std::memory_order_relaxed) ||
            !RedisSourceRegistered(source) || redis_topology_fault_ ||
            source->role_epoch_ !=
                role_epoch_.load(std::memory_order_relaxed)) {
          break;
        }
        source->session_ = session;
      }
      absl::Status connected = co_await RunRedisReplicaSession(source, session);
      session->Cancel();
      {
        std::lock_guard lock(state_mutex_);
        StoreRedisLink(source, false);
        source->syncing_ = false;
        if (source->session_ == session) source->session_.reset();
      }
      RefreshRedisRole();

      bool retry = false;
      {
        std::lock_guard lock(state_mutex_);
        retry =
            redis_psync_.load(std::memory_order_relaxed) &&
            RedisSourceRegistered(source) && !redis_topology_fault_ &&
            source->role_epoch_ == role_epoch_.load(std::memory_order_relaxed);
      }
      if (!retry) break;
      spdlog::warn("Redis replication connection to {}:{} ended: {}",
                   source->upstream_.host_, source->upstream_.port_,
                   connected.message());
      absl::Status slept =
          co_await celer::SleepFor(*celer::ThisWorker().self_, kReconnectDelay);
      if (!slept.ok()) {
        source->coordinator_started_ = false;
        co_return slept;
      }
    }
    source->coordinator_started_ = false;
    co_return absl::OkStatus();
  }

  Task<absl::Status> Coordinator() {
    while (true) {
      ReplicaOfConfig upstream;
      std::uint64_t role_epoch = 0;
      std::shared_ptr<ReplicaSession> session;
      {
        std::lock_guard lock(state_mutex_);
        if (!upstream_.has_value()) break;
        upstream = *upstream_;
        role_epoch = role_epoch_.load(std::memory_order_relaxed);
        session = std::make_shared<ReplicaSession>();
        active_replica_session_ = session;
      }
      StoreRole(ReplicationRole::kConnecting, std::memory_order_release);
      absl::Status connected =
          co_await RunReplicaSession(upstream, role_epoch, session);
      session->Cancel();
      if (session->session_id_ != 0) {
        absl::Status discarded =
            co_await storage_->AbortReplicaRoot(session->session_id_);
        if (!discarded.ok()) {
          spdlog::error("failed to discard partial replica data: {}",
                        discarded.message());
        }
      }

      bool retry = false;
      {
        std::lock_guard lock(state_mutex_);
        if (active_replica_session_ == session) {
          active_replica_session_.reset();
          replica_session_id_ = 0;
          source_worker_count_ = 0;
        }
        retry = upstream_.has_value() &&
                role_epoch_.load(std::memory_order_relaxed) == role_epoch;
      }
      if (!retry) continue;
      StoreRole(ReplicationRole::kConnecting, std::memory_order_release);
      spdlog::warn("replication connection to {}:{} ended: {}", upstream.host_,
                   upstream.port_, connected.message());
      absl::Status slept =
          co_await celer::SleepFor(*celer::ThisWorker().self_, kReconnectDelay);
      if (!slept.ok()) {
        coordinator_started_ = false;
        co_return slept;
      }
    }
    coordinator_started_ = false;
    co_return absl::OkStatus();
  }

  Task<absl::Status> ImportRedisRdb(
      const std::string& path, const std::shared_ptr<RedisSource>& source) {
    auto reader = rdb::FileReader::Open(path);
    if (!reader.ok()) co_return reader.status();

    std::uint64_t entries = 0;
    std::uint64_t skipped = 0;
    std::vector<std::string> function_libraries;
    while (true) {
      auto next = reader->Next();
      if (!next.ok()) co_return next.status();
      if (!next->has_value()) break;
      if ((**next).kind_ == rdb::FileEntryKind::kValue) {
        ++entries;
      } else if ((**next).kind_ == rdb::FileEntryKind::kFunctionLibrary) {
        function_libraries.push_back((**next).function_code_);
      } else {
        ++skipped;
      }
    }
    absl::Status functions_validated =
        co_await ValidateLuaFunctionCatalog(function_libraries);
    if (!functions_validated.ok()) co_return functions_validated;
    reader->Rewind();

    co_await redis_fullsync_mutex_.Lock();
    celer::UnlockGuard fullsync_unlock(&redis_fullsync_mutex_,
                                       celer::ThisWorker().self_);

    while (!CloseAllCommandDbGates()) {
      absl::Status waited = co_await celer::SleepFor(
          *celer::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) co_return waited;
    }
    struct CommandGateGuard {
      ~CommandGateGuard() { OpenAllCommandDbGates(); }
    } command_gate_guard;
    while (CommandDbOperationsActive()) {
      absl::Status waited = co_await celer::SleepFor(
          *celer::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) co_return waited;
    }
    {
      std::lock_guard lock(state_mutex_);
      if (!redis_psync_.load(std::memory_order_relaxed) ||
          source->role_epoch_ != role_epoch_.load(std::memory_order_relaxed) ||
          !RedisSourceRegistered(source) || redis_topology_fault_) {
        co_return absl::CancelledError(
            "Redis full sync was cancelled by a role or topology change");
      }
    }

    const std::vector<std::uint16_t> slots = RedisSlotsVector(source->slots_);
    absl::Status cleared = co_await storage_->ResetPartitionsDetach(slots);
    if (!cleared.ok()) co_return cleared;

    std::uint64_t imported = 0;
    std::uint64_t expired = 0;
    while (true) {
      auto next = reader->Next();
      if (!next.ok()) {
        (void)co_await storage_->ResetPartitionsDetach(slots);
        co_return next.status();
      }
      if (!next->has_value()) break;
      rdb::FileEntry entry = std::move(**next);
      if (entry.kind_ != rdb::FileEntryKind::kValue) {
        if (entry.kind_ == rdb::FileEntryKind::kFunctionLibrary) {
          continue;
        } else if (entry.kind_ == rdb::FileEntryKind::kSkippedModuleValue) {
          spdlog::warn(
              "Redis PSYNC skipped unsupported Module value db={} "
              "key-bytes={}",
              entry.db_id_, entry.key_.size());
        } else if (entry.kind_ == rdb::FileEntryKind::kSkippedModuleAux) {
          spdlog::warn("Redis PSYNC skipped unsupported Module auxiliary data");
        }
        continue;
      }
      const std::uint16_t slot = storage::RedisSlot(entry.key_);
      if (!source->slots_.test(slot)) {
        (void)co_await storage_->ResetPartitionsDetach(slots);
        co_return absl::FailedPreconditionError(
            absl::StrCat("Redis RDB key belongs to slot ", slot,
                         " outside source ownership"));
      }
      const unsigned owner = storage_->OwnerForKey(entry.key_);
      auto apply = [this, entry = std::move(entry)]() mutable
          -> Task<absl::StatusOr<storage::RestoreRawResult>> {
        co_return co_await storage_->RestoreRawValue(
            entry.db_id_, entry.key_, entry.value_, /*replace=*/false, nullptr);
      };
      absl::StatusOr<storage::RestoreRawResult> result;
      if (owner == celer::ThisWorker().id_) {
        result = co_await apply();
      } else {
        result = co_await celer::SubmitTaskTo(owner, std::move(apply));
      }
      if (!result.ok() || result->busy_) {
        const absl::Status failure =
            result.ok() ? absl::AlreadyExistsError("duplicate key in Redis RDB")
                        : result.status();
        absl::Status discarded =
            co_await storage_->ResetPartitionsDetach(slots);
        if (!discarded.ok()) {
          co_return absl::InternalError(absl::StrCat(
              "Redis RDB import failed: ", failure.message(),
              "; failed to discard partial import: ", discarded.message()));
        }
        co_return failure;
      }
      if (result->changed_) {
        ++imported;
      } else {
        ++expired;
      }
    }
    absl::Status functions_installed =
        co_await ReplaceLuaFunctionCatalog(function_libraries);
    if (!functions_installed.ok()) {
      (void)co_await storage_->ResetPartitionsDetach(slots);
      co_return functions_installed;
    }
    spdlog::info(
        "Redis PSYNC loaded RDB version={} entries={} imported={} expired={} "
        "unsupported-skipped={} slots={}",
        reader->version(), entries, imported, expired, skipped,
        FormatRedisSlots(source->slots_));
    co_return absl::OkStatus();
  }

  Task<absl::Status> ExpectRedisReply(TcpStream& stream,
                                      std::vector<std::string> command,
                                      std::string_view expected) {
    const std::string encoded = EncodeRespCommand(command);
    absl::Status sent = co_await WriteText(stream, encoded);
    if (!sent.ok()) co_return sent;
    auto reply = co_await ReadLine(stream);
    if (!reply.ok()) co_return reply.status();
    if (*reply != expected) {
      co_return absl::FailedPreconditionError(
          absl::StrCat("Redis replication handshake failed: ", *reply));
    }
    co_return absl::OkStatus();
  }

  Task<absl::Status> SendRedisAck(TcpStream& stream,
                                  const std::shared_ptr<RedisSource>& source) {
    const std::vector<std::string> command{
        "REPLCONF", "ACK",
        absl::StrCat(source->offset_.load(std::memory_order_acquire))};
    const std::string encoded = EncodeRespCommand(command);
    co_return co_await WriteText(stream, encoded);
  }

  absl::Status ValidateRedisSourceCommand(
      const ReplicatedCommand& command,
      const std::shared_ptr<RedisSource>& source) {
    if (!redis_cluster_) return absl::OkStatus();
    RespCommand wire{.args_ = command.args_};
    auto request = BuildCommandRequest(std::move(wire), command.db_id_);
    if (!request.ok()) return request.status();
    if (request->kind_ == CommandKind::kFlushDb ||
        request->kind_ == CommandKind::kFlushAll ||
        request->kind_ == CommandKind::kFunction) {
      return absl::OkStatus();
    }
    if (request->spec_ == nullptr) {
      return absl::InvalidArgumentError("unknown Redis replication command");
    }
    auto keys = DetermineKeys(*request->spec_, request->args_);
    if (!keys.ok() || keys->count() == 0) {
      return absl::InvalidArgumentError(
          "Redis replication command has no routable key");
    }
    for (std::uint16_t index = keys->first_; index <= keys->last_;
         index = static_cast<std::uint16_t>(index + keys->step_)) {
      const std::uint16_t slot = storage::RedisSlot(request->args_[index]);
      if (!source->slots_.test(slot)) {
        return absl::FailedPreconditionError(
            absl::StrCat("Redis command key belongs to slot ", slot,
                         " outside source ownership"));
      }
    }
    return absl::OkStatus();
  }

  Task<absl::Status> ResetRedisSourceSlots(
      const std::shared_ptr<RedisSource>& source) {
    co_await redis_fullsync_mutex_.Lock();
    celer::UnlockGuard fullsync_unlock(&redis_fullsync_mutex_,
                                       celer::ThisWorker().self_);
    while (!CloseAllCommandDbGates()) {
      absl::Status waited = co_await celer::SleepFor(
          *celer::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) co_return waited;
    }
    struct CommandGateGuard {
      ~CommandGateGuard() { OpenAllCommandDbGates(); }
    } reopen;
    while (CommandDbOperationsActive()) {
      absl::Status waited = co_await celer::SleepFor(
          *celer::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) co_return waited;
    }
    {
      std::lock_guard lock(state_mutex_);
      if (!RedisSourceRegistered(source) || redis_topology_fault_) {
        co_return absl::CancelledError("Redis source was replaced");
      }
    }
    const std::vector<std::uint16_t> slots = RedisSlotsVector(source->slots_);
    co_return co_await storage_->ResetPartitionsDetach(slots);
  }

  Task<absl::Status> ConsumeRedisCommandStream(
      TcpStream& stream, const std::shared_ptr<RedisSource>& source) {
    RedisCommandStream commands(&stream);
    std::uint8_t db_id = 0;
    bool in_multi = false;
    std::uint64_t transaction_bytes = 0;
    std::vector<ReplicatedCommand> transaction;
    while (true) {
      auto wire = co_await commands.Next();
      if (!wire.ok()) co_return wire.status();
      if (wire->command_.args_.empty()) {
        co_return absl::InvalidArgumentError(
            "empty command in Redis replication stream");
      }
      const std::string name = wire->command_.args_.front();
      if (EqualCaseInsensitive(name, "SELECT")) {
        unsigned selected = 0;
        if (in_multi || wire->command_.args_.size() != 2 ||
            !ParseUnsigned(wire->command_.args_[1], &selected) ||
            selected >= storage::kLogicalDatabaseCount) {
          co_return absl::InvalidArgumentError(
              "invalid SELECT in Redis replication stream");
        }
        db_id = static_cast<std::uint8_t>(selected);
        source->offset_.fetch_add(wire->bytes_, std::memory_order_acq_rel);
        continue;
      }
      if (EqualCaseInsensitive(name, "PING")) {
        if (in_multi) {
          co_return absl::InvalidArgumentError(
              "PING inside Redis replicated transaction");
        }
        source->offset_.fetch_add(wire->bytes_, std::memory_order_acq_rel);
        absl::Status acked = co_await SendRedisAck(stream, source);
        if (!acked.ok()) co_return acked;
        continue;
      }
      if (EqualCaseInsensitive(name, "REPLCONF")) {
        if (in_multi || wire->command_.args_.size() != 3 ||
            !EqualCaseInsensitive(wire->command_.args_[1], "GETACK")) {
          co_return absl::InvalidArgumentError(
              "unsupported REPLCONF in Redis replication stream");
        }
        source->offset_.fetch_add(wire->bytes_, std::memory_order_acq_rel);
        absl::Status acked = co_await SendRedisAck(stream, source);
        if (!acked.ok()) co_return acked;
        continue;
      }
      if (EqualCaseInsensitive(name, "MULTI")) {
        if (in_multi || wire->command_.args_.size() != 1) {
          co_return absl::InvalidArgumentError(
              "invalid MULTI in Redis replication stream");
        }
        in_multi = true;
        transaction.clear();
        transaction_bytes = wire->bytes_;
        continue;
      }
      if (EqualCaseInsensitive(name, "EXEC")) {
        if (!in_multi || wire->command_.args_.size() != 1) {
          co_return absl::InvalidArgumentError(
              "EXEC without MULTI in Redis replication stream");
        }
        transaction_bytes += wire->bytes_;
        for (const ReplicatedCommand& command : transaction) {
          absl::Status valid = ValidateRedisSourceCommand(command, source);
          if (!valid.ok()) co_return valid;
        }
        absl::Status applied =
            co_await ApplyRedisReplicatedTransaction(transaction);
        if (!applied.ok()) co_return applied;
        source->offset_.fetch_add(transaction_bytes, std::memory_order_acq_rel);
        transaction.clear();
        transaction_bytes = 0;
        in_multi = false;
        continue;
      }
      ReplicatedCommand command{.db_id_ = db_id,
                                .args_ = std::move(wire->command_.args_)};
      if (in_multi) {
        transaction_bytes += wire->bytes_;
        transaction.push_back(std::move(command));
        continue;
      }
      absl::Status valid = ValidateRedisSourceCommand(command, source);
      if (!valid.ok()) co_return valid;
      if (redis_cluster_ && (EqualCaseInsensitive(name, "FLUSHDB") ||
                             EqualCaseInsensitive(name, "FLUSHALL"))) {
        if (command.args_.size() > 2 ||
            (command.args_.size() == 2 &&
             !EqualCaseInsensitive(command.args_[1], "ASYNC") &&
             !EqualCaseInsensitive(command.args_[1], "SYNC"))) {
          co_return absl::InvalidArgumentError(
              "invalid Redis replicated flush command");
        }
        absl::Status reset = co_await ResetRedisSourceSlots(source);
        if (!reset.ok()) co_return reset;
        source->offset_.fetch_add(wire->bytes_, std::memory_order_acq_rel);
        continue;
      }
      absl::Status applied = co_await ApplyRedisReplicatedCommand(command);
      if (!applied.ok()) {
        co_return absl::Status(applied.code(),
                               absl::StrCat("failed to apply Redis command '",
                                            name, "': ", applied.message()));
      }
      source->offset_.fetch_add(wire->bytes_, std::memory_order_acq_rel);
    }
  }

  Task<absl::StatusOr<RedisPsyncReply>> StartRedisPsync(
      TcpStream& stream, const std::shared_ptr<RedisSource>& source) {
    absl::Status status =
        co_await AuthenticateUpstream(stream, masteruser_, masterauth_);
    if (!status.ok()) co_return status;
    {
      std::vector<std::string> command{"PING"};
      status = co_await ExpectRedisReply(stream, std::move(command), "+PONG");
      if (!status.ok()) co_return status;
    }
    {
      std::vector<std::string> command{"REPLCONF", "listening-port",
                                       absl::StrCat(listen_port_)};
      status = co_await ExpectRedisReply(stream, std::move(command), "+OK");
      if (!status.ok()) co_return status;
    }
    // Do not advertise the EOF capability: length-delimited RDB transfer lets
    // us consume exactly the snapshot bytes without scanning for a delimiter.
    {
      std::vector<std::string> command{"REPLCONF", "capa", "psync2"};
      status = co_await ExpectRedisReply(stream, std::move(command), "+OK");
      if (!status.ok()) co_return status;
    }

    std::optional<std::string> replid;
    {
      std::lock_guard lock(state_mutex_);
      if (source->dataset_valid_.load(std::memory_order_acquire)) {
        replid = source->replid_;
      }
    }
    const bool can_continue = replid.has_value();
    std::vector<std::string> command;
    command.reserve(3);
    command.emplace_back("PSYNC");
    if (can_continue) {
      command.push_back(*replid);
      command.push_back(
          absl::StrCat(source->offset_.load(std::memory_order_acquire)));
    } else {
      command.emplace_back("?");
      command.emplace_back("-1");
    }
    const std::string encoded = EncodeRespCommand(command);
    status = co_await WriteText(stream, encoded);
    if (!status.ok()) co_return status;
    auto response = co_await ReadLine(stream);
    if (!response.ok()) co_return response.status();
    auto parsed = ParseRedisPsyncReply(*response);
    if (!parsed.ok()) co_return parsed.status();
    co_return std::move(*parsed);
  }

  Task<absl::Status> RedisFollowerOnline(
      TcpStream& stream, const std::shared_ptr<RedisSource>& source) {
    if (role_epoch_.load(std::memory_order_acquire) != source->role_epoch_) {
      co_return absl::CancelledError("replication role epoch was replaced");
    }
    StoreRedisLink(source, true);
    source->syncing_ = false;
    RefreshRedisRole();
    absl::Status status = co_await SendRedisAck(stream, source);
    if (!status.ok()) co_return status;
    spdlog::info("Redis PSYNC follower online with {}:{} slots={}",
                 source->upstream_.host_, source->upstream_.port_,
                 FormatRedisSlots(source->slots_));
    co_return co_await ConsumeRedisCommandStream(stream, source);
  }

  Task<absl::Status> CompleteRedisFullSync(
      TcpStream& stream, const std::shared_ptr<RedisSource>& source,
      std::string replid, std::uint64_t offset) {
    source->syncing_ = true;
    source->dataset_valid_ = false;
    RefreshRedisRole();
    auto rdb_path = co_await ReceiveRedisRdb(stream);
    if (!rdb_path.ok()) co_return rdb_path.status();
    absl::Status status = co_await ImportRedisRdb(*rdb_path, source);
    (void)::unlink(rdb_path->c_str());
    if (!status.ok()) co_return status;
    source->offset_.store(offset, std::memory_order_release);
    source->dataset_valid_ = true;
    {
      std::lock_guard lock(state_mutex_);
      source->replid_ = std::move(replid);
      if (redis_sources_.front() == source) {
        upstream_node_id_ = source->replid_;
        upstream_history_id_ = source->replid_;
      }
      source_worker_count_ = static_cast<unsigned>(redis_sources_.size());
    }
    spdlog::info("Redis FULLRESYNC completed from {}:{} at offset {}",
                 source->upstream_.host_, source->upstream_.port_,
                 source->offset_.load(std::memory_order_acquire));
    co_return co_await RedisFollowerOnline(stream, source);
  }

  Task<absl::Status> RunRedisReplicaSession(
      const std::shared_ptr<RedisSource>& source,
      const std::shared_ptr<ReplicaSession>& session) {
    auto connected = co_await ConnectTcp(source->upstream_.host_,
                                         source->upstream_.port_, tls_context_);
    if (!connected.ok()) co_return connected.status();
    TcpStream stream = std::move(*connected);
    const int fd = stream.NativeFd();
    if (!session->sockets_.Add(fd)) {
      stream.Close().IgnoreError();
      co_return absl::CancelledError("replication session was cancelled");
    }
    session->connected_flows_.store(1, std::memory_order_release);
    ReplicationConnectionMetricGuard connection_metric(
        ReplicationConnectionKind::kControl);
    absl::Status result = co_await RunRedisConnectedSession(source, stream);
    session->connected_flows_.store(0, std::memory_order_release);
    session->sockets_.Remove(fd);
    stream.Close().IgnoreError();
    co_return result;
  }

  Task<absl::Status> RunRedisConnectedSession(
      const std::shared_ptr<RedisSource>& source, TcpStream& stream) {
    auto reply = co_await StartRedisPsync(stream, source);
    if (!reply.ok()) co_return reply.status();

    if (reply->full_) {
      co_return co_await CompleteRedisFullSync(
          stream, source, std::move(*reply->replid_), reply->offset_);
    }
    bool valid_cursor = false;
    {
      std::lock_guard lock(state_mutex_);
      valid_cursor = source->dataset_valid_.load(std::memory_order_acquire) &&
                     source->replid_.has_value();
    }
    if (!valid_cursor) {
      co_return absl::FailedPreconditionError(
          "Redis accepted partial sync without a valid local dataset");
    }
    if (reply->replid_.has_value()) {
      std::lock_guard lock(state_mutex_);
      source->replid_ = std::move(reply->replid_);
    }
    spdlog::info("Redis partial resynchronization continued from offset {}",
                 source->offset_.load(std::memory_order_acquire));
    co_return co_await RedisFollowerOnline(stream, source);
  }

  Task<absl::Status> RunReplicaSession(
      const ReplicaOfConfig& upstream, std::uint64_t role_epoch,
      const std::shared_ptr<ReplicaSession>& session) {
    auto connected =
        co_await ConnectTcp(upstream.host_, upstream.port_, tls_context_);
    if (!connected.ok()) co_return connected.status();
    TcpStream control = std::move(*connected);
    const int control_fd = control.NativeFd();
    if (!session->sockets_.Add(control_fd)) {
      control.Close().IgnoreError();
      co_return absl::CancelledError("replication role epoch was replaced");
    }
    ReplicationConnectionMetricGuard connection_metric(
        ReplicationConnectionKind::kControl);

    absl::Status authenticated =
        co_await AuthenticateUpstream(control, masteruser_, masterauth_);
    if (!authenticated.ok()) {
      session->sockets_.Remove(control_fd);
      control.Close().IgnoreError();
      co_return authenticated;
    }

    StoreRole(ReplicationRole::kSyncing, std::memory_order_release);
    // The native protocol carries the replica node identity separately
    // from the history id it wants to continue.
    const std::vector<std::string> sync_args{
        "KLPSYNC", std::string(kProtocolVersion),
        absl::StrCat("?", node_id_, ":", listen_port_),
        upstream_history_id_.value_or("?")};
    absl::Status sent =
        co_await WriteText(control, EncodeRespCommand(sync_args));
    if (!sent.ok()) {
      session->sockets_.Remove(control_fd);
      control.Close().IgnoreError();
      co_return sent;
    }
    auto response = co_await ReadLine(control);
    if (!response.ok()) {
      session->sockets_.Remove(control_fd);
      control.Close().IgnoreError();
      co_return response.status();
    }
    const std::vector<std::string_view> words = SplitWords(*response);
    std::uint64_t session_id = 0;
    unsigned source_workers = 0;
    if (words.size() != 5 || words[0] != "+KLFULLRESYNC" ||
        !ParseUnsigned(words[1], &session_id) || session_id == 0 ||
        words[2].size() != 40 || words[3].size() != 40 ||
        !ParseUnsigned(words[4], &source_workers) || source_workers == 0) {
      session->sockets_.Remove(control_fd);
      control.Close().IgnoreError();
      co_return absl::InvalidArgumentError(
          "invalid KLPSYNC response from upstream");
    }
    if (role_epoch_.load(std::memory_order_acquire) != role_epoch) {
      session->sockets_.Remove(control_fd);
      control.Close().IgnoreError();
      co_return absl::CancelledError("replication role epoch was replaced");
    }
    session->session_id_ = session_id;
    session->source_worker_count_ = source_workers;
    session->transaction_owners_.reserve(storage_->worker_count());
    for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
      session->transaction_owners_.push_back(
          std::make_unique<ReplicaTransactionOwner>());
    }
    session->fullsync_cut_ =
        std::make_unique<celer::CoroutineBarrier>(source_workers);
    session->root_swap_complete_ =
        std::make_unique<celer::CoroutineBarrier>(source_workers);
    // Flow count is defined by the upstream, not by this node's worker count.
    // Build the cursor state before spawning any flow so every flow_id has a
    // valid (lsn, fragment) pair, including when worker counts differ.
    auto next_cursors = std::make_shared<ReplicaCursorState>(source_workers);
    {
      std::lock_guard lock(state_mutex_);
      if (cursor_state_ != nullptr) {
        const unsigned copied = static_cast<unsigned>(
            std::min(cursor_state_->size(), next_cursors->size()));
        for (unsigned i = 0; i < copied; ++i) {
          const auto cursor = cursor_state_->Load(i);
          next_cursors->Store(i, cursor.lsn_, cursor.fragment_index_);
        }
      }
      cursor_state_ = next_cursors;
      session->cursors_ = std::move(next_cursors);
      if (active_replica_session_ == session) {
        upstream_node_id_ = std::string(words[2]);
        upstream_history_id_ = std::string(words[3]);
        replica_session_id_ = session_id;
        source_worker_count_ = source_workers;
      }
    }

    for (unsigned flow_id = 0; flow_id < source_workers; ++flow_id) {
      const unsigned owner = flow_id % storage_->worker_count();
      auto start = [this, upstream, session, flow_id]() {
        session->active_flows_.fetch_add(1, std::memory_order_acq_rel);
        celer::ThisWorker().self_->Spawn(
            RunReplicaFlow(upstream, session, flow_id));
        return absl::OkStatus();
      };
      absl::Status started;
      if (owner == celer::ThisWorker().id_) {
        started = start();
      } else {
        started = co_await celer::SubmitTo(owner, start);
      }
      if (!started.ok()) {
        session->sockets_.Remove(control_fd);
        control.Close().IgnoreError();
        (void)co_await CancelAndWaitForReplicaFlows(session);
        co_return started;
      }
    }

    auto online = co_await ReadLine(control);
    if (!online.ok() || *online != "+KLONLINE") {
      session->sockets_.Remove(control_fd);
      control.Close().IgnoreError();
      absl::Status failed =
          online.ok() ? absl::InvalidArgumentError(
                            "upstream did not complete flow handshake")
                      : online.status();
      absl::Status stopped = co_await CancelAndWaitForReplicaFlows(session);
      co_return stopped.ok() ? failed : stopped;
    }
    StoreRole(ReplicationRole::kOnline, std::memory_order_release);
    spdlog::info(
        "replication session {} online with {}:{} using 1+{} connections",
        session_id, upstream.host_, upstream.port_, source_workers);
    absl::Status waited = co_await WaitForClose(control);
    session->sockets_.Remove(control_fd);
    control.Close().IgnoreError();
    absl::Status stopped = co_await CancelAndWaitForReplicaFlows(session);
    co_return stopped.ok() ? waited : stopped;
  }

  Task<absl::Status> CancelAndWaitForReplicaFlows(
      const std::shared_ptr<ReplicaSession>& session) {
    session->Cancel();
    // Transaction tables are worker-owned, so cancellation must visit them on
    // their owner threads. Resolve every incomplete arrival before waiting for
    // detached apply tasks; otherwise an apply waiting on a predecessor from a
    // disconnected flow could keep the old session alive indefinitely.
    for (unsigned owner = 0; owner < session->transaction_owners_.size();
         ++owner) {
      auto cancel_owner = [session, owner]() {
        auto& transactions =
            session->transaction_owners_[owner]->transactions_;
        std::vector<std::shared_ptr<ReplicaTransactionArrival>> arrivals;
        arrivals.reserve(transactions.size());
        for (auto& [_, arrival] : transactions) {
          arrival->status_ =
              absl::CancelledError("replication session cancelled");
          arrivals.push_back(std::move(arrival));
        }
        transactions.clear();
        for (const auto& arrival : arrivals) {
          (void)arrival->completion_.ResolveOnce(
              storage::ReplicationTransactionResolution::kDiscard);
        }
        return absl::OkStatus();
      };
      absl::Status cancelled;
      if (owner == celer::ThisWorker().id_) {
        cancelled = cancel_owner();
      } else {
        cancelled = co_await celer::SubmitTo(owner, cancel_owner);
      }
      if (!cancelled.ok()) co_return cancelled;
    }
    auto next_warning =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (session->active_flows_.load(std::memory_order_acquire) != 0 ||
           session->active_transaction_applies_.load(
               std::memory_order_acquire) != 0) {
      absl::Status slept = co_await celer::SleepFor(
          *celer::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!slept.ok()) co_return slept;
      if (std::chrono::steady_clock::now() >= next_warning) {
        spdlog::warn(
            "waiting for {} cancelled replication flow(s) and {} transaction "
            "apply task(s) to finish",
            session->active_flows_.load(std::memory_order_acquire),
            session->active_transaction_applies_.load(
                std::memory_order_acquire));
        next_warning =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
      }
    }
    co_return absl::OkStatus();
  }

  Task<absl::Status> RunReplicaFlow(ReplicaOfConfig upstream,
                                    std::shared_ptr<ReplicaSession> session,
                                    unsigned flow_id) {
    ReplicaFlowActivityGuard activity(&session->active_flows_);
    auto connected =
        co_await ConnectTcp(upstream.host_, upstream.port_, tls_context_);
    if (!connected.ok()) {
      session->Cancel();
      co_return connected.status();
    }
    TcpStream stream = std::move(*connected);
    absl::Status bounded_recv = stream.SetReadAhead(false);
    if (!bounded_recv.ok()) {
      stream.Close().IgnoreError();
      session->Cancel();
      co_return bounded_recv;
    }
    const int fd = stream.NativeFd();
    if (!session->sockets_.Add(fd)) {
      stream.Close().IgnoreError();
      co_return absl::CancelledError("replication session was cancelled");
    }
    ReplicationConnectionMetricGuard connection_metric(
        ReplicationConnectionKind::kFlow);
    absl::Status authenticated =
        co_await AuthenticateUpstream(stream, masteruser_, masterauth_);
    if (!authenticated.ok()) {
      session->sockets_.Remove(fd);
      stream.Close().IgnoreError();
      session->Cancel();
      co_return authenticated;
    }
    const auto cursor = session->cursors_->Load(flow_id);
    spdlog::info(
        "replication target session {} flow {} requesting cursor={}:{}",
        session->session_id_, flow_id, cursor.lsn_, cursor.fragment_index_);
    const std::vector<std::string> flow_args{
        "KLFLOW",
        std::string(kProtocolVersion),
        std::to_string(session->session_id_),
        std::to_string(flow_id),
        std::to_string(cursor.lsn_),
        std::to_string(cursor.fragment_index_)};
    absl::Status sent =
        co_await WriteText(stream, EncodeRespCommand(flow_args));
    if (!sent.ok()) {
      session->sockets_.Remove(fd);
      stream.Close().IgnoreError();
      session->Cancel();
      co_return sent;
    }
    auto response = co_await ReadLine(stream);
    const std::string expected =
        absl::StrCat("+KLFLOW ", session->session_id_, " ", flow_id);
    if (!response.ok() || response->substr(0, expected.size()) != expected) {
      session->sockets_.Remove(fd);
      stream.Close().IgnoreError();
      session->Cancel();
      co_return response.ok()
          ? absl::InvalidArgumentError("invalid KLFLOW response")
          : response.status();
    }
    session->connected_flows_.fetch_add(1, std::memory_order_acq_rel);
    absl::Status data_status =
        co_await RunReplicaFlowData(stream, session, flow_id);
    if (!data_status.ok()) {
      spdlog::warn("replication target flow {} ended: {}", flow_id,
                   data_status.message());
    }
    session->connected_flows_.fetch_sub(1, std::memory_order_acq_rel);
    session->sockets_.Remove(fd);
    stream.Close().IgnoreError();
    session->Cancel();
    co_return data_status;
  }

  Task<absl::Status> WaitForReplicaTransaction(
      const std::shared_ptr<ReplicaTransactionArrival>& arrival) {
    const storage::ReplicationTransactionResolution resolution =
        co_await arrival->completion_.Wait(*celer::ThisWorker().self_);
    if (resolution == storage::ReplicationTransactionResolution::kDiscard &&
        arrival->status_.ok()) {
      co_return absl::CancelledError(
          "replicated transaction was discarded before completion");
    }
    co_return arrival->status_;
  }

  Task<absl::Status> ApplyReadyReplicaTransaction(
      std::shared_ptr<ReplicaSession> session,
      unsigned owner,
      std::shared_ptr<ReplicaTransactionArrival> arrival) {
    // The registration leaf spawns this coroutine on the transaction owner.
    // Keep all mutable arrival state on that worker until completion; waiters
    // observe status only after the latch's release/acquire publication.
    ReplicaFlowActivityGuard active(&session->active_transaction_applies_);
    std::vector<std::shared_ptr<ReplicaTransactionArrival>> predecessors =
        std::move(arrival->predecessors_);
    ReplicatedCommand command{
        .db_id_ = arrival->db_id_,
        .args_ = std::move(arrival->command_args_),
    };

    absl::Status status = absl::OkStatus();
    for (const auto& predecessor : predecessors) {
      status = co_await WaitForReplicaTransaction(predecessor);
      if (!status.ok()) break;
    }
    if (status.ok() && session->cancelled()) {
      status = absl::CancelledError(
          "replication session ended before transaction apply");
    }
    if (status.ok()) status = co_await ApplyReplicatedCommand(command);

    const bool cancelled = session->cancelled();
    if (cancelled) {
      arrival->status_ = absl::CancelledError(
          "replication session ended during transaction apply");
    } else {
      arrival->status_ = std::move(status);
      if (arrival->status_.ok()) {
        // Cursor publication precedes completion resolution, so every flow
        // ACK observes one atomic resumable cut for the participant set.
        for (unsigned participant : arrival->participants_) {
          session->cursors_->Store(participant,
                                   arrival->lsns_[participant] + 1, 0);
        }
      }
    }
    auto& transactions =
        session->transaction_owners_[owner]->transactions_;
    auto found = transactions.find(arrival->id_);
    if (found != transactions.end() && found->second == arrival) {
      // All declared participants arrived before apply started. Their flow
      // queues retain the shared arrival until ACK, so the owner table no
      // longer needs to extend its lifetime after publishing the result.
      transactions.erase(found);
    }
    (void)arrival->completion_.ResolveOnce(
        cancelled ? storage::ReplicationTransactionResolution::kDiscard
                  : storage::ReplicationTransactionResolution::kPublish);
    co_return absl::OkStatus();
  }

  absl::StatusOr<PreparedReplicaTransactionArrival>
  PrepareReplicaTransactionArrival(
      const std::shared_ptr<ReplicaSession>& session, unsigned flow_id,
      std::uint64_t lsn, ReplicatedCommand envelope,
      std::shared_ptr<ReplicaTransactionArrival> predecessor) {
    const auto& args = envelope.args_;
    if (args.empty() || !IsReplicationTransactionEnvelope(args[0])) {
      return absl::InvalidArgumentError(
          "malformed replicated transaction envelope");
    }
    auto metadata = DecodeReplicationTransactionEnvelope(args[0]);
    if (!metadata.ok()) return metadata.status();
    const std::uint64_t txid = metadata->id_;
    const unsigned payload_flow = metadata->payload_flow_;
    if (payload_flow >= session->source_worker_count_ ||
        metadata->participants_.size() > session->source_worker_count_) {
      return absl::InvalidArgumentError(
          "replicated transaction metadata exceeds the source flow set");
    }
    const bool has_payload = args.size() > 1;
    std::vector<unsigned> participants = std::move(metadata->participants_);
    bool current_flow_participates = false;
    for (unsigned participant : participants) {
      if (participant >= session->source_worker_count_) {
        return absl::InvalidArgumentError(
            "invalid replicated transaction participant");
      }
      current_flow_participates |= participant == flow_id;
    }
    if (!current_flow_participates) {
      return absl::InvalidArgumentError(
          "replicated transaction arrived on a non-participant flow");
    }
    if ((flow_id == payload_flow) != has_payload ||
        std::find(participants.begin(), participants.end(), payload_flow) ==
            participants.end()) {
      return absl::InvalidArgumentError(
          "replicated transaction payload does not match its flow");
    }
    // Move the sole payload before crossing workers. SubmitTo retains this
    // prepared object in the originating coroutine frame and transfers only
    // its vector owners; the canonical command body is never copied.
    std::vector<std::string> command_args;
    if (has_payload) {
      command_args.reserve(args.size() - 1);
      std::move(envelope.args_.begin() + 1, envelope.args_.end(),
                std::back_inserter(command_args));
    }

    return PreparedReplicaTransactionArrival{
        .id_ = txid,
        .db_id_ = envelope.db_id_,
        .participants_ = std::move(participants),
        .command_args_ = std::move(command_args),
        .predecessor_ = std::move(predecessor),
        .payload_flow_ = payload_flow,
        .flow_id_ = flow_id,
        .lsn_ = lsn,
        .has_payload_ = has_payload,
    };
  }

  absl::StatusOr<std::shared_ptr<ReplicaTransactionArrival>>
  RegisterReplicaTransactionOnOwner(
      const std::shared_ptr<ReplicaSession>& session, unsigned owner,
      PreparedReplicaTransactionArrival prepared) {
    assert(owner == celer::ThisWorker().id_);
    if (session->cancelled()) {
      return absl::CancelledError(
          "replication session ended before transaction arrival");
    }
    auto& transactions =
        session->transaction_owners_[owner]->transactions_;
    std::shared_ptr<ReplicaTransactionArrival> arrival;
    bool start_apply = false;
    auto [it, inserted] = transactions.try_emplace(prepared.id_);
    if (inserted) {
      it->second = std::make_shared<ReplicaTransactionArrival>();
      it->second->id_ = prepared.id_;
      it->second->db_id_ = prepared.db_id_;
      it->second->participants_ = prepared.participants_;
      it->second->payload_flow_ = prepared.payload_flow_;
      if (prepared.has_payload_) {
        it->second->command_args_ = std::move(prepared.command_args_);
        it->second->payload_arrived_ = true;
      }
      it->second->arrived_.resize(session->source_worker_count_);
      it->second->lsns_.resize(session->source_worker_count_);
    }
    arrival = it->second;
    if (arrival->db_id_ != prepared.db_id_ ||
        arrival->participants_ != prepared.participants_ ||
        arrival->payload_flow_ != prepared.payload_flow_ ||
        arrival->arrived_[prepared.flow_id_]) {
      return absl::InvalidArgumentError(
          "conflicting replicated transaction envelope");
    }
    if (!inserted && prepared.has_payload_) {
      if (arrival->payload_arrived_) {
        return absl::InvalidArgumentError(
            "duplicate replicated transaction payload");
      }
      arrival->command_args_ = std::move(prepared.command_args_);
      arrival->payload_arrived_ = true;
    }
    if (prepared.predecessor_ != nullptr &&
        prepared.predecessor_ != arrival &&
        std::find(arrival->predecessors_.begin(),
                  arrival->predecessors_.end(), prepared.predecessor_) ==
            arrival->predecessors_.end()) {
      arrival->predecessors_.push_back(std::move(prepared.predecessor_));
    }
    arrival->arrived_[prepared.flow_id_] = true;
    arrival->lsns_[prepared.flow_id_] = prepared.lsn_;
    ++arrival->arrival_count_;
    if (arrival->arrival_count_ == arrival->participants_.size() &&
        arrival->payload_arrived_ && !arrival->applying_) {
      arrival->applying_ = true;
      start_apply = true;
    }

    if (start_apply) {
      session->active_transaction_applies_.fetch_add(1,
                                                     std::memory_order_acq_rel);
      celer::ThisWorker().self_->Spawn(
          ApplyReadyReplicaTransaction(session, owner, arrival));
    }
    return arrival;
  }

  Task<absl::StatusOr<std::shared_ptr<ReplicaTransactionArrival>>>
  RegisterReplicaTransaction(
      const std::shared_ptr<ReplicaSession>& session, unsigned flow_id,
      std::uint64_t lsn, ReplicatedCommand envelope,
      std::shared_ptr<ReplicaTransactionArrival> predecessor) {
    auto prepared = PrepareReplicaTransactionArrival(
        session, flow_id, lsn, std::move(envelope), std::move(predecessor));
    if (!prepared.ok()) co_return prepared.status();
    if (session->transaction_owners_.empty()) {
      co_return absl::InternalError(
          "replica transaction owners are not initialized");
    }
    const unsigned owner = static_cast<unsigned>(
        prepared->id_ % session->transaction_owners_.size());
    auto register_on_owner =
        [this, session, owner, prepared = std::move(*prepared)]() mutable {
          return RegisterReplicaTransactionOnOwner(
              session, owner, std::move(prepared));
        };
    co_return co_await celer::SubmitTo(owner, std::move(register_on_owner));
  }

  Task<absl::Status> ApplyReplicaControl(
      const std::shared_ptr<ReplicaSession>& session, unsigned flow_id,
      std::uint64_t lsn, ReplicatedCommand command) {
    if (command.args_.size() < 3 ||
        (command.args_[0] != "FLUSHDB" && command.args_[0] != "FLUSHALL")) {
      co_return absl::InvalidArgumentError(
          "malformed replicated control barrier");
    }
    std::uint64_t barrier_id = 0;
    const char* begin = command.args_[1].data();
    const char* end = begin + command.args_[1].size();
    const auto parsed = std::from_chars(begin, end, barrier_id);
    if (parsed.ec != std::errc{} || parsed.ptr != end || barrier_id == 0 ||
        flow_id >= session->source_worker_count_) {
      co_return absl::InvalidArgumentError(
          "invalid replicated control barrier identity");
    }

    std::shared_ptr<ReplicaControlArrival> arrival;
    bool apply_here = false;
    ReplicatedCommand apply_command;
    {
      std::lock_guard lock(session->control_mutex_);
      if (session->cancelled()) {
        co_return absl::CancelledError(
            "replication session ended before control barrier arrival");
      }
      auto [it, inserted] = session->controls_.try_emplace(barrier_id);
      if (inserted) {
        it->second = std::make_shared<ReplicaControlArrival>(
            session->source_worker_count_);
        it->second->command_ = command;
        it->second->arrived_.resize(session->source_worker_count_);
        it->second->lsns_.resize(session->source_worker_count_);
      }
      arrival = it->second;
      if (arrival->command_.db_id_ != command.db_id_ ||
          arrival->command_.args_ != command.args_ ||
          arrival->arrived_[flow_id]) {
        co_return absl::InvalidArgumentError(
            "conflicting replicated control barrier");
      }
      arrival->arrived_[flow_id] = true;
      arrival->lsns_[flow_id] = lsn;
      ++arrival->arrival_count_;
      if (arrival->arrival_count_ == session->source_worker_count_ &&
          !arrival->applying_) {
        arrival->applying_ = true;
        apply_here = true;
        apply_command = std::move(arrival->command_);
      }
    }

    if (apply_here) {
      absl::Status status = co_await ApplyReplicatedCommand(apply_command);
      std::lock_guard lock(session->control_mutex_);
      arrival->status_ = std::move(status);
      if (arrival->status_.ok()) {
        // Every source flow stops at this barrier. Publish their cursors as
        // one vector only after the DB epoch change is fully installed.
        for (unsigned participant = 0;
             participant < session->source_worker_count_; ++participant) {
          session->cursors_->Store(participant, arrival->lsns_[participant] + 1,
                                   0);
        }
      }
    }

    absl::Status completed =
        co_await arrival->completion_.Wait(*celer::ThisWorker().self_);
    if (!completed.ok()) co_return completed;

    absl::Status result;
    {
      std::lock_guard lock(session->control_mutex_);
      result = arrival->status_;
      ++arrival->departure_count_;
      if (arrival->departure_count_ == session->source_worker_count_) {
        session->controls_.erase(barrier_id);
      }
    }
    co_return result;
  }

  struct ReplicaOnlineCommand {
    std::uint64_t lsn_ = 0;
    ReplicatedCommand command_;
  };

  struct ReplicaOnlineCompletion {
    std::uint64_t lsn_ = 0;
    std::shared_ptr<ReplicaTransactionArrival> transaction_;
  };

  struct ReplicaOnlineApplyState {
    std::deque<ReplicaOnlineCommand> commands_;
    std::deque<ReplicaOnlineCompletion> completions_;
    celer::AsyncNotification command_ready_;
    celer::AsyncNotification capacity_ready_;
    celer::AsyncNotification completion_ready_;
    celer::AsyncNotification completion_capacity_ready_;
    celer::AsyncNotification stage_done_ready_;
    celer::AsyncNotification ack_done_ready_;
    absl::Status receiver_status_ =
        absl::UnknownError("replication flow receiver is running");
    absl::Status stage_status_ =
        absl::UnknownError("replication flow staging queue is running");
    absl::Status ack_status_ =
        absl::UnknownError("replication flow ACK queue is running");
    bool receiver_done_ = false;
    bool stage_done_ = false;
    bool ack_done_ = false;
  };

  Task<absl::Status> StageReplicaOnlineCommands(
      const std::shared_ptr<ReplicaSession>& session,
      unsigned flow_id, const std::shared_ptr<ReplicaOnlineApplyState>& state) {
    constexpr std::size_t kOnlineCompletionCommands = 256;
    std::shared_ptr<ReplicaTransactionArrival> last_transaction;
    for (;;) {
      while (state->commands_.empty() && !state->receiver_done_) {
        co_await state->command_ready_.Wait();
      }
      if (session->cancelled()) {
        co_return absl::CancelledError(
            "replication session ended while applying commands");
      }
      if (state->receiver_done_ && !state->receiver_status_.ok()) {
        co_return state->receiver_status_;
      }
      if (state->commands_.empty()) {
        co_return absl::UnavailableError("replication flow closed");
      }

      ReplicaOnlineCommand pending = std::move(state->commands_.front());
      state->commands_.pop_front();
      state->capacity_ready_.NotifyAll(*celer::ThisWorker().self_);
      const bool transaction =
          !pending.command_.args_.empty() &&
          IsReplicationTransactionEnvelope(pending.command_.args_[0]);
      const bool control = !pending.command_.args_.empty() &&
                           (pending.command_.args_[0] == "FLUSHDB" ||
                            pending.command_.args_[0] == "FLUSHALL");
      std::shared_ptr<ReplicaTransactionArrival> transaction_arrival;
      absl::Status applied = absl::OkStatus();
      if (transaction) {
        auto registered = co_await RegisterReplicaTransaction(
            session, flow_id, pending.lsn_, std::move(pending.command_),
            last_transaction);
        if (!registered.ok()) {
          applied = registered.status();
        } else {
          transaction_arrival = std::move(*registered);
          last_transaction = transaction_arrival;
        }
      } else {
        // A non-transaction event is a hard boundary for read-ahead on this
        // flow. Waiting only on the tail is enough because transaction
        // registration linked every earlier flow-local transaction into its
        // predecessor chain.
        if (last_transaction != nullptr) {
          applied = co_await WaitForReplicaTransaction(last_transaction);
          last_transaction.reset();
        }
        if (applied.ok() && control) {
          applied = co_await ApplyReplicaControl(
              session, flow_id, pending.lsn_, std::move(pending.command_));
        } else if (applied.ok()) {
          applied = co_await ApplyReplicatedCommand(pending.command_);
        }
      }
      if (!applied.ok()) {
        InvalidateReplicaContinuation(session);
        co_return applied;
      }
      while (state->completions_.size() >= kOnlineCompletionCommands &&
             !state->ack_done_) {
        co_await state->completion_capacity_ready_.Wait();
      }
      if (state->ack_done_) co_return state->ack_status_;
      state->completions_.push_back(ReplicaOnlineCompletion{
          .lsn_ = pending.lsn_,
          .transaction_ = std::move(transaction_arrival),
      });
      state->completion_ready_.NotifyAll(*celer::ThisWorker().self_);
    }
  }

  Task<absl::Status> AckReplicaOnlineCommands(
      TcpStream& stream, const std::shared_ptr<ReplicaSession>& session,
      unsigned flow_id, const std::shared_ptr<ReplicaOnlineApplyState>& state) {
    auto send_ack = [&stream](std::uint64_t lsn) -> Task<absl::Status> {
      std::string payload;
      payload.reserve(10);
      PutU16(payload, 0);
      PutU64(payload, lsn);
      co_return co_await WriteDataFrame(stream, DataFrameKind::kAck, payload);
    };

    for (;;) {
      while (state->completions_.empty() && !state->stage_done_) {
        co_await state->completion_ready_.Wait();
      }
      if (session->cancelled()) {
        co_return absl::CancelledError(
            "replication session ended while acknowledging commands");
      }
      if (state->stage_done_ && !state->stage_status_.ok()) {
        co_return state->stage_status_;
      }
      if (state->completions_.empty()) {
        co_return absl::UnavailableError(
            "replication flow staging queue closed");
      }

      ReplicaOnlineCompletion pending =
          std::move(state->completions_.front());
      state->completions_.pop_front();
      state->completion_capacity_ready_.NotifyAll(*celer::ThisWorker().self_);
      const bool transaction = pending.transaction_ != nullptr;
      absl::Status applied = absl::OkStatus();
      if (transaction) {
        applied = co_await WaitForReplicaTransaction(pending.transaction_);
      }
      if (!applied.ok()) {
        InvalidateReplicaContinuation(session);
        co_return applied;
      }

      // Transaction apply published every participant cursor before resolving
      // its completion latch. Re-storing this flow is harmless and keeps the
      // single-flow and transaction ACK paths structurally identical.
      session->cursors_->Store(flow_id, pending.lsn_ + 1, 0);
      if (transaction && ShouldInjectFlowDropAfterTransaction(flow_id)) {
        co_return absl::UnavailableError(
            "injected replication flow disconnect after transaction");
      }
      if (!transaction && ShouldInjectFlowDropAfterCommandApply(flow_id)) {
        co_return absl::UnavailableError(
            "injected replication flow disconnect after command apply");
      }
      absl::Status acknowledged = co_await send_ack(pending.lsn_);
      if (!acknowledged.ok()) co_return acknowledged;
    }
  }

  Task<absl::Status> TrackReplicaOnlineStage(
      TcpStream& stream, const std::shared_ptr<ReplicaSession>& session,
      unsigned flow_id, const std::shared_ptr<ReplicaOnlineApplyState>& state) {
    state->stage_status_ =
        co_await StageReplicaOnlineCommands(session, flow_id, state);
    state->stage_done_ = true;
    state->stage_done_ready_.NotifyAll(*celer::ThisWorker().self_);
    state->completion_ready_.NotifyAll(*celer::ThisWorker().self_);
    if (!state->stage_status_.ok()) {
      (void)::shutdown(stream.NativeFd(), SHUT_RDWR);
    }
    co_return state->stage_status_;
  }

  Task<absl::Status> TrackReplicaOnlineAcks(
      TcpStream& stream, const std::shared_ptr<ReplicaSession>& session,
      unsigned flow_id, const std::shared_ptr<ReplicaOnlineApplyState>& state) {
    state->ack_status_ =
        co_await AckReplicaOnlineCommands(stream, session, flow_id, state);
    state->ack_done_ = true;
    state->ack_done_ready_.NotifyAll(*celer::ThisWorker().self_);
    state->completion_capacity_ready_.NotifyAll(*celer::ThisWorker().self_);
    if (!state->ack_status_.ok()) {
      (void)::shutdown(stream.NativeFd(), SHUT_RDWR);
    }
    co_return state->ack_status_;
  }

  Task<absl::Status> RunReplicaOnlineFlowData(
      TcpStream& stream, const std::shared_ptr<ReplicaSession>& session,
      unsigned flow_id,
      std::pair<DataFrameKind, std::string> first_frame) {
    auto state = std::make_shared<ReplicaOnlineApplyState>();
    celer::ThisWorker().self_->Spawn(
        TrackReplicaOnlineStage(stream, session, flow_id, state));
    celer::ThisWorker().self_->Spawn(
        TrackReplicaOnlineAcks(stream, session, flow_id, state));

    std::uint64_t staged_command_lsn = 0;
    std::uint32_t next_command_fragment = 0;
    std::string staged_command;
    std::size_t received_commands = 0;
    std::optional<std::pair<DataFrameKind, std::string>> pending_frame(
        std::move(first_frame));
    absl::Status receiver_status = absl::OkStatus();
    constexpr std::size_t kOnlineQueueCommands = 256;
    while (stream.IsOpen()) {
      if (state->stage_done_ || state->ack_done_) {
        receiver_status = state->stage_done_ ? state->stage_status_
                                             : state->ack_status_;
        break;
      }
      absl::StatusOr<std::pair<DataFrameKind, std::string>> frame =
          pending_frame.has_value()
              ? absl::StatusOr<std::pair<DataFrameKind, std::string>>(
                    std::move(*pending_frame))
              : co_await ReadDataFrame(stream);
      pending_frame.reset();
      if (!frame.ok()) {
        receiver_status = frame.status();
        break;
      }
      if (frame->first != DataFrameKind::kCommand) {
        receiver_status = absl::InvalidArgumentError(
            "replication command frame expected after ONLINE handoff");
        break;
      }
      if (ShouldInjectFlowDrop(flow_id)) {
        receiver_status =
            absl::UnavailableError("injected replication flow disconnect");
        break;
      }

      DataReader reader(frame->second);
      std::uint64_t lsn = 0;
      std::uint32_t fragment = 0;
      std::uint8_t flags = 0;
      if (!reader.U64(&lsn) || !reader.U32(&fragment) ||
          !reader.U8(&flags) || reader.remaining() == 0) {
        receiver_status = absl::InvalidArgumentError(
            "malformed replication command frame");
        break;
      }
      const auto first_flag =
          static_cast<std::uint8_t>(storage::ReplicationFrameFlag::kFirst);
      const auto last_flag =
          static_cast<std::uint8_t>(storage::ReplicationFrameFlag::kLast);
      if ((flags & ~(first_flag | last_flag)) != 0) {
        receiver_status =
            absl::InvalidArgumentError("invalid replication command flags");
        break;
      }
      const bool first = (flags & first_flag) != 0;
      const bool last = (flags & last_flag) != 0;
      if (first) {
        if (fragment != 0 || staged_command_lsn != 0) {
          receiver_status = absl::InvalidArgumentError(
              "replication command fragments overlap");
          break;
        }
        staged_command_lsn = lsn;
        next_command_fragment = 0;
        staged_command.clear();
      }
      if (staged_command_lsn != lsn || fragment != next_command_fragment) {
        receiver_status = absl::InvalidArgumentError(
            "replication command fragment is out of order");
        break;
      }
      absl::Status appended = AppendReplicationString(
          &staged_command,
          std::string_view(frame->second.data() + 13, reader.remaining()));
      if (!appended.ok()) {
        receiver_status = appended;
        break;
      }
      ++next_command_fragment;
      if (!last) continue;
      auto command = DecodeReplicationCommand(staged_command);
      if (!command.ok()) {
        receiver_status = command.status();
        break;
      }
      while (state->commands_.size() >= kOnlineQueueCommands &&
             !state->stage_done_ && !state->ack_done_) {
        co_await state->capacity_ready_.Wait();
      }
      if (state->stage_done_ || state->ack_done_) {
        receiver_status = state->stage_done_ ? state->stage_status_
                                             : state->ack_status_;
        break;
      }
      state->commands_.push_back(ReplicaOnlineCommand{
          .lsn_ = lsn,
          .command_ = std::move(*command),
      });
      state->command_ready_.NotifyAll(*celer::ThisWorker().self_);
      staged_command_lsn = 0;
      next_command_fragment = 0;
      staged_command.clear();
      // Loopback and fast LAN reads can remain immediately-ready for hundreds
      // of megabytes. Give the owner-local FIFO consumer a bounded scheduling
      // opportunity even when ingress never naturally suspends.
      if ((++received_commands % kFullSyncSchedulingItems) == 0) {
        co_await celer::Yield(*celer::ThisWorker().self_);
      }
    }

    if (receiver_status.ok()) {
      receiver_status = absl::UnavailableError("replication flow closed");
    }
    state->receiver_status_ = receiver_status;
    state->receiver_done_ = true;
    state->command_ready_.NotifyAll(*celer::ThisWorker().self_);
    if (!state->stage_done_ || !state->ack_done_) {
      // A broken ingress flow can strand staging, ACK, and predecessor tasks.
      // Cancel the whole session before joining so every cross-worker latch is
      // resolved and neither worker-local task can retain `stream`.
      session->Cancel();
      while (!state->stage_done_) {
        co_await state->stage_done_ready_.Wait();
      }
      while (!state->ack_done_) {
        co_await state->ack_done_ready_.Wait();
      }
    }
    if (!state->stage_status_.ok()) co_return state->stage_status_;
    if (!state->ack_status_.ok()) co_return state->ack_status_;
    co_return receiver_status;
  }

  Task<absl::Status> RunReplicaFlowData(
      TcpStream& stream, const std::shared_ptr<ReplicaSession>& session,
      unsigned flow_id) {
    absl::flat_hash_map<std::uint16_t, std::uint64_t> epochs;
    std::uint64_t expected_fullsync_sequence = 1;
    std::uint64_t staged_command_lsn = 0;
    std::uint32_t next_command_fragment = 0;
    std::string staged_command;
    auto send_ack = [&stream](std::uint16_t partition_id,
                              std::uint64_t sequence) -> Task<absl::Status> {
      std::string payload;
      payload.reserve(10);
      PutU16(payload, partition_id);
      PutU64(payload, sequence);
      co_return co_await WriteDataFrame(stream, DataFrameKind::kAck, payload);
    };
    while (stream.IsOpen()) {
      auto frame = co_await ReadDataFrame(stream);
      if (!frame.ok()) co_return frame.status();
      if (frame->first == DataFrameKind::kReset) {
        DataReader reader(frame->second);
        std::uint32_t reset_count = 0;
        if (!reader.U32(&reset_count) || reset_count == 0 ||
            reset_count > storage::kLogicalStorageShards) {
          co_return absl::InvalidArgumentError("malformed replication reset");
        }
        std::vector<std::vector<storage::ReplicaPartitionReset>> by_owner(
            storage_->worker_count());
        std::array<bool, storage::kLogicalStorageShards> seen{};
        for (std::uint32_t index = 0; index < reset_count; ++index) {
          storage::ReplicaPartitionReset reset;
          if (!reader.U16(&reset.partition_id_) ||
              reset.partition_id_ >= storage::kLogicalStorageShards ||
              seen[reset.partition_id_]) {
            co_return absl::InvalidArgumentError("malformed replication reset");
          }
          seen[reset.partition_id_] = true;
          for (std::uint64_t& epoch : reset.db_epochs_) {
            if (!reader.U64(&epoch) || epoch == 0) {
              co_return absl::InvalidArgumentError(
                  "malformed replication reset");
            }
          }
          by_owner[reset.partition_id_ % storage_->worker_count()].push_back(
              std::move(reset));
        }
        if (reader.remaining() != 0) {
          co_return absl::InvalidArgumentError("trailing replication reset");
        }
        for (unsigned owner = 0; owner < by_owner.size(); ++owner) {
          if (by_owner[owner].empty()) continue;
          if (owner == celer::ThisWorker().id_) {
            auto reset = co_await storage_->ResetReplicaPartitions(
                session->session_id_, by_owner[owner]);
            if (!reset.ok()) co_return reset.status();
            for (const storage::ReplicaPartitionEpoch& result : *reset) {
              epochs[result.partition_id_] = result.replication_epoch_;
            }
          } else {
            auto reset = co_await celer::SubmitTaskTo(
                owner,
                [this, session, resets = std::move(by_owner[owner])]() mutable {
                  return storage_->ResetReplicaPartitions(session->session_id_,
                                                          resets);
                });
            if (!reset.ok()) co_return reset.status();
            for (const storage::ReplicaPartitionEpoch& result : *reset) {
              epochs[result.partition_id_] = result.replication_epoch_;
            }
          }
        }
        absl::Status acknowledged =
            co_await send_ack(kResetBatchAckPartition, 0);
        if (!acknowledged.ok()) co_return acknowledged;
      } else if (frame->first == DataFrameKind::kPartitionHandoff) {
        DataReader reader(frame->second);
        std::uint64_t sequence = 0;
        std::uint16_t partition_id = 0;
        std::uint64_t tail_next_lsn = 0;
        if (!reader.U64(&sequence) || !reader.U16(&partition_id) ||
            !reader.U64(&tail_next_lsn) || reader.remaining() != 0 ||
            sequence != expected_fullsync_sequence || tail_next_lsn == 0 ||
            epochs.find(partition_id) == epochs.end()) {
          co_return absl::InvalidArgumentError(
              "malformed partition handoff frame");
        }
        const unsigned owner = partition_id % storage_->worker_count();
        const std::uint64_t epoch = epochs.at(partition_id);
        absl::Status handed_off = co_await celer::SubmitTaskTo(
            owner, [this, session, partition_id, epoch]() {
              return storage_->HandoffReplicaPartition(session->session_id_,
                                                       partition_id, epoch);
            });
        if (!handed_off.ok()) co_return handed_off;
        absl::Status acknowledged = co_await send_ack(partition_id, sequence);
        if (!acknowledged.ok()) co_return acknowledged;
        ++expected_fullsync_sequence;
      } else if (frame->first == DataFrameKind::kFullSyncCommand) {
        DataReader reader(frame->second);
        std::uint64_t sequence = 0;
        std::uint16_t partition_id = 0;
        std::uint64_t partition_sequence = 0;
        std::uint64_t source_lsn = 0;
        std::uint32_t fragment = 0;
        std::uint8_t flags = 0;
        if (!reader.U64(&sequence) || !reader.U16(&partition_id) ||
            !reader.U64(&partition_sequence) || !reader.U64(&source_lsn) ||
            !reader.U32(&fragment) || !reader.U8(&flags) ||
            sequence != expected_fullsync_sequence || partition_sequence == 0 ||
            source_lsn == 0 || reader.remaining() == 0 ||
            partition_id >= storage::kLogicalStorageShards) {
          co_return absl::InvalidArgumentError(
              "malformed full-sync published command");
        }
        const auto first_flag =
            static_cast<std::uint8_t>(storage::ReplicationFrameFlag::kFirst);
        const auto last_flag =
            static_cast<std::uint8_t>(storage::ReplicationFrameFlag::kLast);
        if ((flags & ~(first_flag | last_flag)) != 0) {
          co_return absl::InvalidArgumentError(
              "invalid full-sync command flags");
        }
        const bool first = (flags & first_flag) != 0;
        const bool last = (flags & last_flag) != 0;
        const unsigned owner = partition_id % storage_->worker_count();
        if (first) {
          if (fragment != 0 || staged_command_lsn != 0) {
            co_return absl::InvalidArgumentError(
                "full-sync command fragments overlap");
          }
          staged_command_lsn = source_lsn;
          next_command_fragment = 0;
          staged_command.clear();
        }
        if (staged_command_lsn != source_lsn ||
            fragment != next_command_fragment) {
          co_return absl::InvalidArgumentError(
              "full-sync command fragment is out of order");
        }
        absl::Status appended = AppendReplicationString(
            &staged_command,
            std::string_view(frame->second.data() + 31, reader.remaining()));
        if (!appended.ok()) co_return appended;
        ++next_command_fragment;
        if (last) {
          auto command = DecodeReplicationCommand(staged_command);
          const bool publish =
              command.ok() && !command->args_.empty() &&
              EqualCaseInsensitive(command->args_[0], "PUBLISH");
          // PUBLISH is routed by its channel slot only to spread transport
          // work; it owns no partition state. Sentinel traffic can therefore
          // reach the bounded full-sync FIFO before that slot's reset batch.
          // Reassemble and validate it normally, but keep the installed-epoch
          // invariant for every command that can touch the hidden dataset.
          if (command.ok() && !publish &&
              epochs.find(partition_id) == epochs.end()) {
            co_return absl::InvalidArgumentError(
                "full-sync mutation precedes partition reset");
          }
          const bool ephemeral =
              publish ||
              (command.ok() && !command->args_.empty() &&
               (command->args_[0] == kReplicatedExecCommand ||
                EqualCaseInsensitive(command->args_[0], "FUNCTION")));
          if (!ephemeral) {
            absl::Status begun = co_await celer::SubmitTaskTo(
                owner, [this, session, partition_id, partition_sequence]() {
                  return storage_->BeginReplicaTailCommand(
                      session->session_id_, partition_id, partition_sequence);
                });
            if (!begun.ok()) co_return begun;
          }
          absl::Status applied;
          if (!command.ok()) {
            applied = command.status();
          } else if (!command->args_.empty() &&
                     (IsReplicationTransactionEnvelope(command->args_[0]) ||
                      command->args_[0] == "FLUSHDB" ||
                      command->args_[0] == "FLUSHALL")) {
            applied = absl::InvalidArgumentError(
                "full-sync publish queue contains a non-mutation event");
          } else {
            applied = co_await ApplyReplicatedCommand(*command);
          }
          absl::Status ended = absl::OkStatus();
          if (!ephemeral) {
            ended = co_await celer::SubmitTaskTo(
                owner, [this, session, partition_id, partition_sequence]() {
                  return storage_->EndReplicaTailCommand(
                      session->session_id_, partition_id, partition_sequence);
                });
          }
          if (!applied.ok()) co_return applied;
          if (!ended.ok()) co_return ended;
          staged_command_lsn = 0;
          next_command_fragment = 0;
          staged_command.clear();
          // Full-sync command fragments are pipelined like ONLINE backlog
          // fragments. Intermediate fragments carry sequence ordering but do
          // not force a stop-and-wait round trip; the final ACK proves that
          // the complete logical command was applied and releases its queue
          // credit on the source.
          absl::Status acknowledged = co_await send_ack(partition_id, sequence);
          if (!acknowledged.ok()) co_return acknowledged;
        }
        ++expected_fullsync_sequence;
      } else if (frame->first == DataFrameKind::kFullSyncCut) {
        DataReader reader(frame->second);
        std::uint64_t sequence = 0;
        std::uint64_t stable_next_lsn = 0;
        if (!reader.U64(&sequence) || !reader.U64(&stable_next_lsn) ||
            reader.remaining() != 0 || sequence != expected_fullsync_sequence ||
            stable_next_lsn == 0 || session->fullsync_cut_ == nullptr ||
            session->root_swap_complete_ == nullptr) {
          co_return absl::InvalidArgumentError("malformed full-sync cut frame");
        }
        absl::Status cut =
            co_await session->fullsync_cut_->Wait(*celer::ThisWorker().self_);
        if (!cut.ok()) co_return cut;
        if (flow_id == 0) {
          while (!CloseAllCommandDbGates()) {
            absl::Status waited = co_await celer::SleepFor(
                *celer::ThisWorker().self_, std::chrono::milliseconds(1));
            if (!waited.ok()) {
              session->root_swap_complete_->Abort(waited);
              co_return waited;
            }
          }
          struct RootSwapGateGuard {
            ~RootSwapGateGuard() { OpenAllCommandDbGates(); }
          } root_swap_gate;
          while (CommandDbOperationsActive()) {
            absl::Status waited = co_await celer::SleepFor(
                *celer::ThisWorker().self_, std::chrono::milliseconds(1));
            if (!waited.ok()) {
              session->root_swap_complete_->Abort(waited);
              co_return waited;
            }
          }
          absl::Status promoted =
              co_await storage_->PromoteReplicaRoot(session->session_id_);
          if (!promoted.ok()) {
            session->root_swap_complete_->Abort(promoted);
            co_return promoted;
          }
          native_dataset_valid_.store(true, std::memory_order_release);
        }
        absl::Status swapped = co_await session->root_swap_complete_->Wait(
            *celer::ThisWorker().self_);
        if (!swapped.ok()) co_return swapped;
        absl::Status acknowledged =
            co_await send_ack(kResetBatchAckPartition, sequence);
        if (!acknowledged.ok()) co_return acknowledged;
        ++expected_fullsync_sequence;
      } else if (frame->first == DataFrameKind::kCommand) {
        co_return co_await RunReplicaOnlineFlowData(
            stream, session, flow_id, std::move(*frame));
      } else if (frame->first == DataFrameKind::kCursor) {
        DataReader reader(frame->second);
        std::uint64_t lsn = 0;
        std::uint32_t fragment = 0;
        if (!reader.U64(&lsn) || !reader.U32(&fragment) ||
            reader.remaining() != 0) {
          co_return absl::InvalidArgumentError("malformed replication cursor");
        }
        session->cursors_->Store(flow_id, lsn, fragment);
        absl::Status acknowledged = co_await send_ack(0, lsn);
        if (!acknowledged.ok()) co_return acknowledged;
      } else if (frame->first == DataFrameKind::kRecords) {
        DataReader sequence_reader(frame->second);
        std::uint64_t sequence = 0;
        if (!sequence_reader.U64(&sequence) ||
            sequence != expected_fullsync_sequence) {
          co_return absl::InvalidArgumentError(
              "full-sync records skip a sequence");
        }
        auto records = DecodeRecords(
            std::string_view(frame->second).substr(sizeof(sequence)));
        if (!records.ok()) co_return records.status();
        const auto found = epochs.find(records->first);
        if (found == epochs.end()) {
          co_return absl::FailedPreconditionError(
              "replication records arrived before reset");
        }
        const unsigned owner = records->first % storage_->worker_count();
        const std::uint16_t partition_id = records->first;
        const std::uint64_t epoch = found->second;
        absl::Status applied = co_await celer::SubmitTaskTo(
            owner, [this, session, partition_id, epoch,
                    records = std::move(records->second)]() mutable {
              return storage_->ApplyReplicaRecords(
                  session->session_id_, partition_id, epoch, records);
            });
        if (!applied.ok()) co_return applied;
        absl::Status acknowledged = co_await send_ack(records->first, sequence);
        if (!acknowledged.ok()) co_return acknowledged;
        ++expected_fullsync_sequence;
      } else {
        co_return absl::InvalidArgumentError(
            "unexpected replication data frame");
      }
    }
    co_return absl::UnavailableError("replication flow closed");
  }

  bool ShouldInjectFlowDrop(unsigned flow_id) {
    const char* configured =
        std::getenv("KEYLANE_REPLICATION_DROP_FLOW_AFTER_COMMAND");
    if (configured == nullptr) return false;
    unsigned target = 0;
    const std::size_t length = std::strlen(configured);
    const auto parsed =
        std::from_chars(configured, configured + length, target);
    if (parsed.ec != std::errc{} || parsed.ptr != configured + length ||
        target != flow_id) {
      return false;
    }
    return !replication_fault_drop_used_.exchange(true,
                                                  std::memory_order_acq_rel);
  }

  void InvalidateReplicaContinuation(
      const std::shared_ptr<ReplicaSession>& session) {
    std::lock_guard lock(state_mutex_);
    if (active_replica_session_ != session ||
        cursor_state_ != session->cursors_) {
      return;
    }
    // Replay may already have committed a successful non-idempotent prefix
    // before a later strict EXEC child failed. Dropping the shared cursor
    // state makes every flow request LSN 1 in the replacement session, which
    // forces one coordinated full sync instead of retrying that prefix.
    cursor_state_.reset();
    upstream_history_id_.reset();
  }

  bool ShouldInjectFlowDropAfterTransaction(unsigned flow_id) {
    const char* configured =
        std::getenv("KEYLANE_REPLICATION_DROP_FLOW_AFTER_TRANSACTION_APPLY");
    if (configured == nullptr) return false;
    unsigned target = 0;
    const std::size_t length = std::strlen(configured);
    const auto parsed =
        std::from_chars(configured, configured + length, target);
    if (parsed.ec != std::errc{} || parsed.ptr != configured + length ||
        target != flow_id) {
      return false;
    }
    return !replication_transaction_fault_drop_used_.exchange(
        true, std::memory_order_acq_rel);
  }

  bool ShouldInjectFlowDropAfterCommandApply(unsigned flow_id) {
    const char* configured =
        std::getenv("KEYLANE_REPLICATION_DROP_FLOW_AFTER_COMMAND_APPLY");
    if (configured == nullptr) return false;
    unsigned target = 0;
    const std::size_t length = std::strlen(configured);
    const auto parsed =
        std::from_chars(configured, configured + length, target);
    if (parsed.ec != std::errc{} || parsed.ptr != configured + length ||
        target != flow_id) {
      return false;
    }
    return !replication_command_apply_fault_drop_used_.exchange(
        true, std::memory_order_acq_rel);
  }

  Task<absl::Status> RunMasterFlowData(
      TcpStream& stream, const std::shared_ptr<MasterSession>& session,
      unsigned flow_id) {
    auto fullsync_start = storage_->BeginFullSyncSession(session->id_);
    if (!fullsync_start.ok()) co_return fullsync_start.status();
    const auto source_db_epochs = fullsync_start->db_epochs_;
    bool fullsync_session_active = true;
    const auto initial_log = storage_->LocalReplicationLogInfo();
    const std::uint64_t backlog_start_lsn =
        initial_log.tail_lsn_ == 0 ? 1 : initial_log.tail_lsn_ + 1;
    session->SetProgress(flow_id, ReplicationPhase::kReset, backlog_start_lsn,
                         0, 0, 0);
    struct CapturedPartition {
      std::uint16_t partition_id_ = 0;
      PartitionReplicationStart start_;
    };
    std::vector<CapturedPartition> captured;
    captured.reserve(
        (storage::kLogicalStorageShards + storage_->worker_count() - 1) /
        storage_->worker_count());
    absl::flat_hash_map<std::uint16_t, std::uint64_t> next_sequence;
    storage::ReplicationLogCursor fullsync_backlog_cursor{
        .lsn_ = backlog_start_lsn, .fragment_index_ = 0};
    std::uint64_t fullsync_sequence = 1;
    auto cleanup = [&]() {
      for (const CapturedPartition& partition : captured) {
        storage_->EndPartitionReplication(session->id_,
                                          partition.partition_id_);
      }
      if (fullsync_session_active) {
        storage_->EndFullSyncSession(session->id_);
        fullsync_session_active = false;
      }
    };

    auto send_records =
        [&](std::uint16_t partition_id,
            std::span<const SnapshotRecord> records) -> Task<absl::Status> {
      if (records.empty()) co_return absl::OkStatus();
      auto send_batch =
          [&](std::span<const SnapshotRecord> batch) -> Task<absl::Status> {
        std::string payload;
        absl::Status encoded = EncodeRecords(partition_id, batch, &payload);
        if (!encoded.ok()) co_return encoded;
        absl::Status sent = co_await WriteFullSyncFrameAndWaitAck(
            stream, DataFrameKind::kRecords, payload, partition_id,
            fullsync_sequence);
        if (!sent.ok()) co_return sent;
        session->TouchProgress(flow_id);
        ++fullsync_sequence;
        co_return absl::OkStatus();
      };

      std::size_t normal_start = 0;
      std::size_t normal_count = 0;
      std::size_t normal_bytes = 2 + 4;
      auto flush_normal = [&]() -> Task<absl::Status> {
        if (normal_count == 0) co_return absl::OkStatus();
        absl::Status sent =
            co_await send_batch(records.subspan(normal_start, normal_count));
        normal_count = 0;
        normal_bytes = 2 + 4;
        co_return sent;
      };
      for (std::size_t record_index = 0; record_index < records.size();
           ++record_index) {
        const SnapshotRecord& record = records[record_index];
        const std::size_t encoded = EncodedRecordBytes(record);
        const bool streamed = record.source_id_ != 0;
        if (!streamed && encoded <= kBacklogBatchBytes - (2 + 4)) {
          if (encoded > kBacklogBatchBytes - normal_bytes) {
            absl::Status sent = co_await flush_normal();
            if (!sent.ok()) co_return sent;
          }
          if (normal_count == 0) normal_start = record_index;
          ++normal_count;
          normal_bytes += encoded;
          continue;
        }
        absl::Status sent = co_await flush_normal();
        if (!sent.ok()) co_return sent;
        if (record.kind_ != SnapshotRecord::Kind::kValue ||
            record.key_.size() > kBacklogBatchBytes / 2 ||
            (!streamed && record.value_.empty()) ||
            (streamed &&
             (record.source_value_bytes_ == 0 || !record.value_.empty()))) {
          co_return absl::ResourceExhaustedError(
              "replication record identity exceeds frame limit");
        }
        const std::uint64_t value_bytes =
            streamed ? record.source_value_bytes_ : record.value_.size();
        const std::uint64_t chunks =
            (value_bytes + storage::kReplicationTransferBytes - 1) /
            storage::kReplicationTransferBytes;
        if (chunks > std::numeric_limits<std::uint32_t>::max()) {
          co_return absl::ResourceExhaustedError(
              "replication value has too many chunks");
        }
        std::string logical_size;
        PutU64(logical_size, record.logical_size_);
        SnapshotRecord begin{
            .kind_ = SnapshotRecord::Kind::kValueBegin,
            .db_id_ = record.db_id_,
            .db_epoch_ = record.db_epoch_,
            .mutation_sequence_ = record.mutation_sequence_,
            .expire_at_ms_ = record.expire_at_ms_,
            .value_type_ = record.value_type_,
            .logical_size_ = value_bytes,
            .chunk_index_ = 0,
            .chunk_count_ = static_cast<std::uint32_t>(chunks),
            .key_ = record.key_,
            .value_ = std::move(logical_size),
        };
        sent = co_await send_batch(std::span(&begin, 1));
        if (!sent.ok()) co_return sent;
        for (std::size_t chunk_index = 0; chunk_index < chunks; ++chunk_index) {
          const std::size_t offset =
              chunk_index * storage::kReplicationTransferBytes;
          std::string chunk_value;
          if (streamed) {
            auto read = co_await storage_->ReadFullSyncValueChunk(
                session->id_, partition_id, record.source_id_, offset,
                storage::kReplicationTransferBytes);
            if (!read.ok()) co_return read.status();
            chunk_value = std::move(*read);
          } else {
            chunk_value = record.value_.substr(
                offset, std::min(storage::kReplicationTransferBytes,
                                 record.value_.size() - offset));
          }
          SnapshotRecord chunk{
              .kind_ = SnapshotRecord::Kind::kValueChunk,
              .db_id_ = record.db_id_,
              .db_epoch_ = record.db_epoch_,
              .mutation_sequence_ = record.mutation_sequence_,
              .expire_at_ms_ = record.expire_at_ms_,
              .value_type_ = record.value_type_,
              .logical_size_ = record.logical_size_,
              .chunk_index_ = static_cast<std::uint32_t>(chunk_index),
              .chunk_count_ = static_cast<std::uint32_t>(chunks),
              .key_ = record.key_,
              .value_ = std::move(chunk_value),
          };
          sent = co_await send_batch(std::span(&chunk, 1));
          if (!sent.ok()) co_return sent;
        }
        SnapshotRecord commit{
            .kind_ = SnapshotRecord::Kind::kValueCommit,
            .db_id_ = record.db_id_,
            .db_epoch_ = record.db_epoch_,
            .mutation_sequence_ = record.mutation_sequence_,
            .expire_at_ms_ = record.expire_at_ms_,
            .value_type_ = record.value_type_,
            .logical_size_ = record.logical_size_,
            .chunk_index_ = static_cast<std::uint32_t>(chunks),
            .chunk_count_ = static_cast<std::uint32_t>(chunks),
            .key_ = record.key_,
            .value_ = {},
        };
        sent = co_await send_batch(std::span(&commit, 1));
        if (!sent.ok()) co_return sent;
      }
      absl::Status sent = co_await flush_normal();
      if (!sent.ok()) co_return sent;
      co_return absl::OkStatus();
    };

    auto drain_fullsync_publish_queue =
        [&](std::size_t max_items) -> Task<absl::Status> {
      std::size_t drained_items = 0;
      while (drained_items < max_items) {
        const std::size_t remaining_items = max_items - drained_items;
        auto pending = storage_->PeekFullSyncPublishItems(
            session->id_, std::min(remaining_items, kBacklogBatchFrames));
        if (!pending.ok()) co_return pending.status();
        if (pending->empty()) co_return absl::OkStatus();

        if (pending->front().record_.has_value()) {
          const storage::FullSyncPublishItem& item = pending->front();
          if (item.command_ != nullptr ||
              item.record_->mutation_sequence_ == 0) {
            co_return absl::InvalidArgumentError(
                "invalid record in full-sync publish queue");
          }
          const std::uint16_t partition_id =
              storage::RedisSlot(item.record_->key_);
          auto materialized =
              co_await storage_->MaterializeFullSyncPublishRecord(
                  session->id_, partition_id, *item.record_);
          if (!materialized.ok()) co_return materialized.status();
          absl::Status sent =
              co_await send_records(partition_id, std::span(&*materialized, 1));
          if (!sent.ok()) co_return sent;
          storage_->ReleaseFullSyncValue(session->id_, partition_id,
                                         materialized->source_id_);
          storage_->AcknowledgeFullSyncPublishItem(session->id_, item.id_);
          ++drained_items;
          continue;
        }

        struct StreamedFullSyncCommand {
          storage::FullSyncPublishItem item_;
          ReplicationCommandPayloadSource source_;
          std::size_t payload_bytes_ = 0;
        };
        std::vector<StreamedFullSyncCommand> commands;
        commands.reserve(pending->size());
        for (storage::FullSyncPublishItem& item : *pending) {
          if (item.record_.has_value()) break;
          if (item.command_ == nullptr || item.command_->args_.empty() ||
              item.command_->partition_id_ >= storage::kLogicalStorageShards ||
              item.command_->partition_sequence_ == 0) {
            co_return absl::InvalidArgumentError(
                "invalid command in full-sync publish queue");
          }
          std::vector<std::string_view> args;
          args.reserve(item.command_->args_.size());
          for (const std::string& arg : item.command_->args_) {
            args.push_back(arg);
          }
          auto source = ReplicationCommandPayloadSource::Create(
              item.command_->db_id_, args);
          if (!source.ok()) co_return source.status();
          if (source->size() > std::numeric_limits<std::size_t>::max()) {
            co_return absl::ResourceExhaustedError(
                "full-sync command is too large for this process");
          }
          const std::size_t payload_bytes =
              static_cast<std::size_t>(source->size());
          commands.push_back(StreamedFullSyncCommand{
              .item_ = std::move(item),
              .source_ = std::move(*source),
              .payload_bytes_ = payload_bytes,
          });
        }

        struct PendingFullSyncAck {
          std::uint16_t partition_id_ = 0;
          std::uint64_t sequence_ = 0;
          std::uint64_t item_id_ = 0;
        };
        constexpr std::size_t kDataFrameHeaderBytes = 5;
        constexpr std::size_t kFullSyncSequenceBytes = 8;
        constexpr std::size_t kCommandHeaderBytes = 2 + 8 + 8 + 4 + 1;
        constexpr std::size_t kWireOverhead = kDataFrameHeaderBytes +
                                              kFullSyncSequenceBytes +
                                              kCommandHeaderBytes;
        static_assert(kBacklogBatchBytes > kWireOverhead);
        constexpr std::size_t kFragmentBytes =
            kBacklogBatchBytes - kWireOverhead;

        std::size_t command_index = 0;
        std::size_t command_offset = 0;
        std::uint32_t command_fragment = 0;
        while (command_index < commands.size()) {
          std::string frame_headers;
          std::vector<std::string> frame_payloads;
          std::vector<PendingFullSyncAck> pending_acks;
          frame_headers.reserve(kBacklogBatchFrames * kDataFrameHeaderBytes);
          frame_payloads.reserve(kBacklogBatchFrames);
          pending_acks.reserve(kBacklogBatchFrames);
          std::size_t batch_bytes = 0;

          while (command_index < commands.size() &&
                 frame_payloads.size() < kBacklogBatchFrames) {
            StreamedFullSyncCommand& encoded = commands[command_index];
            const std::size_t count = std::min(
                kFragmentBytes, encoded.payload_bytes_ - command_offset);
            const std::size_t wire_bytes = kWireOverhead + count;
            if (!frame_payloads.empty() &&
                wire_bytes > kBacklogBatchBytes - batch_bytes) {
              break;
            }
            const bool first = command_offset == 0;
            const bool last = command_offset + count == encoded.payload_bytes_;
            std::uint8_t flags = 0;
            if (first) {
              flags |= static_cast<std::uint8_t>(
                  storage::ReplicationFrameFlag::kFirst);
            }
            if (last) {
              flags |= static_cast<std::uint8_t>(
                  storage::ReplicationFrameFlag::kLast);
            }
            std::string payload;
            payload.reserve(kFullSyncSequenceBytes + kCommandHeaderBytes +
                            count);
            PutU64(payload, fullsync_sequence);
            PutU16(payload, encoded.item_.command_->partition_id_);
            PutU64(payload, encoded.item_.command_->partition_sequence_);
            PutU64(payload, encoded.item_.id_);
            PutU32(payload, command_fragment);
            PutU8(payload, flags);
            const std::size_t payload_offset = payload.size();
            payload.resize(payload_offset + count);
            absl::Status read = co_await encoded.source_.Read(
                command_offset, std::span(reinterpret_cast<std::byte*>(
                                              payload.data() + payload_offset),
                                          count));
            if (!read.ok()) co_return read;
            PutU32(frame_headers,
                   static_cast<std::uint32_t>(1 + payload.size()));
            PutU8(frame_headers,
                  static_cast<std::uint8_t>(DataFrameKind::kFullSyncCommand));
            if (last) {
              pending_acks.push_back(PendingFullSyncAck{
                  .partition_id_ = encoded.item_.command_->partition_id_,
                  .sequence_ = fullsync_sequence,
                  .item_id_ = encoded.item_.id_,
              });
            }
            frame_payloads.push_back(std::move(payload));
            batch_bytes += wire_bytes;
            ++fullsync_sequence;
            command_offset += count;
            ++command_fragment;
            if (last) {
              ++command_index;
              command_offset = 0;
              command_fragment = 0;
            }
          }

          std::vector<iovec> wire_batch;
          wire_batch.reserve(frame_payloads.size() * 2);
          for (std::size_t index = 0; index < frame_payloads.size(); ++index) {
            wire_batch.push_back(
                iovec{.iov_base =
                          frame_headers.data() + index * kDataFrameHeaderBytes,
                      .iov_len = kDataFrameHeaderBytes});
            wire_batch.push_back(
                iovec{.iov_base = frame_payloads[index].data(),
                      .iov_len = frame_payloads[index].size()});
          }
          absl::Status sent = co_await stream.WriteAllV(wire_batch);
          if (!sent.ok()) co_return sent;
          // Sending bytes is protocol progress even when the command's final
          // fragment (and therefore its ACK) is still minutes away.
          session->TouchProgress(flow_id);
          for (const PendingFullSyncAck& expected : pending_acks) {
            absl::Status acknowledged = co_await WaitFullSyncAck(
                stream, expected.partition_id_, expected.sequence_);
            if (!acknowledged.ok()) co_return acknowledged;
            storage_->AcknowledgeFullSyncPublishItem(session->id_,
                                                     expected.item_id_);
            ++drained_items;
          }
        }
      }
      co_return absl::OkStatus();
    };

    // Function libraries live outside the storage snapshot. Send one
    // synthesized, fragmented mutation on flow zero after command admission
    // is closed. RESTORE FLUSH makes the catalog at the native full-sync cut
    // exact even when earlier FUNCTION mutations were also captured while the
    // key snapshot was being scanned.
    auto send_function_catalog = [&]() -> Task<absl::Status> {
      std::vector<std::string> codes;
      for (const LuaFunctionLibrary& library :
           SnapshotLuaFunctionLibraries()) {
        codes.push_back(library.code_);
      }
      std::vector<std::string> args{
          "FUNCTION", "RESTORE", rdb::EncodeFunctionDump(codes), "FLUSH"};
      std::vector<std::string_view> views;
      views.reserve(args.size());
      for (const std::string& arg : args) views.push_back(arg);
      auto source = ReplicationCommandPayloadSource::Create(0, views);
      if (!source.ok()) co_return source.status();
      if (source->size() > std::numeric_limits<std::size_t>::max()) {
        co_return absl::ResourceExhaustedError(
            "function catalog is too large for this process");
      }

      constexpr std::size_t kCommandHeaderBytes = 2 + 8 + 8 + 4 + 1;
      constexpr std::size_t kFragmentBytes =
          kBacklogBatchBytes - 5 - 8 - kCommandHeaderBytes;
      const std::size_t total = static_cast<std::size_t>(source->size());
      std::size_t offset = 0;
      std::uint32_t fragment = 0;
      do {
        const std::size_t count = std::min(kFragmentBytes, total - offset);
        const bool first = offset == 0;
        const bool last = offset + count == total;
        std::uint8_t flags = 0;
        if (first) {
          flags |= static_cast<std::uint8_t>(
              storage::ReplicationFrameFlag::kFirst);
        }
        if (last) {
          flags |= static_cast<std::uint8_t>(
              storage::ReplicationFrameFlag::kLast);
        }
        const std::uint64_t sequence = fullsync_sequence++;
        std::string payload;
        payload.reserve(8 + kCommandHeaderBytes + count);
        PutU64(payload, sequence);
        PutU16(payload, 0);
        PutU64(payload, 1);
        PutU64(payload, 1);
        PutU32(payload, fragment++);
        PutU8(payload, flags);
        const std::size_t payload_offset = payload.size();
        payload.resize(payload_offset + count);
        absl::Status read = co_await source->Read(
            offset, std::span(reinterpret_cast<std::byte*>(
                                  payload.data() + payload_offset),
                              count));
        if (!read.ok()) co_return read;
        absl::Status sent = co_await WriteDataFrame(
            stream, DataFrameKind::kFullSyncCommand, payload);
        if (!sent.ok()) co_return sent;
        session->TouchProgress(flow_id);
        if (last) {
          co_return co_await WaitFullSyncAck(stream, 0, sequence);
        }
        offset += count;
      } while (offset < total);
      co_return absl::InternalError("empty function catalog command");
    };

    // A fixed command ratio cannot keep the publisher stable: the number and
    // byte size of writes arriving during one snapshot slice vary with load,
    // partition coverage, and storage latency. Normally yield back to the
    // snapshot after one small quantum. Once this flow's own queue crosses the
    // high watermark, prioritize live commands until it reaches the low
    // watermark. This changes publisher duty cycle before capacity admission
    // has to stop foreground writes; it does not weaken the capacity limit.
    auto drain_interleaved_publish_queue = [&]() -> Task<absl::Status> {
      absl::Status drained =
          co_await drain_fullsync_publish_queue(kFullSyncInterleaveCommands);
      if (!drained.ok()) co_return drained;

      auto info = storage_->GetFullSyncPublishQueueInfo(session->id_);
      if (!info.ok()) co_return info.status();
      const std::size_t high_watermark =
          std::max<std::size_t>(1, info->capacity_bytes_ / 8);
      if (info->queued_bytes_ + info->admitted_bytes_ <= high_watermark) {
        co_return absl::OkStatus();
      }
      const std::size_t low_watermark =
          std::max<std::size_t>(1, high_watermark / 2);
      do {
        drained = co_await drain_fullsync_publish_queue(kBacklogBatchFrames);
        if (!drained.ok()) co_return drained;
        info = storage_->GetFullSyncPublishQueueInfo(session->id_);
        if (!info.ok()) co_return info.status();
        // Admitted bytes have reserved capacity but are not dequeueable until
        // their writes commit. Do not spin this worker waiting for those
        // writes.
      } while (info->queued_bytes_ > low_watermark);
      co_return absl::OkStatus();
    };

    auto drain_partition_overrides =
        [&](std::uint16_t partition_id) -> Task<absl::Status> {
      while (true) {
        auto batch = co_await storage_->ReadPartitionFullSyncOverrides(
            session->id_, partition_id, kOverrideRecordsPerBatch);
        if (!batch.ok()) co_return batch.status();
        if (batch->records_.empty()) co_return absl::OkStatus();
        absl::Status sent =
            co_await send_records(partition_id, batch->records_);
        if (!sent.ok()) co_return sent;
        next_sequence[partition_id] = batch->records_.back().mutation_sequence_;
        session->SetProgress(flow_id, ReplicationPhase::kOverrideCatchup,
                             fullsync_backlog_cursor.lsn_,
                             fullsync_backlog_cursor.fragment_index_,
                             partition_id, next_sequence[partition_id]);
        storage_->AcknowledgePartitionFullSyncOverrides(
            session->id_, partition_id, batch->records_);
      }
    };

    auto drain_all_overrides = [&]() -> Task<absl::Status> {
      while (true) {
        bool sent_any = false;
        for (const CapturedPartition& partition : captured) {
          auto batch = co_await storage_->ReadPartitionFullSyncOverrides(
              session->id_, partition.partition_id_, kOverrideRecordsPerBatch);
          if (!batch.ok()) co_return batch.status();
          if (batch->records_.empty()) continue;
          absl::Status sent =
              co_await send_records(partition.partition_id_, batch->records_);
          if (!sent.ok()) co_return sent;
          next_sequence[partition.partition_id_] =
              batch->records_.back().mutation_sequence_;
          storage_->AcknowledgePartitionFullSyncOverrides(
              session->id_, partition.partition_id_, batch->records_);
          sent_any = true;
        }
        if (!sent_any) co_return absl::OkStatus();
      }
    };

    struct SnapshotGateReopen {
      bool active_ = false;
      ~SnapshotGateReopen() {
        if (active_) OpenSnapshotTransactionGate();
      }
      void Open() {
        if (!active_) return;
        OpenSnapshotTransactionGate();
        active_ = false;
      }
    } gate_reopen;
    struct CommandGateReopen {
      bool active_ = false;
      ~CommandGateReopen() {
        if (active_) OpenAllCommandDbGates();
      }
      void Open() {
        if (!active_) return;
        OpenAllCommandDbGates();
        active_ = false;
      }
    } command_gate_reopen;

    auto close_transaction_gate = [&]() -> Task<absl::Status> {
      while (!CloseSnapshotTransactionGate()) {
        if (session->cancelled()) {
          co_return absl::CancelledError(
              "replication session ended while waiting for transaction gate");
        }
        absl::Status waited = co_await celer::SleepFor(
            *celer::ThisWorker().self_, std::chrono::milliseconds(1));
        if (!waited.ok()) co_return waited;
      }
      gate_reopen.active_ = true;
      while (SnapshotTransactionsActive()) {
        if (session->cancelled()) {
          co_return absl::CancelledError(
              "replication session ended while draining transactions");
        }
        absl::Status waited = co_await celer::SleepFor(
            *celer::ThisWorker().self_, std::chrono::milliseconds(1));
        if (!waited.ok()) co_return waited;
      }
      co_return absl::OkStatus();
    };

    std::size_t processed_partitions = 0;
    std::uint32_t next_partition = flow_id;
    while (next_partition < storage::kLogicalStorageShards) {
      std::vector<std::uint16_t> reset_partitions;
      reset_partitions.reserve(kFullSyncResetBatch);
      while (next_partition < storage::kLogicalStorageShards &&
             reset_partitions.size() < kFullSyncResetBatch) {
        reset_partitions.push_back(static_cast<std::uint16_t>(next_partition));
        next_partition += storage_->worker_count();
      }

      std::string reset;
      reset.reserve(4 + reset_partitions.size() *
                            (2 + 8 * storage::kLogicalDatabaseCount));
      PutU32(reset, static_cast<std::uint32_t>(reset_partitions.size()));
      for (const std::uint16_t partition_id : reset_partitions) {
        PutU16(reset, partition_id);
        for (std::uint64_t epoch : source_db_epochs) {
          PutU64(reset, epoch);
        }
      }
      session->SetProgress(
          flow_id, ReplicationPhase::kReset, fullsync_backlog_cursor.lsn_,
          fullsync_backlog_cursor.fragment_index_, reset_partitions.back(), 0);
      absl::Status sent = co_await WriteFrameAndWaitAck(
          stream, DataFrameKind::kReset, reset, kResetBatchAckPartition);
      if (!sent.ok()) {
        cleanup();
        co_return sent;
      }
      if (const char* configured =
              std::getenv("KEYLANE_REPLICATION_PAUSE_FULLSYNC_AFTER_RESET_MS");
          configured != nullptr && !replication_fullsync_pause_used_.exchange(
                                       true, std::memory_order_acq_rel)) {
        std::uint64_t pause_ms = 0;
        const std::size_t length = std::strlen(configured);
        const auto parsed =
            std::from_chars(configured, configured + length, pause_ms);
        if (parsed.ec == std::errc{} && parsed.ptr == configured + length &&
            pause_ms != 0) {
          absl::Status paused = co_await celer::SleepFor(
              *celer::ThisWorker().self_, std::chrono::milliseconds(pause_ms));
          if (!paused.ok()) {
            cleanup();
            co_return paused;
          }
        }
      }

      for (const std::uint16_t partition_id : reset_partitions) {
        auto start =
            storage_->BeginPartitionReplication(session->id_, partition_id);
        if (!start.ok()) {
          cleanup();
          co_return start.status();
        }
        next_sequence[partition_id] = start->baseline_version_;
        captured.push_back(CapturedPartition{
            .partition_id_ = partition_id,
            .start_ = *start,
        });
        session->SetProgress(flow_id, ReplicationPhase::kSnapshot,
                             fullsync_backlog_cursor.lsn_,
                             fullsync_backlog_cursor.fragment_index_,
                             partition_id, start->baseline_version_);
        // Snapshot reads use a small scheduling quantum so captured writes get
        // a chance to run frequently. Keep the records across those reads and
        // flush only transfer-sized frames (or at a DB boundary) so fairness
        // does not cost a network ACK for every 64 records.
        constexpr std::size_t kRecordsFrameHeaderBytes = 2 + 4;
        std::vector<SnapshotRecord> pending_snapshot_records;
        std::size_t pending_snapshot_bytes = kRecordsFrameHeaderBytes;
        auto flush_snapshot_records = [&]() -> Task<absl::Status> {
          if (pending_snapshot_records.empty()) {
            co_return absl::OkStatus();
          }
          absl::Status flushed =
              co_await send_records(partition_id, pending_snapshot_records);
          if (!flushed.ok()) co_return flushed;
          storage_->AcknowledgePartitionSnapshotRecords(
              session->id_, partition_id, pending_snapshot_records);
          pending_snapshot_records.clear();
          pending_snapshot_bytes = kRecordsFrameHeaderBytes;
          co_return absl::OkStatus();
        };
        for (std::uint8_t db_id = 0; db_id < storage::kLogicalDatabaseCount;
             ++db_id) {
          for (;;) {
            absl::Status db_started = storage_->BeginPartitionDbReplication(
                session->id_, partition_id, db_id);
            if (db_started.ok()) break;
            if (db_started.code() != absl::StatusCode::kUnavailable) {
              cleanup();
              co_return db_started;
            }
            co_await celer::Yield(*celer::ThisWorker().self_);
          }
          if ((start->nonempty_db_mask_ & (std::uint16_t{1} << db_id)) != 0) {
            std::uint64_t cursor = 0;
            do {
              auto batch = co_await storage_->SnapshotPartition(
                  session->id_, partition_id, db_id, cursor,
                  snapshot_batch_size(), snapshot_read_concurrency());
              if (!batch.ok()) {
                cleanup();
                co_return batch.status();
              }
              if (!batch->records_.empty()) {
                for (SnapshotRecord& record : batch->records_) {
                  const std::size_t encoded = EncodedRecordBytes(record);
                  if (encoded > kBacklogBatchBytes - kRecordsFrameHeaderBytes) {
                    sent = co_await flush_snapshot_records();
                    if (!sent.ok()) {
                      cleanup();
                      co_return sent;
                    }
                    sent = co_await send_records(
                        partition_id,
                        std::span<const SnapshotRecord>(&record, 1));
                    if (!sent.ok()) {
                      cleanup();
                      co_return sent;
                    }
                    storage_->AcknowledgePartitionSnapshotRecords(
                        session->id_, partition_id,
                        std::span<const SnapshotRecord>(&record, 1));
                    continue;
                  }
                  if (!pending_snapshot_records.empty() &&
                      encoded > kBacklogBatchBytes - pending_snapshot_bytes) {
                    sent = co_await flush_snapshot_records();
                    if (!sent.ok()) {
                      cleanup();
                      co_return sent;
                    }
                  }
                  pending_snapshot_bytes += encoded;
                  pending_snapshot_records.push_back(std::move(record));
                }
              }
              absl::Status published =
                  co_await drain_interleaved_publish_queue();
              if (!published.ok()) {
                cleanup();
                co_return published;
              }
              absl::Status replacements =
                  co_await drain_partition_overrides(partition_id);
              if (!replacements.ok()) {
                cleanup();
                co_return replacements;
              }
              cursor = batch->cursor_;
            } while (cursor != 0);
          }
          sent = co_await flush_snapshot_records();
          if (!sent.ok()) {
            cleanup();
            co_return sent;
          }
          for (;;) {
            absl::Status replacements =
                co_await drain_partition_overrides(partition_id);
            if (!replacements.ok()) {
              cleanup();
              co_return replacements;
            }
            absl::Status db_completed =
                storage_->CompletePartitionDbReplication(session->id_,
                                                         partition_id, db_id);
            if (db_completed.ok()) break;
            if (db_completed.code() != absl::StatusCode::kUnavailable) {
              cleanup();
              co_return db_completed;
            }
          }
        }
        if ((++processed_partitions & 63U) == 0) {
          absl::Status yielded = co_await celer::SleepFor(
              *celer::ThisWorker().self_, std::chrono::milliseconds(1));
          if (!yielded.ok()) {
            cleanup();
            co_return yielded;
          }
        }

        // Close this partition's scan window without suspending between the
        // final empty replacement observation and the phase transition.
        // Writes after CompletePartitionReplication enter the same worker
        // publish FIFO directly; writes racing before it remain replacements.
        session->SetProgress(flow_id, ReplicationPhase::kOverrideCatchup,
                             fullsync_backlog_cursor.lsn_,
                             fullsync_backlog_cursor.fragment_index_,
                             partition_id, next_sequence[partition_id]);
        for (;;) {
          absl::Status drained =
              co_await drain_partition_overrides(partition_id);
          if (!drained.ok()) {
            cleanup();
            co_return drained;
          }
          absl::Status completed = storage_->CompletePartitionReplication(
              session->id_, partition_id);
          if (completed.ok()) break;
          if (completed.code() != absl::StatusCode::kUnavailable) {
            cleanup();
            co_return completed;
          }
        }
        absl::Status published = co_await drain_interleaved_publish_queue();
        if (!published.ok()) {
          cleanup();
          co_return published;
        }
        std::string handoff_body;
        PutU16(handoff_body, partition_id);
        // Partition handoff no longer identifies a shared-backlog cursor. It
        // is only a target-side completion marker; the final cut supplies the
        // stable ONLINE cursor for the whole flow.
        PutU64(handoff_body, 1);
        sent = co_await WriteFullSyncFrameAndWaitAck(
            stream, DataFrameKind::kPartitionHandoff, handoff_body,
            partition_id, fullsync_sequence);
        if (!sent.ok()) {
          cleanup();
          co_return sent;
        }
        ++fullsync_sequence;
        if (const char* configured = std::getenv(
                "KEYLANE_REPLICATION_PAUSE_FULLSYNC_AFTER_HANDOFF_MS");
            configured != nullptr &&
            !replication_fullsync_handoff_pause_used_.exchange(
                true, std::memory_order_acq_rel)) {
          std::uint64_t pause_ms = 0;
          const std::size_t length = std::strlen(configured);
          const auto parsed =
              std::from_chars(configured, configured + length, pause_ms);
          if (parsed.ec == std::errc{} && parsed.ptr == configured + length &&
              pause_ms != 0) {
            absl::Status paused =
                co_await celer::SleepFor(*celer::ThisWorker().self_,
                                         std::chrono::milliseconds(pause_ms));
            if (!paused.ok()) {
              cleanup();
              co_return paused;
            }
          }
        }
      }
    }

    // Flows do not finish scanning at exactly the same time.  Waiting on the
    // cut barrier immediately would stop an early flow's publisher while
    // writes to its already-tailing partitions continue to enqueue.  Keep
    // that FIFO moving until the last flow has finished its scan, then do one
    // final bounded pass so the gate-closed cut has only a small race tail to
    // drain.
    session->MarkSnapshotScanComplete();
    do {
      absl::Status published = co_await drain_fullsync_publish_queue(
          session->AllSnapshotScansComplete() ? kFullSyncInterleaveCommands
                                              : kFullSyncReadyWaitCommands);
      if (!published.ok()) {
        cleanup();
        co_return published;
      }
      if (session->cancelled()) {
        cleanup();
        co_return absl::CancelledError(
            "replication session ended while waiting for snapshot scans");
      }
      if (session->AllSnapshotScansComplete()) break;
      absl::Status waited = co_await celer::SleepFor(
          *celer::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) {
        cleanup();
        co_return waited;
      }
    } while (true);

    absl::Status cut_ready = co_await session->WaitSnapshotReady();
    if (!cut_ready.ok()) {
      cleanup();
      co_return cut_ready;
    }

    if (flow_id == 0) {
      // Close and drain transaction admission before closing ordinary DB
      // admission. Commands waiting for the DB gate must never hold a
      // snapshot-transaction slot needed by this cut.
      absl::Status gated = co_await close_transaction_gate();
      if (!gated.ok()) {
        cleanup();
        co_return gated;
      }
      while (!CloseAllCommandDbGates()) {
        if (session->cancelled()) {
          cleanup();
          co_return absl::CancelledError(
              "replication session ended while waiting for command gate");
        }
        absl::Status waited = co_await celer::SleepFor(
            *celer::ThisWorker().self_, std::chrono::milliseconds(1));
        if (!waited.ok()) {
          cleanup();
          co_return waited;
        }
      }
      command_gate_reopen.active_ = true;
      while (CommandDbOperationsActive()) {
        if (session->cancelled()) {
          cleanup();
          co_return absl::CancelledError(
              "replication session ended while draining commands");
        }
        absl::Status waited = co_await celer::SleepFor(
            *celer::ThisWorker().self_, std::chrono::milliseconds(1));
        if (!waited.ok()) {
          cleanup();
          co_return waited;
        }
      }
    }

    absl::Status gate_closed = co_await session->WaitSnapshotGateClosed();
    if (!gate_closed.ok()) {
      cleanup();
      co_return gate_closed;
    }

    // No transaction can now straddle source flows. Transactions admitted
    // before the close have fully resolved and are represented by these
    // coalesced after-image overrides; transactions admitted after reopen will
    // be behind every flow's publisher fence and therefore represented by the
    // backlog.
    absl::Status drained = co_await drain_all_overrides();
    if (!drained.ok()) {
      cleanup();
      co_return drained;
    }
    absl::Status queue_drained = co_await drain_fullsync_publish_queue(
        std::numeric_limits<std::size_t>::max());
    if (!queue_drained.ok()) {
      cleanup();
      co_return queue_drained;
    }
    if (flow_id == 0) {
      absl::Status functions_sent = co_await send_function_catalog();
      if (!functions_sent.ok()) {
        cleanup();
        co_return functions_sent;
      }
    }

    auto backlog_cursor = co_await storage_->FenceReplicationLog();
    if (!backlog_cursor.ok()) {
      cleanup();
      co_return backlog_cursor.status();
    }
    // Pin the stable post-full-sync cursor before any source admission gate is
    // reopened. The cut frame itself may take arbitrarily long to reach or be
    // acknowledged by the target; writes after this point must backpressure
    // rather than evicting history below the cut.
    absl::Status retained =
        storage_->RetainReplicationLog(session->id_, *backlog_cursor);
    if (!retained.ok()) {
      cleanup();
      co_return retained;
    }
    absl::Status fenced = co_await session->WaitSnapshotFenced();
    if (!fenced.ok()) {
      cleanup();
      co_return fenced;
    }
    // The cursor is pinned and the full-sync prefix is finite. Stop this
    // worker's capture before reopening source admission, then use only an
    // owner-local barrier (no network round trip) to prove every flow has
    // done the same. Post-reopen writes now enter the retained backlog only.
    cleanup();
    absl::Status capture_stopped =
        co_await session->WaitSnapshotCaptureStopped();
    if (!capture_stopped.ok()) {
      co_return capture_stopped;
    }
    if (flow_id == 0) {
      gate_reopen.Open();
      command_gate_reopen.Open();
      if (const char* configured =
              std::getenv("KEYLANE_REPLICATION_PAUSE_FULLSYNC_BEFORE_CUT_MS");
          configured != nullptr) {
        std::uint64_t pause_ms = 0;
        const std::size_t length = std::strlen(configured);
        const auto parsed =
            std::from_chars(configured, configured + length, pause_ms);
        if (parsed.ec == std::errc{} && parsed.ptr == configured + length &&
            pause_ms != 0) {
          absl::Status paused = co_await celer::SleepFor(
              *celer::ThisWorker().self_, std::chrono::milliseconds(pause_ms));
          if (!paused.ok()) co_return paused;
        }
      }
    }
    // The fence fixed this flow's stable ONLINE cursor. Source admission is
    // already open again; a slow target can delay only this session while
    // backlog pressure accounts for post-fence writes normally.
    std::string cut_body;
    PutU64(cut_body, *backlog_cursor);
    absl::Status cut_sent = co_await WriteFullSyncFrameAndWaitAck(
        stream, DataFrameKind::kFullSyncCut, cut_body, kResetBatchAckPartition,
        fullsync_sequence);
    if (!cut_sent.ok()) {
      co_return cut_sent;
    }
    ++fullsync_sequence;
    co_return co_await EnterMasterFlowBacklog(stream, session, flow_id,
                                              *backlog_cursor, 0);
  }

  Task<absl::Status> EnterMasterFlowBacklog(
      TcpStream& stream, const std::shared_ptr<MasterSession>& session,
      unsigned flow_id, std::uint64_t next_lsn, std::uint32_t fragment_index) {
    storage::ReplicationLogCursor cursor{.lsn_ = next_lsn,
                                         .fragment_index_ = fragment_index};
    absl::Status retained =
        storage_->RetainReplicationLog(session->id_, cursor.lsn_);
    if (!retained.ok()) co_return retained;
    session->SetBacklogCursor(flow_id, ReplicationPhase::kBacklog, cursor);
    std::string cursor_payload;
    PutU64(cursor_payload, next_lsn);
    PutU32(cursor_payload, fragment_index);
    absl::Status cursor_sent =
        co_await WriteDataFrame(stream, DataFrameKind::kCursor, cursor_payload);
    if (!cursor_sent.ok()) co_return cursor_sent;
    auto cursor_ack = co_await ReadDataFrame(stream);
    if (!cursor_ack.ok()) co_return cursor_ack.status();
    DataReader ack_reader(cursor_ack->second);
    std::uint16_t ignored_partition = 0;
    std::uint64_t acknowledged_lsn = 0;
    if (cursor_ack->first != DataFrameKind::kAck ||
        !ack_reader.U16(&ignored_partition) ||
        !ack_reader.U64(&acknowledged_lsn) || ack_reader.remaining() != 0 ||
        acknowledged_lsn != next_lsn) {
      co_return absl::InvalidArgumentError(
          "malformed replication backlog cursor ACK");
    }
    session->SetBacklogCursor(flow_id, ReplicationPhase::kReady, cursor);
    celer::ThisWorker().self_->Spawn(
        MonitorBacklogStall(session, flow_id, stream.NativeFd(),
                            session->ProgressGeneration(flow_id)));
    co_return co_await RunMasterFlowBacklog(stream, session, flow_id, cursor);
  }

  struct MasterBacklogDuplexState {
    std::deque<std::uint64_t> expected_acks_;
    celer::AsyncNotification expected_ack_ready_;
    celer::AsyncNotification receiver_done_ready_;
    absl::Status receiver_status_ =
        absl::UnknownError("replication backlog ACK receiver is running");
    bool sender_done_ = false;
    bool receiver_done_ = false;
  };

  Task<absl::Status> ReceiveMasterFlowBacklogAcks(
      TcpStream& stream, const std::shared_ptr<MasterSession>& session,
      unsigned flow_id,
      const std::shared_ptr<MasterBacklogDuplexState>& duplex) {
    while (stream.IsOpen()) {
      while (duplex->expected_acks_.empty() && !duplex->sender_done_) {
        co_await duplex->expected_ack_ready_.Wait();
      }
      if (duplex->expected_acks_.empty()) {
        co_return duplex->sender_done_
                      ? absl::OkStatus()
                      : absl::UnavailableError(
                            "replication backlog flow closed");
      }

      const std::uint64_t expected_lsn = duplex->expected_acks_.front();
      auto ack = co_await ReadDataFrame(stream);
      if (!ack.ok()) co_return ack.status();
      if (ack->first != DataFrameKind::kAck) {
        co_return absl::InvalidArgumentError(
            "replication command ACK expected");
      }
      DataReader ack_reader(ack->second);
      std::uint16_t ignored_partition = 0;
      std::uint64_t acknowledged_lsn = 0;
      if (!ack_reader.U16(&ignored_partition) ||
          !ack_reader.U64(&acknowledged_lsn) ||
          acknowledged_lsn != expected_lsn) {
        co_return absl::InvalidArgumentError(
            "malformed replication command ACK");
      }
      const storage::ReplicationLogCursor acknowledged{
          .lsn_ = expected_lsn + 1, .fragment_index_ = 0};
      absl::Status retained =
          storage_->RetainReplicationLog(session->id_, acknowledged.lsn_);
      if (!retained.ok()) co_return retained;
      session->SetBacklogCursor(flow_id, ReplicationPhase::kReady,
                                acknowledged);
      duplex->expected_acks_.pop_front();
    }
    co_return absl::UnavailableError("replication backlog flow closed");
  }

  Task<absl::Status> TrackMasterFlowBacklogAcks(
      TcpStream& stream, const std::shared_ptr<MasterSession>& session,
      unsigned flow_id,
      const std::shared_ptr<MasterBacklogDuplexState>& duplex) {
    duplex->receiver_status_ =
        co_await ReceiveMasterFlowBacklogAcks(stream, session, flow_id, duplex);
    duplex->receiver_done_ = true;
    duplex->receiver_done_ready_.NotifyAll(*celer::ThisWorker().self_);
    if (!duplex->receiver_status_.ok()) {
      (void)::shutdown(stream.NativeFd(), SHUT_RDWR);
    }
    co_return duplex->receiver_status_;
  }

  Task<absl::Status> MonitorBacklogStall(std::shared_ptr<MasterSession> session,
                                         unsigned flow_id, int fd,
                                         std::uint64_t observed_generation) {
    auto deadline = std::chrono::steady_clock::now() + kFullSyncStallTimeout;
    while (!session->cancelled()) {
      absl::Status slept = co_await celer::SleepFor(*celer::ThisWorker().self_,
                                                    std::chrono::seconds(1));
      if (!slept.ok() || session->cancelled()) co_return slept;
      const auto retained = session->RetainedLsn(flow_id);
      if (!retained.has_value()) co_return absl::OkStatus();
      const std::uint64_t generation = session->ProgressGeneration(flow_id);
      const auto log = storage_->LocalReplicationLogInfo();
      if (generation != observed_generation || log.tail_lsn_ < *retained) {
        observed_generation = generation;
        deadline = std::chrono::steady_clock::now() + kFullSyncStallTimeout;
        continue;
      }
      if (std::chrono::steady_clock::now() < deadline) continue;
      spdlog::warn(
          "replication session {} flow {} made no backlog ACK progress for "
          "10 minutes; disconnecting it to release write backpressure",
          session->id_, flow_id);
      (void)::shutdown(fd, SHUT_RDWR);
      co_return absl::DeadlineExceededError(
          "replica made no backlog ACK progress for 10 minutes");
    }
    co_return absl::OkStatus();
  }

  Task<absl::Status> RunMasterFlowBacklog(
      TcpStream& stream, const std::shared_ptr<MasterSession>& session,
      unsigned flow_id, storage::ReplicationLogCursor cursor) {
    auto duplex = std::make_shared<MasterBacklogDuplexState>();
    celer::ThisWorker().self_->Spawn(
        TrackMasterFlowBacklogAcks(stream, session, flow_id, duplex));
    absl::Status sender_status = absl::OkStatus();
    while (stream.IsOpen()) {
      if (duplex->receiver_done_) {
        sender_status = duplex->receiver_status_;
        break;
      }
      if (session->cancelled()) {
        sender_status = absl::CancelledError(
            "replication session ended while sending backlog");
        break;
      }
      auto batch = co_await storage_->ReadReplicationLog(
          cursor, kBacklogBatchBytes, kBacklogBatchFrames);
      if (!batch.ok()) {
        sender_status = batch.status();
        break;
      }
      std::string frame_headers;
      frame_headers.reserve(batch->frames_.size() * 18);
      std::vector<iovec> wire_batch;
      wire_batch.reserve(batch->frames_.size() * 2);
      std::vector<std::uint64_t> pending_acks;
      pending_acks.reserve(batch->frames_.size());
      for (const auto& frame : batch->frames_) {
        if (frame.payload_.size() > kMaxDataFrame - 13) {
          co_return absl::ResourceExhaustedError(
              "replication data frame exceeds configured limit");
        }
        PutU32(frame_headers,
               static_cast<std::uint32_t>(14 + frame.payload_.size()));
        PutU8(frame_headers,
              static_cast<std::uint8_t>(DataFrameKind::kCommand));
        PutU64(frame_headers, frame.header_.lsn_);
        PutU32(frame_headers, frame.header_.fragment_index_);
        PutU8(frame_headers, frame.header_.flags_);
      }
      for (std::size_t index = 0; index < batch->frames_.size(); ++index) {
        const auto& frame = batch->frames_[index];
        wire_batch.push_back(iovec{
            .iov_base = frame_headers.data() + index * 18, .iov_len = 18});
        wire_batch.push_back(
            iovec{.iov_base = const_cast<char*>(frame.payload_.data()),
                  .iov_len = frame.payload_.size()});
        const bool last =
            (frame.header_.flags_ &
             static_cast<std::uint8_t>(storage::ReplicationFrameFlag::kLast)) !=
            0;
        if (last) pending_acks.push_back(frame.header_.lsn_);
      }
      if (!wire_batch.empty()) {
        // Publish the expected ACK order before the write can yield. The
        // receiver runs concurrently on this worker and may observe an ACK as
        // soon as the first complete frame reaches the peer.
        duplex->expected_acks_.insert(duplex->expected_acks_.end(),
                                      pending_acks.begin(),
                                      pending_acks.end());
        duplex->expected_ack_ready_.NotifyAll(*celer::ThisWorker().self_);
        absl::Status sent = co_await stream.WriteAllV(wire_batch);
        if (!sent.ok()) {
          sender_status = sent;
          break;
        }
      }
      // Keep sending independently from the durable cursor. In particular,
      // never stop a source flow at an arbitrary batch boundary waiting for a
      // cross-worker transaction ACK: another participant's envelope may be
      // in that flow's next batch. The concurrent receiver advances retained
      // history as ACKs arrive while socket backpressure bounds wire output.
      cursor = batch->next_;
      if (batch->at_tail_) {
        absl::Status slept = co_await celer::SleepFor(
            *celer::ThisWorker().self_, std::chrono::milliseconds(1));
        if (!slept.ok()) {
          sender_status = slept;
          break;
        }
        continue;
      }
      cursor = batch->next_;
    }
    if (sender_status.ok()) {
      sender_status = absl::UnavailableError(
          "replication backlog flow closed");
    }
    duplex->sender_done_ = true;
    duplex->expected_ack_ready_.NotifyAll(*celer::ThisWorker().self_);
    if (!duplex->receiver_done_) {
      (void)::shutdown(stream.NativeFd(), SHUT_RDWR);
      while (!duplex->receiver_done_) {
        co_await duplex->receiver_done_ready_.Wait();
      }
    }
    if (!duplex->receiver_status_.ok()) {
      co_return duplex->receiver_status_;
    }
    co_return sender_status;
  }

  Task<absl::Status> RunAdoptedConnection(
      Connection* connection, std::vector<std::string> args,
      std::uint64_t client_id, std::string client_address, bool tls,
      std::uint64_t replication_session_id) {
    TcpStream stream(connection);
    RegisterClientConnection(client_id, stream.NativeFd(),
                             std::move(client_address), tls, true,
                             replication_session_id);
    absl::Status status =
        co_await ServeOwnedNativeConnection(stream, std::move(args), client_id);
    UnregisterClientConnection(client_id);
    if (connection->state_ == celer::ConnectionState::kActive &&
        !connection->closing_) {
      celer::ThisWorker().self_->BeginClose(
          connection, status,
          status.ok() ? celer::CloseMode::kLocalClose
                      : celer::CloseMode::kLocalError);
    }
    if (!status.ok()) {
      spdlog::warn("replication native handshake failed: {}", status.message());
    }
    co_return status;
  }

  Task<absl::Status> ServeOwnedNativeConnection(TcpStream& stream,
                                                std::vector<std::string> args,
                                                std::uint64_t client_id) {
    ReplicationConnectionMetricGuard connection_metric(
        EqualCaseInsensitive(args.front(), "KLPSYNC")
            ? ReplicationConnectionKind::kControl
            : ReplicationConnectionKind::kFlow);
    if (EqualCaseInsensitive(args.front(), "KLPSYNC")) {
      co_return co_await ServeMasterControl(stream, std::move(args), client_id);
    }
    co_return co_await ServeMasterFlow(stream, std::move(args));
  }

  Task<absl::Status> ServeMasterControl(TcpStream& stream,
                                        std::vector<std::string> args,
                                        std::uint64_t client_id) {
    if (args.size() != 4 || args[1] != kProtocolVersion ||
        (args[3] != "?" && !IsReplicationId(args[3]))) {
      co_return absl::InvalidArgumentError("invalid KLPSYNC handshake");
    }
    const bool protocol_probe = args[2] == "?";
    absl::Status history_ready = co_await EnsureReplicationHistoryReady();
    if (!history_ready.ok()) co_return history_ready;
    std::string replica_node_id;
    std::uint16_t replica_port = 0;
    std::string replica_host;
    if (args[2] != "?") {
      const std::string_view identity = args[2];
      constexpr std::size_t kNodeIdEnd = 1 + 40;
      if (identity.size() <= kNodeIdEnd + 1 || identity.front() != '?' ||
          identity[kNodeIdEnd] != ':' ||
          !IsReplicationId(identity.substr(1, 40)) ||
          !ParseUnsigned(identity.substr(kNodeIdEnd + 1), &replica_port) ||
          replica_port == 0) {
        co_return absl::InvalidArgumentError(
            "invalid KLPSYNC replica identity");
      }
      replica_node_id = std::string(identity.substr(1, 40));
      replica_host = PeerHost(stream.NativeFd());
      if (replica_host.empty()) {
        co_return absl::InternalError(
            "failed to identify KLPSYNC replica endpoint");
      }
    }
    const std::uint64_t session_id =
        next_master_session_id_.fetch_add(1, std::memory_order_relaxed);
    SetClientReplicationSession(client_id, session_id);
    std::string source_history_id;
    {
      std::lock_guard lock(master_mutex_);
      source_history_id = history_id_;
    }
    const bool allow_continue = args[3] == source_history_id;
    auto session = std::make_shared<MasterSession>(
        session_id, storage_->worker_count(), std::move(replica_node_id),
        std::move(replica_host), replica_port, allow_continue);
    for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
      const std::size_t flow_capacity = BacklogCapacityForFlow(
          worker, backlog_size_bytes_.load(std::memory_order_acquire));
      absl::Status enabled = co_await celer::SubmitTaskTo(
          worker, [this, session_id, flow_capacity]() -> Task<absl::Status> {
            // Runtime backlog is bounded process memory and intentionally
            // disappears when the source history id changes.
            co_return co_await storage_->EnableReplicationLog(session_id,
                                                              flow_capacity);
          });
      if (!enabled.ok()) co_return enabled;
    }
    if (!session->SetControl(stream.NativeFd())) {
      co_return absl::InternalError("failed to register control connection");
    }
    {
      std::lock_guard lock(master_mutex_);
      master_sessions_[session_id] = session;
    }
    absl::Status sent = co_await WriteText(
        stream,
        absl::StrCat("+KLFULLRESYNC ", session_id, " ", node_id_, " ",
                     source_history_id, " ", storage_->worker_count(), "\r\n"));
    if (!sent.ok()) {
      RemoveMasterSession(session);
      co_return sent;
    }
    // Anonymous KLPSYNC is reserved for protocol detection. It deliberately
    // avoids leaving a ten-minute flow-stall session or appearing in INFO as
    // a downstream replica. Real native replicas always advertise their
    // node id and listening port.
    if (protocol_probe) {
      RemoveMasterSession(session);
      co_return absl::OkStatus();
    }

    const auto started = std::chrono::steady_clock::now();
    std::vector<std::uint64_t> observed_progress(session->worker_count());
    std::vector<std::chrono::steady_clock::time_point> stall_deadlines(
        session->worker_count(), started + kFullSyncStallTimeout);
    for (unsigned flow = 0; flow < session->worker_count(); ++flow) {
      observed_progress[flow] = session->ProgressGeneration(flow);
    }
    auto next_progress_log = started + std::chrono::seconds(30);
    bool stalled = false;
    while (!session->cancelled() && !session->all_flows_ready() && !stalled) {
      const auto now = std::chrono::steady_clock::now();
      for (unsigned flow = 0; flow < session->worker_count(); ++flow) {
        const std::uint64_t current = session->ProgressGeneration(flow);
        if (current != observed_progress[flow]) {
          observed_progress[flow] = current;
          stall_deadlines[flow] = now + kFullSyncStallTimeout;
        }
        const auto progress = session->Progress(flow);
        if (progress.has_value() &&
            progress->phase_ != ReplicationPhase::kReady &&
            now >= stall_deadlines[flow]) {
          stalled = true;
          break;
        }
      }
      if (stalled) break;
      absl::Status slept = co_await celer::SleepFor(
          *celer::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!slept.ok()) {
        RemoveMasterSession(session);
        co_return slept;
      }
      if (std::chrono::steady_clock::now() >= next_progress_log) {
        for (unsigned flow = 0; flow < session->worker_count(); ++flow) {
          const auto progress = session->Progress(flow);
          if (!progress.has_value()) continue;
          spdlog::info(
              "replication session {} flow {} phase={} partition={} "
              "sequence={} cursor={}:{}",
              session_id, flow, ReplicationPhaseName(progress->phase_),
              progress->current_partition_, progress->partition_sequence_,
              progress->lsn_, progress->fragment_index_);
        }
        next_progress_log =
            std::chrono::steady_clock::now() + std::chrono::seconds(30);
      }
    }
    if (session->cancelled() || !session->all_flows_ready()) {
      RemoveMasterSession(session);
      co_return absl::DeadlineExceededError(
          stalled ? "replica flow made no full-sync progress for 10 minutes"
                  : "replica flows did not complete full synchronization");
    }
    sent = co_await WriteText(stream, "+KLONLINE\r\n");
    if (!sent.ok()) {
      RemoveMasterSession(session);
      co_return sent;
    }
    session->MarkOnline();
    spdlog::info("accepted replication session {} with {} data flows",
                 session_id, session->worker_count());
    absl::Status waited = co_await WaitForClose(stream);
    RemoveMasterSession(session);
    co_return waited;
  }

  Task<absl::Status> ServeMasterFlow(TcpStream& stream,
                                     std::vector<std::string> args) {
    std::uint64_t session_id = 0;
    unsigned flow_id = 0;
    std::uint64_t next_lsn = 0;
    std::uint32_t fragment_index = 0;
    if (args.size() != 6 || args[1] != kProtocolVersion ||
        !ParseUnsigned(args[2], &session_id) || session_id == 0 ||
        !ParseUnsigned(args[3], &flow_id) ||
        flow_id != celer::ThisWorker().id_ ||
        !ParseUnsigned(args[4], &next_lsn) || next_lsn == 0 ||
        !ParseUnsigned(args[5], &fragment_index)) {
      co_return absl::InvalidArgumentError("invalid KLFLOW handshake");
    }
    std::shared_ptr<MasterSession> session;
    {
      std::lock_guard lock(master_mutex_);
      const auto found = master_sessions_.find(session_id);
      if (found != master_sessions_.end()) session = found->second;
    }
    if (session == nullptr || !session->SetFlow(flow_id, stream.NativeFd())) {
      co_return absl::FailedPreconditionError(
          "KLFLOW references an unavailable session");
    }
    const auto log_info = storage_->LocalReplicationLogInfo();
    const bool continue_mode =
        session->allow_continue_ && (next_lsn > 1 || fragment_index != 0) &&
        log_info.state_ == storage::ReplicationLogState::kActive &&
        next_lsn >= log_info.floor_lsn_ && next_lsn <= log_info.tail_lsn_ + 1;
    spdlog::info(
        "replication session {} flow {} requested cursor={}:{} "
        "history-match={} "
        "backlog-state={} floor={} tail={} selected={}",
        session_id, flow_id, next_lsn, fragment_index, session->allow_continue_,
        static_cast<unsigned>(log_info.state_), log_info.floor_lsn_,
        log_info.tail_lsn_, continue_mode ? "CONTINUE" : "FULL");
    if (!session->SetFlowResumePossible(flow_id, continue_mode)) {
      session->ClearFlow(flow_id, stream.NativeFd());
      session->Cancel();
      co_return absl::FailedPreconditionError(
          "replication flow mode was already registered");
    }
    std::optional<bool> session_continue_mode;
    while (!session->cancelled() &&
           !(session_continue_mode = session->ContinueMode()).has_value()) {
      absl::Status waited = co_await celer::SleepFor(
          *celer::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) {
        session->ClearFlow(flow_id, stream.NativeFd());
        session->Cancel();
        co_return waited;
      }
    }
    if (!session_continue_mode.has_value()) {
      session->ClearFlow(flow_id, stream.NativeFd());
      co_return absl::CancelledError(
          "replication session ended before flow mode selection");
    }
    const bool selected_continue_mode = *session_continue_mode;
    absl::Status sent = co_await WriteText(
        stream,
        absl::StrCat("+KLFLOW ", session_id, " ", flow_id, " ",
                     selected_continue_mode ? "CONTINUE" : "FULL", "\r\n"));
    if (!sent.ok()) {
      session->ClearFlow(flow_id, stream.NativeFd());
      session->Cancel();
      co_return sent;
    }
    absl::Status waited =
        selected_continue_mode
            ? co_await EnterMasterFlowBacklog(stream, session, flow_id,
                                              next_lsn, fragment_index)
            : co_await RunMasterFlowData(stream, session, flow_id);
    if (!waited.ok()) {
      spdlog::warn("replication source flow {} ended: {}", flow_id,
                   waited.message());
      // A disconnected session must stop pinning history before any cleanup
      // that may need the replication-log mutex. This also immediately wakes
      // foreground writes backpressured on that replica.
      storage_->ReleaseReplicationLogRetention(session->id_);
      // A backlog encoding/allocation failure marks worker history invalid.
      // Reset it immediately rather than waiting for a replica reconnect:
      // every downstream connection is fenced by a new history id and all
      // in-memory backlog chunks are released.
      absl::Status reset = co_await ResetInvalidReplicationHistory();
      if (!reset.ok()) {
        spdlog::warn("replication history cleanup failed: {}", reset.message());
      }
    }
    storage_->ReleaseReplicationLogRetention(session->id_);
    session->MarkFailed(flow_id);
    session->ClearFlow(flow_id, stream.NativeFd());
    session->Cancel();
    co_return waited;
  }

  void RemoveMasterSession(const std::shared_ptr<MasterSession>& session) {
    {
      std::lock_guard lock(master_mutex_);
      const auto found = master_sessions_.find(session->id_);
      if (found != master_sessions_.end() && found->second == session) {
        master_sessions_.erase(found);
      }
    }
    session->Cancel();
  }

  Task<absl::Status> ResetInvalidReplicationHistory() {
    if (celer::ThisWorker().id_ != 0) {
      co_return co_await celer::SubmitTaskTo(
          0, [this]() { return EnsureReplicationHistoryReady(); });
    }
    co_return co_await EnsureReplicationHistoryReady();
  }

  Task<absl::Status> EnsureReplicationHistoryReady() {
    // Control handshakes are owned by worker 0. Keep reset ownership local and
    // let another handshake yield while the first one performs storage IO;
    // there is no process-global lock on the write path.
    while (history_reset_running_) {
      absl::Status waited = co_await celer::SleepFor(
          *celer::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) co_return waited;
    }
    history_reset_running_ = true;
    struct ResetGuard {
      bool* flag_;
      ~ResetGuard() { *flag_ = false; }
    } reset_guard{&history_reset_running_};
    bool invalid = false;
    for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
      storage::ReplicationLogInfo info;
      if (worker == celer::ThisWorker().id_) {
        info = storage_->LocalReplicationLogInfo();
      } else {
        info = co_await celer::SubmitTo(
            worker, [this] { return storage_->LocalReplicationLogInfo(); });
      }
      invalid |= info.state_ == storage::ReplicationLogState::kInvalid;
    }
    if (!invalid) co_return absl::OkStatus();
    std::vector<std::shared_ptr<MasterSession>> cancelled;
    {
      std::lock_guard lock(master_mutex_);
      cancelled.reserve(master_sessions_.size());
      for (auto& [id, session] : master_sessions_) {
        (void)id;
        cancelled.push_back(session);
      }
      master_sessions_.clear();
      history_id_ = NewReplicationId();
    }
    for (const auto& session : cancelled) session->Cancel();
    for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
      absl::Status disabled =
          co_await celer::SubmitTaskTo(worker, [this]() -> Task<absl::Status> {
            co_return co_await storage_->DisableReplicationLog();
          });
      if (!disabled.ok()) co_return disabled;
    }
    co_return absl::OkStatus();
  }

  storage::StorageEngine* storage_;
  mutable std::mutex state_mutex_;
  std::optional<ReplicaOfConfig> upstream_;
  std::shared_ptr<ReplicaSession> active_replica_session_;
  std::atomic<ReplicationRole> role_{ReplicationRole::kMaster};
  std::atomic<std::uint64_t> link_state_changed_nanos_{SteadyNanos()};
  std::atomic<std::uint64_t> role_epoch_{0};
  std::atomic<bool> native_dataset_valid_{false};
  std::atomic<unsigned> ready_workers_{0};
  std::uint64_t replica_session_id_ = 0;
  unsigned source_worker_count_ = 0;
  bool ready_waiter_started_ = false;            // worker 0 only
  bool coordinator_started_ = false;             // worker 0 only
  bool initial_protocol_probe_pending_ = false;  // worker 0 only
  std::shared_ptr<ReplicaCursorState> cursor_state_;
  std::optional<std::string> upstream_node_id_;
  std::optional<std::string> upstream_history_id_;
  std::atomic<bool> replication_fault_drop_used_{false};
  std::atomic<bool> replication_transaction_fault_drop_used_{false};
  std::atomic<bool> replication_command_apply_fault_drop_used_{false};
  std::atomic<bool> replication_fullsync_pause_used_{false};
  std::atomic<bool> replication_fullsync_handoff_pause_used_{false};
  std::atomic<unsigned> snapshot_read_concurrency_{
      kDefaultReplicationSnapshotReadConcurrency};
  std::atomic<std::size_t> snapshot_batch_size_{kSnapshotKeysPerBatch};
  std::atomic<std::size_t> backlog_size_bytes_{0};
  std::atomic<std::size_t> publish_queue_bytes_per_worker_{0};
  std::atomic<unsigned> replica_priority_{100};
  bool history_reset_running_ = false;  // worker 0 only

  const std::string node_id_;
  std::string history_id_;  // guarded by master_mutex_
  const std::uint16_t listen_port_;
  const std::shared_ptr<celer::TlsContext> tls_context_;
  const std::string masteruser_;
  const std::string masterauth_;
  std::atomic<bool> redis_psync_{false};
  std::atomic<bool> redis_export_backpressure_{false};
  std::atomic<bool> redis_export_active_{false};
  // Each Redis source owns an independent PSYNC cursor. Cursors intentionally
  // remain process-local until storage and offsets can share a crash-atomic
  // commit record; process restart therefore requests a fresh RDB per source.
  std::vector<std::shared_ptr<RedisSource>> redis_sources_;
  std::optional<RedisClusterTopology> expected_redis_topology_;
  bool redis_cluster_ = false;
  bool redis_topology_fault_ = false;
  bool redis_topology_monitor_started_ = false;  // worker 0 only
  celer::AsyncMutex redis_fullsync_mutex_;       // worker 0 only
  std::atomic<std::uint64_t> next_master_session_id_{1};
  mutable std::mutex master_mutex_;
  absl::flat_hash_map<std::uint64_t, std::shared_ptr<MasterSession>>
      master_sessions_;
};

ReplicationManager::ReplicationManager(
    storage::StorageEngine* storage, ReplicationOptions options,
    std::optional<ReplicaOfConfig> initial_upstream)
    : impl_(std::make_unique<Impl>(storage, std::move(initial_upstream),
                                   options)),
      options_(std::move(options)) {}

ReplicationManager::~ReplicationManager() = default;

void ReplicationManager::StorageReady(celer::Worker& worker) {
  impl_->StorageReady(worker);
}

Task<absl::Status> ReplicationManager::SetUpstream(
    std::optional<ReplicaOfConfig> upstream) {
  return impl_->SetUpstream(std::move(upstream));
}

Task<absl::Status> ReplicationManager::AddUpstream(ReplicaOfConfig upstream) {
  return impl_->AddUpstream(std::move(upstream));
}

absl::Status ReplicationManager::SetSnapshotReadConcurrency(
    unsigned concurrency) noexcept {
  return impl_->SetSnapshotReadConcurrency(concurrency);
}

unsigned ReplicationManager::snapshot_read_concurrency() const noexcept {
  return impl_->snapshot_read_concurrency();
}

absl::Status ReplicationManager::SetSnapshotBatchSize(
    std::size_t count) noexcept {
  return impl_->SetSnapshotBatchSize(count);
}

std::size_t ReplicationManager::snapshot_batch_size() const noexcept {
  return impl_->snapshot_batch_size();
}

Task<absl::Status> ReplicationManager::SetBacklogSizeBytes(std::size_t bytes) {
  return impl_->SetBacklogSizeBytes(bytes);
}

std::size_t ReplicationManager::backlog_size_bytes() const noexcept {
  return impl_->backlog_size_bytes();
}

Task<absl::Status> ReplicationManager::SetPublishQueueBytesPerWorker(
    std::size_t bytes) {
  return impl_->SetPublishQueueBytesPerWorker(bytes);
}

std::size_t ReplicationManager::publish_queue_bytes_per_worker()
    const noexcept {
  return impl_->publish_queue_bytes_per_worker();
}

absl::Status ReplicationManager::SetReplicaPriority(unsigned priority) noexcept {
  return impl_->SetReplicaPriority(priority);
}

unsigned ReplicationManager::replica_priority() const noexcept {
  return impl_->replica_priority();
}

bool ReplicationManager::IsNativeHandshake(
    std::span<const std::string> args) noexcept {
  return !args.empty() && (EqualCaseInsensitive(args.front(), "KLPSYNC") ||
                           EqualCaseInsensitive(args.front(), "KLFLOW"));
}

Task<absl::Status> ReplicationManager::ServeNativeConnection(
    TcpStream& stream, std::vector<std::string> args, std::uint64_t client_id,
    std::string client_address, bool tls) {
  if (!IsNativeHandshake(args)) {
    co_return absl::InvalidArgumentError(
        "connection did not start with a replication handshake");
  }
  co_return co_await impl_->ServeNativeConnection(
      stream, std::move(args), client_id, std::move(client_address), tls);
}

Task<absl::Status> ReplicationManager::ServeRedisExportConnection(
    TcpStream& stream, std::vector<std::string> args, std::uint64_t client_id,
    std::string client_address, bool tls, bool eof_capable) {
  return impl_->ServeRedisExportConnection(stream, std::move(args), client_id,
                                           std::move(client_address), tls,
                                           eof_capable);
}

ReplicationStatus ReplicationManager::status() const { return impl_->status(); }

bool ReplicationManager::is_replica() const noexcept {
  return impl_->is_replica();
}

bool ReplicationManager::is_loading() const noexcept {
  return impl_->is_loading();
}

bool ReplicationManager::reject_writes() const noexcept {
  return impl_->is_redis_follower() ||
         (options_.replica_read_only_ && is_replica());
}

bool ReplicationManager::redirects_clients_to_upstream() const noexcept {
  return !impl_->is_redis_follower();
}

std::string_view ReplicationRoleName(ReplicationRole role) noexcept {
  switch (role) {
    case ReplicationRole::kMaster:
      return "master";
    case ReplicationRole::kConnecting:
      return "connecting";
    case ReplicationRole::kSyncing:
      return "syncing";
    case ReplicationRole::kOnline:
      return "online";
  }
  return "unknown";
}

}  // namespace keylane
