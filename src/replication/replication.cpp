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

#include "lavik/replication.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <openssl/crypto.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <bitset>
#include <cassert>
#include <cerrno>
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
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "../redis/blocking_wait.h"
#include "../redis/function_catalog.h"
#include "../redis/lua_eval.h"
#include "absl/container/flat_hash_map.h"
#include "absl/crc/crc32c.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "bycorf/net/connection.h"
#include "bycorf/net/tls.h"
#include "bycorf/runtime/concurrentqueue.h"
#include "bycorf/runtime/cross_core.h"
#include "bycorf/runtime/sync.h"
#include "bycorf/runtime/worker.h"
#include "lavik/cluster/control_protocol.h"
#include "lavik/cluster/lease_clock.h"
#include "lavik/command.h"
#include "lavik/command_table.h"
#include "lavik/fault_injection.h"
#include "lavik/memory.h"
#include "lavik/metrics.h"
#include "lavik/rdb.h"
#include "lavik/rdb_collection.h"
#include "lavik/replication_command.h"
#include "lavik/replication_group.h"
#include "lavik/resp.h"
#include "lavik/storage/engine.h"
#include "population_recovery.h"
#include "replica_applied_frontier.h"
#include "source_authorization.h"
#include "spdlog/spdlog.h"

namespace lavik {

namespace detail {

// The attempt context and every replay handle share one result. A mutex keeps
// the non-trivial Status payload coherent across flow workers; Await() polls on
// its own worker so queued coroutine handles never outlive a cancelled caller.
class ClusterRebuildCompletionState {
 public:
  void Resolve(absl::Status result) {
    std::lock_guard lock(mutex_);
    if (!result_.has_value()) result_ = std::move(result);
  }

  std::optional<absl::Status> result() const {
    std::lock_guard lock(mutex_);
    return result_;
  }

  bycorf::Task<absl::Status> Await() const {
    bycorf::Worker* worker = bycorf::ThisWorker().self_;
    if (worker == nullptr) {
      co_return absl::FailedPreconditionError(
          "cluster rebuild completion requires a Bycorf worker");
    }
    for (;;) {
      if (std::optional<absl::Status> terminal = result();
          terminal.has_value()) {
        co_return *terminal;
      }
      // Rebuilds can legitimately run for hours. This low-frequency poll is
      // cancellation-safe (no borrowed coroutine handle remains registered)
      // and there is at most one active target population per process.
      absl::Status waited =
          co_await bycorf::SleepFor(*worker, std::chrono::milliseconds(10));
      if (!waited.ok()) co_return waited;
    }
  }

 private:
  mutable std::mutex mutex_;
  std::optional<absl::Status> result_;
};

class ClusterPromotionPrepareCompletionState {
 public:
  using Result = absl::StatusOr<ClusterPromotionPrepared>;

  void Resolve(Result result) {
    std::lock_guard lock(mutex_);
    if (!result_.has_value()) result_ = std::move(result);
  }

  std::optional<Result> result() const {
    std::lock_guard lock(mutex_);
    return result_;
  }

  bycorf::Task<Result> Await() const {
    bycorf::Worker* worker = bycorf::ThisWorker().self_;
    if (worker == nullptr) {
      co_return absl::FailedPreconditionError(
          "cluster promotion completion requires a Bycorf worker");
    }
    for (;;) {
      if (std::optional<Result> terminal = result(); terminal.has_value()) {
        co_return *terminal;
      }
      absl::Status waited =
          co_await bycorf::SleepFor(*worker, std::chrono::milliseconds(10));
      if (!waited.ok()) co_return waited;
    }
  }

 private:
  mutable std::mutex mutex_;
  std::optional<Result> result_;
};

}  // namespace detail

namespace {

using bycorf::Connection;
using bycorf::Task;
using bycorf::TcpStream;
using storage::PartitionFullSyncBatch;
using storage::PartitionReplicationStart;
using storage::PartitionSnapshotBatch;
using storage::SnapshotRecord;

constexpr std::string_view kProtocolVersion = "1";
constexpr auto kHandshakeTimeout = std::chrono::seconds(10);
constexpr std::string_view kLeaseAdmissionSuspendedReply = "-LVLEASESUSPENDED";
constexpr std::string_view kLeaseAdmissionSuspendedStatus =
    "cluster source admission is suspended until lease renewal";
constexpr unsigned kLeaseAdmissionPreMutationRetries = 3;

bool IsLeaseAdmissionSuspended(const absl::Status& status) {
  return status.code() == absl::StatusCode::kUnavailable &&
         status.message() == kLeaseAdmissionSuspendedStatus;
}

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

#if LAVIK_FAULTS_ENABLED
// A configured path turns the corresponding promotion stall into a
// deterministic coroutine barrier. Tests observe the created file, then
// supersede the action through the public reconciliation API. The runner
// resumes only after that action is no longer current, eliminating timing as
// evidence for which side of the durability boundary was exercised.
absl::Status SignalFaultBarrier(const char* variable,
                                std::string_view barrier_name) {
  const char* path = std::getenv(variable);
  if (path == nullptr || *path == '\0') return absl::OkStatus();
  const int fd = ::open(path, O_WRONLY | O_CREAT | O_CLOEXEC, 0600);
  if (fd < 0) {
    return absl::InternalError(absl::StrCat("could not signal ", barrier_name,
                                            ": ", std::strerror(errno)));
  }
  if (::close(fd) != 0) {
    return absl::InternalError(absl::StrCat("could not close ", barrier_name,
                                            ": ", std::strerror(errno)));
  }
  return absl::OkStatus();
}

Task<absl::Status> WaitAtSourceAdmissionFaultBarrier(
    const std::atomic<bool>& shutdown_requested) {
  constexpr const char* kBarrierVariable =
      "LAVIK_REPLICATION_SOURCE_ADMISSION_BARRIER_PATH";
  const char* path = std::getenv(kBarrierVariable);
  if (path == nullptr || *path == '\0') co_return absl::OkStatus();
  absl::Status signalled = SignalFaultBarrier(
      kBarrierVariable, "source authorization fault barrier");
  if (!signalled.ok()) co_return signalled;
  for (;;) {
    if (shutdown_requested.load(std::memory_order_acquire)) {
      co_return absl::CancelledError(
          "source admission fault barrier stopped for shutdown");
    }
    if (::access(path, F_OK) != 0) {
      if (errno == ENOENT) co_return absl::OkStatus();
      co_return absl::InternalError(
          absl::StrCat("could not observe source admission fault barrier: ",
                       std::strerror(errno)));
    }
    absl::Status waited = co_await bycorf::SleepFor(
        *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
    if (!waited.ok()) co_return waited;
  }
}
#endif
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
constexpr std::uint32_t kDataFrameMagic = 0x3146564c;  // "LVF1" in LE.
constexpr std::uint8_t kDataFrameVersion = 1;
constexpr std::size_t kDataFrameHeaderBytes = 16;
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
    allocation_capacity = std::max(allocation_capacity, output->capacity() * 2);
  } else {
    return ReplicationMemoryExhausted("replication buffer");
  }
  if (allocation_capacity == std::numeric_limits<std::size_t>::max()) {
    return ReplicationMemoryExhausted("replication buffer");
  }
  try {
    output->reserve(allocation_capacity);
  } catch (const std::length_error&) {
    return absl::ResourceExhaustedError("replication buffer is too large");
  }
  return absl::OkStatus();
}

absl::Status AppendReplicationString(std::string* output,
                                     std::string_view value) {
  if (output->size() > kMaxNativeReplicationEventBytes ||
      value.size() > kMaxNativeReplicationEventBytes - output->size()) {
    return absl::ResourceExhaustedError(
        "replication command exceeds the native event limit");
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
      return absl::InvalidArgumentError("malformed replication record payload");
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

Task<absl::Status> WriteText(TcpStream& stream, const char* text) {
  return WriteText(stream, std::string_view(text));
}

// A Task starts after the call expression has finished. Force temporary
// command/reply buffers into a caller-owned local instead of allowing a
// string_view to outlive them.
Task<absl::Status> WriteText(TcpStream&, std::string&&) = delete;

std::uint32_t DataFrameCrc32c(std::string_view first,
                              std::string_view second = {}) noexcept {
  absl::crc32c_t crc = absl::ComputeCrc32c(first);
  crc = absl::ExtendCrc32c(crc, second);
  return static_cast<std::uint32_t>(crc);
}

absl::Status AppendDataFrameHeader(std::string* output, DataFrameKind kind,
                                   std::size_t payload_bytes,
                                   std::uint32_t payload_crc32c) {
  if (payload_bytes > kMaxDataFrame ||
      payload_bytes > std::numeric_limits<std::uint32_t>::max()) {
    return absl::ResourceExhaustedError(
        "replication data frame exceeds configured limit");
  }
  PutU32(*output, kDataFrameMagic);
  PutU8(*output, kDataFrameVersion);
  PutU8(*output, static_cast<std::uint8_t>(kind));
  PutU16(*output, kDataFrameHeaderBytes);
  PutU32(*output, static_cast<std::uint32_t>(payload_bytes));
  PutU32(*output, payload_crc32c);
  return absl::OkStatus();
}

absl::Status AppendDataFrame(std::string* output, DataFrameKind kind,
                             std::string_view payload) {
  absl::Status header = AppendDataFrameHeader(output, kind, payload.size(),
                                              DataFrameCrc32c(payload));
  if (!header.ok()) return header;
  output->append(payload);
  return absl::OkStatus();
}

Task<absl::Status> WriteDataFrame(TcpStream& stream, DataFrameKind kind,
                                  std::string_view payload) {
  std::string frame;
  if (payload.size() >
      std::numeric_limits<std::size_t>::max() - kDataFrameHeaderBytes) {
    co_return ReplicationMemoryExhausted("replication frame");
  }
  absl::Status reserved =
      ReserveReplicationString(&frame, kDataFrameHeaderBytes + payload.size());
  if (!reserved.ok()) co_return reserved;
  absl::Status appended = AppendDataFrame(&frame, kind, payload);
  if (!appended.ok()) co_return appended;
  LAVIK_FAULT_INJECT(if (kind == DataFrameKind::kRecords && !payload.empty()) {
    const char* corrupt_record =
        std::getenv("LAVIK_REPLICATION_CORRUPT_FULLSYNC_RECORD_FRAME_ONCE");
    static std::atomic<bool> corrupt_record_used{false};
    if (corrupt_record != nullptr && std::string_view(corrupt_record) == "1" &&
        !corrupt_record_used.exchange(true, std::memory_order_acq_rel)) {
      // Deliberately mutate the wire image after its checksum was encoded.
      // This verifies that the target rejects corruption instead of persisting
      // it.
      frame.back() ^= 0x01;
      spdlog::warn("injected corrupt full-sync record frame");
    }
  });
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
  auto header = co_await ReadExact(stream, kDataFrameHeaderBytes);
  if (!header.ok()) co_return header.status();
  DataReader reader(*header);
  std::uint32_t magic = 0;
  std::uint8_t version = 0, kind = 0;
  std::uint16_t header_bytes = 0;
  std::uint32_t payload_bytes = 0, payload_crc32c = 0;
  if (!reader.U32(&magic) || !reader.U8(&version) || !reader.U8(&kind) ||
      !reader.U16(&header_bytes) || !reader.U32(&payload_bytes) ||
      !reader.U32(&payload_crc32c) || magic != kDataFrameMagic ||
      version != kDataFrameVersion || header_bytes != kDataFrameHeaderBytes ||
      payload_bytes > kMaxDataFrame || kind < 1 ||
      kind > static_cast<std::uint8_t>(DataFrameKind::kFullSyncCommand)) {
    co_return absl::InvalidArgumentError("malformed replication frame header");
  }
  auto payload = co_await ReadExact(stream, payload_bytes);
  if (!payload.ok()) co_return payload.status();
  if (DataFrameCrc32c(*payload) != payload_crc32c) {
    co_return absl::DataLossError("replication frame CRC32C mismatch");
  }
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
  constexpr std::string_view prefix = "/tmp/lavik-redis-rdb-XXXXXX";
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
      : storage_(storage), session_id_(session_id), producer_(queue_) {}

  void Start() {
    remaining_.store(storage_->worker_count(), std::memory_order_release);
    for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
      auto context = std::make_unique<std::shared_ptr<RedisRdbStreamQueue>>(
          shared_from_this());
      bycorf::PostNotification(
          bycorf::ThisWorker().cross_core_, worker,
          bycorf::RemoteNotification{
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
    bycorf::ThisWorker().self_->SpawnBackground(
        RunOwned(shared_from_this(), worker_id));
  }

  bool TryBeginEntry(unsigned owner) {
    unsigned available = std::numeric_limits<unsigned>::max();
    return entry_owner_.compare_exchange_strong(
        available, owner, std::memory_order_acquire, std::memory_order_relaxed);
  }

  bool TryPush(std::string* fragment, unsigned owner) {
    assert(entry_owner_.load(std::memory_order_relaxed) == owner);
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
    if (!queue_.enqueue(producer_, std::move(*fragment))) {
      queued_bytes_.fetch_sub(bytes, std::memory_order_acq_rel);
      Fail(absl::ResourceExhaustedError(
          "failed to allocate Redis RDB stream queue entry"));
      aborted_.store(true, std::memory_order_release);
      return false;
    }
    return true;
  }

  Task<absl::Status> PushEntrySpan(unsigned owner, std::string_view bytes) {
    while (!bytes.empty()) {
      const auto piece = bytes.substr(0, 1024 * 1024);
      std::string fragment(piece);
      while (!TryPush(&fragment, owner)) {
        if (aborted_.load(std::memory_order_acquire))
          co_return absl::CancelledError("Redis RDB export cancelled");
        auto status = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                                std::chrono::milliseconds(1));
        if (!status.ok()) co_return status;
      }
      bytes.remove_prefix(piece.size());
    }
    co_return absl::OkStatus();
  }

  Task<absl::Status> WriteCollection(unsigned owner,
                                     const storage::RdbSnapshotValue& value) {
    auto encoder = rdb::CollectionFileEncoder::Create(
        value.db_id_, value.key_, value.value_.value_type_,
        value.value_.logical_size_, value.value_.expire_at_ms_);
    if (!encoder.ok()) co_return encoder.status();
    auto drain = [&]() -> Task<absl::Status> {
      while (auto span = encoder->Next()) {
        auto status = co_await PushEntrySpan(owner, *span);
        if (!status.ok()) co_return status;
      }
      co_return absl::OkStatus();
    };
    auto status = co_await drain();
    if (!status.ok()) co_return status;
    std::uint64_t cursor = 0;
    for (;;) {
      if (aborted_.load(std::memory_order_acquire))
        co_return absl::CancelledError("Redis RDB export cancelled");
      auto page = co_await storage_->ReadRdbCollectionPage(
          session_id_, value.collection_token_, cursor);
      if (!page.ok()) co_return page.status();
      status = encoder->StartPage(*page);
      if (!status.ok()) co_return status;
      // The admitted page owns all borrowed strings until network queue
      // backpressure has accepted every span; no whole-object copy is made.
      status = co_await drain();
      if (!status.ok()) co_return status;
      cursor = page->next_cursor_;
      if (page->done_) break;
    }
    status = encoder->Finish();
    if (!status.ok()) co_return status;
    co_return co_await storage_->FinishRdbCollection(session_id_,
                                                     value.collection_token_);
  }

  Task<absl::Status> ScanWorker(unsigned worker_id) {
    absl::Status status;
    storage::RdbSnapshotCursor cursor;
    unsigned reads_since_yield = 0;
    try {
      while (status.ok() && !aborted_.load(std::memory_order_acquire)) {
        auto batch = co_await storage_->ReadRdbSnapshotBatch(
            session_id_, cursor, 1, 8ULL * 1024 * 1024);
        if (!batch.ok()) {
          status = batch.status();
          break;
        }
        cursor = batch->cursor_;
        for (storage::RdbSnapshotValue& value : batch->values_) {
          while (!TryBeginEntry(worker_id)) {
            if (aborted_.load(std::memory_order_acquire)) {
              status = absl::CancelledError("Redis RDB export cancelled");
              break;
            }
            status = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                               std::chrono::milliseconds(1));
            if (!status.ok()) break;
          }
          if (!status.ok()) break;
          struct EntryLease {
            std::atomic<unsigned>* owner;
            ~EntryLease() {
              owner->store(std::numeric_limits<unsigned>::max(),
                           std::memory_order_release);
            }
          } lease{&entry_owner_};
          if (value.collection_token_ != 0) {
            status = co_await WriteCollection(worker_id, value);
            if (!status.ok()) break;
            continue;
          }
          auto fragment =
              rdb::EncodeFileEntry(value.db_id_, value.key_, value.value_);
          std::string().swap(value.value_.encoded_);
          std::string().swap(value.key_);
          if (!fragment.ok()) {
            status = fragment.status();
            break;
          }
          status = co_await PushEntrySpan(worker_id, *fragment);
          if (!status.ok() || aborted_.load(std::memory_order_acquire)) break;
        }
        if (!status.ok() || batch->done_) break;
        if (++reads_since_yield == 64) {
          reads_since_yield = 0;
          co_await bycorf::Yield(*bycorf::ThisWorker().self_);
        }
      }
    } catch (const std::bad_alloc&) {
      // Entry leases and admitted pages unwind before cancelling the retained
      // snapshot. Never let a background allocation failure strand its pins.
      RecordMemoryRejection();
      status = absl::ResourceExhaustedError("OOM Redis RDB export");
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
  // ConcurrentQueue orders within one producer, not between producers. A
  // whole-key lease serializes this shared token across workers, preserving
  // contiguous entry fragments even when another worker acquires the lease
  // before the consumer has drained the previous key.
  moodycamel::ProducerToken producer_;
  std::atomic<unsigned> entry_owner_{std::numeric_limits<unsigned>::max()};
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

  // Redis does not know Lavik extensions. Export a successful whole-Hash
  // replacement as DEL + HSET inside the enclosing atomic envelope. Source
  // append/transaction settlement already supplies the final absolute expiry
  // effect, so this neither reads old fields nor extends the key's lifetime.
  for (const auto& child : children) {
    if (!child.args_.empty() &&
        EqualCaseInsensitive(child.args_[0], "LAVIK.HREPLACE")) {
      if (child.args_.size() < 4 || child.args_.size() % 2 != 0)
        return absl::InvalidArgumentError("malformed Hash replacement export");
      transactional = true;
    }
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
  for (Child& child : children) {
    if (current_db != child.db_) {
      output += EncodeRespCommand(
          std::vector<std::string>{"SELECT", std::to_string(child.db_)});
      current_db = child.db_;
    }
    if (!child.args_.empty() &&
        EqualCaseInsensitive(child.args_[0], "LAVIK.HREPLACE")) {
      output +=
          EncodeRespCommand(std::vector<std::string>{"DEL", child.args_[1]});
      child.args_[0] = "HSET";
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
    auto event = co_await bycorf::SubmitTaskTo(
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
  co_return co_await bycorf::SubmitTo(worker, [storage, session_id, cursor] {
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
             storage::ReplicationEventKind::kEphemeral &&
         state->heads_[worker]->kind_ !=
             storage::ReplicationEventKind::kCatalogMutation)) {
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
  absl::Status status = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
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
  const std::string encoded = EncodeRespCommand(args);
  absl::Status sent = co_await WriteText(stream, encoded);
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

class SocketSet {
 public:
  // A target session also registers its sockets with the process shutdown
  // set. Main can close ingress without inspecting worker-owned sessions.
  // Registration/removal always lock child before parent; cancellation never
  // takes a child lock while holding its parent's lock.
  explicit SocketSet(SocketSet* shutdown_parent = nullptr)
      : shutdown_parent_(shutdown_parent) {}

  ~SocketSet() {
    // Destruction follows session/flow join. Do not leave a retired session's
    // descriptor numbers in the process set, where a reused fd could later
    // identify an unrelated connection.
    if (shutdown_parent_ != nullptr) {
      for (int fd : fds_) shutdown_parent_->Remove(fd);
    }
  }

  bool Add(int fd) {
    std::lock_guard lock(mutex_);
    if (cancelled_) {
      ::shutdown(fd, SHUT_RDWR);
      return false;
    }
    fds_.push_back(fd);
    if (shutdown_parent_ != nullptr && !shutdown_parent_->Add(fd)) {
      fds_.pop_back();
      return false;
    }
    return true;
  }

  void Remove(int fd) {
    std::lock_guard lock(mutex_);
    const auto found = std::find(fds_.begin(), fds_.end(), fd);
    if (found != fds_.end()) {
      if (shutdown_parent_ != nullptr) shutdown_parent_->Remove(fd);
      fds_.erase(found);
    }
  }

  void Cancel() {
    std::lock_guard lock(mutex_);
    if (cancelled_) return;
    cancelled_ = true;
    for (int fd : fds_) ::shutdown(fd, SHUT_RDWR);
  }

  bool cancelled() const {
    std::lock_guard lock(mutex_);
    return cancelled_ ||
           (shutdown_parent_ != nullptr && shutdown_parent_->cancelled());
  }

 private:
  mutable std::mutex mutex_;
  std::vector<int> fds_;
  bool cancelled_ = false;
  SocketSet* const shutdown_parent_;
};

class ScopedSocketSetMembership {
 public:
  ScopedSocketSetMembership(SocketSet* sockets, int fd)
      : sockets_(sockets), fd_(fd) {}
  ScopedSocketSetMembership(const ScopedSocketSetMembership&) = delete;
  ScopedSocketSetMembership& operator=(const ScopedSocketSetMembership&) =
      delete;
  ~ScopedSocketSetMembership() {
    if (sockets_ != nullptr) sockets_->Remove(fd_);
  }

 private:
  SocketSet* sockets_;
  int fd_;
};

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
    const std::shared_ptr<bycorf::TlsContext>& tls_context,
    SocketSet* sockets) {
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
    absl::Status configured = ConfigureConnectedFd(fd);
    if (!configured.ok()) {
      ::close(fd);
      continue;
    }
    if (sockets != nullptr && !sockets->Add(fd)) {
      ::close(fd);
      ::freeaddrinfo(addresses);
      co_return absl::CancelledError(
          "replication connection was cancelled before connect");
    }

    int connect_result = ::connect(fd, address->ai_addr, address->ai_addrlen);
    if (connect_result != 0 && errno == EINPROGRESS) {
      // The old blocking connect could pin worker zero through process
      // shutdown. Poll in short slices so a thread-safe SocketSet cancellation
      // can terminate even a black-holed connect before the coroutine runtime
      // has adopted the descriptor.
      for (;;) {
        if (sockets != nullptr && sockets->cancelled()) {
          connect_result = -1;
          errno = ECANCELED;
          break;
        }
        pollfd pending{.fd = fd, .events = POLLOUT, .revents = 0};
        const int polled = ::poll(&pending, 1, 100);
        if (polled < 0 && errno == EINTR) continue;
        if (polled == 0) continue;
        if (polled < 0) {
          connect_result = -1;
          break;
        }
        int socket_error = 0;
        socklen_t error_size = sizeof(socket_error);
        if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &socket_error,
                         &error_size) != 0) {
          connect_result = -1;
          break;
        }
        if (socket_error == 0) {
          connect_result = 0;
        } else {
          connect_result = -1;
          errno = socket_error;
        }
        break;
      }
    }
    if (connect_result == 0) {
      connected_fd = fd;
      break;
    }
    if (sockets != nullptr) sockets->Remove(fd);
    ::close(fd);
  }
  ::freeaddrinfo(addresses);
  if (connected_fd < 0) {
    if (sockets != nullptr && sockets->cancelled()) {
      co_return absl::CancelledError("replication connect was cancelled");
    }
    co_return absl::UnavailableError("replication connect failed");
  }
  Connection connection;
  connection.worker_ = bycorf::ThisWorker().self_;
  connection.file_.fd_ = connected_fd;
  connection.closed_ = false;
  Connection* registered =
      bycorf::ThisWorker().self_->AddConnection(std::move(connection));
  if (registered == nullptr) {
    if (sockets != nullptr) sockets->Remove(connected_fd);
    ::close(connected_fd);
    co_return absl::InternalError("failed to register replication connection");
  }
  TcpStream stream(registered);
  if (tls_context != nullptr) {
    absl::Status started = co_await stream.StartTls(tls_context, false, host);
    if (!started.ok()) {
      if (sockets != nullptr) sockets->Remove(connected_fd);
      stream.Close().IgnoreError();
      co_return started;
    }
  }
  co_return stream;
}

std::string NewReplicationId() {
  auto generated = cluster::control::GenerateIdentity160();
  if (!generated.ok()) {
    // These values distinguish process/storage incarnations. Falling back to
    // a predictable or repeated id could make an old replication session look
    // current, so the legacy infallible constructor contract fails closed.
    spdlog::critical("cannot generate replication identity: {}",
                     generated.status().ToString());
    std::abort();
  }
  return std::move(*generated);
}

bool IsReplicationId(std::string_view value) {
  return value.size() == 40 &&
         std::all_of(value.begin(), value.end(), [](unsigned char digit) {
           return (digit >= '0' && digit <= '9') ||
                  (digit >= 'a' && digit <= 'f');
         });
}

template <std::size_t Size>
bool IsZeroBytes(const std::array<std::uint8_t, Size>& value) noexcept {
  return std::all_of(value.begin(), value.end(),
                     [](std::uint8_t byte) { return byte == 0; });
}

template <std::size_t Size>
std::string HexBytes(const std::array<std::uint8_t, Size>& value) {
  constexpr char kHex[] = "0123456789abcdef";
  std::string result;
  result.reserve(value.size() * 2);
  for (std::uint8_t byte : value) {
    result.push_back(kHex[byte >> 4]);
    result.push_back(kHex[byte & 0x0f]);
  }
  return result;
}

bool SameFailoverActionExceptAuthorization(
    const DesiredClusterFailoverAction& lhs,
    const DesiredClusterFailoverAction& rhs) {
  DesiredClusterFailoverAction lhs_copy = lhs;
  DesiredClusterFailoverAction rhs_copy = rhs;
  lhs_copy.transition_revision_ = rhs_copy.transition_revision_ = 0;
  lhs_copy.authorized_revision_.reset();
  rhs_copy.authorized_revision_.reset();
  return lhs_copy == rhs_copy;
}

bool IsRetainedControlledDegrade(
    const DesiredClusterFailoverAction& current,
    const DesiredClusterFailoverAction& replacement) {
  if (current.mode_ != ClusterFailoverMode::kControlled ||
      replacement.mode_ != ClusterFailoverMode::kUncontrolled ||
      current.transition_revision_ >= replacement.transition_revision_ ||
      current.committed_group_term_ ==
          std::numeric_limits<std::uint64_t>::max() ||
      current.committed_group_term_ + 1 != current.target_term_ ||
      replacement.committed_group_term_ != current.target_term_ ||
      !current.committed_grant_active_ || replacement.committed_grant_active_ ||
      !replacement.authorized_revision_.has_value() ||
      (current.authorized_revision_.has_value() &&
       current.authorized_revision_ != replacement.authorized_revision_)) {
    return false;
  }

  // DegradeControlledFailover retains the exact candidate action while its
  // atomic term fence changes only these five projection fields. Normalize
  // them before comparing so candidate, source lineage, population, and
  // transition identities remain immutable execution anchors.
  DesiredClusterFailoverAction current_copy = current;
  DesiredClusterFailoverAction replacement_copy = replacement;
  current_copy.transition_revision_ = replacement_copy.transition_revision_ = 0;
  current_copy.authorized_revision_.reset();
  replacement_copy.authorized_revision_.reset();
  current_copy.mode_ = replacement_copy.mode_ =
      ClusterFailoverMode::kUncontrolled;
  current_copy.committed_group_term_ = replacement_copy.committed_group_term_ =
      0;
  current_copy.committed_grant_active_ =
      replacement_copy.committed_grant_active_ = false;
  return current_copy == replacement_copy;
}

ClusterPromotionPrepareDirective BuildFailoverPrepareDirective(
    const DesiredClusterFailoverAction& desired,
    std::vector<std::uint64_t> current_frontier) {
  const std::string transition = HexBytes(desired.transition_id_);
  const std::string action = HexBytes(desired.action_id_);
  ClusterPromotionPrepareDirective directive{
      .identity_ =
          {
              .group_id_ = desired.group_id_,
              .assignment_id_ = desired.candidate_assignment_id_,
              .term_ = desired.target_term_,
              .directive_revision_ = *desired.authorized_revision_,
              .authority_id_ = transition,
              .source_node_id_ = desired.domain_.source_node_id_,
              .source_assignment_id_ = desired.domain_.source_assignment_id_,
              .source_boot_id_ = desired.domain_.source_boot_id_,
              .source_history_id_ = desired.domain_.source_history_id_,
              .target_node_id_ = desired.candidate_node_id_,
              .target_boot_id_ = desired.candidate_boot_id_,
              .target_history_id_ = {},
              .operation_id_ = transition,
              .directive_id_ = action,
              .attempt_id_ = action,
              .manifest_revision_ = desired.manifest_revision_,
              .manifest_id_ = desired.manifest_id_,
              .partition_replication_epoch_ =
                  desired.partition_replication_epoch_,
          },
      .parent_history_id_ = desired.domain_.source_history_id_,
      .required_applied_next_lsns_ = std::move(current_frontier),
      .excluded_group_term_ = desired.target_term_,
  };
  return directive;
}

std::string PopulationGroupToken(std::string_view group_id) {
  constexpr char kHex[] = "0123456789abcdef";
  std::string token;
  token.reserve(group_id.size() * 2);
  for (unsigned char byte : group_id) {
    token.push_back(kHex[byte >> 4]);
    token.push_back(kHex[byte & 0x0f]);
  }
  return token;
}

bool IsPopulationGroupToken(std::string_view value) {
  return !value.empty() && value.size() <= 128 && value.size() % 2 == 0 &&
         std::all_of(value.begin(), value.end(), [](unsigned char digit) {
           return (digit >= '0' && digit <= '9') ||
                  (digit >= 'a' && digit <= 'f');
         });
}

absl::StatusOr<PopulationManifestId> ParsePopulationManifestId(
    std::string_view value) {
  if (value.size() != 64) {
    return absl::InvalidArgumentError(
        "population manifest identity must contain 64 hex digits");
  }
  auto nibble = [](unsigned char digit) -> std::optional<std::uint8_t> {
    if (digit >= '0' && digit <= '9') return digit - '0';
    if (digit >= 'a' && digit <= 'f') return digit - 'a' + 10;
    return std::nullopt;
  };
  PopulationManifestId result;
  for (std::size_t index = 0; index < result.bytes_.size(); ++index) {
    const std::optional<std::uint8_t> high = nibble(value[index * 2]);
    const std::optional<std::uint8_t> low = nibble(value[index * 2 + 1]);
    if (!high.has_value() || !low.has_value()) {
      return absl::InvalidArgumentError(
          "population manifest identity contains non-hex data");
    }
    result.bytes_[index] = static_cast<std::uint8_t>((*high << 4) | *low);
  }
  return result;
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

std::string EncodeAppliedVector(std::span<const std::uint64_t> next_lsns) {
  std::string result;
  for (std::uint64_t next_lsn : next_lsns) {
    if (!result.empty()) result.push_back(',');
    absl::StrAppend(&result, next_lsn);
  }
  return result.empty() ? "?" : result;
}

absl::StatusOr<std::vector<std::uint64_t>> DecodeAppliedVector(
    std::string_view encoded) {
  std::vector<std::uint64_t> result;
  if (encoded == "?") return result;
  while (!encoded.empty()) {
    const std::size_t separator = encoded.find(',');
    const std::string_view item = encoded.substr(0, separator);
    std::uint64_t cursor = 0;
    if (!ParseUnsigned(item, &cursor) || cursor == 0) {
      return absl::InvalidArgumentError("invalid replication Applied vector");
    }
    result.push_back(cursor);
    if (separator == std::string_view::npos) break;
    encoded.remove_prefix(separator + 1);
  }
  return result;
}

// Transaction apply is detached from flow staging, so both participant ACK
// tasks and later dependency tasks may begin waiting after completion. This
// one-shot latch closes that race and resumes each coroutine on its owner
// worker without polling or blocking a runtime thread.
class ReplicaCompletionLatch {
  struct Waiter {
    bycorf::Worker* worker_ = nullptr;
    std::coroutine_handle<> handle_{};
    Waiter* next_ = nullptr;
  };

 public:
  class Awaiter {
   public:
    Awaiter(ReplicaCompletionLatch* latch, bycorf::Worker* worker) noexcept
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

  Awaiter Wait(bycorf::Worker& worker) noexcept {
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

    const bycorf::CurrentWorker& current = bycorf::ThisWorker();
    while (wake != nullptr) {
      Waiter* waiter = wake;
      wake = wake->next_;
      if (waiter->worker_->id() == current.id_) {
        waiter->worker_->Enqueue(waiter->handle_);
      } else {
        bycorf::PostNotification(
            current.cross_core_, waiter->worker_->id(),
            bycorf::RemoteNotification{
                .context_ = waiter->worker_,
                .value_ =
                    static_cast<std::uint64_t>(reinterpret_cast<std::uintptr_t>(
                        waiter->handle_.address())),
                .run_fn_ = &ReplicaCompletionLatch::ResumeRemote,
            });
      }
    }
    return true;
  }

 private:
  static void ResumeRemote(void* context, std::uint64_t value) noexcept {
    static_cast<bycorf::Worker*>(context)->Enqueue(
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

  bool Register(Waiter* waiter, std::coroutine_handle<> awaiting) noexcept {
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
  bycorf::CoroutineBarrier completion_;
};

struct ReplicaTransactionOwner {
  // This table is initialized before flow startup and thereafter touched only
  // by the worker whose id indexes the owner vector. Cross-worker flow
  // coroutines submit registrations to that worker instead of sharing a lock.
  absl::flat_hash_map<std::uint64_t, std::shared_ptr<ReplicaTransactionArrival>>
      transactions_;
};

// Immutable directive/manifest data is shared by the flow workers, while the
// ReplicationGroup itself remains owned by coordinator worker zero. The atomic
// state communicates its lifecycle to flow workers; only worker zero reads
// or publishes ReadyToken and changes the manager's current attempt.
struct ClusterRebuildContext {
  ClusterRebuildContext(RebuildDirective directive, PopulationManifest manifest,
                        DestructiveResetAuthorization authorization)
      : directive_(std::move(directive)),
        manifest_(std::move(manifest)),
        authorization_(std::move(authorization)),
        completion_(std::make_shared<detail::ClusterRebuildCompletionState>()) {
  }

  explicit ClusterRebuildContext(ReadyToken recovered)
      : directive_{.identity_ = recovered.identity(),
                   .flow_count_ = static_cast<std::uint32_t>(
                       recovered.cut_vector().size())},
        ready_token_(std::move(recovered)),
        completion_(std::make_shared<detail::ClusterRebuildCompletionState>()) {
    state_.store(ReplicationGroupState::kReady);
    completion_->Resolve(absl::OkStatus());
  }

  const RebuildDirective directive_;
  std::optional<PopulationManifest> manifest_;
  const std::optional<DestructiveResetAuthorization> authorization_;
  std::atomic<ReplicationGroupState> state_{ReplicationGroupState::kRebuilding};
  std::optional<ReadyToken> ready_token_;
  // Worker zero retains this count across fresh transport sessions for the
  // same immutable directive. It bounds only the explicit source lease-gate
  // response before any local destructive boundary has started.
  unsigned lease_admission_pre_mutation_retries_ = 0;
  const std::shared_ptr<detail::ClusterRebuildCompletionState> completion_;
};

struct ClusterPromotionPrepareContext {
  explicit ClusterPromotionPrepareContext(
      ClusterPromotionPrepareDirective directive)
      : directive_(std::move(directive)),
        completion_(std::make_shared<
                    detail::ClusterPromotionPrepareCompletionState>()) {}

  const ClusterPromotionPrepareDirective directive_;
  const std::shared_ptr<detail::ClusterPromotionPrepareCompletionState>
      completion_;
  // Worker zero may cancel an FDS-owned preparation only before the runner
  // crosses into storage durability/history mutation. Past that point the
  // caller must join the result and retire any child history rather than
  // pretending that the physical outcome is known.
  bool cancellation_requested_ = false;
  bool durability_mutation_started_ = false;
};

struct ClusterSourcePauseContext {
  explicit ClusterSourcePauseContext(DesiredClusterSourcePause desired)
      : desired_(std::move(desired)) {}

  DesiredClusterSourcePause desired_;
  std::optional<std::vector<std::uint64_t>> stable_next_lsns_;
  std::string failure_detail_;
  bool expiration_pause_held_ = false;
};

struct ClusterFailoverActionContext {
  explicit ClusterFailoverActionContext(DesiredClusterFailoverAction desired)
      : desired_(std::move(desired)) {}

  DesiredClusterFailoverAction desired_;
  ClusterFailoverActionState state_ =
      ClusterFailoverActionState::kWaitingForAuthorization;
  std::optional<ClusterFailoverPreparedContext> prepared_;
  std::optional<ClusterPromotionPrepareDirective> prepare_directive_;
  std::string failure_class_;
  std::string failure_detail_;
  bool cancelled_ = false;
  bool runner_started_ = false;
  bool runner_finished_ = true;
  bool failure_published_ = false;
  // This private cleanup fact is distinct from Prepared observation. A
  // replacement may cancel the action after the durability kernel creates a
  // child history but before the action is still allowed to publish it.
  bool prepared_child_history_created_ = false;
};

struct ClusterFollowOwnerContext {
  ClusterFollowOwnerContext(DesiredClusterUpstream desired,
                            PopulationManifest manifest)
      : desired_(std::move(desired)), manifest_(std::move(manifest)) {}

  const DesiredClusterUpstream desired_;
  const PopulationManifest manifest_;
  // A matching-history handshake can still discover that one source flow no
  // longer retains the requested cursor. The first session fails before any
  // reset and sets this latch; its fixed-delay retry then enters the ordinary
  // FULL path under a fresh boot-local population attempt.
  std::atomic<bool> force_full_{false};
};

struct ClusterSteadyExport {
  std::string group_id_;
  std::uint64_t group_term_ = 0;
  std::string target_node_id_;
  std::string target_assignment_id_;
  std::string source_node_id_;
  std::string source_assignment_id_;
  std::uint64_t manifest_revision_ = 0;
  PopulationManifestId manifest_id_;
  std::uint64_t partition_replication_epoch_ = 0;

  bool operator==(const ClusterSteadyExport&) const = default;
};

struct ReplicaSession {
  explicit ReplicaSession(SocketSet* shutdown_sockets)
      : sockets_(shutdown_sockets) {}

  enum class FlowProtocolPhase : std::uint8_t {
    kAwaitMode,
    kFullRebuild,
    kFullCutRecorded,
    kFullCutAwaitCursor,
    kContinueAwaitCursor,
    kOnline,
  };

  std::uint64_t session_id_ = 0;
  // Unpredictable bearer capability returned only on the control channel.
  // Data flows present it before they may claim a flow id or read bytes.
  std::string flow_capability_;
  unsigned source_worker_count_ = 0;
  std::shared_ptr<ClusterRebuildContext> cluster_rebuild_;

  std::shared_ptr<ClusterFollowOwnerContext> cluster_follow_;
  std::shared_ptr<detail::ReplicaAppliedFrontier> applied_frontier_;
  // No flow may consume data until every LVFLOW response selected the same
  // session mode. FULL then has a second barrier: flow zero drains old client
  // work and maintenance before any flow can issue a destructive reset.
  std::unique_ptr<bycorf::CoroutineBarrier> flow_modes_selected_;
  std::unique_ptr<bycorf::CoroutineBarrier> fullsync_begin_complete_;
  std::unique_ptr<bycorf::CoroutineBarrier> fullsync_cut_;
  std::unique_ptr<bycorf::CoroutineBarrier> promotion_complete_;
  // Flow coroutines are detached onto their owner workers. Track their whole
  // lifetime, including connect/handshake and storage apply, so a failed
  // session cannot start a replacement while old flows are still mutating
  // replica storage or holding network buffers.
  std::atomic<unsigned> active_flows_{0};
  std::atomic<unsigned> active_transaction_applies_{0};
  std::atomic<unsigned> connected_flows_{0};
  // Set before either control or flow code invokes BeginReplicaFullSync. A
  // false value therefore proves that retrying a lease-gate response cannot
  // conceal an uncertain in-place root mutation.
  std::atomic<bool> destructive_root_started_{false};
  SocketSet sockets_;
  std::atomic<bool> cancelled_{false};
  std::vector<std::unique_ptr<ReplicaTransactionOwner>> transaction_owners_;
  std::mutex control_mutex_;
  absl::flat_hash_map<std::uint64_t, std::shared_ptr<ReplicaControlArrival>>
      controls_;
  mutable std::mutex fullsync_mutex_;
  std::optional<bool> fullsync_mode_;
  std::vector<FlowProtocolPhase> flow_phases_;
  std::vector<storage::ReplicationLogCursor> requested_cursors_;
  std::vector<std::optional<std::uint64_t>> fullsync_cuts_;
  bool fullsync_cursors_reset_ = false;
  bool fullsync_cursors_installed_ = false;
  mutable std::mutex failure_mutex_;
  std::string fail_stop_reason_;

  void InitializeFullSyncState(std::span<const std::uint64_t> requested) {
    std::lock_guard lock(fullsync_mutex_);
    const unsigned flow_count = static_cast<unsigned>(requested.size());
    flow_phases_.assign(flow_count, FlowProtocolPhase::kAwaitMode);
    requested_cursors_.resize(flow_count);
    for (unsigned flow = 0; flow < flow_count; ++flow) {
      requested_cursors_[flow] = {.lsn_ = requested[flow],
                                  .fragment_index_ = 0};
    }
    fullsync_cuts_.assign(flow_count, std::nullopt);
  }

  storage::ReplicationLogCursor RequestedCursor(unsigned flow) const {
    std::lock_guard lock(fullsync_mutex_);
    if (flow >= requested_cursors_.size()) return {};
    return requested_cursors_[flow];
  }

  absl::Status SelectFlowMode(unsigned flow_id, bool fullsync) {
    std::lock_guard lock(fullsync_mutex_);
    if (flow_id >= flow_phases_.size() ||
        flow_phases_[flow_id] != FlowProtocolPhase::kAwaitMode) {
      return absl::InvalidArgumentError(
          "replication flow mode is duplicate or out of range");
    }
    if (fullsync_mode_.has_value() && *fullsync_mode_ != fullsync) {
      return absl::InvalidArgumentError(
          "replication session selected inconsistent flow modes");
    }
    if (!fullsync && requested_cursors_[flow_id].lsn_ == 1 &&
        requested_cursors_[flow_id].fragment_index_ == 0) {
      return absl::InvalidArgumentError(
          "CONTINUE cannot accept the initial full-sync cursor");
    }
    fullsync_mode_ = fullsync;
    flow_phases_[flow_id] = fullsync ? FlowProtocolPhase::kFullRebuild
                                     : FlowProtocolPhase::kContinueAwaitCursor;
    return absl::OkStatus();
  }

  absl::Status PrepareFullSyncCursors() {
    std::lock_guard lock(fullsync_mutex_);
    if (!fullsync_mode_.value_or(false) ||
        std::any_of(flow_phases_.begin(), flow_phases_.end(),
                    [](FlowProtocolPhase phase) {
                      return phase != FlowProtocolPhase::kFullRebuild;
                    })) {
      return absl::FailedPreconditionError(
          "full-sync cursors require unanimous flow mode selection");
    }
    if (fullsync_cursors_reset_) return absl::OkStatus();
    // No shared cursor changes until every flow has selected FULL. A failure
    // after this point must never retry with a mixture of old-history and
    // replacement-population cursors.
    std::vector<std::uint64_t> reset(applied_frontier_->size(), 1);
    absl::Status installed = applied_frontier_->InstallNextLsns(reset);
    if (!installed.ok()) return installed;
    fullsync_cursors_reset_ = true;
    return absl::OkStatus();
  }

  absl::Status RecordFullSyncCut(unsigned flow_id,
                                 std::uint64_t stable_next_lsn) {
    std::lock_guard lock(fullsync_mutex_);
    if (!fullsync_mode_.value_or(false) || flow_id >= fullsync_cuts_.size() ||
        stable_next_lsn == 0 ||
        flow_phases_[flow_id] != FlowProtocolPhase::kFullRebuild) {
      return absl::InvalidArgumentError(
          "full-sync cut does not match the selected flow mode");
    }
    fullsync_cuts_[flow_id] = stable_next_lsn;
    flow_phases_[flow_id] = FlowProtocolPhase::kFullCutRecorded;
    return absl::OkStatus();
  }

  absl::StatusOr<std::vector<std::uint64_t>> FullSyncCutVector() const {
    std::lock_guard lock(fullsync_mutex_);
    absl::Status complete = ValidateFullSyncCutVectorLocked();
    if (!complete.ok()) return complete;
    std::vector<std::uint64_t> result;
    result.reserve(fullsync_cuts_.size());
    for (const std::optional<std::uint64_t>& cut : fullsync_cuts_) {
      result.push_back(*cut);
    }
    return result;
  }

  absl::Status InstallFullSyncCutVector() {
    std::lock_guard lock(fullsync_mutex_);
    absl::Status complete = ValidateFullSyncCutVectorLocked();
    if (!complete.ok()) return complete;
    std::vector<std::uint64_t> cut;
    cut.reserve(fullsync_cuts_.size());
    for (const std::optional<std::uint64_t>& next_lsn : fullsync_cuts_) {
      cut.push_back(*next_lsn);
    }
    absl::Status installed = applied_frontier_->InstallNextLsns(cut);
    if (!installed.ok()) return installed;
    for (unsigned flow = 0; flow < fullsync_cuts_.size(); ++flow) {
      flow_phases_[flow] = FlowProtocolPhase::kFullCutAwaitCursor;
    }
    fullsync_cursors_installed_ = true;
    return absl::OkStatus();
  }

  absl::Status AcceptBacklogCursor(unsigned flow_id, std::uint64_t lsn,
                                   std::uint32_t fragment) {
    std::lock_guard lock(fullsync_mutex_);
    if (flow_id >= flow_phases_.size() || lsn == 0) {
      return absl::InvalidArgumentError(
          "replication cursor is zero or belongs to an unknown flow");
    }
    if (flow_phases_[flow_id] == FlowProtocolPhase::kFullCutAwaitCursor) {
      if (!fullsync_cursors_installed_ ||
          !fullsync_cuts_[flow_id].has_value() ||
          lsn != *fullsync_cuts_[flow_id] || fragment != 0) {
        return absl::InvalidArgumentError(
            "replication cursor disagrees with the full-sync cut");
      }
      flow_phases_[flow_id] = FlowProtocolPhase::kOnline;
      return absl::OkStatus();
    }
    if (flow_phases_[flow_id] == FlowProtocolPhase::kContinueAwaitCursor) {
      const storage::ReplicationLogCursor& requested =
          requested_cursors_[flow_id];
      if (lsn != requested.lsn_ || fragment != requested.fragment_index_) {
        return absl::InvalidArgumentError(
            "CONTINUE cursor disagrees with the requested position");
      }
      // The exact cursor is already installed. Publish only the protocol
      // phase transition; duplicate cursors are rejected as post-handoff
      // frames rather than silently rewriting continuation evidence.
      flow_phases_[flow_id] = FlowProtocolPhase::kOnline;
      return absl::OkStatus();
    }
    return absl::InvalidArgumentError(
        "replication cursor arrived outside its handoff phase");
  }

  absl::Status ValidateDataFramePhase(unsigned flow_id, DataFrameKind kind) {
    std::lock_guard lock(fullsync_mutex_);
    if (flow_id >= flow_phases_.size()) {
      return absl::InvalidArgumentError(
          "replication frame belongs to an unknown flow");
    }
    const FlowProtocolPhase phase = flow_phases_[flow_id];
    if (phase == FlowProtocolPhase::kFullRebuild) {
      if (kind == DataFrameKind::kCursor || kind == DataFrameKind::kCommand) {
        return absl::FailedPreconditionError(
            "ONLINE frame arrived before the full-sync cut");
      }
      return absl::OkStatus();
    }
    if (phase == FlowProtocolPhase::kFullCutAwaitCursor ||
        phase == FlowProtocolPhase::kContinueAwaitCursor) {
      return kind == DataFrameKind::kCursor
                 ? absl::OkStatus()
                 : absl::FailedPreconditionError(
                       "replication flow must confirm its initial cursor");
    }
    if (phase == FlowProtocolPhase::kOnline) {
      return kind == DataFrameKind::kCommand
                 ? absl::OkStatus()
                 : absl::FailedPreconditionError(
                       "full-sync frame arrived after ONLINE handoff");
    }
    return absl::FailedPreconditionError(
        "replication frame arrived before its protocol phase was ready");
  }

  bool ReadyForOnline(bool native_dataset_valid) const {
    std::lock_guard lock(fullsync_mutex_);
    if (!fullsync_mode_.has_value() || flow_phases_.empty() ||
        std::any_of(flow_phases_.begin(), flow_phases_.end(),
                    [](FlowProtocolPhase phase) {
                      return phase != FlowProtocolPhase::kOnline;
                    })) {
      return false;
    }
    if (*fullsync_mode_ ? !fullsync_cursors_installed_
                        : !native_dataset_valid) {
      return false;
    }
    return !cancelled_.load(std::memory_order_acquire) &&
           connected_flows_.load(std::memory_order_acquire) ==
               flow_phases_.size();
  }

  void RequireFailStop(std::string reason) {
    if (reason.empty()) return;
    std::lock_guard lock(failure_mutex_);
    if (fail_stop_reason_.empty()) fail_stop_reason_ = std::move(reason);
  }

  std::optional<std::string> FailStopReason() const {
    std::lock_guard lock(failure_mutex_);
    if (fail_stop_reason_.empty()) return std::nullopt;
    return fail_stop_reason_;
  }

  absl::Status ValidateFullSyncCutVectorLocked() const {
    if (!fullsync_mode_.value_or(false) || !fullsync_cursors_reset_ ||
        std::any_of(fullsync_cuts_.begin(), fullsync_cuts_.end(),
                    [](const std::optional<std::uint64_t>& cut) {
                      return !cut.has_value();
                    }) ||
        std::any_of(flow_phases_.begin(), flow_phases_.end(),
                    [](FlowProtocolPhase phase) {
                      return phase != FlowProtocolPhase::kFullCutRecorded;
                    })) {
      return absl::FailedPreconditionError(
          "full-sync cut vector is incomplete");
    }
    return absl::OkStatus();
  }

  void Cancel() {
    if (cancelled_.exchange(true, std::memory_order_acq_rel)) return;
    sockets_.Cancel();
    std::vector<std::shared_ptr<ReplicaControlArrival>> controls;
    {
      std::lock_guard lock(control_mutex_);
      controls.reserve(controls_.size());
      for (auto it = controls_.begin(); it != controls_.end();) {
        if (it->second->applying_) {
          ++it;
          continue;
        }
        controls.push_back(std::move(it->second));
        const auto discarded = it++;
        controls_.erase(discarded);
      }
    }
    const absl::Status cancelled =
        absl::CancelledError("replication session cancelled");
    for (const auto& arrival : controls) {
      arrival->completion_.Abort(cancelled);
    }
    if (flow_modes_selected_ != nullptr) flow_modes_selected_->Abort(cancelled);
    if (fullsync_begin_complete_ != nullptr)
      fullsync_begin_complete_->Abort(cancelled);
    if (fullsync_cut_ != nullptr) fullsync_cut_->Abort(cancelled);
    if (promotion_complete_ != nullptr) promotion_complete_->Abort(cancelled);
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
  // Upper bound on what the peer could have consumed. The sender advances it
  // only after a complete socket write; unlike the ACK cursor, it remains a
  // safe reconnect bound when the final ACK was lost with the connection.
  std::atomic<std::uint64_t> highest_sent_next_lsn_{0};
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
                std::string host, std::uint16_t port,
                std::string source_history_id, bool allow_continue,
                std::vector<std::uint64_t> applied,
                std::shared_ptr<const ClusterRebuildContext> population_export,
                std::optional<ClusterSteadyExport> steady_export = std::nullopt)
      : id_(id),
        flow_capability_(NewReplicationId()),
        node_id_(std::move(node_id)),
        host_(std::move(host)),
        port_(port),
        source_history_id_(std::move(source_history_id)),
        allow_continue_(allow_continue),
        applied_(std::move(applied)),
        population_export_(std::move(population_export)),
        steady_export_(std::move(steady_export)),
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

  bool SetFlow(unsigned flow_id, std::string_view capability, int fd) {
    std::lock_guard lock(mutex_);
    const bool capability_matches =
        capability.size() == flow_capability_.size() &&
        CRYPTO_memcmp(capability.data(), flow_capability_.data(),
                      flow_capability_.size()) == 0;
    if (!capability_matches || cancelled_.load(std::memory_order_acquire) ||
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

  bycorf::CoroutineBarrier::Awaiter WaitSnapshotReady() {
    return snapshot_ready_.Wait(*bycorf::ThisWorker().self_);
  }

  void MarkSnapshotScanComplete() {
    snapshot_scans_complete_.fetch_add(1, std::memory_order_acq_rel);
  }

  bool AllSnapshotScansComplete() const {
    return snapshot_scans_complete_.load(std::memory_order_acquire) ==
           flows_.size();
  }

  bycorf::CoroutineBarrier::Awaiter WaitSnapshotGateClosed() {
    return snapshot_gate_closed_.Wait(*bycorf::ThisWorker().self_);
  }

  bycorf::CoroutineBarrier::Awaiter WaitSnapshotFenced() {
    return snapshot_fenced_.Wait(*bycorf::ThisWorker().self_);
  }

  bycorf::CoroutineBarrier::Awaiter WaitSnapshotCaptureStopped() {
    return snapshot_capture_stopped_.Wait(*bycorf::ThisWorker().self_);
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

  void SetHighestSentNextLsn(unsigned flow_id, std::uint64_t next_lsn) {
    if (flow_id >= flows_.size() || next_lsn == 0) return;
    auto& highest = flows_[flow_id].highest_sent_next_lsn_;
    std::uint64_t observed = highest.load(std::memory_order_relaxed);
    while (observed < next_lsn &&
           !highest.compare_exchange_weak(observed, next_lsn,
                                          std::memory_order_release,
                                          std::memory_order_relaxed)) {
    }
  }

  std::vector<std::uint64_t> HighestSentNextLsns() const {
    std::vector<std::uint64_t> result;
    result.reserve(flows_.size());
    for (const ReplicaFlowProgress& flow : flows_) {
      result.push_back(
          flow.highest_sent_next_lsn_.load(std::memory_order_acquire));
    }
    return result;
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

  bool IncludesPopulationPartition(std::uint16_t partition_id) const noexcept {
    return population_export_ == nullptr ||
           population_export_->manifest_->logical_epochs()[partition_id] != 0;
  }

  std::uint64_t min_lsn() const noexcept {
    std::uint64_t result = std::numeric_limits<std::uint64_t>::max();
    for (const ReplicaFlowProgress& flow : flows_) {
      result = std::min(result, flow.lsn_.load(std::memory_order_acquire));
    }
    return result == std::numeric_limits<std::uint64_t>::max() ? 0 : result;
  }

  bool Acknowledged(const NativeReplicationWatermark& watermark) const {
    if (!online() || source_history_id_ != watermark.history_id_ ||
        watermark.next_lsns_.size() != flows_.size()) {
      return false;
    }
    for (std::size_t flow = 0; flow < flows_.size(); ++flow) {
      if (flows_[flow].lsn_.load(std::memory_order_acquire) <
          watermark.next_lsns_[flow]) {
        return false;
      }
    }
    return true;
  }

  void MarkOnline() noexcept {
    ever_online_.store(true, std::memory_order_release);
    online_.store(true, std::memory_order_release);
  }

  bool ever_online() const noexcept {
    return ever_online_.load(std::memory_order_acquire);
  }

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

  void MarkControlComplete() noexcept {
    control_active_.store(false, std::memory_order_release);
  }

  bool control_active() const noexcept {
    return control_active_.load(std::memory_order_acquire);
  }

  std::uint64_t id_ = 0;
  const std::string flow_capability_;
  const std::string node_id_;
  const std::string host_;
  const std::uint16_t port_ = 0;
  const std::string source_history_id_;
  const bool allow_continue_ = false;
  const std::vector<std::uint64_t> applied_;
  // Holding the authorized source population for the session makes the
  // transfer set immutable even if a later control-plane event revokes the
  // manager's authorization while cancellation is propagating to flow workers.
  const std::shared_ptr<const ClusterRebuildContext> population_export_;
  // Present only for an export admitted by the current steady FDS
  // relationship. Reconciliation can therefore revoke a removed member
  // without conflating it with a one-shot population rebuild export.
  const std::optional<ClusterSteadyExport> steady_export_;

 private:
  mutable std::mutex mutex_;
  int control_fd_ = -1;
  std::vector<int> flow_fds_;
  std::vector<ReplicaFlowProgress> flows_;
  std::vector<std::int8_t> flow_resume_possible_;
  std::size_t flow_modes_registered_ = 0;
  bool all_flows_resume_possible_ = true;
  bycorf::CoroutineBarrier snapshot_ready_;
  bycorf::CoroutineBarrier snapshot_gate_closed_;
  bycorf::CoroutineBarrier snapshot_fenced_;
  bycorf::CoroutineBarrier snapshot_capture_stopped_;
  std::atomic<unsigned> snapshot_scans_complete_{0};
  std::atomic<unsigned> connected_flows_{0};
  // Set only by the owning LVPSYNC coroutine after it has removed the session
  // from the registry. Revocation can therefore join a specific non-preserved
  // control instead of subtracting an unstable count of preserved controls.
  std::atomic<bool> control_active_{true};
  std::atomic<bool> ever_online_{false};
  std::atomic<bool> online_{false};
  std::atomic<bool> cancelled_{false};
};

struct DisconnectedReplicaLease {
  std::uint64_t session_id_ = 0;
  std::string history_id_;
  std::vector<std::uint64_t> highest_sent_next_lsns_;
};

struct RedisSource {
  ReplicaOfConfig upstream_;
  std::string node_id_;
  RedisSlotSet slots_;
  std::shared_ptr<ReplicaSession> session_;
  std::optional<std::string> replid_;
  std::atomic<std::uint64_t> offset_{0};
  std::uint64_t role_epoch_ = 0;
  // Owned by coordinator worker zero. A nonzero value means this
  // source installed its RDB for the active whole-group replacement.
  std::uint64_t full_sync_session_id_ = 0;
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

class ReplicationManager::ReplicationGroup {
 public:
  ReplicationGroup(storage::StorageEngine* storage,
                   std::optional<ReplicaOfConfig> initial_upstream,
                   const ReplicationOptions& options,
                   std::atomic<std::uint64_t>* serving_generation)
      : storage_(storage),
        serving_generation_(serving_generation),
        cluster_enabled_(options.cluster_enabled_),
        upstream_(cluster_enabled_ ? std::nullopt
                                   : std::move(initial_upstream)),
        upstream_caches_(
            std::make_unique<UpstreamSnapshot[]>(storage->worker_count())),
        applied_frontier_(std::make_shared<detail::ReplicaAppliedFrontier>(
            storage->worker_count(), storage->worker_count())),
        replica_priority_(options.replica_priority_),
        node_id_(options.node_id_override_.has_value()
                     ? *options.node_id_override_
                     : NewReplicationId()),
        group_id_(NewReplicationId()),
        boot_id_(NewReplicationId()),
        replica_incarnation_(NewReplicationId()),
        history_id_(NewReplicationId()),
        listen_port_(options.listen_port_),
        tls_context_(options.use_tls_ ? options.tls_context_ : nullptr),
        masteruser_(options.masteruser_),
        masterauth_(options.masterauth_),
        redis_psync_(cluster_enabled_ ? false : options.redis_psync_),
        redis_export_backpressure_(options.redis_export_backpressure_) {
    if (cluster_enabled_) {
      cluster_group_ =
          std::make_unique<lavik::ReplicationGroup>(node_id_, boot_id_);
    }
    const auto boot_bytes = std::span<const std::byte>(
        reinterpret_cast<const std::byte*>(boot_id_.data()), boot_id_.size());
    next_redis_full_sync_session_id_.store(storage::Crc64(boot_bytes),
                                           std::memory_order_relaxed);
    if (cluster_enabled_ && initial_upstream.has_value()) {
      // Server config rejects this combination, but ReplicationManager is also
      // a public embedding seam. Ignore the standalone source here so a direct
      // caller cannot bypass cluster replication policy.
      spdlog::warn("ignoring standalone initial upstream in cluster mode");
    }
    const std::size_t minimum_blocks = storage_->worker_count();
    const std::size_t configured_blocks =
        options.backlog_size_bytes_ / storage::kStorageBlockBytes;
    backlog_size_bytes_.store(std::max(minimum_blocks, configured_blocks) *
                                  storage::kStorageBlockBytes,
                              std::memory_order_relaxed);
    backlog_backpressure_.store(options.backlog_backpressure_,
                                std::memory_order_relaxed);
    publish_queue_bytes_per_worker_.store(
        options.publish_queue_bytes_per_worker_, std::memory_order_relaxed);
    snapshot_batch_size_.store(options.snapshot_batch_size_,
                               std::memory_order_relaxed);
    if (!upstream_.has_value()) {
      auto recovered_base = storage_->RecoverPromotionBase();
      auto recovered_catalog = storage_->RecoverFunctionCatalog();
      if (recovered_base.ok() && recovered_base->has_value() &&
          recovered_catalog.ok() && recovered_catalog->has_value() &&
          (**recovered_catalog).token_.catalog_generation_ >=
              (**recovered_base).catalog_token_.catalog_generation_) {
        // Promotion preserves the one group lineage across process boots. A
        // later catalog generation is valid because every catalog commit is
        // serialized through the same system-state writer and preserves the
        // base; an earlier generation can never authorize the recovered data.
        group_id_ = (**recovered_base).group_id_;
      }
    }
    if (upstream_.has_value() || cluster_enabled_) {
      StoreRole(ReplicationRole::kConnecting, std::memory_order_relaxed);
      role_epoch_.store(1, std::memory_order_relaxed);
    }
    if (upstream_.has_value()) {
      if (options.redis_psync_) {
        auto source = std::make_shared<RedisSource>();
        source->upstream_ = *upstream_;
        source->role_epoch_ = 1;
        redis_sources_.push_back(std::move(source));
      } else {
        initial_protocol_probe_pending_ = true;
      }
    }
    PublishUpstreamSnapshot();
  }

  void StorageReady(bycorf::Worker& worker) {
    ready_workers_.fetch_add(1, std::memory_order_acq_rel);
    if (worker.id() == 0 && !ready_waiter_started_) {
      ready_waiter_started_ = true;
      worker.Spawn(WaitUntilStorageReady());
    }
  }

  Task<absl::StatusOr<std::shared_ptr<detail::ClusterRebuildCompletionState>>>
  StartClusterRebuildDirective(ReplicaOfConfig upstream,
                               RebuildDirective directive,
                               PopulationManifest manifest) {
    if (bycorf::ThisWorker().id_ != 0) {
      co_return co_await bycorf::SubmitTaskTo(
          0, [this, upstream = std::move(upstream),
              directive = std::move(directive),
              manifest = std::move(manifest)]() mutable {
            return StartClusterRebuildDirective(
                std::move(upstream), std::move(directive), std::move(manifest));
          });
    }
    if (!cluster_enabled_ || cluster_group_ == nullptr) {
      co_return absl::FailedPreconditionError(
          "cluster rebuild directives require Meta-managed population mode");
    }
    if (upstream.host_.empty() || upstream.port_ == 0) {
      co_return absl::InvalidArgumentError(
          "cluster rebuild source endpoint is invalid");
    }
    if (directive.identity_.manifest_id_ != manifest.id()) {
      co_return absl::FailedPreconditionError(
          "rebuild directive does not match the supplied manifest");
    }
    if (cluster_control_stopping_) {
      co_return absl::CancelledError(
          "cluster rebuild admission stopped for process shutdown");
    }
    // An automatic session teardown already owns cancellation, proof
    // invalidation, and candidate abort. Let it finish before evaluating a
    // directive so two coroutines never retire the same in-place attempt.
    for (;;) {
      bool teardown_running = false;
      {
        AssertStateOwner();
        teardown_running = replica_session_teardown_running_;
      }
      if (!teardown_running) break;
      absl::Status waited = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) co_return waited;
    }
    if (cluster_control_stopping_) {
      co_return absl::CancelledError(
          "cluster rebuild admission stopped during prior teardown");
    }

    absl::Status validated =
        cluster_group_->ValidateRebuild(directive, manifest);

    std::shared_ptr<ClusterRebuildContext> previous_context;
    bool superseding = false;
    {
      AssertStateOwner();
      if (cluster_promotion_prepare_ != nullptr) {
        co_return absl::FailedPreconditionError(
            "a prepared promotion must be retired before another rebuild");
      }
      if (failed_stopped_.load(std::memory_order_relaxed)) {
        co_return absl::FailedPreconditionError(absl::StrCat(
            "replication is failed-stopped until restart: ", failure_reason_));
      }
      const bool same_directive = cluster_rebuild_ != nullptr &&
                                  cluster_rebuild_->directive_ == directive;
      if (same_directive) {
        if (replica_reconfiguration_running_) {
          co_return absl::FailedPreconditionError(
              "another cluster population transition is active");
        }
        const ReplicationGroupState population_state =
            cluster_rebuild_->state_.load(std::memory_order_relaxed);
        const bool retryable =
            population_state == ReplicationGroupState::kRebuilding ||
            (population_state == ReplicationGroupState::kReady &&
             cluster_rebuild_->ready_token_.has_value());
        // ReplicationGroup rejects BeginRebuild once this directive is READY.
        // The manager is the control replay seam, so the exact active or
        // completed directive returns its existing result without another
        // destructive rebuild.
        if (!retryable) {
          co_return absl::FailedPreconditionError(
              "cluster rebuild proof is invalidated; a fresh attempt "
              "identity is required");
        }
        if (upstream_ != std::optional<ReplicaOfConfig>(upstream)) {
          co_return absl::FailedPreconditionError(
              "cluster rebuild retry conflicts with its accepted source "
              "endpoint");
        }
        co_return cluster_rebuild_->completion_;
      }
      if (!validated.ok()) co_return validated;
      if (replica_reconfiguration_running_) {
        co_return absl::FailedPreconditionError(
            "another cluster population transition is active");
      }
      if (cluster_rebuild_ != nullptr) {
        previous_context = cluster_rebuild_;
        superseding = true;
      } else if (active_replica_session_ != nullptr || upstream_.has_value()) {
        co_return absl::FailedPreconditionError(
            "replication teardown must complete before a cluster rebuild");
      }
    }

    // The candidate is already fully validated, so closing admission cannot
    // turn a malformed or stale directive into a denial of service against a
    // healthy population. From here on, any uncertain teardown is fail-stop.
    StoreRole(ReplicationRole::kConnecting, std::memory_order_release);
    storage_->SetReplicaLoading(true);
    storage_->SetExpirationAuthority(false);

    std::shared_ptr<ReplicaSession> previous_session;
    {
      AssertStateOwner();
      if (replica_reconfiguration_running_ ||
          cluster_rebuild_ != previous_context ||
          failed_stopped_.load(std::memory_order_relaxed)) {
        co_return absl::AbortedError(
            "cluster rebuild state changed before directive transition");
      }
      replica_reconfiguration_running_ = true;
      if (superseding) {
        // Status and serving become invalid synchronously. The strong source
        // retirement below clears capabilities under master_mutex_ before it
        // joins old flow work, so LVPSYNC classification and publication see
        // one ordered transition rather than racing a worker-local clear.
        previous_context->ready_token_.reset();
        previous_context->state_.store(ReplicationGroupState::kNotReady,
                                       std::memory_order_release);
        previous_session = std::move(active_replica_session_);
        SetDesiredUpstream(std::nullopt);
        applied_frontier_.reset();
        upstream_node_id_.reset();
        upstream_history_id_.reset();
        replica_session_id_ = 0;
        source_worker_count_ = 0;
        role_epoch_.fetch_add(1, std::memory_order_acq_rel);
      }
    }
    // Cancellation is synchronous and must precede the first await below.
    // Otherwise the old control coroutine could publish ONLINE after serving
    // was closed but before its sockets were revoked.
    if (previous_session != nullptr) previous_session->Cancel();

    if (superseding) {
      // A cluster node owns one population. Revoke every downstream export,
      // then cancel/join its one upstream session and abort any candidate root
      // before replacing the attempt capability in ReplicationGroup.
      absl::Status revoked =
          co_await RevokeClusterRebuildSourceAuthorizations();
      if (!revoked.ok()) {
        const std::string reason =
            absl::StrCat("cluster source revocation outcome is uncertain: ",
                         revoked.message());
        (void)cluster_group_->FailStop(previous_context->directive_.identity_);
        LatchReplicationFailure(reason);
        co_return revoked;
      }
      if (previous_session != nullptr) {
        absl::Status stopped =
            co_await CancelAndWaitForReplicaFlows(previous_session);
        std::optional<std::string> fail_stop =
            previous_session->FailStopReason();
        if (!stopped.ok() && !fail_stop.has_value()) {
          fail_stop =
              absl::StrCat("replica cancellation/join outcome is uncertain: ",
                           stopped.message());
        }
        if (stopped.ok() && !fail_stop.has_value() &&
            previous_session->session_id_ != 0) {
          absl::Status discarded = co_await storage_->AbortReplicaRoot(
              previous_session->session_id_);
          if (!discarded.ok()) {
            fail_stop = absl::StrCat("replica abort outcome is uncertain: ",
                                     discarded.message());
          }
        }
        if (fail_stop.has_value()) {
          (void)cluster_group_->FailStop(
              previous_context->directive_.identity_);
          LatchReplicationFailure(*fail_stop);
          co_return absl::FailedPreconditionError(absl::StrCat(
              "replication is failed-stopped until restart: ", *fail_stop));
        }
      }

      absl::Status retired = cluster_group_->InvalidateProof(
          previous_context->directive_.identity_);
      if (!retired.ok()) {
        const std::string reason =
            absl::StrCat("superseded cluster proof could not be retired: ",
                         retired.message());
        (void)cluster_group_->FailStop(previous_context->directive_.identity_);
        LatchReplicationFailure(reason);
        co_return absl::FailedPreconditionError(reason);
      }

      // The old coordinator owns the control connection rather than a
      // separately joinable task. Cancellation closes that socket; wait until
      // it observes the moved session and exits before starting its successor.
      while (coordinator_started_) {
        absl::Status waited = co_await bycorf::SleepFor(
            *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
        if (!waited.ok()) {
          const std::string reason = absl::StrCat(
              "superseded replication coordinator could not be joined: ",
              waited.message());
          LatchReplicationFailure(reason);
          co_return waited;
        }
      }
      previous_context->completion_->Resolve(absl::CancelledError(
          "cluster rebuild attempt was superseded after cleanup"));
      if (cluster_control_stopping_) {
        AssertStateOwner();
        replica_reconfiguration_running_ = false;
        if (cluster_rebuild_ == previous_context) {
          cluster_rebuild_.reset();
          SetDesiredUpstream(std::nullopt);
          upstream_node_id_.reset();
          upstream_history_id_.reset();
        }
        co_return absl::CancelledError(
            "cluster rebuild supersession stopped for process shutdown");
      }
    }

    auto authorization = cluster_group_->BeginRebuild(directive, manifest);
    if (!authorization.ok()) {
      const std::string reason =
          absl::StrCat("validated cluster directive could not begin: ",
                       authorization.status().message());
      LatchReplicationFailure(reason);
      co_return absl::FailedPreconditionError(reason);
    }
    auto context = std::make_shared<ClusterRebuildContext>(
        std::move(directive), std::move(manifest), std::move(*authorization));
    bool installed = false;
    {
      AssertStateOwner();
      installed = replica_reconfiguration_running_ &&
                  cluster_rebuild_ == previous_context &&
                  active_replica_session_ == nullptr &&
                  !upstream_.has_value() &&
                  !failed_stopped_.load(std::memory_order_relaxed);
      if (installed) {
        cluster_rebuild_ = context;
        SetDesiredUpstream(std::move(upstream));
        applied_frontier_.reset();
        upstream_node_id_.reset();
        upstream_history_id_.reset();
        native_dataset_valid_.store(false, std::memory_order_release);
        role_epoch_.fetch_add(1, std::memory_order_acq_rel);
        replica_reconfiguration_running_ = false;
      }
    }
    if (!installed) {
      const std::string reason =
          "cluster rebuild state changed while installing the directive";
      (void)cluster_group_->FailStop(context->directive_.identity_);
      LatchReplicationFailure(reason);
      co_return absl::AbortedError(reason);
    }

    StartCoordinator();
    co_return context->completion_;
  }

  Task<absl::Status> ApplyClusterRebuildDirective(ReplicaOfConfig upstream,
                                                  RebuildDirective directive,
                                                  PopulationManifest manifest) {
    auto started = co_await StartClusterRebuildDirective(
        std::move(upstream), std::move(directive), std::move(manifest));
    if (!started.ok()) co_return started.status();
    co_return co_await (*started)->Await();
  }

  Task<absl::StatusOr<std::shared_ptr<detail::ClusterRebuildCompletionState>>>
  StartEmptyPopulationInitialization(RebuildIdentity identity,
                                     PopulationManifest manifest) {
    if (bycorf::ThisWorker().id_ != 0) {
      co_return co_await bycorf::SubmitTaskTo(
          0, [this, identity = std::move(identity),
              manifest = std::move(manifest)]() mutable {
            return StartEmptyPopulationInitialization(std::move(identity),
                                                      std::move(manifest));
          });
    }
    if (!cluster_enabled_ || cluster_group_ == nullptr) {
      co_return absl::FailedPreconditionError(
          "empty population initialization requires Meta-managed mode");
    }
    if (!StorageIsReady()) {
      co_return absl::UnavailableError(
          "storage is not ready for population initialization");
    }
    if (cluster_control_stopping_) {
      co_return absl::CancelledError(
          "population initialization stopped for process shutdown");
    }
    if (identity.manifest_id_ != manifest.id()) {
      co_return absl::FailedPreconditionError(
          "empty population directive does not match its manifest");
    }
    {
      co_await master_mutex_.Lock(*bycorf::ThisWorker().self_);
      bycorf::CrossWorkerMutex::Guard lock(&master_mutex_);
      if (identity.target_history_id_ != history_id_) {
        co_return absl::FailedPreconditionError(
            "empty population directive uses stale target history");
      }
    }
    absl::Status validated =
        cluster_group_->ValidateEmptyPopulation(identity, manifest);

    RebuildDirective directive{.identity_ = std::move(identity)};
    {
      AssertStateOwner();
      if (cluster_promotion_prepare_ != nullptr) {
        co_return absl::FailedPreconditionError(
            "a prepared promotion must be retired before population "
            "initialization");
      }
      if (failed_stopped_.load(std::memory_order_relaxed)) {
        co_return absl::FailedPreconditionError(absl::StrCat(
            "replication is failed-stopped until restart: ", failure_reason_));
      }
      if (cluster_rebuild_ != nullptr &&
          cluster_rebuild_->directive_ == directive) {
        if (replica_reconfiguration_running_) {
          co_return absl::FailedPreconditionError(
              "another cluster population transition is active");
        }
        const ReplicationGroupState state =
            cluster_rebuild_->state_.load(std::memory_order_relaxed);
        // ReplicationGroup rejects starting an already-READY directive again.
        // Exact replay instead shares the accepted attempt's completion, so
        // handle it before returning the new-initialization validation error.
        // Full identity/manifest equality and a still-valid proof are required;
        // replay must never perform another destructive reset.
        if (state == ReplicationGroupState::kRebuilding ||
            (state == ReplicationGroupState::kReady &&
             cluster_rebuild_->ready_token_.has_value())) {
          co_return cluster_rebuild_->completion_;
        }
        co_return absl::FailedPreconditionError(
            "empty population proof was invalidated; a fresh attempt is "
            "required");
      }
      if (!validated.ok()) co_return validated;
      if (replica_reconfiguration_running_ || cluster_rebuild_ != nullptr ||
          active_replica_session_ != nullptr || upstream_.has_value() ||
          coordinator_started_) {
        co_return absl::FailedPreconditionError(
            "another cluster population transition is active");
      }
      replica_reconfiguration_running_ = true;
    }

    StoreRole(ReplicationRole::kConnecting, std::memory_order_release);
    storage_->SetReplicaLoading(true);
    storage_->SetExpirationAuthority(false);
    native_dataset_valid_.store(false, std::memory_order_release);

    auto authorization =
        cluster_group_->BeginEmptyPopulation(directive.identity_, manifest);
    if (!authorization.ok()) {
      AssertStateOwner();
      replica_reconfiguration_running_ = false;
      co_return authorization.status();
    }
    auto context = std::make_shared<ClusterRebuildContext>(
        std::move(directive), std::move(manifest), std::move(*authorization));
    bool install_failed = false;
    {
      AssertStateOwner();
      if (!replica_reconfiguration_running_ || cluster_rebuild_ != nullptr ||
          failed_stopped_.load(std::memory_order_relaxed) ||
          cluster_control_stopping_) {
        replica_reconfiguration_running_ = false;
        install_failed = true;
      } else {
        cluster_rebuild_ = context;
        applied_frontier_.reset();
        upstream_node_id_.reset();
        upstream_history_id_.reset();
        replica_session_id_ = 0;
        source_worker_count_ = 0;
        role_epoch_.fetch_add(1, std::memory_order_acq_rel);
        replica_reconfiguration_running_ = false;
        coordinator_started_ = true;
      }
    }
    if (install_failed) {
      const std::string reason =
          "population state changed while installing empty initialization";
      (void)cluster_group_->FailStop(context->directive_.identity_);
      LatchReplicationFailure(reason);
      co_return absl::AbortedError(reason);
    }
    bycorf::ThisWorker().self_->Spawn(
        RunEmptyPopulationInitialization(context));
    co_return context->completion_;
  }

#if LAVIK_FAULTS_ENABLED
  // Fault builds can synthesize the already-proven candidate boundary so the
  // promotion kernel can be exercised without a second process implementing
  // the full destructive-rebuild protocol. The selector is attempt-scoped,
  // and this entire path is erased from ordinary release binaries.
  Task<absl::Status> SeedReadyPromotionCandidateForFaultTest(
      const ClusterPromotionPrepareDirective& directive) {
    {
      AssertStateOwner();
      if (cluster_rebuild_ != nullptr) co_return absl::OkStatus();
    }
    auto manifest = PopulationManifest::Create({});
    if (!manifest.ok()) co_return manifest.status();
    if (manifest->id() != directive.identity_.manifest_id_) {
      co_return absl::InvalidArgumentError(
          "fault-seeded promotion requires the empty population manifest");
    }

    constexpr std::uint64_t kFaultFullSyncSession = 0x50524f4d4f5445ULL;
    absl::Status storage_ready =
        co_await storage_->BeginReplicaFullSync(kFaultFullSyncSession);
    if (!storage_ready.ok()) co_return storage_ready;
    storage_ready =
        co_await GlobalFunctionCatalog().ReplaceFromLibraryCodes({});
    if (!storage_ready.ok()) co_return storage_ready;
    storage_ready = co_await storage_->CompleteReplicaFullSync(
        kFaultFullSyncSession,
        storage::PopulationToken{.generation_ = kFaultFullSyncSession,
                                 .digest_ = 1});
    if (!storage_ready.ok()) co_return storage_ready;

    if (directive.identity_.term_ <= 1) {
      co_return absl::InvalidArgumentError(
          "fault-seeded promotion requires a prior group term");
    }
    RebuildIdentity ready_identity = directive.identity_;
    // A normal failover begins a new fenced authority term without rebuilding
    // unchanged population content. Seed the proof under the preceding term so
    // this fixture exercises that production reconciliation boundary.
    --ready_identity.term_;
    RebuildDirective rebuild{
        .identity_ = std::move(ready_identity),
        .flow_count_ = static_cast<std::uint32_t>(
            directive.required_applied_next_lsns_.size()),
        .safe_source_active_ = true,
    };
    auto authorization = cluster_group_->BeginRebuild(rebuild, *manifest);
    if (!authorization.ok()) co_return authorization.status();
    auto population = std::make_shared<ClusterRebuildContext>(
        rebuild, *manifest, std::move(*authorization));
    for (std::uint32_t partition = 0; partition < kReplicationPartitionCount;
         ++partition) {
      const std::uint64_t target_epoch = partition + 1;
      absl::Status recorded = cluster_group_->RecordPartitionReset(
          rebuild.identity_, partition, target_epoch);
      if (recorded.ok()) {
        recorded = cluster_group_->RecordPartitionHandoff(
            rebuild.identity_, partition, 0, target_epoch);
      }
      if (!recorded.ok()) co_return recorded;
    }
    absl::Status proof =
        cluster_group_->MarkFunctionCatalogComplete(rebuild.identity_);
    if (proof.ok()) {
      proof = cluster_group_->RecordFlowCutVector(
          rebuild.identity_, directive.required_applied_next_lsns_);
    }
    if (proof.ok()) {
      proof = cluster_group_->MarkStoragePromoted(rebuild.identity_);
    }
    if (!proof.ok()) co_return proof;
    auto ready = cluster_group_->PublishReady(rebuild.identity_);
    if (!ready.ok()) co_return ready.status();

    auto frontier = std::make_shared<detail::ReplicaAppliedFrontier>(
        directive.required_applied_next_lsns_.size(), storage_->worker_count());
    absl::Status frontier_installed =
        frontier->InstallNextLsns(directive.required_applied_next_lsns_);
    if (!frontier_installed.ok()) co_return frontier_installed;
    population->ready_token_ = *ready;
    population->state_.store(ReplicationGroupState::kReady,
                             std::memory_order_release);
    population->completion_->Resolve(absl::OkStatus());
    {
      AssertStateOwner();
      if (cluster_rebuild_ != nullptr) {
        co_return absl::AbortedError(
            "cluster candidate changed during fault seeding");
      }
      cluster_rebuild_ = std::move(population);
      applied_frontier_ = std::move(frontier);
      upstream_node_id_ = directive.identity_.source_node_id_;
      upstream_history_id_ = directive.parent_history_id_;
      group_id_ = PopulationGroupToken(directive.identity_.group_id_);
      source_worker_count_ = directive.required_applied_next_lsns_.size();
      native_dataset_valid_.store(true, std::memory_order_release);
    }
    co_return absl::OkStatus();
  }
#endif

#if LAVIK_FAULTS_ENABLED
  Task<absl::StatusOr<bool>> WaitAtPromotionFaultBarrier(
      const std::shared_ptr<ClusterPromotionPrepareContext>& context,
      const char* signal_variable) {
    const char* signal_path = std::getenv(signal_variable);
    if (signal_path == nullptr || *signal_path == '\0') co_return false;
    absl::Status signalled =
        SignalFaultBarrier(signal_variable, "promotion fault barrier");
    if (!signalled.ok()) co_return signalled;
    for (;;) {
      AssertStateOwner();
      const bool exact_action_current =
          cluster_failover_action_ != nullptr &&
          cluster_failover_action_->prepare_directive_.has_value() &&
          *cluster_failover_action_->prepare_directive_ == context->directive_;
      if (!exact_action_current || context->cancellation_requested_) break;
      absl::Status waited = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) co_return waited;
    }
    co_return true;
  }
#endif

  Task<absl::Status> RunClusterPromotionPrepare(
      std::shared_ptr<ClusterPromotionPrepareContext> context,
      std::shared_ptr<ClusterRebuildContext> population,
      std::shared_ptr<detail::ReplicaAppliedFrontier> frontier,
      std::shared_ptr<ReplicaSession> session, bool native_population) {
    auto fail_stop = [&](absl::Status status, std::string_view boundary) {
      const std::string reason =
          absl::StrCat("cluster promotion-prepare ", boundary,
                       " outcome is uncertain: ", status.message());
      (void)cluster_group_->FailStop(population->directive_.identity_);
      LatchReplicationFailure(reason);
      const absl::Status terminal = absl::InternalError(reason);
      context->completion_->Resolve(terminal);
      return terminal;
    };
    const auto cancellation_requested = [&] {
      return context->cancellation_requested_ &&
             !context->durability_mutation_started_;
    };
    const auto finish_cancelled = [&] {
      const absl::Status cancelled = absl::CancelledError(
          "cluster promotion preparation was superseded before durability");
      AssertStateOwner();
      if (cluster_promotion_prepare_ == context &&
          cluster_rebuild_ == population) {
        replica_reconfiguration_running_ = false;
        // A self-origin Candidate was a fenced primary population before
        // preparation. Restore that boot-local shape so a replacement action
        // can enter the same promotion kernel; authority remains governed by
        // its committed grant, and expiration remains disabled.
        if (native_population && !cluster_control_stopping_ &&
            !failed_stopped_.load(std::memory_order_relaxed)) {
          StoreRole(ReplicationRole::kMaster, std::memory_order_release);
        } else if (!native_population && !cluster_control_stopping_ &&
                   !failed_stopped_.load(std::memory_order_relaxed)) {
          StoreRole(ReplicationRole::kConnecting, std::memory_order_release);
        }
        // Both native and replica Candidates retain their proven population
        // when preparation is cancelled before durability. The next
        // FollowOwner may therefore resume with CONTINUE, whose ordinary
        // command applies do not install a destructive FULL write context.
        if (!cluster_control_stopping_ &&
            !failed_stopped_.load(std::memory_order_relaxed) &&
            !storage_->ReplicaRecoveryFenced()) {
          storage_->SetReplicaLoading(false);
        }
      }
      context->completion_->Resolve(cancelled);
      return cancelled;
    };

#if LAVIK_FAULTS_ENABLED
    if (LAVIK_FAULT_MATCHES("LAVIK_REPLICATION_STALL_PROMOTION_ACTION",
                            context->directive_.identity_.attempt_id_)) {
      auto barrier = co_await WaitAtPromotionFaultBarrier(
          context,
          "LAVIK_REPLICATION_PROMOTION_PRE_DURABILITY_BARRIER_ACK_PATH");
      if (!barrier.ok()) co_return fail_stop(barrier.status(), "fault barrier");
      if (!*barrier) {
        auto remaining = std::chrono::milliseconds(200);
        while (remaining > std::chrono::milliseconds::zero() &&
               !cancellation_requested()) {
          constexpr auto kSlice = std::chrono::milliseconds(1);
          absl::Status stalled =
              co_await bycorf::SleepFor(*bycorf::ThisWorker().self_, kSlice);
          if (!stalled.ok()) co_return fail_stop(stalled, "fault stall");
          remaining -= kSlice;
        }
      }
    }
#endif

    absl::Status revoked = co_await RevokeClusterRebuildSourceAuthorizations();
    if (!revoked.ok()) co_return fail_stop(revoked, "source revocation");
    if (session != nullptr) {
      absl::Status stopped = co_await CancelAndWaitForReplicaFlows(session);
      if (!stopped.ok()) co_return fail_stop(stopped, "upstream join");
      if (std::optional<std::string> uncertain = session->FailStopReason();
          uncertain.has_value()) {
        co_return fail_stop(absl::InternalError(*uncertain), "upstream join");
      }
    }
    while (coordinator_started_) {
      absl::Status waited = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) co_return fail_stop(waited, "coordinator join");
    }
    // Admission already detached the old target session. Even a cancellation
    // that arrives before source revocation must join those flows before the
    // Ready population can be handed to another action.
    if (cancellation_requested()) co_return finish_cancelled();

    while (!CloseAllCommandDbGates()) {
      if (cancellation_requested()) co_return finish_cancelled();
      absl::Status waited = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) co_return fail_stop(waited, "command drain");
    }
    struct CommandGateGuard {
      ~CommandGateGuard() { OpenAllCommandDbGates(); }
    } command_gate;
    if (cancellation_requested()) co_return finish_cancelled();
    while (CommandDbOperationsActive()) {
      if (cancellation_requested()) co_return finish_cancelled();
      absl::Status waited = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) co_return fail_stop(waited, "command drain");
    }
    auto catalog_guard = co_await AcquireFunctionCatalogOperation();
    (void)catalog_guard;
    if (cancellation_requested()) co_return finish_cancelled();
    absl::Status quiesced = co_await storage_->QuiesceExpiration();
    if (!quiesced.ok()) co_return fail_stop(quiesced, "expiration quiesce");
    struct ExpirationResumeGuard {
      storage::StorageEngine* storage_;
      ~ExpirationResumeGuard() { storage_->ResumeExpiration(); }
    } expiration_resume{storage_};
    if (cancellation_requested()) co_return finish_cancelled();

    std::vector<std::uint64_t> frozen;
    if (native_population) {
      auto watermark = co_await CaptureNativeReplicationWatermark();
      if (!watermark.ok()) {
        co_return fail_stop(watermark.status(), "native frontier freeze");
      }
      if (!watermark->has_value() ||
          (*watermark)->history_id_ != context->directive_.parent_history_id_) {
        co_return fail_stop(
            absl::FailedPreconditionError(
                "native candidate history changed during preparation"),
            "native frontier freeze");
      }
      frozen = std::move((*watermark)->next_lsns_);
    } else {
      auto frozen_snapshot = frontier->TrySnapshot();
      if (!frozen_snapshot.ok()) {
        co_return fail_stop(frozen_snapshot.status(), "frontier freeze");
      }
      frozen = std::move(*frozen_snapshot);
    }
    if (cancellation_requested()) co_return finish_cancelled();
    if (frozen.size() !=
        context->directive_.required_applied_next_lsns_.size()) {
      co_return fail_stop(absl::FailedPreconditionError(
                              "joined candidate frontier changed flow layout"),
                          "frontier freeze");
    }
    for (std::size_t flow = 0; flow < frozen.size(); ++flow) {
      if (frozen[flow] <
          context->directive_.required_applied_next_lsns_[flow]) {
        co_return fail_stop(
            absl::FailedPreconditionError(
                "joined candidate frontier regressed below its requirement"),
            "frontier freeze");
      }
    }
    if (cancellation_requested()) co_return finish_cancelled();
    storage::PromotionBase promotion_base{
        .group_id_ = context->directive_.identity_.group_id_,
        .parent_history_id_ = context->directive_.parent_history_id_,
        .parent_frontier_ =
            {
                .history_context_ = context->directive_.parent_history_id_,
                .flow_cursors_ = std::move(frozen),
            },
        .storage_accumulator_ = absl::StrCat(
            "failover:", context->directive_.identity_.operation_id_, ":",
            context->directive_.identity_.directive_id_, ":",
            context->directive_.identity_.attempt_id_),
    };
    // This assignment and the first durability call are consecutive on the
    // owner worker. Once set, FDS supersession must wait for a known terminal
    // outcome and retire any child publisher; early cancellation would make
    // the durable PromotionBase/history outcome unknowable.
    context->durability_mutation_started_ = true;
#if LAVIK_FAULTS_ENABLED
    if (LAVIK_FAULT_MATCHES(
            "LAVIK_REPLICATION_STALL_PROMOTION_AFTER_DURABILITY_BOUNDARY",
            context->directive_.identity_.attempt_id_)) {
      auto barrier = co_await WaitAtPromotionFaultBarrier(
          context,
          "LAVIK_REPLICATION_PROMOTION_POST_DURABILITY_BARRIER_ACK_PATH");
      if (!barrier.ok()) {
        co_return fail_stop(barrier.status(), "durability fault barrier");
      }
      if (!*barrier) {
        absl::Status stalled = co_await bycorf::SleepFor(
            *bycorf::ThisWorker().self_, std::chrono::milliseconds(200));
        if (!stalled.ok()) {
          co_return fail_stop(stalled, "durability fault stall");
        }
      }
    }
#endif
    auto prepared = co_await PreparePromotion(std::move(promotion_base));
    if (!prepared.ok()) {
      co_return fail_stop(prepared.status(), "durability/history preparation");
    }
    bool context_changed = false;
    {
      AssertStateOwner();
      context_changed = cluster_promotion_prepare_ != context ||
                        cluster_rebuild_ != population;
      if (!context_changed) {
        applied_frontier_.reset();
        upstream_node_id_.reset();
        upstream_history_id_.reset();
        replica_reconfiguration_running_ = false;
      }
    }
    if (context_changed) {
      co_return fail_stop(
          absl::AbortedError(
              "promotion context changed before evidence publication"),
          "evidence publication");
    }
    if (ShouldInjectPromotionPrepareFailure("evidence-publication")) {
      co_return fail_stop(
          absl::InternalError(
              "injected promotion-prepare evidence publication failure"),
          "evidence publication");
    }
    recovered_population_fenced_ = false;
    native_dataset_valid_.store(true, std::memory_order_release);
    context->completion_->Resolve(*prepared);
    co_return absl::OkStatus();
  }

  Task<absl::StatusOr<
      std::shared_ptr<detail::ClusterPromotionPrepareCompletionState>>>
  StartClusterPromotionPrepareDirective(
      ClusterPromotionPrepareDirective directive) {
    if (bycorf::ThisWorker().id_ != 0) {
      co_return co_await bycorf::SubmitTaskTo(
          0, [this, directive = std::move(directive)]() mutable {
            return StartClusterPromotionPrepareDirective(std::move(directive));
          });
    }
    if (!cluster_enabled_ || cluster_group_ == nullptr) {
      co_return absl::FailedPreconditionError(
          "promotion prepare requires Meta-managed population mode");
    }
    const RebuildIdentity& identity = directive.identity_;
    if (identity.group_id_.empty() || identity.assignment_id_.empty() ||
        identity.term_ == 0 || identity.directive_revision_ == 0 ||
        identity.authority_id_.empty() || identity.source_node_id_.empty() ||
        identity.source_assignment_id_.empty() ||
        identity.source_boot_id_.empty() ||
        identity.source_history_id_.empty() ||
        identity.target_node_id_ != node_id_ ||
        identity.target_boot_id_ != boot_id_ ||
        identity.operation_id_.empty() || identity.directive_id_.empty() ||
        identity.attempt_id_.empty() || identity.manifest_revision_ == 0 ||
        identity.partition_replication_epoch_ == 0 ||
        directive.parent_history_id_.empty() ||
        directive.parent_history_id_ != identity.source_history_id_ ||
        directive.required_applied_next_lsns_.empty() ||
        std::any_of(directive.required_applied_next_lsns_.begin(),
                    directive.required_applied_next_lsns_.end(),
                    [](std::uint64_t cursor) { return cursor == 0; }) ||
        directive.excluded_group_term_ != identity.term_) {
      co_return absl::InvalidArgumentError(
          "cluster promotion-prepare identity is incomplete");
    }
    if (cluster_control_stopping_) {
      co_return absl::CancelledError(
          "promotion-prepare admission stopped for process shutdown");
    }
#if LAVIK_FAULTS_ENABLED
    if (LAVIK_FAULT_MATCHES("LAVIK_REPLICATION_SEED_READY_PROMOTION_CANDIDATE",
                            identity.attempt_id_)) {
      absl::Status seeded =
          co_await SeedReadyPromotionCandidateForFaultTest(directive);
      if (!seeded.ok()) co_return seeded;
    }
#endif

    std::shared_ptr<ClusterRebuildContext> population;
    std::shared_ptr<detail::ReplicaAppliedFrontier> frontier;
    std::shared_ptr<ReplicaSession> session;
    std::shared_ptr<ClusterPromotionPrepareContext> context;
    {
      AssertStateOwner();
      if (cluster_promotion_prepare_ != nullptr) {
        if (cluster_promotion_prepare_->directive_ == directive) {
          co_return cluster_promotion_prepare_->completion_;
        }
        co_return absl::FailedPreconditionError(
            "another promotion-prepare identity is retained for this boot");
      }
      if (failed_stopped_.load(std::memory_order_relaxed)) {
        co_return absl::FailedPreconditionError(absl::StrCat(
            "replication is failed-stopped until restart: ", failure_reason_));
      }
      if (replica_reconfiguration_running_) {
        co_return absl::FailedPreconditionError(
            "another cluster population transition is active");
      }
      population = cluster_rebuild_;
      if (population == nullptr ||
          population->state_.load(std::memory_order_acquire) !=
              ReplicationGroupState::kReady ||
          !population->ready_token_.has_value()) {
        co_return absl::FailedPreconditionError(
            "promotion-prepare candidate population is not ready");
      }
      const RebuildIdentity& ready = population->ready_token_->identity();
      if (ready.group_id_ != identity.group_id_ ||
          ready.assignment_id_ != identity.assignment_id_ ||
          !population->ready_token_->CanCarryForwardToTerm(identity.term_) ||
          ready.source_node_id_ != identity.source_node_id_ ||
          ready.source_assignment_id_ != identity.source_assignment_id_ ||
          ready.source_boot_id_ != identity.source_boot_id_ ||
          ready.source_history_id_ != identity.source_history_id_ ||
          ready.target_node_id_ != identity.target_node_id_ ||
          ready.target_boot_id_ != identity.target_boot_id_ ||
          ready.manifest_revision_ != identity.manifest_revision_ ||
          ready.manifest_id_ != identity.manifest_id_ ||
          ready.partition_replication_epoch_ !=
              identity.partition_replication_epoch_ ||
          upstream_history_id_ !=
              std::optional<std::string>(directive.parent_history_id_) ||
          applied_frontier_ == nullptr ||
          population->ready_token_->cut_vector().size() !=
              directive.required_applied_next_lsns_.size() ||
          applied_frontier_->size() !=
              directive.required_applied_next_lsns_.size()) {
        co_return absl::FailedPreconditionError(
            "promotion-prepare does not match the ready candidate anchors");
      }
      auto current = applied_frontier_->TrySnapshot();
      if (!current.ok()) co_return current.status();
      for (std::size_t flow = 0; flow < current->size(); ++flow) {
        if ((*current)[flow] < directive.required_applied_next_lsns_[flow]) {
          co_return absl::FailedPreconditionError(
              "promotion candidate has not reached the required frontier");
        }
      }

      context = std::make_shared<ClusterPromotionPrepareContext>(
          std::move(directive));
      cluster_promotion_prepare_ = context;
      replica_reconfiguration_running_ = true;
      session = std::move(active_replica_session_);
      frontier = applied_frontier_;
      upstream_.reset();
      replica_session_id_ = 0;
      source_worker_count_ = 0;
      role_epoch_.fetch_add(1, std::memory_order_acq_rel);
    }

    StoreRole(ReplicationRole::kSyncing, std::memory_order_release);
    storage_->SetReplicaLoading(true);
    storage_->SetExpirationAuthority(false);
    if (session != nullptr) session->Cancel();
    bycorf::ThisWorker().self_->Spawn(RunClusterPromotionPrepare(
        context, population, std::move(frontier), std::move(session),
        /*native_population=*/false));
    co_return context->completion_;
  }

  Task<absl::StatusOr<
      std::shared_ptr<detail::ClusterPromotionPrepareCompletionState>>>
  StartNativeClusterFailoverPromotionPrepare(
      ClusterPromotionPrepareDirective directive) {
    AssertStateOwner();
    if (cluster_promotion_prepare_ != nullptr) {
      if (cluster_promotion_prepare_->directive_ == directive) {
        co_return cluster_promotion_prepare_->completion_;
      }
      co_return absl::FailedPreconditionError(
          "another promotion-prepare identity is retained for this boot");
    }
    if (failed_stopped_.load(std::memory_order_relaxed)) {
      co_return absl::FailedPreconditionError(absl::StrCat(
          "replication is failed-stopped until restart: ", failure_reason_));
    }
    if (replica_reconfiguration_running_ ||
        active_replica_session_ != nullptr || upstream_.has_value() ||
        role_.load(std::memory_order_acquire) != ReplicationRole::kMaster) {
      co_return absl::FailedPreconditionError(
          "native failover candidate is not a stable primary population");
    }
    std::shared_ptr<ClusterRebuildContext> population = cluster_rebuild_;
    if (population == nullptr ||
        population->state_.load(std::memory_order_acquire) !=
            ReplicationGroupState::kReady ||
        !population->ready_token_.has_value()) {
      co_return absl::FailedPreconditionError(
          "native failover candidate population is not ready");
    }
    const RebuildIdentity& ready = population->ready_token_->identity();
    const RebuildIdentity& requested = directive.identity_;
    if (ready.group_id_ != requested.group_id_ ||
        ready.assignment_id_ != requested.assignment_id_ ||
        !population->ready_token_->CanCarryForwardToTerm(requested.term_) ||
        ready.target_node_id_ != requested.target_node_id_ ||
        ready.target_boot_id_ != requested.target_boot_id_ ||
        ready.manifest_revision_ != requested.manifest_revision_ ||
        ready.manifest_id_ != requested.manifest_id_ ||
        ready.partition_replication_epoch_ !=
            requested.partition_replication_epoch_ ||
        requested.source_node_id_ != node_id_ ||
        requested.source_assignment_id_ != ready.assignment_id_ ||
        requested.source_boot_id_ != boot_id_) {
      co_return absl::FailedPreconditionError(
          "native failover action does not match the ready population");
    }

    auto context =
        std::make_shared<ClusterPromotionPrepareContext>(std::move(directive));
    cluster_promotion_prepare_ = context;
    replica_reconfiguration_running_ = true;
    role_epoch_.fetch_add(1, std::memory_order_acq_rel);
    StoreRole(ReplicationRole::kSyncing, std::memory_order_release);
    storage_->SetReplicaLoading(true);
    storage_->SetExpirationAuthority(false);
    bycorf::ThisWorker().self_->Spawn(RunClusterPromotionPrepare(
        context, std::move(population), nullptr, nullptr,
        /*native_population=*/true));
    co_return context->completion_;
  }

  absl::Status ValidateClusterSourcePause(
      const DesiredClusterSourcePause& desired) const {
    if (IsZeroBytes(desired.transition_id_) ||
        desired.transition_revision_ == 0 || desired.group_id_.empty() ||
        desired.source_node_id_ != node_id_ ||
        desired.source_assignment_id_.empty() ||
        desired.source_boot_id_ != boot_id_ ||
        desired.source_history_id_.empty() || desired.source_group_term_ == 0 ||
        desired.flow_count_ == 0 ||
        desired.flow_count_ > cluster::control::kMaxCandidateFlows ||
        desired.manifest_revision_ == 0 ||
        IsZeroBytes(desired.manifest_id_.bytes_) ||
        desired.partition_replication_epoch_ == 0) {
      return absl::InvalidArgumentError(
          "cluster source pause identity is incomplete");
    }
    return absl::OkStatus();
  }

  bool SourcePausePopulationMatches(
      const DesiredClusterSourcePause& desired) const {
    AssertStateOwner();
    if (failed_stopped_.load(std::memory_order_relaxed) ||
        replica_reconfiguration_running_ || cluster_rebuild_ == nullptr ||
        cluster_rebuild_->state_.load(std::memory_order_acquire) !=
            ReplicationGroupState::kReady ||
        !cluster_rebuild_->ready_token_.has_value() ||
        role_.load(std::memory_order_acquire) != ReplicationRole::kMaster ||
        upstream_.has_value() ||
        !native_dataset_valid_.load(std::memory_order_acquire)) {
      return false;
    }
    const ReadyToken& ready = *cluster_rebuild_->ready_token_;
    const RebuildIdentity& identity = ready.identity();
    std::uint64_t current_group_term = identity.term_;
    if (activated_failover_activation_.has_value() &&
        activated_failover_activation_->group_id_ == identity.group_id_ &&
        activated_failover_activation_->candidate_assignment_id_ ==
            identity.assignment_id_ &&
        activated_failover_activation_->manifest_revision_ ==
            identity.manifest_revision_ &&
        activated_failover_activation_->manifest_id_ == identity.manifest_id_ &&
        activated_failover_activation_->partition_replication_epoch_ ==
            identity.partition_replication_epoch_) {
      current_group_term = activated_failover_activation_->target_term_;
    }
    return identity.group_id_ == desired.group_id_ &&
           identity.assignment_id_ == desired.source_assignment_id_ &&
           identity.target_node_id_ == desired.source_node_id_ &&
           identity.target_boot_id_ == desired.source_boot_id_ &&
           desired.source_group_term_ == current_group_term &&
           identity.manifest_revision_ == desired.manifest_revision_ &&
           identity.manifest_id_ == desired.manifest_id_ &&
           identity.partition_replication_epoch_ ==
               desired.partition_replication_epoch_ &&
           desired.flow_count_ == storage_->worker_count();
  }

  Task<absl::Status> CaptureClusterSourcePause(
      const std::shared_ptr<ClusterSourcePauseContext>& context) {
    AssertStateOwner();
    if (cluster_source_pause_ != context) {
      co_return absl::AbortedError("cluster source pause was replaced");
    }
    context->stable_next_lsns_.reset();
    context->failure_detail_.clear();
    if (!SourcePausePopulationMatches(context->desired_)) {
      const absl::Status mismatch = absl::FailedPreconditionError(
          "cluster source pause does not match the ready owner population");
      context->failure_detail_ = mismatch.ToString();
      co_return mismatch;
    }

    auto watermark = co_await CaptureNativeReplicationWatermark();
    if (cluster_source_pause_ != context) {
      co_return absl::AbortedError("cluster source pause was replaced");
    }
    if (!watermark.ok()) {
      context->failure_detail_ = watermark.status().ToString();
      co_return watermark.status();
    }
    if (!watermark->has_value()) {
      const absl::Status unavailable = absl::UnavailableError(
          "native source frontier is not currently available");
      context->failure_detail_ = unavailable.ToString();
      co_return unavailable;
    }
    if ((*watermark)->history_id_ != context->desired_.source_history_id_ ||
        (*watermark)->next_lsns_.size() != context->desired_.flow_count_) {
      const absl::Status mismatch = absl::FailedPreconditionError(
          "captured native source frontier changed compatibility domain");
      context->failure_detail_ = mismatch.ToString();
      co_return mismatch;
    }
    context->stable_next_lsns_ = std::move((*watermark)->next_lsns_);
    co_return absl::OkStatus();
  }

  Task<absl::Status> ReconcileClusterSourcePause(
      std::optional<DesiredClusterSourcePause> desired) {
    if (bycorf::ThisWorker().id_ != 0) {
      co_return co_await bycorf::SubmitTaskTo(
          0, [this, desired = std::move(desired)]() mutable {
            return ReconcileClusterSourcePause(std::move(desired));
          });
    }
    if (!cluster_enabled_ || cluster_group_ == nullptr) {
      co_return absl::FailedPreconditionError(
          "source pause reconciliation requires Meta-managed population mode");
    }
    if (cluster_control_stopping_) {
      co_return absl::CancelledError(
          "source pause reconciliation stopped for process shutdown");
    }
    AssertStateOwner();

    if (!desired.has_value()) {
      const std::shared_ptr<ClusterSourcePauseContext> previous =
          std::move(cluster_source_pause_);
      if (previous != nullptr && previous->expiration_pause_held_) {
        previous->expiration_pause_held_ = false;
        storage_->ResumeExpiration();
      }
      co_return absl::OkStatus();
    }
    if (absl::Status valid = ValidateClusterSourcePause(*desired);
        !valid.ok()) {
      co_return valid;
    }
    if (!SourcePausePopulationMatches(*desired)) {
      co_return absl::FailedPreconditionError(
          "cluster source pause does not match the ready owner population");
    }
    if (cluster_source_pause_ != nullptr &&
        cluster_source_pause_->desired_ == *desired &&
        cluster_source_pause_->stable_next_lsns_.has_value()) {
      co_return absl::OkStatus();
    }

    auto next =
        std::make_shared<ClusterSourcePauseContext>(std::move(*desired));
    if (cluster_source_pause_ != nullptr &&
        cluster_source_pause_->expiration_pause_held_) {
      next->expiration_pause_held_ = true;
      cluster_source_pause_->expiration_pause_held_ = false;
    }
    cluster_source_pause_ = next;
    if (!next->expiration_pause_held_) {
      absl::Status quiesced = co_await storage_->QuiesceExpiration();
      if (!quiesced.ok()) {
        if (cluster_source_pause_ == next) {
          next->failure_detail_ = quiesced.ToString();
        }
        co_return quiesced;
      }
      if (cluster_source_pause_ != next) {
        storage_->ResumeExpiration();
        co_return absl::AbortedError("cluster source pause was replaced");
      }
      next->expiration_pause_held_ = true;
    }
    co_return co_await CaptureClusterSourcePause(next);
  }

  Task<ClusterSourcePauseStatus> cluster_source_pause_status() const {
    if (bycorf::ThisWorker().id_ != 0) {
      co_return co_await bycorf::SubmitTaskTo(
          0, [this] { return cluster_source_pause_status(); });
    }
    AssertStateOwner();
    ClusterSourcePauseStatus result;
    const std::shared_ptr<ClusterSourcePauseContext> context =
        cluster_source_pause_;
    if (context == nullptr) co_return result;
    result.desired_ = context->desired_;
    result.failure_detail_ = context->failure_detail_;
    if (!context->stable_next_lsns_.has_value() ||
        !SourcePausePopulationMatches(context->desired_)) {
      co_return result;
    }
    const ReplicationIdentity current = co_await identity();
    if (cluster_source_pause_ != context ||
        current.local_node_id_ != context->desired_.source_node_id_ ||
        current.boot_id_ != context->desired_.source_boot_id_ ||
        current.local_history_id_ != context->desired_.source_history_id_) {
      if (cluster_source_pause_ == context) {
        context->stable_next_lsns_.reset();
        context->failure_detail_ =
            "native source identity changed after pause capture";
        result.failure_detail_ = context->failure_detail_;
      }
      co_return result;
    }
    result.stable_next_lsns_ = context->stable_next_lsns_;
    co_return result;
  }

  bool FailoverPopulationMatches(
      const DesiredClusterFailoverAction& desired) const {
    AssertStateOwner();
    if (cluster_rebuild_ == nullptr ||
        cluster_rebuild_->state_.load(std::memory_order_acquire) !=
            ReplicationGroupState::kReady ||
        !cluster_rebuild_->ready_token_.has_value()) {
      return false;
    }
    const RebuildIdentity& ready = cluster_rebuild_->ready_token_->identity();
    return ready.group_id_ == desired.group_id_ &&
           ready.assignment_id_ == desired.candidate_assignment_id_ &&
           ready.target_node_id_ == desired.candidate_node_id_ &&
           ready.target_boot_id_ == desired.candidate_boot_id_ &&
           ready.manifest_revision_ == desired.manifest_revision_ &&
           ready.manifest_id_ == desired.manifest_id_ &&
           ready.partition_replication_epoch_ ==
               desired.partition_replication_epoch_;
  }

  bool FailoverReplicaDomainMatches(
      const DesiredClusterFailoverAction& desired) const {
    AssertStateOwner();
    if (!FailoverPopulationMatches(desired)) return false;
    if (desired.operator_recovery_)
      return operator_recovery_active_ && applied_frontier_ != nullptr;
    const ReadyToken& ready = *cluster_rebuild_->ready_token_;
    const RebuildIdentity& identity = ready.identity();
    return identity.term_ == desired.domain_.source_group_term_ &&
           identity.source_node_id_ == desired.domain_.source_node_id_ &&
           identity.source_assignment_id_ ==
               desired.domain_.source_assignment_id_ &&
           identity.source_boot_id_ == desired.domain_.source_boot_id_ &&
           identity.source_history_id_ == desired.domain_.source_history_id_ &&
           ready.cut_vector().size() == desired.domain_.flow_count_ &&
           applied_frontier_ != nullptr &&
           applied_frontier_->size() == desired.domain_.flow_count_;
  }

  bool IsNativeSelfOriginDomain(
      const DesiredClusterFailoverAction& desired) const {
    return desired.mode_ == ClusterFailoverMode::kUncontrolled &&
           desired.domain_.source_node_id_ == node_id_ &&
           desired.domain_.source_assignment_id_ ==
               desired.candidate_assignment_id_ &&
           desired.domain_.source_boot_id_ == boot_id_ &&
           desired.domain_.source_group_term_ !=
               std::numeric_limits<std::uint64_t>::max() &&
           desired.domain_.source_group_term_ + 1 == desired.target_term_ &&
           desired.domain_.flow_count_ == storage_->worker_count();
  }

  bool ReadyPopulationIsSuppressed(
      const DesiredClusterFailoverAction& failed) const {
    AssertStateOwner();
    if (!FailoverPopulationMatches(failed)) return false;
    if (failed.operator_recovery_) return operator_recovery_active_;
    const ReadyToken& ready = *cluster_rebuild_->ready_token_;
    const RebuildIdentity& identity = ready.identity();
    const bool replica_domain =
        identity.term_ == failed.domain_.source_group_term_ &&
        identity.source_node_id_ == failed.domain_.source_node_id_ &&
        identity.source_assignment_id_ ==
            failed.domain_.source_assignment_id_ &&
        identity.source_boot_id_ == failed.domain_.source_boot_id_ &&
        identity.source_history_id_ == failed.domain_.source_history_id_ &&
        ready.cut_vector().size() == failed.domain_.flow_count_;
    const bool self_origin_domain = IsNativeSelfOriginDomain(failed);
    return replica_domain || self_origin_domain;
  }

  void PublishFailoverActionFailure(
      const std::shared_ptr<ClusterFailoverActionContext>& context,
      std::string failure_class, std::string failure_detail) {
    AssertStateOwner();
    if (context->failure_published_ || context->cancelled_ ||
        cluster_failover_action_ != context) {
      return;
    }
    context->state_ = ClusterFailoverActionState::kFailed;
    context->failure_class_ = std::move(failure_class);
    context->failure_detail_ = std::move(failure_detail);
    context->failure_published_ = true;
    failed_failover_candidate_ = context->desired_;
  }

  std::chrono::milliseconds FailoverActionWatchdog() const {
#if LAVIK_FAULTS_ENABLED
    if (const char* configured =
            std::getenv("LAVIK_REPLICATION_ACTION_WATCHDOG_MS");
        configured != nullptr) {
      std::uint64_t parsed = 0;
      const std::string_view text(configured);
      const auto [end, error] =
          std::from_chars(text.data(), text.data() + text.size(), parsed);
      if (error == std::errc{} && end == text.data() + text.size() &&
          parsed > 0 && parsed <= 60'000) {
        return std::chrono::milliseconds(parsed);
      }
    }
#endif
    return std::chrono::seconds(30);
  }

  void RequestFailoverPromotionCancellation(
      const std::shared_ptr<ClusterFailoverActionContext>& action) {
    AssertStateOwner();
    if (!action->prepare_directive_.has_value() ||
        cluster_promotion_prepare_ == nullptr ||
        cluster_promotion_prepare_->directive_ != *action->prepare_directive_ ||
        cluster_promotion_prepare_->durability_mutation_started_) {
      return;
    }
    cluster_promotion_prepare_->cancellation_requested_ = true;
  }

  Task<absl::Status> WaitForFailoverActionRetry(
      const std::shared_ptr<ClusterFailoverActionContext>& context,
      std::chrono::steady_clock::time_point watchdog_deadline) {
    constexpr auto kRetry = std::chrono::milliseconds(1000);
    constexpr auto kSlice = std::chrono::milliseconds(10);
    auto remaining = kRetry;
    while (remaining > std::chrono::milliseconds::zero()) {
      if (context->cancelled_ || cluster_control_stopping_) {
        co_return absl::CancelledError(
            "cluster failover action was replaced or stopped");
      }
      if (std::chrono::steady_clock::now() >= watchdog_deadline) {
        PublishFailoverActionFailure(
            context, "watchdog",
            "promotion preparation exceeded the boot-local watchdog");
        co_return absl::DeadlineExceededError(context->failure_detail_);
      }
      const auto wait = std::min(remaining, kSlice);
      absl::Status waited =
          co_await bycorf::SleepFor(*bycorf::ThisWorker().self_, wait);
      if (!waited.ok()) co_return waited;
      remaining -= wait;
    }
    co_return absl::OkStatus();
  }

  Task<absl::Status> RunClusterFailoverAction(
      std::shared_ptr<ClusterFailoverActionContext> context) {
    AssertStateOwner();
    const auto finish = [&] { context->runner_finished_ = true; };
    // RunClusterFailoverAction is spawned only after an exact authorized FDS
    // has been installed for this local node/assignment/boot. That is the
    // action's first local execution boundary: time spent offline or awaiting
    // authorization is excluded, while population loss and all later
    // transient retries remain bounded by this one non-refreshable deadline.
    const auto watchdog_deadline =
        std::chrono::steady_clock::now() + FailoverActionWatchdog();
    const auto watchdog_expired = [&] {
      if (std::chrono::steady_clock::now() < watchdog_deadline) {
        return false;
      }
      PublishFailoverActionFailure(
          context, "watchdog",
          "promotion preparation exceeded the boot-local watchdog");
      return true;
    };
    if (context->desired_.operator_recovery_ && !operator_recovery_active_) {
      const auto& desired = context->desired_;
      if (!recovered_population_.has_value() || cluster_rebuild_ != nullptr ||
          storage_->ReplicaRecoveryFenced() ||
          failed_stopped_.load(std::memory_order_acquire)) {
        PublishFailoverActionFailure(context, "operator-recovery",
                                     "no complete recovered local population");
        finish();
        co_return absl::FailedPreconditionError(context->failure_detail_);
      }
      detail::RecoveredPopulation base = *recovered_population_;
      auto& scope = base.identity_;
      if (scope.group_id_ != desired.group_id_ ||
          scope.assignment_id_ != desired.candidate_assignment_id_ ||
          scope.target_node_id_ != desired.candidate_node_id_ ||
          scope.manifest_revision_ != desired.manifest_revision_ ||
          scope.manifest_id_ != desired.manifest_id_ ||
          scope.partition_replication_epoch_ !=
              desired.partition_replication_epoch_) {
        PublishFailoverActionFailure(context, "operator-recovery",
                                     "recovered population scope changed");
        finish();
        co_return absl::FailedPreconditionError(context->failure_detail_);
      }
      // This is a NEW base over locally recovered data, not a reconstruction
      // of the lost source cursor. Meta records no comparable source domain
      // and authorizes this path only with an explicit loss=unknown action.
      scope.term_ = desired.target_term_;
      scope.source_node_id_ = node_id_;
      scope.source_assignment_id_ = desired.candidate_assignment_id_;
      scope.source_boot_id_ = boot_id_;
      scope.source_history_id_ = NewReplicationId();
      base.frontier_.assign(storage_->worker_count(), 1);
      absl::Status installed = InstallRecoveredPopulation(std::move(base));
      if (!installed.ok()) {
        PublishFailoverActionFailure(context, "operator-recovery",
                                     installed.ToString());
        finish();
        co_return installed;
      }
      operator_recovery_active_ = true;
    }
    if (cluster_rebuild_ != nullptr &&
        !cluster_rebuild_->manifest_.has_value()) {
      auto manifest =
          PopulationManifest::Create(context->desired_.manifest_entries_);
      if (!manifest.ok() || manifest->id() != context->desired_.manifest_id_) {
        PublishFailoverActionFailure(
            context, "recovery-manifest",
            "FDS manifest does not match recovered population");
        finish();
        co_return absl::FailedPreconditionError(context->failure_detail_);
      }
      cluster_rebuild_->manifest_ = std::move(*manifest);
    }
    std::vector<std::uint64_t> minimum_frontier(
        context->desired_.domain_.flow_count_, 1);
#if LAVIK_FAULTS_ENABLED
    // The fault fixture seeds a complete Ready population before this action
    // enters production validation. It is compiled out of ordinary binaries
    // and still passes through every exact domain check below.
    ClusterPromotionPrepareDirective seed =
        BuildFailoverPrepareDirective(context->desired_, minimum_frontier);
    if (LAVIK_FAULT_MATCHES("LAVIK_REPLICATION_SEED_READY_PROMOTION_CANDIDATE",
                            seed.identity_.attempt_id_)) {
      absl::Status seeded =
          co_await SeedReadyPromotionCandidateForFaultTest(seed);
      if (!seeded.ok()) {
        PublishFailoverActionFailure(context, "population-seed",
                                     seeded.ToString());
        finish();
        co_return seeded;
      }
    }
#endif

    for (;;) {
      if (context->cancelled_ || cluster_control_stopping_) {
        finish();
        co_return absl::CancelledError(
            "cluster failover action was replaced or stopped");
      }
      if (watchdog_expired()) {
        finish();
        co_return absl::DeadlineExceededError(context->failure_detail_);
      }
      if (!FailoverPopulationMatches(context->desired_)) {
        context->state_ = ClusterFailoverActionState::kWaitingForPopulation;
        absl::Status waited =
            co_await WaitForFailoverActionRetry(context, watchdog_deadline);
        if (!waited.ok()) {
          finish();
          co_return waited;
        }
        continue;
      }
      const bool native_population =
          !FailoverReplicaDomainMatches(context->desired_) &&
          IsNativeSelfOriginDomain(context->desired_);
      std::vector<std::uint64_t> current_frontier;
      if (native_population) {
        auto watermark = co_await CaptureNativeReplicationWatermark();
        if (!watermark.ok()) {
          context->state_ = ClusterFailoverActionState::kRetrying;
          absl::Status waited =
              co_await WaitForFailoverActionRetry(context, watchdog_deadline);
          if (!waited.ok()) {
            finish();
            co_return waited;
          }
          continue;
        }
        if (!watermark->has_value()) {
          context->state_ = ClusterFailoverActionState::kWaitingForPopulation;
          absl::Status waited =
              co_await WaitForFailoverActionRetry(context, watchdog_deadline);
          if (!waited.ok()) {
            finish();
            co_return waited;
          }
          continue;
        }
        if ((*watermark)->history_id_ !=
                context->desired_.domain_.source_history_id_ ||
            (*watermark)->next_lsns_.size() !=
                context->desired_.domain_.flow_count_) {
          PublishFailoverActionFailure(
              context, "population-domain",
              "native population no longer matches its self-origin domain");
          finish();
          co_return absl::FailedPreconditionError(context->failure_detail_);
        }
        current_frontier = std::move((*watermark)->next_lsns_);
      } else {
        if (!FailoverReplicaDomainMatches(context->desired_)) {
          PublishFailoverActionFailure(
              context, "population-domain",
              "ready population does not match the committed compatibility "
              "domain");
          finish();
          co_return absl::FailedPreconditionError(context->failure_detail_);
        }
        auto current = applied_frontier_->TrySnapshot();
        if (!current.ok()) {
          context->state_ = ClusterFailoverActionState::kRetrying;
          absl::Status waited =
              co_await WaitForFailoverActionRetry(context, watchdog_deadline);
          if (!waited.ok()) {
            finish();
            co_return waited;
          }
          continue;
        }
        current_frontier = std::move(*current);
      }
      ClusterPromotionPrepareDirective directive =
          BuildFailoverPrepareDirective(context->desired_,
                                        std::move(current_frontier));
      if (context->desired_.operator_recovery_) {
        const auto& base = cluster_rebuild_->ready_token_->identity();
        directive.identity_.source_node_id_ = base.source_node_id_;
        directive.identity_.source_assignment_id_ = base.source_assignment_id_;
        directive.identity_.source_boot_id_ = base.source_boot_id_;
        directive.identity_.source_history_id_ = base.source_history_id_;
        directive.parent_history_id_ = base.source_history_id_;
      }
      context->prepare_directive_ = directive;
      context->state_ = ClusterFailoverActionState::kPreparing;
      absl::StatusOr<
          std::shared_ptr<detail::ClusterPromotionPrepareCompletionState>>
          started{absl::UnknownError("failover promotion was not dispatched")};
#if LAVIK_FAULTS_ENABLED
      if (LAVIK_FAULT_MATCHES(
              "LAVIK_REPLICATION_RETRY_FAILOVER_PROMOTION_ADMISSION",
              directive.identity_.attempt_id_)) {
        started = absl::UnavailableError(
            "injected retryable failover promotion admission failure");
      } else
#endif
          if (native_population) {
        started = co_await StartNativeClusterFailoverPromotionPrepare(
            std::move(directive));
      } else {
        started = co_await StartClusterPromotionPrepareDirective(
            std::move(directive));
      }
      if (!started.ok()) {
        const bool transient =
            absl::IsUnavailable(started.status()) ||
            absl::IsResourceExhausted(started.status()) ||
            (absl::IsFailedPrecondition(started.status()) &&
             !failed_stopped_.load(std::memory_order_relaxed) &&
             cluster_promotion_prepare_ == nullptr);
        if (transient) {
          context->state_ = ClusterFailoverActionState::kRetrying;
          absl::Status waited =
              co_await WaitForFailoverActionRetry(context, watchdog_deadline);
          if (!waited.ok()) {
            finish();
            co_return waited;
          }
          continue;
        }
        PublishFailoverActionFailure(context, "promotion-admission",
                                     started.status().ToString());
        finish();
        co_return started.status();
      }

      // Reconciliation can supersede the action while admission is suspended
      // inside Start*PromotionPrepare. Forward that already-observed cancel to
      // the exact preparation before waiting for its private completion.
      if (context->cancelled_) {
        RequestFailoverPromotionCancellation(context);
      }
#if LAVIK_FAULTS_ENABLED
      const std::string_view attempt_id =
          context->prepare_directive_->identity_.attempt_id_;
      if (LAVIK_FAULT_MATCHES("LAVIK_REPLICATION_STALL_PROMOTION_ACTION",
                              attempt_id) ||
          LAVIK_FAULT_MATCHES(
              "LAVIK_REPLICATION_STALL_PROMOTION_AFTER_DURABILITY_BOUNDARY",
              attempt_id)) {
        (void)SignalFaultBarrier(
            "LAVIK_REPLICATION_FAILOVER_RUNNER_WAITING_ACK_PATH",
            "promotion fault barrier");
      }
#endif

      std::optional<ClusterPromotionPrepareCompletion::Result> result;
      while (!(result = (*started)->result()).has_value()) {
        (void)watchdog_expired();
        absl::Status waited = co_await bycorf::SleepFor(
            *bycorf::ThisWorker().self_, std::chrono::milliseconds(10));
        if (!waited.ok()) {
          finish();
          co_return waited;
        }
      }
#if LAVIK_FAULTS_ENABLED
      (void)SignalFaultBarrier(
          "LAVIK_REPLICATION_FAILOVER_RUNNER_TERMINAL_ACK_PATH",
          "promotion fault barrier");
#endif
      if (!result->ok()) {
        PublishFailoverActionFailure(context, "promotion-prepare",
                                     result->status().ToString());
        finish();
        co_return result->status();
      }
      context->prepared_child_history_created_ = true;
      if (!context->failure_published_ && !context->cancelled_ &&
          cluster_failover_action_ == context) {
        auto context_id = cluster::control::GenerateId128();
        if (!context_id.ok()) {
          PublishFailoverActionFailure(context, "prepared-context",
                                       context_id.status().ToString());
          finish();
          co_return context_id.status();
        }
        context->prepared_ = ClusterFailoverPreparedContext{
            .transition_id_ = context->desired_.transition_id_,
            .action_id_ = context->desired_.action_id_,
            .context_id_ = *context_id,
            .promotion_ = **result,
        };
        context->state_ = ClusterFailoverActionState::kPrepared;
      }
      finish();
      co_return absl::OkStatus();
    }
  }

  absl::Status ValidateClusterFailoverAction(
      const DesiredClusterFailoverAction& desired) const {
    const ClusterFailoverCompatibilityDomain& domain = desired.domain_;
    if (IsZeroBytes(desired.transition_id_) ||
        IsZeroBytes(desired.action_id_) || desired.transition_revision_ == 0 ||
        desired.target_term_ == 0 || desired.committed_group_term_ == 0 ||
        desired.group_id_.empty() || desired.candidate_node_id_ != node_id_ ||
        desired.candidate_assignment_id_.empty() ||
        desired.candidate_boot_id_ != boot_id_ ||
        (!desired.operator_recovery_ &&
         (domain.source_group_term_ == 0 ||
          domain.source_group_term_ > desired.target_term_ ||
          domain.source_node_id_.empty() ||
          domain.source_assignment_id_.empty() ||
          domain.source_boot_id_.empty() || domain.source_history_id_.empty() ||
          domain.flow_count_ == 0 ||
          domain.flow_count_ > cluster::control::kMaxCandidateFlows)) ||
        desired.manifest_revision_ == 0 ||
        IsZeroBytes(desired.manifest_id_.bytes_) ||
        desired.partition_replication_epoch_ == 0 ||
        (desired.authorized_revision_.has_value() &&
         (*desired.authorized_revision_ == 0 ||
          *desired.authorized_revision_ > desired.transition_revision_))) {
      return absl::InvalidArgumentError(
          "cluster failover action identity is incomplete");
    }
    if (desired.operator_recovery_ &&
        (desired.mode_ != ClusterFailoverMode::kUncontrolled ||
         domain != ClusterFailoverCompatibilityDomain{})) {
      return absl::InvalidArgumentError(
          "operator recovery requires a fenced action without source lineage");
    }
    if (desired.mode_ == ClusterFailoverMode::kControlled) {
      if (desired.committed_group_term_ ==
              std::numeric_limits<std::uint64_t>::max() ||
          desired.committed_group_term_ + 1 != desired.target_term_) {
        return absl::FailedPreconditionError(
            "controlled failover action does not target the successor term");
      }
    } else if (desired.mode_ == ClusterFailoverMode::kUncontrolled) {
      if (desired.committed_group_term_ != desired.target_term_ ||
          desired.committed_grant_active_) {
        return absl::FailedPreconditionError(
            "uncontrolled failover action requires the fenced target term");
      }
    } else {
      return absl::InvalidArgumentError("unknown cluster failover mode");
    }
    return absl::OkStatus();
  }

  Task<absl::Status> ReconcileClusterFailoverAction(
      std::optional<DesiredClusterFailoverAction> desired,
      std::optional<ClusterFailoverActionId> pending_activation_action_id) {
    if (bycorf::ThisWorker().id_ != 0) {
      co_return co_await bycorf::SubmitTaskTo(
          0, [this, desired = std::move(desired),
              pending_activation_action_id]() mutable {
            return ReconcileClusterFailoverAction(std::move(desired),
                                                  pending_activation_action_id);
          });
    }
    if (!cluster_enabled_ || cluster_group_ == nullptr) {
      co_return absl::FailedPreconditionError(
          "failover action reconciliation requires Meta-managed population "
          "mode");
    }
    if (desired.has_value()) {
      absl::Status validated = ValidateClusterFailoverAction(*desired);
      if (!validated.ok()) co_return validated;
    }
    if (pending_activation_action_id.has_value() &&
        IsZeroBytes(*pending_activation_action_id)) {
      co_return absl::InvalidArgumentError(
          "pending failover activation action id is zero");
    }
    if (desired.has_value() && pending_activation_action_id.has_value()) {
      co_return absl::FailedPreconditionError(
          "an active failover transition cannot also be pending activation");
    }
    if (cluster_control_stopping_) {
      co_return absl::CancelledError(
          "failover action reconciliation stopped for process shutdown");
    }

    AssertStateOwner();
    if (cluster_failover_action_ != nullptr && desired.has_value() &&
        cluster_failover_action_->desired_.transition_id_ ==
            desired->transition_id_ &&
        cluster_failover_action_->desired_.action_id_ == desired->action_id_) {
      DesiredClusterFailoverAction& current =
          cluster_failover_action_->desired_;
      const bool retained_controlled_degrade =
          IsRetainedControlledDegrade(current, *desired);
      if (!SameFailoverActionExceptAuthorization(current, *desired) &&
          !retained_controlled_degrade) {
        co_return absl::FailedPreconditionError(
            "committed failover action changed immutable execution anchors");
      }
      if (desired->transition_revision_ < current.transition_revision_ ||
          (current.authorized_revision_.has_value() &&
           current.authorized_revision_ != desired->authorized_revision_)) {
        co_return absl::FailedPreconditionError(
            "committed failover action authorization regressed or changed");
      }
      current.transition_revision_ = desired->transition_revision_;
      current.authorized_revision_ = desired->authorized_revision_;
      if (retained_controlled_degrade) {
        current.mode_ = desired->mode_;
        current.committed_group_term_ = desired->committed_group_term_;
        current.committed_grant_active_ = desired->committed_grant_active_;
      }
      if (current.authorized_revision_.has_value() &&
          !cluster_failover_action_->runner_started_ &&
          !cluster_failover_action_->failure_published_) {
        cluster_failover_action_->state_ =
            ClusterFailoverActionState::kWaitingForPopulation;
        cluster_failover_action_->runner_started_ = true;
        cluster_failover_action_->runner_finished_ = false;
        bycorf::ThisWorker().self_->Spawn(
            RunClusterFailoverAction(cluster_failover_action_));
      }
      co_return absl::OkStatus();
    }

    bool retire_abandoned_prepared_history = false;
    if (cluster_failover_action_ != nullptr) {
      // Status is withdrawn before any join. A stale task may still finish its
      // private durability work, but it can no longer reach the heartbeat or
      // activation seams once FDS replacement begins.
      const std::shared_ptr<ClusterFailoverActionContext> previous =
          cluster_failover_action_;
      previous->cancelled_ = true;
      cluster_failover_action_.reset();
      RequestFailoverPromotionCancellation(previous);
      while (!previous->runner_finished_) {
        absl::Status waited = co_await bycorf::SleepFor(
            *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
        if (!waited.ok()) co_return waited;
      }
      const bool retain_for_activation =
          pending_activation_action_id ==
              std::optional<ClusterFailoverActionId>(
                  previous->desired_.action_id_) &&
          previous->state_ == ClusterFailoverActionState::kPrepared &&
          previous->prepared_.has_value();
      if (retain_for_activation) {
        retained_failover_activation_action_id_ = previous->desired_.action_id_;
        retained_failover_prepared_context_ = previous->prepared_;
        retained_failover_desired_action_ = previous->desired_;
      } else {
        const bool exact_prepare_retained =
            previous->prepare_directive_.has_value() &&
            cluster_promotion_prepare_ != nullptr &&
            cluster_promotion_prepare_->directive_ ==
                *previous->prepare_directive_;
        // After the durability boundary, failure can occur after the child
        // publisher was created but before Prepared evidence reached the
        // action runner. Its terminal result alone therefore cannot prove
        // that no active child exists; conservatively drain and retire the
        // current non-cutover history before acknowledging supersession.
        retire_abandoned_prepared_history =
            previous->prepared_child_history_created_ ||
            (exact_prepare_retained &&
             cluster_promotion_prepare_->durability_mutation_started_);
        if (exact_prepare_retained) {
          cluster_promotion_prepare_.reset();
        }
        retained_failover_activation_action_id_.reset();
        retained_failover_prepared_context_.reset();
        retained_failover_desired_action_.reset();
      }
    } else if (pending_activation_action_id !=
               retained_failover_activation_action_id_) {
      // A newer ordinary grant or a mismatched failover grant makes any
      // retained prepared context unconsumable. Once activation succeeds the
      // child history is the live Owner history and must not be retired by
      // later context cleanup; before activation it belongs only to this
      // abandoned handoff.
      const bool retained_was_activated =
          retained_failover_activation_action_id_.has_value() &&
          retained_failover_prepared_context_.has_value() &&
          activated_failover_activation_.has_value() &&
          activated_failover_prepared_context_.has_value() &&
          activated_failover_activation_->action_id_ ==
              *retained_failover_activation_action_id_ &&
          *activated_failover_prepared_context_ ==
              *retained_failover_prepared_context_;
      retire_abandoned_prepared_history =
          retained_failover_prepared_context_.has_value() &&
          !retained_was_activated;
      cluster_promotion_prepare_.reset();
      retained_failover_activation_action_id_.reset();
      retained_failover_prepared_context_.reset();
      retained_failover_desired_action_.reset();
    }
    if (retire_abandoned_prepared_history) {
      // Withdraw every activation handle before suspending so an interleaved
      // request cannot consume the child while its source sessions and logs
      // are being joined. PromotionBase has no safe targeted clear API; it is
      // durable recovery evidence and is deliberately left intact.
      absl::Status retired = co_await RetireSourceHistory();
      if (!retired.ok()) {
        const std::string failure = absl::StrCat(
            "abandoned failover child history could not be retired: ",
            retired.message());
        LatchReplicationFailure(failure);
        co_return absl::InternalError(failure);
      }
    }
    if (desired.has_value()) {
      cluster_failover_action_ =
          std::make_shared<ClusterFailoverActionContext>(std::move(*desired));
      retained_failover_activation_action_id_.reset();
      retained_failover_prepared_context_.reset();
      retained_failover_desired_action_.reset();
      if (cluster_failover_action_->desired_.authorized_revision_.has_value()) {
        cluster_failover_action_->state_ =
            ClusterFailoverActionState::kWaitingForPopulation;
        cluster_failover_action_->runner_started_ = true;
        cluster_failover_action_->runner_finished_ = false;
        bycorf::ThisWorker().self_->Spawn(
            RunClusterFailoverAction(cluster_failover_action_));
      }
    }
    co_return absl::OkStatus();
  }

  Task<ClusterFailoverActionStatus> cluster_failover_action_status() const {
    if (bycorf::ThisWorker().id_ != 0) {
      co_return co_await bycorf::SubmitTaskTo(
          0, [this] { return cluster_failover_action_status(); });
    }
    AssertStateOwner();
    ClusterFailoverActionStatus result;
    if (cluster_failover_action_ == nullptr) co_return result;
    result.state_ = cluster_failover_action_->state_;
    result.action_ = cluster_failover_action_->desired_;
    result.prepared_ = cluster_failover_action_->prepared_;
    result.failure_class_ = cluster_failover_action_->failure_class_;
    result.failure_detail_ = cluster_failover_action_->failure_detail_;
    co_return result;
  }

  Task<std::optional<ClusterFailoverPreparedContext>>
  FindClusterFailoverPreparedContext(
      const ClusterFailoverActionId& action_id) const {
    if (bycorf::ThisWorker().id_ != 0) {
      co_return co_await bycorf::SubmitTaskTo(0, [this, action_id] {
        return FindClusterFailoverPreparedContext(action_id);
      });
    }
    AssertStateOwner();
    if (retained_failover_activation_action_id_ != action_id) {
      co_return std::nullopt;
    }
    co_return retained_failover_prepared_context_;
  }

  absl::Status ValidateClusterFailoverActivation(
      const ClusterFailoverActivation& activation) const {
    if (IsZeroBytes(activation.action_id_) || activation.group_id_.empty() ||
        activation.candidate_node_id_ != node_id_ ||
        activation.candidate_assignment_id_.empty() ||
        activation.candidate_boot_id_ != boot_id_ ||
        activation.target_term_ == 0 || activation.manifest_revision_ == 0 ||
        IsZeroBytes(activation.manifest_id_.bytes_) ||
        activation.partition_replication_epoch_ == 0) {
      return absl::InvalidArgumentError(
          "cluster failover activation identity is incomplete");
    }
    return absl::OkStatus();
  }

  bool ClusterActivationPopulationMatches(
      const ClusterFailoverActivation& activation) const {
    AssertStateOwner();
    if (failed_stopped_.load(std::memory_order_relaxed) ||
        replica_reconfiguration_running_ || cluster_rebuild_ == nullptr ||
        cluster_rebuild_->state_.load(std::memory_order_acquire) !=
            ReplicationGroupState::kReady ||
        !cluster_rebuild_->ready_token_.has_value() || upstream_.has_value() ||
        !native_dataset_valid_.load(std::memory_order_acquire)) {
      return false;
    }
    const ReadyToken& ready = *cluster_rebuild_->ready_token_;
    const RebuildIdentity& identity = ready.identity();
    return identity.group_id_ == activation.group_id_ &&
           identity.assignment_id_ == activation.candidate_assignment_id_ &&
           identity.target_node_id_ == activation.candidate_node_id_ &&
           identity.target_boot_id_ == activation.candidate_boot_id_ &&
           ready.CanCarryForwardToTerm(activation.target_term_) &&
           identity.manifest_revision_ == activation.manifest_revision_ &&
           identity.manifest_id_ == activation.manifest_id_ &&
           identity.partition_replication_epoch_ ==
               activation.partition_replication_epoch_;
  }

  Task<absl::Status> ActivateClusterPreparedPromotion(
      ClusterFailoverActivation activation) {
    if (bycorf::ThisWorker().id_ != 0) {
      co_return co_await bycorf::SubmitTaskTo(
          0, [this, activation = std::move(activation)]() mutable {
            return ActivateClusterPreparedPromotion(std::move(activation));
          });
    }
    if (!cluster_enabled_ || cluster_group_ == nullptr) {
      co_return absl::FailedPreconditionError(
          "cluster promotion activation requires Meta-managed population "
          "mode");
    }
    if (cluster_control_stopping_) {
      co_return absl::CancelledError(
          "cluster promotion activation stopped for process shutdown");
    }
    if (absl::Status valid = ValidateClusterFailoverActivation(activation);
        !valid.ok()) {
      co_return valid;
    }
    AssertStateOwner();
    if (!ClusterActivationPopulationMatches(activation)) {
      co_return absl::FailedPreconditionError(
          "cluster activation does not match the ready population");
    }

    const ReplicationIdentity current = co_await identity();
    AssertStateOwner();
    if (!ClusterActivationPopulationMatches(activation)) {
      co_return absl::FailedPreconditionError(
          "cluster activation population changed during validation");
    }
    if (activated_failover_activation_.has_value() &&
        *activated_failover_activation_ == activation) {
      if (!activated_failover_prepared_context_.has_value() ||
          current.local_history_id_ != activated_failover_prepared_context_
                                           ->promotion_.child_history_id_ ||
          role_.load(std::memory_order_acquire) != ReplicationRole::kMaster ||
          storage_->ReplicaRecoveryFenced() || is_loading()) {
        co_return absl::FailedPreconditionError(
            "activated cluster promotion no longer matches local state");
      }
      co_return absl::OkStatus();
    }

    if (retained_failover_activation_action_id_ !=
            std::optional<ClusterFailoverActionId>(activation.action_id_) ||
        !retained_failover_prepared_context_.has_value() ||
        !retained_failover_desired_action_.has_value()) {
      co_return absl::FailedPreconditionError(
          "cluster activation has no matching retained prepared action");
    }
    const DesiredClusterFailoverAction& desired =
        *retained_failover_desired_action_;
    const ClusterFailoverPreparedContext& prepared =
        *retained_failover_prepared_context_;
    if (desired.action_id_ != activation.action_id_ ||
        desired.group_id_ != activation.group_id_ ||
        desired.candidate_node_id_ != activation.candidate_node_id_ ||
        desired.candidate_assignment_id_ !=
            activation.candidate_assignment_id_ ||
        desired.candidate_boot_id_ != activation.candidate_boot_id_ ||
        desired.target_term_ != activation.target_term_ ||
        desired.manifest_revision_ != activation.manifest_revision_ ||
        desired.manifest_id_ != activation.manifest_id_ ||
        desired.partition_replication_epoch_ !=
            activation.partition_replication_epoch_ ||
        prepared.action_id_ != activation.action_id_ ||
        current.local_history_id_ != prepared.promotion_.child_history_id_) {
      co_return absl::FailedPreconditionError(
          "cluster activation does not match prepared action anchors");
    }
    if (cluster_promotion_prepare_ == nullptr) {
      co_return absl::FailedPreconditionError(
          "cluster activation lost its private prepare context");
    }
    const auto terminal = cluster_promotion_prepare_->completion_->result();
    if (!terminal.has_value() || !terminal->ok() ||
        **terminal != prepared.promotion_) {
      co_return absl::FailedPreconditionError(
          "cluster activation prepare evidence is not terminal and exact");
    }
    if (role_.load(std::memory_order_acquire) != ReplicationRole::kSyncing ||
        !is_loading() || storage_->ReplicaRecoveryFenced()) {
      co_return absl::FailedPreconditionError(
          "cluster prepared promotion is not safely fenced for activation");
    }

    ActivatePreparedPromotionRole();
    if (is_loading()) {
      co_return absl::FailedPreconditionError(
          "cluster promotion remained recovery-fenced during activation");
    }
    owner_source_term_ = activation.target_term_;
    operator_recovery_active_ = false;
    activated_failover_activation_ = activation;
    activated_failover_prepared_context_ = prepared;
    co_return absl::OkStatus();
  }

  Task<absl::Status> EnableClusterExpirationAuthorityUntil(
      std::chrono::nanoseconds deadline_since_boot) {
    if (bycorf::ThisWorker().id_ != 0) {
      co_return co_await bycorf::SubmitTaskTo(0, [this, deadline_since_boot] {
        return EnableClusterExpirationAuthorityUntil(deadline_since_boot);
      });
    }
    if (!cluster_enabled_ || cluster_group_ == nullptr) {
      co_return absl::FailedPreconditionError(
          "finite expiration authority requires Meta-managed population mode");
    }
    AssertStateOwner();
    if (cluster_control_stopping_ ||
        role_.load(std::memory_order_acquire) != ReplicationRole::kMaster ||
        is_loading() || storage_->ReplicaRecoveryFenced() ||
        cluster_rebuild_ == nullptr ||
        cluster_rebuild_->state_.load(std::memory_order_acquire) !=
            ReplicationGroupState::kReady ||
        !cluster_rebuild_->ready_token_.has_value()) {
      co_return absl::FailedPreconditionError(
          "finite expiration authority requires an active ready owner");
    }
    co_return storage_->SetExpirationAuthorityUntil(deadline_since_boot);
  }

  Task<absl::Status> RevokeClusterExpirationAuthority() {
    if (bycorf::ThisWorker().id_ != 0) {
      co_return co_await bycorf::SubmitTaskTo(
          0, [this] { return RevokeClusterExpirationAuthority(); });
    }
    if (!cluster_enabled_ || cluster_group_ == nullptr) {
      co_return absl::FailedPreconditionError(
          "expiration revocation requires Meta-managed population mode");
    }
    {
      co_await master_mutex_.Lock(*bycorf::ThisWorker().self_);
      bycorf::CrossWorkerMutex::Guard lock(&master_mutex_);
      // The lease gate and native POPULATION admission share this lock. Once
      // this critical section completes, no new session can publish;
      // already-published sessions remain owned by their current FDS
      // capability until a stronger fence/session/population transition.
      source_authorizations_.SuspendLeaseAdmission();
    }
    storage_->SetExpirationAuthority(false);
    absl::Status drained = co_await storage_->QuiesceExpiration();
    if (!drained.ok()) co_return drained;
    storage_->ResumeExpiration();
    co_return absl::OkStatus();
  }

  absl::StatusOr<std::pair<DesiredClusterUpstream, PopulationManifest>>
  NormalizeClusterFollowOwner(DesiredClusterUpstream desired) const {
    if (!cluster_enabled_ || cluster_group_ == nullptr) {
      return absl::FailedPreconditionError(
          "follow-owner reconciliation requires Meta-managed population "
          "mode");
    }
    if (desired.group_id_.empty() || desired.group_term_ == 0 ||
        desired.local_node_id_ != node_id_ ||
        desired.local_assignment_id_.empty() ||
        desired.local_boot_id_ != boot_id_ || desired.owner_node_id_.empty() ||
        desired.owner_assignment_id_.empty() ||
        desired.manifest_revision_ == 0 ||
        IsZeroBytes(desired.manifest_id_.bytes_) ||
        desired.partition_replication_epoch_ == 0) {
      return absl::InvalidArgumentError(
          "follow-owner desired identity is incomplete or belongs to another "
          "local boot");
    }
    const bool local_is_owner =
        desired.local_node_id_ == desired.owner_node_id_;
    if (local_is_owner) {
      if (desired.local_assignment_id_ != desired.owner_assignment_id_ ||
          desired.owner_endpoint_.has_value()) {
        return absl::InvalidArgumentError(
            "local Owner follow scope has a conflicting assignment or "
            "upstream endpoint");
      }
    } else if (!desired.owner_endpoint_.has_value() ||
               desired.owner_endpoint_->host_.empty() ||
               desired.owner_endpoint_->port_ == 0) {
      return absl::InvalidArgumentError(
          "follower desired state has no usable Owner endpoint");
    }

    std::sort(desired.members_.begin(), desired.members_.end(),
              [](const ClusterReplicationMember& left,
                 const ClusterReplicationMember& right) {
                return std::tie(left.node_id_, left.assignment_id_) <
                       std::tie(right.node_id_, right.assignment_id_);
              });
    if (desired.members_.empty() ||
        std::any_of(desired.members_.begin(), desired.members_.end(),
                    [](const ClusterReplicationMember& member) {
                      return member.node_id_.empty() ||
                             member.assignment_id_.empty();
                    }) ||
        std::adjacent_find(desired.members_.begin(), desired.members_.end(),
                           [](const ClusterReplicationMember& left,
                              const ClusterReplicationMember& right) {
                             return left.node_id_ == right.node_id_;
                           }) != desired.members_.end()) {
      return absl::InvalidArgumentError(
          "follow-owner membership is empty, incomplete, or duplicated");
    }
    const auto local = std::find_if(
        desired.members_.begin(), desired.members_.end(),
        [&](const ClusterReplicationMember& member) {
          return member.node_id_ == desired.local_node_id_ &&
                 member.assignment_id_ == desired.local_assignment_id_;
        });
    const auto owner = std::find_if(
        desired.members_.begin(), desired.members_.end(),
        [&](const ClusterReplicationMember& member) {
          return member.node_id_ == desired.owner_node_id_ &&
                 member.assignment_id_ == desired.owner_assignment_id_;
        });
    if (local == desired.members_.end() || owner == desired.members_.end()) {
      return absl::FailedPreconditionError(
          "follow-owner local member or Owner is absent from the exact "
          "membership");
    }

    std::sort(desired.manifest_entries_.begin(),
              desired.manifest_entries_.end(),
              [](const PopulationManifestEntry& left,
                 const PopulationManifestEntry& right) {
                return left.partition_id_ < right.partition_id_;
              });
    auto manifest = PopulationManifest::Create(desired.manifest_entries_);
    if (!manifest.ok()) return manifest.status();
    if (manifest->id() != desired.manifest_id_) {
      return absl::FailedPreconditionError(
          "follow-owner manifest entries do not match their FDS digest");
    }
    return std::make_pair(std::move(desired), std::move(*manifest));
  }

  bool ClusterFollowReadyPopulationMatches(
      const DesiredClusterUpstream& desired) const {
    AssertStateOwner();
    if (cluster_rebuild_ == nullptr ||
        cluster_rebuild_->state_.load(std::memory_order_acquire) !=
            ReplicationGroupState::kReady ||
        !cluster_rebuild_->ready_token_.has_value()) {
      return false;
    }
    const ReadyToken& ready = *cluster_rebuild_->ready_token_;
    const RebuildIdentity& identity = ready.identity();
    return identity.group_id_ == desired.group_id_ &&
           identity.assignment_id_ == desired.local_assignment_id_ &&
           identity.target_node_id_ == desired.local_node_id_ &&
           identity.target_boot_id_ == desired.local_boot_id_ &&
           ready.CanCarryForwardToTerm(desired.group_term_) &&
           identity.manifest_revision_ == desired.manifest_revision_ &&
           identity.manifest_id_ == desired.manifest_id_ &&
           identity.partition_replication_epoch_ ==
               desired.partition_replication_epoch_;
  }

  Task<absl::Status> StopClusterFollowIngress(
      const std::shared_ptr<ClusterFollowOwnerContext>& previous) {
    AssertStateOwner();
    std::shared_ptr<ReplicaSession> session;
    if (active_replica_session_ != nullptr &&
        active_replica_session_->cluster_follow_ == previous) {
      session = std::move(active_replica_session_);
    }
    SetDesiredUpstream(std::nullopt);
    role_epoch_.fetch_add(1, std::memory_order_acq_rel);
    if (session != nullptr) session->Cancel();

    if (session != nullptr) {
      absl::Status stopped = co_await CancelAndWaitForReplicaFlows(session);
      if (!stopped.ok()) co_return stopped;
      if (std::optional<std::string> uncertain = session->FailStopReason();
          uncertain.has_value()) {
        co_return absl::InternalError(*uncertain);
      }
      const std::shared_ptr<ClusterRebuildContext> population =
          session->cluster_rebuild_;
      if (population != nullptr && cluster_rebuild_ == population &&
          population->state_.load(std::memory_order_acquire) ==
              ReplicationGroupState::kRebuilding) {
        if (session->session_id_ != 0) {
          absl::Status aborted =
              co_await storage_->AbortReplicaRoot(session->session_id_);
          if (!aborted.ok()) co_return aborted;
        }
        absl::Status invalidated =
            cluster_group_->InvalidateProof(population->directive_.identity_);
        if (!invalidated.ok()) co_return invalidated;
        population->state_.store(ReplicationGroupState::kNotReady,
                                 std::memory_order_release);
        population->ready_token_.reset();
        population->completion_->Resolve(absl::CancelledError(
            "steady follow population attempt was replaced"));
        cluster_rebuild_.reset();
        applied_frontier_.reset();
        upstream_node_id_.reset();
        upstream_history_id_.reset();
        native_dataset_valid_.store(false, std::memory_order_release);
      }
    }
    while (coordinator_started_) {
      absl::Status waited = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) co_return waited;
    }
    co_return absl::OkStatus();
  }

  absl::Status BeginClusterFollowFullPopulation(
      const std::shared_ptr<ReplicaSession>& session,
      std::string source_boot_id, std::string source_history_id,
      std::uint32_t source_flow_count) {
    AssertStateOwner();
    if (session == nullptr || session->cluster_follow_ == nullptr ||
        active_replica_session_ != session ||
        cluster_follow_owner_ != session->cluster_follow_ ||
        session->cancelled()) {
      return absl::CancelledError(
          "follow-owner source became stale before FULL admission");
    }
    const DesiredClusterUpstream& desired = session->cluster_follow_->desired_;
    if (source_boot_id.empty() || source_history_id.empty() ||
        source_flow_count == 0) {
      return absl::InvalidArgumentError(
          "follow-owner source incarnation is incomplete");
    }

    // ReplicationGroup owns the accepted-version watermark even after a
    // failed CONTINUE invalidates and releases cluster_rebuild_. Deriving the
    // next revision from that owner prevents every FULL retry from being
    // rejected forever as stale after the transient context disappears.
    auto revision = cluster_group_->NextDirectiveRevision(desired.group_term_);
    if (!revision.ok()) return revision.status();

    RebuildDirective directive{
        .identity_ =
            {
                .group_id_ = desired.group_id_,
                .assignment_id_ = desired.local_assignment_id_,
                .term_ = desired.group_term_,
                .directive_revision_ = *revision,
                .authority_id_ =
                    absl::StrCat("steady-follow:", desired.group_id_),
                .source_node_id_ = desired.owner_node_id_,
                .source_assignment_id_ = desired.owner_assignment_id_,
                .source_boot_id_ = std::move(source_boot_id),
                .source_history_id_ = std::move(source_history_id),
                .target_node_id_ = desired.local_node_id_,
                .target_boot_id_ = desired.local_boot_id_,
                .target_history_id_ = {},
                .operation_id_ =
                    absl::StrCat("steady-follow:", desired.group_id_, ":",
                                 desired.group_term_),
                .directive_id_ =
                    absl::StrCat("steady-follow-owner:", desired.owner_node_id_,
                                 ":", desired.owner_assignment_id_),
                .attempt_id_ =
                    absl::StrCat("steady-follow-attempt:", *revision),
                .manifest_revision_ = desired.manifest_revision_,
                .manifest_id_ = desired.manifest_id_,
                .partition_replication_epoch_ =
                    desired.partition_replication_epoch_,
            },
        .flow_count_ = source_flow_count,
        .safe_source_active_ = true,
    };
    absl::Status validated = cluster_group_->ValidateRebuild(
        directive, session->cluster_follow_->manifest_);
    if (!validated.ok()) return validated;

    const std::shared_ptr<ClusterRebuildContext> previous = cluster_rebuild_;
    if (previous != nullptr) {
      absl::Status invalidated =
          cluster_group_->InvalidateProof(previous->directive_.identity_);
      if (!invalidated.ok()) return invalidated;
      previous->state_.store(ReplicationGroupState::kNotReady,
                             std::memory_order_release);
      previous->ready_token_.reset();
    }
    auto authorization = cluster_group_->BeginRebuild(
        directive, session->cluster_follow_->manifest_);
    if (!authorization.ok()) return authorization.status();
    auto population = std::make_shared<ClusterRebuildContext>(
        std::move(directive), session->cluster_follow_->manifest_,
        std::move(*authorization));
    cluster_rebuild_ = population;
    session->cluster_rebuild_ = std::move(population);
    native_dataset_valid_.store(false, std::memory_order_release);
    applied_frontier_.reset();
    upstream_node_id_.reset();
    upstream_history_id_.reset();
    retained_failover_activation_action_id_.reset();
    retained_failover_prepared_context_.reset();
    retained_failover_desired_action_.reset();
    activated_failover_activation_.reset();
    activated_failover_prepared_context_.reset();
    cluster_promotion_prepare_.reset();
    return absl::OkStatus();
  }

  Task<absl::Status> ReconcileClusterFollowOwner(
      std::optional<DesiredClusterUpstream> desired) {
    if (bycorf::ThisWorker().id_ != 0) {
      co_return co_await bycorf::SubmitTaskTo(
          0, [this, desired = std::move(desired)]() mutable {
            return ReconcileClusterFollowOwner(std::move(desired));
          });
    }
    if (!cluster_enabled_ || cluster_group_ == nullptr) {
      co_return absl::FailedPreconditionError(
          "follow-owner reconciliation requires Meta-managed population "
          "mode");
    }
    if (cluster_control_stopping_) {
      co_return absl::CancelledError(
          "follow-owner reconciliation stopped for process shutdown");
    }

    std::shared_ptr<ClusterFollowOwnerContext> next;
    if (desired.has_value()) {
      auto normalized = NormalizeClusterFollowOwner(std::move(*desired));
      if (!normalized.ok()) co_return normalized.status();
      if (cluster_rebuild_ != nullptr &&
          !cluster_rebuild_->manifest_.has_value() &&
          cluster_rebuild_->directive_.identity_.manifest_id_ ==
              normalized->second.id()) {
        cluster_rebuild_->manifest_ = normalized->second;
      }
      next = std::make_shared<ClusterFollowOwnerContext>(
          std::move(normalized->first), std::move(normalized->second));
      if (cluster_follow_owner_ != nullptr &&
          cluster_follow_owner_->desired_ == next->desired_) {
        if (next->desired_.local_node_id_ != next->desired_.owner_node_id_ &&
            !coordinator_started_ && !replication_shutdown_requested_ &&
            !failed_stopped_.load(std::memory_order_relaxed)) {
          SetDesiredUpstream(next->desired_.owner_endpoint_);
          StartCoordinator();
        }
        co_return absl::OkStatus();
      }
    }

    const std::shared_ptr<ClusterFollowOwnerContext> previous =
        std::move(cluster_follow_owner_);
    const bool previous_was_owner =
        previous != nullptr &&
        previous->desired_.local_node_id_ == previous->desired_.owner_node_id_;
    const bool previous_was_follower =
        previous != nullptr && !previous_was_owner;
    // The level-triggered adapter may be installed over an already-running
    // pre-FDS population coordinator on its first FDS. Treat that ingress as
    // the relationship being replaced too; otherwise two sessions can race
    // while the new exact Owner/scope is being installed.
    const bool legacy_ingress =
        previous == nullptr &&
        (upstream_.has_value() || active_replica_session_ != nullptr ||
         coordinator_started_);
    const bool local_was_source =
        role_.load(std::memory_order_acquire) == ReplicationRole::kMaster &&
        !upstream_.has_value();
    const bool next_is_owner =
        next != nullptr &&
        next->desired_.local_node_id_ == next->desired_.owner_node_id_;
    const bool must_fence_target =
        !next_is_owner &&
        (next != nullptr || previous_was_follower || legacy_ingress);

    if (must_fence_target) {
      // Close serving before cancellation can suspend. The role generation
      // fences clients, and StopClusterFollowIngress joins every old apply
      // before a replacement coordinator starts. Keep storage on the live
      // Ready root: replica_loading is owned by a destructive FULL after it
      // installs per-partition apply contexts; setting it here would route
      // same-history CONTINUE writes through contexts that do not exist.
      StoreRole(ReplicationRole::kConnecting, std::memory_order_release);
      storage_->SetExpirationAuthority(false);
      retained_failover_activation_action_id_.reset();
      retained_failover_prepared_context_.reset();
      retained_failover_desired_action_.reset();
      activated_failover_activation_.reset();
      activated_failover_prepared_context_.reset();
      cluster_promotion_prepare_.reset();
    }

    // Closing role/serving above is the synchronous fail-closed edge. Clear
    // any source capability and join every admission that crossed that edge
    // before an ingress or source-history teardown can suspend. The strong
    // path owns the ledger under master_mutex_, the same lock as LVPSYNC
    // classification and registry publication.
    if (previous_was_owner || must_fence_target) {
      absl::Status revoked =
          co_await RevokeClusterRebuildSourceAuthorizations();
      if (!revoked.ok()) co_return revoked;
    }
    if (previous_was_follower || legacy_ingress) {
      absl::Status stopped = co_await StopClusterFollowIngress(previous);
      if (!stopped.ok()) {
        const std::string failure = absl::StrCat(
            "follow-owner ingress replacement could not be joined: ",
            stopped.message());
        LatchReplicationFailure(failure);
        co_return absl::InternalError(failure);
      }
    }
    if (next == nullptr) co_return absl::OkStatus();

    if (next_is_owner) {
      SetDesiredUpstream(std::nullopt);
      cluster_follow_owner_ = std::move(next);
      co_return absl::OkStatus();
    }

    if (local_was_source) {
      absl::Status retired = co_await RetireSourceHistory();
      if (!retired.ok()) {
        const std::string failure =
            absl::StrCat("former Owner source history could not be retired: ",
                         retired.message());
        LatchReplicationFailure(failure);
        co_return absl::InternalError(failure);
      }
    }
    cluster_follow_owner_ = std::move(next);
    SetDesiredUpstream(cluster_follow_owner_->desired_.owner_endpoint_);
    role_epoch_.fetch_add(1, std::memory_order_acq_rel);
    StartCoordinator();
    co_return absl::OkStatus();
  }

  Task<absl::Status> RetireClusterPopulation(
      std::optional<DesiredClusterPopulation> desired, bool preserve_any_ready,
      bool preserve_current_follow_attempt, std::string_view reason) {
    if (bycorf::ThisWorker().id_ != 0) {
      co_return co_await bycorf::SubmitTaskTo(
          0, [this, desired = std::move(desired), preserve_any_ready,
              preserve_current_follow_attempt,
              reason = std::string(reason)]() mutable {
            return RetireClusterPopulation(
                std::move(desired), preserve_any_ready,
                preserve_current_follow_attempt, reason);
          });
    }
    if (!cluster_enabled_ || cluster_group_ == nullptr) {
      co_return absl::FailedPreconditionError(
          "population reconciliation requires Meta-managed population mode");
    }

    // Automatic teardown owns the same session/root cleanup. Joining it first
    // prevents two coroutines from aborting one in-place candidate.
    for (;;) {
      bool teardown_running = false;
      {
        AssertStateOwner();
        teardown_running = replica_session_teardown_running_;
      }
      if (!teardown_running) break;
      absl::Status waited = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) co_return waited;
    }

    // An FDS replacement cannot retire a Ready population out from under the
    // storage transaction that is preparing it. Wait for that transaction's
    // exact terminal boundary, then re-evaluate the new desired population;
    // it remains fenced throughout, so this wait grants no authority.
    if (!preserve_any_ready) {
      for (;;) {
        bool promotion_running = false;
        {
          AssertStateOwner();
          promotion_running =
              replica_reconfiguration_running_ &&
              cluster_promotion_prepare_ != nullptr &&
              !cluster_promotion_prepare_->completion_->result().has_value();
        }
        if (!promotion_running) break;
        absl::Status waited = co_await bycorf::SleepFor(
            *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
        if (!waited.ok()) co_return waited;
      }
    }

    std::shared_ptr<ClusterRebuildContext> context;
    std::shared_ptr<ClusterPromotionPrepareContext> promotion_context;
    std::shared_ptr<ReplicaSession> session;
    std::optional<RebuildIdentity> shutdown_identity;
    std::shared_ptr<detail::ReplicaAppliedFrontier> shutdown_frontier;
    bool shutdown_native = false;
    {
      AssertStateOwner();
      if (failed_stopped_.load(std::memory_order_relaxed)) {
        co_return absl::FailedPreconditionError(absl::StrCat(
            "replication is failed-stopped until restart: ", failure_reason_));
      }
      context = cluster_rebuild_;
      if (context == nullptr) co_return absl::OkStatus();

      const RebuildIdentity& identity = context->directive_.identity_;
      const ReplicationGroupState state =
          context->state_.load(std::memory_order_acquire);
      const bool matches_population =
          desired.has_value() && identity.group_id_ == desired->group_id_ &&
          identity.assignment_id_ == desired->assignment_id_ &&
          identity.manifest_revision_ == desired->manifest_revision_ &&
          identity.manifest_id_ == desired->manifest_id_ &&
          identity.partition_replication_epoch_ ==
              desired->partition_replication_epoch_;
      const bool matches_attempt =
          matches_population && identity.term_ == desired->term_;
      const bool completed_ready = state == ReplicationGroupState::kReady &&
                                   context->ready_token_.has_value();
      const bool desired_attempt_still_live =
          matches_attempt && desired->population_transition_expected_ &&
          state == ReplicationGroupState::kRebuilding;
      const bool desired_follow_attempt_still_live =
          matches_population && desired->term_ >= identity.term_ &&
          state == ReplicationGroupState::kRebuilding &&
          cluster_follow_owner_ != nullptr &&
          active_replica_session_ != nullptr &&
          active_replica_session_->cluster_follow_ == cluster_follow_owner_ &&
          active_replica_session_->cluster_rebuild_ == context;
      const bool current_follow_attempt_still_live =
          state == ReplicationGroupState::kRebuilding &&
          cluster_follow_owner_ != nullptr &&
          active_replica_session_ != nullptr &&
          active_replica_session_->cluster_follow_ == cluster_follow_owner_ &&
          active_replica_session_->cluster_rebuild_ == context;
      // A term transition fences authority but does not change the physical
      // population. Preserve a completed proof across that transition when
      // membership incarnation and immutable manifest remain exact; the Data
      // controller re-anchors its heartbeat proof to the newer FDS term.
      const bool desired_ready_population =
          matches_population && completed_ready;
      // ReconcileClusterControl runs before population reconciliation for one
      // FDS. A steady FollowOwner FULL has no operation directive flag, so its
      // exact active session/context ownership is the proof that this
      // rebuilding attempt is still desired. Its source-term identity remains
      // valid across an authority-only term fence; owner replacement changes
      // that ownership, while assignment/manifest/epoch replacement changes
      // matches_population, and both continue through the retirement path.
      if (desired_attempt_still_live || desired_follow_attempt_still_live ||
          desired_ready_population || (preserve_any_ready && completed_ready) ||
          (preserve_current_follow_attempt &&
           current_follow_attempt_still_live)) {
        co_return absl::OkStatus();
      }
      if (replica_reconfiguration_running_) {
        co_return absl::AbortedError(
            "cluster population changed during FDS reconciliation");
      }

      // Population retirement is also an abort boundary for a controlled
      // source pause. Release only this context's nesting level before the
      // population becomes unobservable.
      if (cluster_source_pause_ != nullptr) {
        const std::shared_ptr<ClusterSourcePauseContext> source_pause =
            std::move(cluster_source_pause_);
        if (source_pause->expiration_pause_held_) {
          source_pause->expiration_pause_held_ = false;
          storage_->ResumeExpiration();
        }
      }
      retained_failover_activation_action_id_.reset();
      retained_failover_prepared_context_.reset();
      retained_failover_desired_action_.reset();
      activated_failover_activation_.reset();
      activated_failover_prepared_context_.reset();

      if (cluster_control_stopping_ && completed_ready &&
          (cluster_promotion_prepare_ == nullptr ||
           role_.load(std::memory_order_acquire) == ReplicationRole::kMaster)) {
        shutdown_identity = context->ready_token_->identity();
        shutdown_frontier = applied_frontier_;
        shutdown_native =
            role_.load(std::memory_order_acquire) == ReplicationRole::kMaster;
      }

      // Close every externally observable proof before the first suspension.
      // The moved session gives this transition exclusive ownership of flow
      // join and root abort; Coordinator observes the move and exits.
      context->state_.store(ReplicationGroupState::kNotReady,
                            std::memory_order_release);
      context->ready_token_.reset();
      promotion_context = std::move(cluster_promotion_prepare_);
      replica_reconfiguration_running_ = true;
      session = std::move(active_replica_session_);
      SetDesiredUpstream(std::nullopt);
      applied_frontier_.reset();
      upstream_node_id_.reset();
      upstream_history_id_.reset();
      replica_session_id_ = 0;
      source_worker_count_ = 0;
      role_epoch_.fetch_add(1, std::memory_order_acq_rel);
    }
    native_dataset_valid_.store(false, std::memory_order_release);
    storage_->SetReplicaLoading(true);
    storage_->SetExpirationAuthority(false);
    StoreRole(ReplicationRole::kConnecting, std::memory_order_release);
    if (session != nullptr) session->Cancel();

    absl::Status revoked = co_await RevokeClusterRebuildSourceAuthorizations();
    if (!revoked.ok()) {
      const std::string failure =
          absl::StrCat("cluster population source revocation is uncertain: ",
                       revoked.message());
      (void)cluster_group_->FailStop(context->directive_.identity_);
      LatchReplicationFailure(failure);
      co_return revoked;
    }

    if (session != nullptr) {
      absl::Status stopped = co_await CancelAndWaitForReplicaFlows(session);
      std::optional<std::string> failure = session->FailStopReason();
      if (!stopped.ok() && !failure.has_value()) {
        failure = absl::StrCat("replica cancellation/join is uncertain: ",
                               stopped.message());
      }
      if (stopped.ok() && !failure.has_value() && session->session_id_ != 0) {
        absl::Status discarded =
            co_await storage_->AbortReplicaRoot(session->session_id_);
        if (!discarded.ok()) {
          failure = absl::StrCat("replica root abort is uncertain: ",
                                 discarded.message());
        }
      }
      if (failure.has_value()) {
        (void)cluster_group_->FailStop(context->directive_.identity_);
        LatchReplicationFailure(*failure);
        co_return absl::FailedPreconditionError(absl::StrCat(
            "replication is failed-stopped until restart: ", *failure));
      }
    }

    if (context->directive_.flow_count_ == 0) {
      // Source-less initialization owns no ReplicaSession to cancel. Its
      // coordinator observes the reconfiguration bit, aborts any known
      // candidate root, and exits before this owner retires the proof.
      while (coordinator_started_) {
        absl::Status waited = co_await bycorf::SleepFor(
            *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
        if (!waited.ok()) co_return waited;
      }
    }

    if (shutdown_identity.has_value()) {
      absl::Status drained = co_await storage_->QuiesceExpiration();
      if (!drained.ok()) co_return drained;
      struct ExpirationResumeGuard {
        storage::StorageEngine* storage_;
        ~ExpirationResumeGuard() { storage_->ResumeExpiration(); }
      } expiration_guard{storage_};
      // Pair this drain's pause on every exit. Population retirement already
      // revoked expiration authority and set replica loading, so releasing
      // the pause cannot admit expiry writes during final storage flushing.
      // Client admission and upstream apply have both drained as well.
      std::vector<std::uint64_t> frozen;
      if (shutdown_native) {
        auto watermark = co_await CaptureNativeReplicationWatermark();
        if (!watermark.ok()) co_return watermark.status();
        if (watermark->has_value()) {
          shutdown_identity->term_ = owner_source_term_;
          shutdown_identity->source_node_id_ = node_id_;
          shutdown_identity->source_assignment_id_ =
              shutdown_identity->assignment_id_;
          shutdown_identity->source_boot_id_ = boot_id_;
          shutdown_identity->source_history_id_ = (*watermark)->history_id_;
          frozen = (*watermark)->next_lsns_;
        }
      } else if (shutdown_frontier != nullptr) {
        auto snapshot = shutdown_frontier->TrySnapshot();
        if (!snapshot.ok()) co_return snapshot.status();
        frozen = std::move(*snapshot);
      }
      if (!frozen.empty()) {
        storage_->StageCleanShutdownProof(
            detail::EncodeRecoveredPopulation(*shutdown_identity, frozen));
      }
    }

    absl::Status retired =
        cluster_group_->InvalidateProof(context->directive_.identity_);
    if (!retired.ok()) {
      const std::string failure = absl::StrCat(
          "cluster population proof could not be retired: ", retired.message());
      (void)cluster_group_->FailStop(context->directive_.identity_);
      LatchReplicationFailure(failure);
      co_return absl::FailedPreconditionError(failure);
    }

    while (coordinator_started_) {
      absl::Status waited = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) {
        const std::string failure =
            absl::StrCat("cluster population coordinator could not be joined: ",
                         waited.message());
        LatchReplicationFailure(failure);
        co_return waited;
      }
    }
    {
      AssertStateOwner();
      if (cluster_rebuild_ == context) cluster_rebuild_.reset();
      replica_reconfiguration_running_ = false;
    }
    context->completion_->Resolve(absl::CancelledError(std::string(reason)));
    if (promotion_context != nullptr) {
      promotion_context->completion_->Resolve(
          absl::CancelledError(std::string(reason)));
    }
    co_return absl::OkStatus();
  }

  Task<absl::Status> ReconcileClusterPopulation(
      std::optional<DesiredClusterPopulation> desired) {
    return RetireClusterPopulation(std::move(desired),
                                   /*preserve_any_ready=*/false,
                                   /*preserve_current_follow_attempt=*/false,
                                   "cluster population was retired by FDS");
  }

  Task<absl::Status> CancelInProgressClusterPopulation(
      bool preserve_current_follow_attempt) {
    // Session-scoped directives lose their observable completion channel and
    // are retired below. Steady FollowOwner is instead level-triggered FDS
    // state. Its first FULL rotates local history and intentionally reconnects
    // this same Meta session, so cancelling that exact live relationship here
    // would make every sufficiently slow follower rebuild cancel itself.
    return RetireClusterPopulation(
        std::nullopt, /*preserve_any_ready=*/true,
        preserve_current_follow_attempt,
        "in-progress cluster rebuild was cancelled after control loss");
  }

  absl::Status InstallRecoveredPopulation(
      detail::RecoveredPopulation population) {
    auto& identity = population.identity_;
    identity.target_boot_id_ = boot_id_;
    identity.directive_revision_ = 1;
    identity.authority_id_ = "recovered-population";
    identity.operation_id_ = "recovery";
    identity.directive_id_ = "recovery";
    identity.attempt_id_ = boot_id_;
    auto ready =
        cluster_group_->RecoverPopulation(identity, population.frontier_);
    if (!ready.ok()) return ready.status();
    auto frontier = std::make_shared<detail::ReplicaAppliedFrontier>(
        population.frontier_.size(), storage_->worker_count());
    absl::Status installed = frontier->InstallNextLsns(population.frontier_);
    if (!installed.ok()) return installed;
    cluster_rebuild_ =
        std::make_shared<ClusterRebuildContext>(std::move(*ready));
    applied_frontier_ = std::move(frontier);
    upstream_node_id_ = identity.source_node_id_;
    upstream_history_id_ = identity.source_history_id_;
    group_id_ = PopulationGroupToken(identity.group_id_);
    recovered_population_fenced_ = true;
    storage_->SetReplicaLoading(true);
    storage_->SetExpirationAuthority(false);
    return absl::OkStatus();
  }

  Task<absl::Status> RecoverClusterPopulation() {
    AssertStateOwner();
    auto record = co_await storage_->ConsumePopulationRecovery();
    if (!record.ok()) co_return record.status();
    if (!cluster_enabled_ || !record->has_value()) co_return absl::OkStatus();
    auto scope = detail::DecodeRecoveredPopulation((**record).identity_);
    if (!scope.ok()) co_return scope.status();
    if (scope->identity_.target_node_id_ != node_id_) {
      co_return absl::FailedPreconditionError(
          "recovered storage belongs to another Data node");
    }
    recovered_population_ = *scope;
    if ((**record).clean_proof_.empty()) {
      spdlog::info(
          "cluster population requires rebuild or operator recovery: no clean "
          "shutdown proof");
      co_return absl::OkStatus();
    }
    auto proof = detail::DecodeRecoveredPopulation((**record).clean_proof_);
    if (!proof.ok()) co_return proof.status();
    if (proof->frontier_.empty() || !detail::SameRecoveredPopulationScope(
                                        scope->identity_, proof->identity_)) {
      co_return absl::DataLossError(
          "clean shutdown proof does not match durable population");
    }
    absl::Status installed = InstallRecoveredPopulation(std::move(*proof));
    if (installed.ok())
      spdlog::info(
          "consumed clean shutdown proof; recovered candidate remains fenced");
    co_return installed;
  }

  Task<absl::Status> CancelClusterRebuildForShutdown() {
    if (bycorf::ThisWorker().id_ != 0) {
      co_return co_await bycorf::SubmitTaskTo(
          0, [this]() { return CancelClusterRebuildForShutdown(); });
    }
    if (!cluster_enabled_ || cluster_group_ == nullptr) {
      co_return absl::FailedPreconditionError(
          "population shutdown requires Meta-managed population mode");
    }
    AssertStateOwner();
    cluster_control_stopping_ = true;
    cluster_follow_owner_.reset();
    SetDesiredUpstream(std::nullopt);

    const std::shared_ptr<ClusterSourcePauseContext> source_pause =
        std::move(cluster_source_pause_);
    if (source_pause != nullptr && source_pause->expiration_pause_held_) {
      source_pause->expiration_pause_held_ = false;
      storage_->ResumeExpiration();
    }

    // A desired action may be polling for its population or awaiting the
    // shared promotion kernel. Withdraw its observation first, then join its
    // runner before population retirement touches the same private prepare
    // context. Shutdown never preserves an activation handoff: no later
    // control session in this boot may consume it.
    const std::shared_ptr<ClusterFailoverActionContext> action =
        std::move(cluster_failover_action_);
    if (action != nullptr) {
      action->cancelled_ = true;
      // The action runner forwards cancellation once after admission. Shutdown
      // can arrive after that check while the detached preparation is still at
      // a reversible barrier, so route it through the same exact-context
      // handshake used by ordinary FDS supersession.
      RequestFailoverPromotionCancellation(action);
    }
    retained_failover_activation_action_id_.reset();
    retained_failover_prepared_context_.reset();
    retained_failover_desired_action_.reset();
    activated_failover_activation_.reset();
    activated_failover_prepared_context_.reset();
    while (action != nullptr && !action->runner_finished_) {
      absl::Status waited = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) co_return waited;
    }

    co_return co_await RetireClusterPopulation(
        std::nullopt, /*preserve_any_ready=*/false,
        /*preserve_current_follow_attempt=*/false,
        "cluster population was retired for process shutdown");
  }

  void RequestShutdown() noexcept {
    replication_shutdown_requested_.store(true, std::memory_order_release);
    // These transport-only sets cover connecting/TLS sockets too. Closing
    // them wakes the owner coordinator, which cancels barriers and joins its
    // flows. Main never reads the mutable session registry or partially
    // initialized session proof/barrier fields. Late socket registration
    // observes cancellation and cannot escape this one-way shutdown fence.
    outbound_sockets_.Cancel();
    source_sockets_.Cancel();
  }

  Task<absl::Status> QuiesceForShutdown() {
    if (bycorf::ThisWorker().id_ != 0) {
      co_return co_await bycorf::SubmitTaskTo(
          0, [this]() { return QuiesceForShutdown(); });
    }
    RequestShutdown();
    absl::Status result = absl::OkStatus();
    if (cluster_enabled_) {
      // The Meta transition additionally retires its Ready proof. It is
      // idempotent when the control client already completed the same barrier.
      result = co_await CancelClusterRebuildForShutdown();
    } else {
      std::vector<std::shared_ptr<ReplicaSession>> sessions;
      std::uint64_t redis_full_sync_session = 0;
      {
        AssertStateOwner();
        role_epoch_.fetch_add(1, std::memory_order_acq_rel);
        initial_protocol_probe_pending_ = false;
        replica_reconfiguration_running_ = true;
        if (active_replica_session_ != nullptr) {
          sessions.push_back(std::move(active_replica_session_));
        }
        for (const auto& source : redis_sources_) {
          StoreRedisLink(source, false);
          source->syncing_ = false;
          if (source->session_ != nullptr) {
            sessions.push_back(std::move(source->session_));
          }
        }
        redis_full_sync_session = redis_full_sync_session_id_;
        redis_full_sync_session_id_ = 0;
        replica_session_id_ = 0;
        source_worker_count_ = 0;
      }
      for (const auto& session : sessions) session->Cancel();

      for (const auto& session : sessions) {
        absl::Status stopped = co_await CancelAndWaitForReplicaFlows(session);
        if (result.ok() && !stopped.ok()) result = stopped;
        if (!stopped.ok() || session->session_id_ == 0) continue;
        absl::Status aborted =
            co_await storage_->AbortReplicaRoot(session->session_id_);
        if (result.ok() && !aborted.ok()) result = aborted;
      }
      if (redis_full_sync_session != 0) {
        absl::Status aborted =
            co_await storage_->AbortReplicaRoot(redis_full_sync_session);
        if (result.ok() && !aborted.ok()) result = aborted;
      }
    }

    // A protocol probe owns a connection before it creates ReplicaSession.
    // The stop bit makes each completion path abstain from retrying or
    // publishing a source; join those coordinator roots too.
    for (;;) {
      bool running = coordinator_started_ || redis_topology_monitor_started_;
      for (const auto& source : redis_sources_) {
        running = running || source->coordinator_started_;
      }
      if (!running) break;
      absl::Status waited = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) {
        if (result.ok()) result = waited;
        break;
      }
    }
    // Source handshakes can enable replication logs and source flows hold
    // snapshot/log retention. Join them and disable history before the storage
    // shutdown path performs its final freeze.
    absl::Status source_retired = co_await RetireSourceHistory();
    if (result.ok() && !source_retired.ok()) result = source_retired;
    co_return result;
  }

  std::shared_ptr<detail::ClusterRebuildCompletionState>
  FindCompletedClusterPopulation(const RebuildDirective& directive) const {
    AssertStateOwner();
    if (failed_stopped_.load(std::memory_order_relaxed) ||
        replica_reconfiguration_running_ || cluster_rebuild_ == nullptr ||
        cluster_rebuild_->directive_ != directive ||
        cluster_rebuild_->state_.load(std::memory_order_relaxed) !=
            ReplicationGroupState::kReady ||
        !cluster_rebuild_->ready_token_.has_value())
      return nullptr;
    return cluster_rebuild_->completion_;
  }

  Task<ClusterPopulationStatus> cluster_population_status() const {
    if (bycorf::ThisWorker().id_ != 0) {
      co_return co_await bycorf::SubmitTaskTo(
          0, [this] { return cluster_population_status(); });
    }
    ClusterPopulationStatus result;
    result.local_node_id_ = node_id_;
    result.local_boot_id_ = boot_id_;
    result.recovered_ = recovered_population_fenced_;
    if (operator_recovery_active_) result.failover_candidate_eligible_ = false;
    if (recovered_population_.has_value() &&
        !storage_->ReplicaRecoveryFenced() &&
        !failed_stopped_.load(std::memory_order_acquire) &&
        cluster_rebuild_ == nullptr) {
      result.operator_recovery_identity_ = recovered_population_->identity_;
      result.operator_recovery_identity_->target_boot_id_ = boot_id_;
    }
    std::shared_ptr<detail::ReplicaAppliedFrontier> frontier;
    {
      AssertStateOwner();
      result.state_ =
          failed_stopped_.load(std::memory_order_relaxed)
              ? ReplicationGroupState::kFailedStopped
          : cluster_rebuild_ == nullptr
              ? ReplicationGroupState::kNotReady
              : cluster_rebuild_->state_.load(std::memory_order_relaxed);
      if (result.state_ == ReplicationGroupState::kReady &&
          cluster_rebuild_ != nullptr &&
          cluster_rebuild_->ready_token_.has_value()) {
        result.ready_token_ = cluster_rebuild_->ready_token_;
        frontier = applied_frontier_;
        if (failed_failover_candidate_.has_value() &&
            ReadyPopulationIsSuppressed(*failed_failover_candidate_)) {
          result.failover_candidate_eligible_ = false;
        }
      }
      result.failure_reason_ = failure_reason_;
    }
    // Flow workers publish frontier cells independently. This owner-local
    // sample never suspends, so the population proof cannot change beneath
    // it; only the frontier's own concurrent publication needs validation.
    if (frontier != nullptr && result.ready_token_.has_value()) {
      // A missing vector withdraws the node's previous Meta candidate, so
      // preserve bounded retries for short publication races.
      auto snapshot = frontier->TrySnapshot();
      if (snapshot.ok() &&
          snapshot->size() == result.ready_token_->cut_vector().size() &&
          std::equal(snapshot->begin(), snapshot->end(),
                     result.ready_token_->cut_vector().begin(),
                     [](std::uint64_t live, std::uint64_t cut) {
                       return live >= cut;
                     })) {
        result.applied_next_lsns_ = std::move(*snapshot);
      }
    }
    co_return result;
  }

  Task<absl::Status> AuthorizeClusterRebuildSource(RebuildDirective directive) {
    if (bycorf::ThisWorker().id_ != 0) {
      co_return co_await bycorf::SubmitTaskTo(
          0, [this, directive = std::move(directive)]() mutable {
            return AuthorizeClusterRebuildSource(std::move(directive));
          });
    }
    if (!cluster_enabled_ || cluster_group_ == nullptr) {
      co_return absl::FailedPreconditionError(
          "cluster source authorization requires Meta-managed population "
          "mode");
    }
    const RebuildIdentity& identity = directive.identity_;
    if (!directive.safe_source_active_ ||
        directive.flow_count_ != storage_->worker_count() ||
        identity.term_ == 0 || identity.directive_revision_ == 0 ||
        identity.group_id_.empty() || identity.assignment_id_.empty() ||
        identity.authority_id_.empty() || identity.operation_id_.empty() ||
        identity.directive_id_.empty() || identity.attempt_id_.empty() ||
        identity.manifest_revision_ == 0 || identity.target_node_id_.empty() ||
        identity.target_boot_id_.empty() ||
        identity.source_assignment_id_.empty() ||
        identity.source_node_id_ != node_id_ ||
        identity.source_boot_id_ != boot_id_) {
      co_return absl::InvalidArgumentError(
          "cluster source authorization identity is incomplete or local "
          "source incarnation does not match");
    }
    for (;;) {
      detail::SourceAuthorizationAction action;
      {
        co_await master_mutex_.Lock(*bycorf::ThisWorker().self_);
        bycorf::CrossWorkerMutex::Guard master_lock(&master_mutex_);
        if (identity.source_history_id_ != history_id_) {
          co_return absl::FailedPreconditionError(
              "cluster source authorization uses stale source history");
        }
        AssertStateOwner();
        if (cluster_source_revocations_in_flight_ != 0) {
          co_return absl::FailedPreconditionError(
              "cluster source authorization is being revoked");
        }
        if (role_.load(std::memory_order_acquire) != ReplicationRole::kMaster ||
            upstream_.has_value() ||
            !native_dataset_valid_.load(std::memory_order_acquire)) {
          co_return absl::FailedPreconditionError(
              "cluster source is not the active local primary");
        }
        if (cluster_rebuild_ == nullptr ||
            cluster_rebuild_->state_.load(std::memory_order_relaxed) !=
                ReplicationGroupState::kReady ||
            !cluster_rebuild_->ready_token_.has_value() ||
            cluster_rebuild_->ready_token_->identity().group_id_ !=
                identity.group_id_ ||
            cluster_rebuild_->ready_token_->identity().assignment_id_ !=
                identity.source_assignment_id_ ||
            cluster_rebuild_->ready_token_->identity().manifest_revision_ !=
                identity.manifest_revision_ ||
            cluster_rebuild_->ready_token_->identity().manifest_id_ !=
                identity.manifest_id_ ||
            cluster_rebuild_->ready_token_->identity()
                    .partition_replication_epoch_ !=
                identity.partition_replication_epoch_) {
          co_return absl::FailedPreconditionError(
              "cluster source population is not ready for this "
              "group/manifest");
        }
        // assignment_id_ names the rebuild target. source_assignment_id_
        // separately binds the local ReadyToken, so remove/re-add of the same
        // stable source node cannot revive an old export authorization.
        auto authorized = source_authorizations_.Authorize(directive);
        if (!authorized.ok()) co_return authorized.status();
        action = *authorized;
      }
      if (action != detail::SourceAuthorizationAction::kRevokeOlder) {
        co_return absl::OkStatus();
      }

      // A new revision replaces source authority as one whole-session action.
      // Do not publish it beside older capabilities: cancel/join their exports
      // first, then retry against the retained watermark and current
      // role/history.
      absl::Status revoked =
          co_await RevokeClusterRebuildSourceAuthorizations();
      if (!revoked.ok()) co_return revoked;
    }
  }

  Task<absl::Status> RevokeClusterRebuildSourceAuthorizations() {
    return RetireClusterRebuildSourceAuthorizations(
        SourceAuthorizationRetirementMode::kStrongRevoke,
        /*preserve_current_population_exports=*/false);
  }

  Task<absl::Status> EnableClusterRebuildSourceAdmissionUntil(
      std::chrono::nanoseconds deadline_since_boot) {
    if (bycorf::ThisWorker().id_ != 0) {
      co_return co_await bycorf::SubmitTaskTo(0, [this, deadline_since_boot] {
        return EnableClusterRebuildSourceAdmissionUntil(deadline_since_boot);
      });
    }
    if (!cluster_enabled_ || cluster_group_ == nullptr) {
      co_return absl::FailedPreconditionError(
          "cluster source admission requires Meta-managed population mode");
    }
    co_await master_mutex_.Lock(*bycorf::ThisWorker().self_);
    bycorf::CrossWorkerMutex::Guard lock(&master_mutex_);
    AssertStateOwner();
    if (cluster_control_stopping_ ||
        cluster_source_revocations_in_flight_ != 0 ||
        role_.load(std::memory_order_acquire) != ReplicationRole::kMaster ||
        upstream_.has_value() || is_loading() ||
        !native_dataset_valid_.load(std::memory_order_acquire) ||
        cluster_rebuild_ == nullptr ||
        cluster_rebuild_->state_.load(std::memory_order_relaxed) !=
            ReplicationGroupState::kReady ||
        !cluster_rebuild_->ready_token_.has_value()) {
      co_return absl::FailedPreconditionError(
          "cluster source admission requires an active ready owner");
    }
    const std::chrono::nanoseconds now_since_boot =
        cluster::LeaseClockNow().time_since_epoch();
    if (deadline_since_boot <= now_since_boot) {
      co_return absl::DeadlineExceededError(
          "cluster source admission lease already expired");
    }
    source_authorizations_.EnableLeaseAdmissionUntil(deadline_since_boot);
    co_return absl::OkStatus();
  }

  Task<absl::Status>
  ClearClusterRebuildSourceAuthorizationsForSessionReplacement(
      bool preserve_established_exports) {
    return RetireClusterRebuildSourceAuthorizations(
        SourceAuthorizationRetirementMode::kSessionReplacement,
        preserve_established_exports);
  }

  Task<absl::Status> RefreshClusterRebuildSourceAuthorizationsForFdsReplacement(
      bool preserve_current_population_exports,
      std::size_t expected_authorization_replays) {
    return RetireClusterRebuildSourceAuthorizations(
        SourceAuthorizationRetirementMode::kFdsReplacement,
        preserve_current_population_exports, expected_authorization_replays);
  }

  enum class SourceAuthorizationRetirementMode {
    kStrongRevoke,
    kSessionReplacement,
    kFdsReplacement,
  };

  Task<absl::Status> RetireClusterRebuildSourceAuthorizations(
      SourceAuthorizationRetirementMode mode,
      bool preserve_current_population_exports,
      std::size_t expected_authorization_replays = 0) {
    if (bycorf::ThisWorker().id_ != 0) {
      co_return co_await bycorf::SubmitTaskTo(
          0, [this, mode, preserve_current_population_exports,
              expected_authorization_replays] {
            return RetireClusterRebuildSourceAuthorizations(
                mode, preserve_current_population_exports,
                expected_authorization_replays);
          });
    }
    if (!cluster_enabled_ || cluster_group_ == nullptr) {
      co_return absl::FailedPreconditionError(
          "cluster source revocation requires Meta-managed population mode");
    }
    struct RevocationGuard {
      unsigned* in_flight_ = nullptr;
      bool active_ = false;
      ~RevocationGuard() {
        if (!active_) return;
        AssertStateOwner();
        assert(*in_flight_ != 0);
        --*in_flight_;
      }
    } revocation_guard{&cluster_source_revocations_in_flight_};
    std::vector<std::shared_ptr<MasterSession>> sessions;
    {
      co_await master_mutex_.Lock(*bycorf::ThisWorker().self_);
      bycorf::CrossWorkerMutex::Guard lock(&master_mutex_);
      {
        AssertStateOwner();
        ++cluster_source_revocations_in_flight_;
        revocation_guard.active_ = true;
        if (mode != SourceAuthorizationRetirementMode::kStrongRevoke) {
          if (mode == SourceAuthorizationRetirementMode::kFdsReplacement) {
            source_authorizations_.ClearActiveForFdsReplacement(
                expected_authorization_replays);
          } else {
            source_authorizations_.ClearActiveForSessionReplacement();
          }
        } else {
          source_authorizations_.RevokeAll();
        }
      }
      sessions.reserve(master_sessions_.size() +
                       retired_master_sessions_.size());
      sessions.insert(sessions.end(), retired_master_sessions_.begin(),
                      retired_master_sessions_.end());
      for (auto session = master_sessions_.begin();
           session != master_sessions_.end();) {
        const bool current_population_export =
            preserve_current_population_exports &&
            session->second->population_export_ != nullptr;
        // Session loss invalidates the transport incarnation and may retain
        // only ONLINE exports. A live FDS replacement has already proven the
        // exact source/population scope unchanged; retain every session that
        // was published under that scope, including the LVFULLRESYNC-to-ONLINE
        // window. Cancelling that window turns a safe projection refresh into
        // an unclassified peer-close after target admission.
        const bool preserve =
            current_population_export &&
            (mode == SourceAuthorizationRetirementMode::kFdsReplacement ||
             session->second->online());
        if (preserve) {
          ++session;
          continue;
        }
        const std::shared_ptr<MasterSession> retiring = session->second;
        const auto erase = session++;
        master_sessions_.erase(erase);
        sessions.push_back(retiring);
        retired_master_sessions_.push_back(std::move(retiring));
      }
      // Removing the registry entries under the same mutex as LVPSYNC
      // publication prevents a revoked control session from accepting a late
      // LVFLOW while cancellation is propagating.
      disconnected_replica_leases_.clear();
    }
#if LAVIK_FAULTS_ENABLED
    // A test barrier observes the completed ledger/publication critical
    // section before cancellation joins an admission deliberately paused at
    // its second check. Release builds contain neither the environment lookup
    // nor the extra syscall.
    absl::Status revocation_signalled = SignalFaultBarrier(
        "LAVIK_REPLICATION_SOURCE_REVOCATION_BARRIER_ACK_PATH",
        "source authorization fault barrier");
    if (!revocation_signalled.ok()) co_return revocation_signalled;
#endif
    for (const auto& session : sessions) session->Cancel();
    auto next_warning =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (
        active_unpublished_master_controls_.load(std::memory_order_acquire) !=
            0 ||
        std::any_of(sessions.begin(), sessions.end(), [](const auto& session) {
          return session->control_active() || session->connected_flows() != 0;
        })) {
      absl::Status waited = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) co_return waited;
      if (std::chrono::steady_clock::now() >= next_warning) {
        spdlog::warn(
            "waiting for revoked cluster source handshakes/flows to release "
            "their population snapshots");
        next_warning =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
      }
    }
    {
      co_await master_mutex_.Lock(*bycorf::ThisWorker().self_);
      bycorf::CrossWorkerMutex::Guard lock(&master_mutex_);
      // Finalization normally creates reconnect leases for cleanly
      // disconnected online sessions. Authority revocation is stronger: no
      // session retired by this transition may preserve such a lease.
      FinalizeRetiredMasterSessionsLocked();
      disconnected_replica_leases_.clear();
    }
    co_return absl::OkStatus();
  }

  // Shared durability/history kernel for native Cluster promotion and
  // standalone/Sentinel REPLICAOF NO ONE. The caller owns admission drain,
  // Function catalog exclusion, and expiration quiescence. Success creates a
  // child publisher but deliberately does not open serving or expiration.
  Task<absl::StatusOr<ClusterPromotionPrepared>> PreparePromotion(
      storage::PromotionBase promotion_base) {
    auto population = storage_->RecoverPopulationToken();
    if (!population.ok()) co_return population.status();
    promotion_base.population_token_ = *population;
    promotion_base.catalog_token_ = GlobalFunctionCatalog().durability_token();

    if (ShouldInjectPromotionPrepareFailure("storage-barrier")) {
      co_return absl::InternalError(
          "injected promotion-prepare storage barrier failure");
    }
    absl::Status durable = co_await storage_->MakeDurable(
        promotion_base.parent_frontier_, promotion_base.storage_accumulator_);
    if (!durable.ok()) co_return durable;
    if (promotion_base.catalog_token_ !=
        GlobalFunctionCatalog().durability_token()) {
      co_return absl::AbortedError(
          "Function catalog changed while preparing promotion");
    }
    if (ShouldInjectPromotionPrepareFailure("promotion-base")) {
      co_return absl::InternalError(
          "injected promotion-prepare base commit failure");
    }
    absl::Status committed =
        co_await storage_->CommitPromotionBase(promotion_base);
    if (!committed.ok()) co_return committed;

    absl::Status retired = co_await RetireSourceHistory();
    if (!retired.ok()) co_return retired;
    if (ShouldInjectPromotionPrepareFailure("child-history")) {
      co_return absl::InternalError(
          "injected promotion-prepare child history failure");
    }
    const std::uint64_t child_log_epoch =
        role_epoch_.load(std::memory_order_acquire);
    for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
      const std::size_t flow_capacity = BacklogCapacityForFlow(
          worker, backlog_size_bytes_.load(std::memory_order_acquire));
      absl::Status enabled = co_await bycorf::SubmitTaskTo(
          worker,
          [this, child_log_epoch, flow_capacity]() -> Task<absl::Status> {
            co_return co_await storage_->EnableReplicationLog(child_log_epoch,
                                                              flow_capacity);
          });
      if (!enabled.ok()) co_return enabled;
    }

    std::string child_history;
    {
      co_await master_mutex_.Lock(*bycorf::ThisWorker().self_);
      bycorf::CrossWorkerMutex::Guard lock(&master_mutex_);
      child_history = history_id_;
    }
    co_return ClusterPromotionPrepared{
        .parent_history_id_ = promotion_base.parent_history_id_,
        .frozen_applied_next_lsns_ =
            promotion_base.parent_frontier_.flow_cursors_,
        .population_generation_ = promotion_base.population_token_.generation_,
        .population_digest_ = promotion_base.population_token_.digest_,
        .catalog_generation_ =
            promotion_base.catalog_token_.catalog_generation_,
        .catalog_dump_crc64_ = promotion_base.catalog_token_.dump_crc64_,
        .child_history_id_ = std::move(child_history),
    };
  }

  // Opens the prepared publisher/storage role without assuming ownership of
  // any expiration pause or authority capability. Cluster uses this kernel
  // between provisional lease validation and NodeControl's final lease/FDS
  // recheck; standalone adds its own expiration pairing below.
  void ActivatePreparedPromotionRole() {
    {
      AssertStateOwner();
      StoreRole(ReplicationRole::kMaster, std::memory_order_release);
    }
    if (!storage_->ReplicaRecoveryFenced()) {
      storage_->SetReplicaLoading(false);
    }
  }

  // Standalone/Sentinel has no external authority commit between prepare and
  // activation, so its synchronous command also consumes the expiration pause
  // that SetUpstream acquired and restores permanent expiration authority.
  void ActivatePreparedPromotion() {
    AssertStateOwner();
    pending_promotion_.reset();
    ActivatePreparedPromotionRole();
    if (!storage_->ReplicaRecoveryFenced()) {
      storage_->SetExpirationAuthority(true);
    }
    storage_->ResumeExpiration();
  }

  Task<absl::Status> SetUpstream(std::optional<ReplicaOfConfig> upstream) {
    if (bycorf::ThisWorker().id_ != 0) {
      co_return co_await bycorf::SubmitTaskTo(
          0, [this, upstream = std::move(upstream)]() mutable {
            return SetUpstream(std::move(upstream));
          });
    }
    if (cluster_enabled_) {
      co_return absl::FailedPreconditionError(
          "REPLICAOF is unavailable in cluster mode");
    }
    if (replication_shutdown_requested_) {
      co_return absl::CancelledError(
          "replication reconfiguration stopped for process shutdown");
    }
    if (failed_stopped_.load(std::memory_order_acquire)) {
      AssertStateOwner();
      co_return absl::FailedPreconditionError(absl::StrCat(
          "replication is failed-stopped until restart: ", failure_reason_));
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

    // A coordinator may already own automatic teardown. Do not race its
    // candidate abort with the explicit transition's cancellation and join.
    for (;;) {
      bool teardown_running = false;
      {
        AssertStateOwner();
        if (replica_reconfiguration_running_) {
          co_return absl::FailedPreconditionError(
              "another replication role transition is active");
        }
        teardown_running = replica_session_teardown_running_;
        if (!teardown_running) replica_reconfiguration_running_ = true;
      }
      if (!teardown_running) break;
      absl::Status waited = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) co_return waited;
    }
    struct ReconfigurationGuard {
      bool& running_;
      ~ReconfigurationGuard() {
        AssertStateOwner();
        running_ = false;
      }
    } reconfiguration_guard{replica_reconfiguration_running_};

    // Stop old upstream ingress while database admission is still open. A
    // command that already crossed the apply-FIFO boundary may be waiting for
    // that admission; closing it first would deadlock role transition against
    // the flow that must publish the final applied cursor. Complete read-ahead
    // events that have not started apply remain unacknowledged and are outside
    // the frozen promotion frontier. The reconfiguration gate must already
    // be closed before moving the session: changing role_epoch alone cannot
    // stop the coordinator from retrying with that new epoch while join
    // suspends. Such a retry could invalidate the population being promoted.
    std::optional<std::uint64_t> detached_role_epoch;
    std::vector<std::shared_ptr<ReplicaSession>> draining_sessions;
    bool retire_source_before_gates = false;
    {
      AssertStateOwner();
      const bool had_upstream = upstream_.has_value();
      if (had_upstream || upstream.has_value()) {
        detached_role_epoch =
            role_epoch_.fetch_add(1, std::memory_order_acq_rel) + 1;
        StoreRole(upstream.has_value() ? ReplicationRole::kConnecting
                                       : ReplicationRole::kSyncing,
                  std::memory_order_release);
        // A master full-sync flow can hold the command gates while waiting
        // for its target to acknowledge the catalog. Retire source egress
        // before this transition tries to acquire those same gates.
        retire_source_before_gates = !had_upstream && upstream.has_value();
        if (active_replica_session_ != nullptr) {
          draining_sessions.push_back(std::move(active_replica_session_));
        }
        for (const auto& source : redis_sources_) {
          if (source->session_ == nullptr) continue;
          draining_sessions.push_back(std::move(source->session_));
        }
      }
    }
    for (const auto& session : draining_sessions) {
      absl::Status stopped = co_await CancelAndWaitForReplicaFlows(session);
      if (!stopped.ok()) {
        LatchReplicationFailure(session->FailStopReason().value_or(
            absl::StrCat("replica cancellation/join outcome is uncertain: ",
                         stopped.message())));
        co_return stopped;
      }
      if (std::optional<std::string> fail_stop = session->FailStopReason();
          fail_stop.has_value()) {
        // The session can cross an uncertain promotion boundary before this
        // role change takes ownership of teardown. A successful join cannot
        // make that storage outcome knowable, and aborting it would mutate
        // state after the uncertainty point.
        LatchReplicationFailure(*fail_stop);
        co_return absl::FailedPreconditionError(absl::StrCat(
            "replication is failed-stopped until restart: ", *fail_stop));
      }
      if (session->session_id_ != 0) {
        absl::Status discarded =
            co_await storage_->AbortReplicaRoot(session->session_id_);
        if (!discarded.ok()) {
          LatchReplicationFailure(absl::StrCat(
              "replica abort outcome is uncertain: ", discarded.message()));
          co_return discarded;
        }
      }
    }
    if (retire_source_before_gates) {
      absl::Status drained = co_await DrainSourceEgress();
      if (!drained.ok()) co_return drained;
    }

    while (!CloseAllCommandDbGates()) {
      absl::Status waited = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) co_return waited;
    }
    struct RoleGateGuard {
      ~RoleGateGuard() { OpenAllCommandDbGates(); }
    } role_gate;
    while (CommandDbOperationsActive()) {
      absl::Status waited = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) co_return waited;
    }

    // FUNCTION and FCALL take database admission before the Function guard.
    // Close and drain that admission first so promotion cannot hold the guard
    // while waiting for a command that is itself waiting for the guard. Once
    // the gates are closed, no new catalog user can enter before token capture.
    std::unique_ptr<FunctionCatalogOperationGuard> promotion_catalog_guard;
    if (!upstream.has_value()) {
      promotion_catalog_guard = co_await AcquireFunctionCatalogOperation();
    }

    absl::Status quiesced = co_await storage_->QuiesceExpiration();
    if (!quiesced.ok()) co_return quiesced;
    bool expiration_quiesced = true;
    struct ExpirationResumeGuard {
      storage::StorageEngine* storage_ = nullptr;
      bool* quiesced_ = nullptr;
      ~ExpirationResumeGuard() {
        if (storage_ != nullptr && quiesced_ != nullptr && *quiesced_) {
          storage_->ResumeExpiration();
        }
      }
    } expiration_resume{storage_, &expiration_quiesced};
    if (upstream.has_value()) {
      // Stop maintenance before the destructive replica transition. Tomb
      // Raider checks this boundary again at its safe yield points.
      absl::Status maintenance =
          co_await storage_->QuiesceTombRaiderForReplica();
      if (!maintenance.ok()) co_return maintenance;
      storage_->SetExpirationAuthority(false);
      storage_->ResumeExpiration();
      expiration_quiesced = false;
    }

    if (failed_stopped_.load(std::memory_order_acquire)) {
      AssertStateOwner();
      co_return absl::FailedPreconditionError(absl::StrCat(
          "replication is failed-stopped until restart: ", failure_reason_));
    }

    std::vector<std::shared_ptr<ReplicaSession>> cancelled;
    bool start_upstream = false;
    bool discard_incomplete_root = false;
    bool promotion_required = false;
    storage::PromotionBase promotion_base;
    {
      AssertStateOwner();
      if (failed_stopped_.load(std::memory_order_relaxed)) {
        co_return absl::FailedPreconditionError(absl::StrCat(
            "replication is failed-stopped until restart: ", failure_reason_));
      }
      const bool had_upstream = upstream_.has_value();
      const bool retry_promotion =
          !upstream.has_value() && pending_promotion_.has_value();
      bool dataset_valid =
          native_dataset_valid_.load(std::memory_order_acquire);
      for (const auto& source : redis_sources_) {
        dataset_valid = dataset_valid ||
                        source->dataset_valid_.load(std::memory_order_acquire);
      }
      // A lost transport changes ONLINE to CONNECTING, but the last committed
      // replica root remains a valid promotion candidate. Only an explicit
      // source switch whose replacement full sync has not reached its cut
      // publishes an empty dataset on REPLICAOF NO ONE.
      discard_incomplete_root = !retry_promotion && !upstream.has_value() &&
                                had_upstream && !dataset_valid;
      promotion_required = retry_promotion || (!upstream.has_value() &&
                                               had_upstream && dataset_valid);
      if (retry_promotion) {
        promotion_base = *pending_promotion_;
      } else if (promotion_required) {
        promotion_base.group_id_ = group_id_;
        promotion_base.parent_history_id_ =
            upstream_history_id_.value_or("redis-parent");
        promotion_base.parent_frontier_.history_context_ =
            promotion_base.parent_history_id_;
        if (!redis_sources_.empty()) {
          promotion_base.parent_frontier_.flow_cursors_.reserve(
              redis_sources_.size());
          for (const auto& source : redis_sources_) {
            promotion_base.parent_frontier_.flow_cursors_.push_back(
                source->offset_.load(std::memory_order_acquire) + 1);
          }
        } else if (applied_frontier_ != nullptr) {
          auto snapshot = applied_frontier_->TrySnapshot();
          if (!snapshot.ok()) co_return snapshot.status();
          promotion_base.parent_frontier_.flow_cursors_ = std::move(*snapshot);
        }
        promotion_base.storage_accumulator_ = absl::StrCat(
            "role-epoch:", role_epoch_.load(std::memory_order_relaxed));
        // Retain only the frozen parent-side proof. Population and catalog
        // tokens are refreshed on every retry under their own durability
        // barriers.
        pending_promotion_ = promotion_base;
      }
      if (upstream.has_value()) pending_promotion_.reset();
      SetDesiredUpstream(upstream);
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
      redis_full_sync_session_id_ = 0;
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
      applied_frontier_.reset();
      std::uint64_t next_epoch = 0;
      if (detached_role_epoch.has_value()) {
        next_epoch = *detached_role_epoch;
      } else {
        next_epoch = role_epoch_.fetch_add(1, std::memory_order_acq_rel) + 1;
      }
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
                : promotion_required  ? ReplicationRole::kSyncing
                                      : ReplicationRole::kMaster,
                std::memory_order_release);
      start_upstream = upstream_.has_value();
    }
    if (promotion_required) {
      // From this point onward any failed durability or child-history step is
      // fail-closed. The role remains syncing and request handling observes
      // LOADING until a later promotion attempt or full sync succeeds.
      storage_->SetReplicaLoading(true);
    }
    for (const auto& session : cancelled) session->Cancel();
    for (const auto& session : cancelled) {
      absl::Status stopped = co_await CancelAndWaitForReplicaFlows(session);
      if (!stopped.ok()) {
        LatchReplicationFailure(session->FailStopReason().value_or(
            absl::StrCat("replica cancellation/join outcome is uncertain: ",
                         stopped.message())));
        co_return stopped;
      }
      if (std::optional<std::string> fail_stop = session->FailStopReason();
          fail_stop.has_value()) {
        // A concurrent session may have crossed an uncertain promotion/cut
        // before reconfiguration took ownership of its teardown. Successful
        // join does not make that storage outcome knowable, and aborting the
        // candidate would be another mutation after the uncertainty point.
        LatchReplicationFailure(*fail_stop);
        co_return absl::FailedPreconditionError(absl::StrCat(
            "replication is failed-stopped until restart: ", *fail_stop));
      }
      if (session->session_id_ != 0) {
        absl::Status discarded =
            co_await storage_->AbortReplicaRoot(session->session_id_);
        if (!discarded.ok()) {
          LatchReplicationFailure(absl::StrCat(
              "replica abort outcome is uncertain: ", discarded.message()));
          co_return discarded;
        }
      }
    }
    if (start_upstream) {
      absl::Status retired;
      if (retire_source_before_gates) {
        retired = co_await DisableSourceHistory();
      } else {
        retired = co_await RetireSourceHistory();
      }
      if (!retired.ok()) co_return retired;
    }
    if (discard_incomplete_root) {
      absl::Status cleared = co_await storage_->FlushAllDetach();
      if (!cleared.ok()) {
        LatchReplicationFailure(
            absl::StrCat("replica empty-root cleanup outcome is uncertain: ",
                         cleared.message()));
        co_return cleared;
      }
    }
    if (promotion_required) {
      auto prepared = co_await PreparePromotion(std::move(promotion_base));
      if (!prepared.ok()) co_return prepared.status();
      ActivatePreparedPromotion();
      expiration_quiesced = false;
    }
    if (!start_upstream && !promotion_required) {
      if (!storage_->ReplicaRecoveryFenced()) {
        storage_->SetReplicaLoading(false);
        storage_->SetExpirationAuthority(true);
      }
      storage_->ResumeExpiration();
      expiration_quiesced = false;
    }
    {
      AssertStateOwner();
      replica_reconfiguration_running_ = false;
      if (failed_stopped_.load(std::memory_order_relaxed)) {
        co_return absl::FailedPreconditionError(absl::StrCat(
            "replication is failed-stopped until restart: ", failure_reason_));
      }
    }
    if (start_upstream && StorageIsReady()) StartCoordinator();
    co_return absl::OkStatus();
  }

  Task<absl::Status> AddUpstream(ReplicaOfConfig upstream) {
    if (bycorf::ThisWorker().id_ != 0) {
      co_return co_await bycorf::SubmitTaskTo(
          0, [this, upstream = std::move(upstream)]() mutable {
            return AddUpstream(std::move(upstream));
          });
    }
    if (cluster_enabled_) {
      co_return absl::FailedPreconditionError(
          "ADDREPLICAOF is unavailable in cluster mode");
    }
    if (failed_stopped_.load(std::memory_order_acquire)) {
      AssertStateOwner();
      co_return absl::FailedPreconditionError(absl::StrCat(
          "replication is failed-stopped until restart: ", failure_reason_));
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
      AssertStateOwner();
      if (failed_stopped_.load(std::memory_order_relaxed)) {
        co_return absl::FailedPreconditionError(absl::StrCat(
            "replication is failed-stopped until restart: ", failure_reason_));
      }
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

  Task<absl::StatusOr<std::optional<NativeReplicationWatermark>>>
  CaptureNativeReplicationWatermark() {
    if (bycorf::ThisWorker().id_ != 0) {
      co_return co_await bycorf::SubmitTaskTo(
          0, [this]() { return CaptureNativeReplicationWatermark(); });
    }

    // History reset owns worker 0 and can replace every flow's LSN domain.
    // Finish or observe that transition before constructing an all-flow cut.
    absl::Status history_ready = co_await ResetInvalidReplicationHistory();
    if (!history_ready.ok()) co_return history_ready;

    std::string history_id;
    {
      co_await master_mutex_.Lock(*bycorf::ThisWorker().self_);
      bycorf::CrossWorkerMutex::Guard lock(&master_mutex_);
      history_id = history_id_;
    }
    std::vector<std::uint64_t> next_lsns(storage_->worker_count());
    // Keep the suspension paths in separate statements. GCC 13 can reuse the
    // wrong coroutine-frame slot when both arms of ?: contain co_await.
    for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
      auto fence = [this]() { return storage_->FenceReplicationLog(); };
      absl::StatusOr<std::uint64_t> next{
          absl::UnknownError("replication-log fence was not dispatched")};
      if (worker == bycorf::ThisWorker().id_) {
        next = co_await fence();
      } else {
        next = co_await bycorf::SubmitTaskTo(worker, fence);
      }
      if (!next.ok()) {
        // With no native consumer the runtime backlog is intentionally
        // disabled. WAIT keeps the connection dirty and retries if a replica
        // arrives before its timeout instead of treating that idle state as a
        // command error.
        if (absl::IsFailedPrecondition(next.status())) {
          co_return std::optional<NativeReplicationWatermark>{};
        }
        co_return next.status();
      }
      next_lsns[worker] = *next;
    }
    {
      co_await master_mutex_.Lock(*bycorf::ThisWorker().self_);
      bycorf::CrossWorkerMutex::Guard lock(&master_mutex_);
      if (history_id_ != history_id) {
        co_return std::optional<NativeReplicationWatermark>{};
      }
    }
    co_return std::optional<NativeReplicationWatermark>(
        NativeReplicationWatermark{.history_id_ = std::move(history_id),
                                   .next_lsns_ = std::move(next_lsns)});
  }

  Task<std::optional<std::uint64_t>> CountAcknowledgedNativeReplicas(
      const NativeReplicationWatermark& watermark) const {
    co_await master_mutex_.Lock(*bycorf::ThisWorker().self_);
    bycorf::CrossWorkerMutex::Guard lock(&master_mutex_);
    if (watermark.history_id_ != history_id_) co_return std::nullopt;
    std::uint64_t count = 0;
    for (const auto& [session_id, session] : master_sessions_) {
      (void)session_id;
      count += session->Acknowledged(watermark) ? 1 : 0;
    }
    co_return count;
  }

  Task<std::uint64_t> CountOnlineNativeReplicas() const {
    co_await master_mutex_.Lock(*bycorf::ThisWorker().self_);
    bycorf::CrossWorkerMutex::Guard lock(&master_mutex_);
    co_return static_cast<std::uint64_t>(std::count_if(
        master_sessions_.begin(), master_sessions_.end(),
        [](const auto& entry) { return entry.second->online(); }));
  }

  Task<ReplicationIdentity> identity() const {
    co_await master_mutex_.Lock(*bycorf::ThisWorker().self_);
    bycorf::CrossWorkerMutex::Guard lock(&master_mutex_);
    co_return ReplicationIdentity{
        .local_node_id_ = node_id_,
        .boot_id_ = boot_id_,
        .local_history_id_ = history_id_,
    };
  }

  std::optional<ReplicaOfConfig> upstream() const {
    if (bycorf::ThisWorker().self_ == nullptr) {
      return published_upstream_.load(std::memory_order_acquire)->endpoint_;
    }
    assert(bycorf::ThisWorker().id_ < storage_->worker_count());
    auto& cached = upstream_caches_[bycorf::ThisWorker().id_];
    if (cached.version_ != upstream_version_.load(std::memory_order_acquire)) {
      // Only a configuration change enters atomic shared_ptr's cold path.
      // Normal MOVED replies copy their worker's cached endpoint after one
      // read-only atomic version load, without a shared reference-count RMW.
      cached = *published_upstream_.load(std::memory_order_acquire);
    }
    return cached.endpoint_;
  }

  Task<ReplicationStatus> status() const {
    if (bycorf::ThisWorker().id_ != 0) {
      co_return co_await bycorf::SubmitTaskTo(0, [this] { return status(); });
    }
    ReplicationStatus result;
    result.role_ = role_.load(std::memory_order_acquire);
    result.role_epoch_ = role_epoch_.load(std::memory_order_acquire);
    result.local_node_id_ = node_id_;
    result.boot_id_ = boot_id_;
    result.replica_incarnation_ = replica_incarnation_;
    {
      AssertStateOwner();
      result.group_id_ = group_id_;
      result.failed_stopped_ = failed_stopped_.load(std::memory_order_acquire);
      result.failure_reason_ = failure_reason_;
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
      if (result.redis_sources_.empty() && applied_frontier_ != nullptr) {
        // INFO/ROLE expose a compatibility scalar. Sampling current cells
        // keeps a busy coherent snapshot from becoming a false zero offset,
        // without retaining an old history's total across a frontier reset.
        result.replica_repl_offset_ =
            applied_frontier_->ApproximateTotalNextLsn();
      }
      result.replica_priority_ =
          replica_priority_.load(std::memory_order_acquire);
      if (result.upstream_.has_value()) {
        std::uint64_t down_seconds = 0;
        if (!redis_sources_.empty()) {
          for (const auto& source : redis_sources_) {
            if (!source->link_up_.load(std::memory_order_acquire)) {
              down_seconds =
                  std::max(down_seconds,
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
      co_await master_mutex_.Lock(*bycorf::ThisWorker().self_);
      bycorf::CrossWorkerMutex::Guard lock(&master_mutex_);
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
    co_return result;
  }

  void StoreRole(ReplicationRole next, std::memory_order order) noexcept {
    constexpr std::uint64_t kServingOpen = 1;
    const auto serves_dataset = [](ReplicationRole role) {
      return role == ReplicationRole::kMaster ||
             role == ReplicationRole::kOnline;
    };

    // Construction precedes worker startup; afterwards only worker zero may
    // publish a role. These two stores cannot interleave with another role
    // transition because there is no suspension between them. Data-command
    // admission still reads only the packed atomic generation/open token.
    assert(bycorf::ThisWorker().self_ == nullptr ||
           bycorf::ThisWorker().id_ == 0);
    const ReplicationRole previous = role_.load(std::memory_order_relaxed);
    if (previous == next) {
      if (next == ReplicationRole::kOnline) {
        link_state_changed_nanos_.store(SteadyNanos(),
                                        std::memory_order_release);
      }
      return;
    }

    const bool was_serving = serves_dataset(previous);
    const bool will_serve = serves_dataset(next);
    if (was_serving) {
      const std::uint64_t current =
          serving_generation_->load(std::memory_order_relaxed);
      std::uint64_t generation = (current & ~kServingOpen) + 2;
      if (generation == 0) generation = 2;  // Reserve zero for closed capture.
      serving_generation_->store(generation | (will_serve ? kServingOpen : 0),
                                 std::memory_order_release);
      // Blocking commands own no DB gate while asleep. Wake all of them so
      // they can observe the new generation before examining replacement
      // data; baseline population is not required to emit key notifications.
      NotifyServingGenerationChanged();
    }

    // Closing publishes the generation fence before the non-serving role.
    // Opening publishes the role first and the open bit last, so no command
    // can capture a generation that has not yet become authoritative.
    role_.store(next, order);
    if (!was_serving && will_serve) {
      const std::uint64_t current =
          serving_generation_->load(std::memory_order_relaxed);
      serving_generation_->store(current | kServingOpen,
                                 std::memory_order_release);
    }
    if (next == ReplicationRole::kOnline ||
        previous == ReplicationRole::kOnline ||
        previous == ReplicationRole::kMaster) {
      link_state_changed_nanos_.store(SteadyNanos(), std::memory_order_release);
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

  std::uint64_t role_epoch() const noexcept {
    return role_epoch_.load(std::memory_order_acquire);
  }

  bool is_redis_follower() const noexcept {
    return redis_psync_.load(std::memory_order_acquire);
  }

  bool is_loading() const noexcept {
    const ReplicationRole role = role_.load(std::memory_order_acquire);
    return storage_->ReplicaRecoveryFenced() ||
           role == ReplicationRole::kConnecting ||
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
    if (bycorf::ThisWorker().id_ != 0) {
      co_return co_await bycorf::SubmitTaskTo(
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
            co_await bycorf::SubmitTaskTo(worker, [this, flow_capacity]() {
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

  Task<absl::Status> SetBacklogBackpressure(bool enabled) {
    if (bycorf::ThisWorker().id_ != 0) {
      co_return co_await bycorf::SubmitTaskTo(
          0, [this, enabled]() { return SetBacklogBackpressure(enabled); });
    }
    // Every log has a worker-local waiter. Apply the policy and wake each
    // owner before publishing the CONFIG value as successfully installed.
    for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
      absl::Status configured;
      if (worker == 0) {
        configured =
            co_await storage_->SetReplicationBacklogBackpressure(enabled);
      } else {
        configured = co_await bycorf::SubmitTaskTo(worker, [this, enabled]() {
          return storage_->SetReplicationBacklogBackpressure(enabled);
        });
      }
      if (!configured.ok()) co_return configured;
    }
    backlog_backpressure_.store(enabled, std::memory_order_release);
    co_return absl::OkStatus();
  }

  bool backlog_backpressure() const noexcept {
    return backlog_backpressure_.load(std::memory_order_acquire);
  }

  Task<absl::Status> SetPublishQueueBytesPerWorker(std::size_t bytes) {
    if (bycorf::ThisWorker().id_ != 0) {
      co_return co_await bycorf::SubmitTaskTo(
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
        configured = co_await bycorf::SubmitTaskTo(worker, [this, bytes]() {
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
    if (is_loading()) {
      absl::Status sent = co_await WriteText(
          stream,
          "-LOADING node has no valid native replication source state\r\n");
      co_return sent.ok() ? absl::FailedPreconditionError(
                                "a fenced node cannot serve native replication")
                          : sent;
    }
    if (is_replica()) {
      absl::Status sent = co_await WriteText(
          stream, "-ERR native cascading replication is not supported\r\n");
      co_return sent.ok()
          ? absl::FailedPreconditionError(
                "a Lavik replica cannot serve downstream native replication")
          : sent;
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
    if (EqualCaseInsensitive(args.front(), "LVFLOW")) {
      unsigned flow_id = 0;
      if (args.size() != 7 ||
          !ParseUnsigned(args[2], &replication_session_id) ||
          replication_session_id == 0 || !ParseUnsigned(args[3], &flow_id)) {
        co_return absl::InvalidArgumentError("invalid LVFLOW handshake");
      }
      // flow_id belongs to the upstream worker set. Multiple upstream flows
      // may share one local worker when the worker counts differ.
      owner = flow_id % storage_->worker_count();
    }
    if (owner == bycorf::ThisWorker().id_) {
      RegisterClientConnection(client_id, stream.NativeFd(),
                               std::move(client_address), tls, true,
                               replication_session_id);
      absl::Status status = co_await ServeOwnedNativeConnection(
          stream, std::move(args), client_id);
      UnregisterClientConnection(client_id);
      if (!status.ok()) {
        spdlog::warn("replication native handshake failed: {}",
                     status.message());
      }
      co_return status;
    }

    absl::Status paused = co_await stream.PauseRead();
    if (!paused.ok()) co_return paused;
    std::shared_ptr<bycorf::TlsState> tls_state = stream.TakeTlsState();
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
    co_return co_await bycorf::SubmitTo(
        owner, [this, duplicate, tls_state = std::move(tls_state),
                args = std::move(args), client_id,
                client_address = std::move(client_address), tls,
                replication_session_id]() mutable {
          Connection connection;
          connection.worker_ = bycorf::ThisWorker().self_;
          connection.file_.fd_ = duplicate;
          connection.closed_ = false;
          if (tls_state != nullptr) {
            connection.recv_mode_ = bycorf::RecvMode::kOneShot;
            connection.tls_state_ = std::move(tls_state);
          }
          Connection* registered =
              bycorf::ThisWorker().self_->AddConnection(std::move(connection));
          if (registered == nullptr) {
            ::close(duplicate);
            return absl::InternalError(
                "failed to adopt replication connection");
          }
          bycorf::ThisWorker().self_->Spawn(RunAdoptedConnection(
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
    if (!source_sockets_.Add(stream.NativeFd())) {
      co_return absl::CancelledError(
          "Redis replication export stopped for process shutdown");
    }
    ScopedSocketSetMembership source_socket(&source_sockets_,
                                            stream.NativeFd());
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
    if (is_loading()) {
      absl::Status sent = co_await WriteText(
          stream,
          "-LOADING node has no valid Redis replication source state\r\n");
      co_return sent.ok() ? absl::FailedPreconditionError(
                                "a fenced node cannot export Redis PSYNC")
                          : sent;
    }
    if (cluster_enabled_) {
      absl::Status sent = co_await WriteText(
          stream,
          "-ERR Redis replication export is unavailable in cluster mode\r\n");
      co_return sent.ok()
          ? absl::FailedPreconditionError(
                "cluster mode has no authorized Redis replication "
                "export")
          : sent;
    }
    if (is_replica()) {
      absl::Status sent = co_await WriteText(
          stream, "-ERR detach this Lavik replica before Redis export\r\n");
      co_return sent.ok() ? absl::FailedPreconditionError(
                                "a Lavik replica cannot export Redis PSYNC")
                          : sent;
    }
    const std::uint64_t source_role_epoch =
        role_epoch_.load(std::memory_order_acquire);
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
    redis_export_fd_.store(stream.NativeFd(), std::memory_order_release);
    struct ActiveGuard {
      std::atomic<bool>* active_;
      std::atomic<int>* fd_;
      ~ActiveGuard() {
        fd_->store(-1, std::memory_order_release);
        active_->store(false, std::memory_order_release);
      }
    } active_guard{&redis_export_active_, &redis_export_fd_};
    // CAS-before-check pairs with DrainSourceEgress's active observation. If
    // shutdown won first, this handler exits before any await/history setup;
    // otherwise the barrier sees active=true and joins the whole export.
    if (replication_shutdown_requested_.load(std::memory_order_acquire)) {
      co_return absl::CancelledError(
          "Redis replication export stopped for process shutdown");
    }
    if (is_replica() || is_loading()) {
      absl::Status sent = co_await WriteText(
          stream,
          "-LOADING node has no valid Redis replication source state\r\n");
      co_return sent.ok()
          ? absl::FailedPreconditionError(
                "node lost valid source state during PSYNC setup")
          : sent;
    }
    if (bycorf::ThisWorker().id_ == 0) {
      StartIdleReplicationHistoryMonitor();
    } else {
      (void)co_await bycorf::SubmitTo(0, [this] {
        StartIdleReplicationHistoryMonitor();
        return true;
      });
    }

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
      status = co_await bycorf::SubmitTaskTo(
          worker, [this, session_id, flow_capacity]() -> Task<absl::Status> {
            co_return co_await storage_->EnableReplicationLog(session_id,
                                                              flow_capacity);
          });
      if (!status.ok()) co_return status;
    }

    LAVIK_FAULT_INJECT(status =
                           co_await MaybePauseBeforeRedisExportDbAdmission(););
    if (!status.ok()) co_return status;
    while (!CloseAllCommandDbGates()) {
      if (role_epoch_.load(std::memory_order_acquire) != source_role_epoch ||
          is_replica() || is_loading()) {
        co_return absl::CancelledError(
            "source role changed before Redis export snapshot admission");
      }
      status = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
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
      status = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
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
      status = co_await bycorf::SubmitTo(
          snapshots_begun, [this, session_id, snapshot_time_ms] {
            return storage_->BeginRdbSnapshot(session_id, snapshot_time_ms);
          });
      if (!status.ok()) break;
    }
    if (!status.ok()) {
      for (unsigned worker = 0; worker < snapshots_begun; ++worker) {
        (void)co_await bycorf::SubmitTaskTo(worker, [this, session_id] {
          return storage_->EndRdbSnapshot(session_id);
        });
      }
      co_return status;
    }

    std::vector<storage::ReplicationLogCursor> cursors(
        storage_->worker_count());
    for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
      auto fenced = co_await bycorf::SubmitTaskTo(
          worker, [this]() { return storage_->FenceReplicationLog(); });
      if (!fenced.ok()) {
        status = fenced.status();
        break;
      }
      cursors[worker] =
          storage::ReplicationLogCursor{.lsn_ = *fenced, .fragment_index_ = 0};
      if (redis_export_backpressure_.load(std::memory_order_acquire)) {
        status = co_await bycorf::SubmitTo(
            worker, [this, session_id, cursor = cursors[worker]] {
              return storage_->RetainReplicationLog(session_id, cursor.lsn_);
            });
        if (!status.ok()) break;
      }
    }
    if (!status.ok()) {
      for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
        (void)co_await bycorf::SubmitTaskTo(worker, [this, session_id] {
          return storage_->EndRdbSnapshot(session_id);
        });
        (void)co_await bycorf::SubmitTo(worker, [this, session_id] {
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
    const std::string full_resync_header = absl::StrCat(
        "+FULLRESYNC ", node_id_, " 0\r\n$EOF:", eof_token, "\r\n");
    status = co_await WriteText(stream, full_resync_header);
    if (status.ok()) {
      // RDB v10 is accepted by Redis 7.0 and later. Lavik's value opcodes
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
          status = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
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
      absl::Status waited = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) break;
    }
    std::shared_ptr<RedisExportAckState> ack_state;
    if (status.ok()) {
      spdlog::info("Redis PSYNC export {} completed diskless RDB cut",
                   session_id);
      ack_state = std::make_shared<RedisExportAckState>();
      bycorf::ThisWorker().self_->Spawn(
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
        absl::Status waited = co_await bycorf::SleepFor(
            *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
        if (!waited.ok()) break;
      }
    }
    for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
      (void)co_await bycorf::SubmitTo(worker, [this, session_id] {
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
  static void AssertStateOwner() noexcept {
    assert(bycorf::ThisWorker().self_ != nullptr &&
           bycorf::ThisWorker().id_ == 0);
  }

  void PublishUpstreamSnapshot() {
    const auto version = upstream_version_.load(std::memory_order_relaxed);
    if (version == std::numeric_limits<std::uint64_t>::max()) std::terminate();
    auto snapshot = std::make_shared<const UpstreamSnapshot>(
        UpstreamSnapshot{version + 1, upstream_});
    // Publish the immutable value before notifying caches. A reader racing
    // the notification may observe the newer snapshot early; its embedded
    // version prevents labeling an old endpoint with a new version.
    published_upstream_.store(std::move(snapshot), std::memory_order_release);
    upstream_version_.store(version + 1, std::memory_order_release);
  }

  void SetDesiredUpstream(std::optional<ReplicaOfConfig> upstream) {
    AssertStateOwner();
    if (upstream_ == upstream) return;
    upstream_ = std::move(upstream);
    PublishUpstreamSnapshot();
  }

  bool EmptyPopulationCurrent(
      const std::shared_ptr<ClusterRebuildContext>& context) {
    AssertStateOwner();
    return cluster_rebuild_ == context && !replica_reconfiguration_running_ &&
           !cluster_control_stopping_ &&
           !failed_stopped_.load(std::memory_order_relaxed) &&
           context->state_.load(std::memory_order_relaxed) ==
               ReplicationGroupState::kRebuilding;
  }

  Task<absl::Status> FinishEmptyPopulationFailure(
      const std::shared_ptr<ClusterRebuildContext>& context,
      std::uint64_t session_id, bool root_started, bool promoted,
      absl::Status failure) {
    if (promoted) {
      const std::string reason = absl::StrCat(
          "empty population failed after promotion: ", failure.message());
      (void)cluster_group_->FailStop(context->directive_.identity_);
      storage_->FenceRequestServingUntilRestart();
      LatchReplicationFailure(reason);
      absl::Status terminal = absl::InternalError(reason);
      context->completion_->Resolve(terminal);
      co_return terminal;
    }
    if (root_started) {
      absl::Status aborted = co_await storage_->AbortReplicaRoot(session_id);
      if (!aborted.ok()) {
        const std::string reason = absl::StrCat(
            "empty population abort outcome is uncertain: ", aborted.message());
        (void)cluster_group_->FailStop(context->directive_.identity_);
        storage_->FenceRequestServingUntilRestart();
        LatchReplicationFailure(reason);
        absl::Status terminal = absl::InternalError(reason);
        context->completion_->Resolve(terminal);
        co_return terminal;
      }
    }
    {
      AssertStateOwner();
      if (cluster_rebuild_ == context &&
          !failed_stopped_.load(std::memory_order_relaxed)) {
        context->state_.store(ReplicationGroupState::kNotReady,
                              std::memory_order_release);
      }
    }
    context->completion_->Resolve(failure);
    co_return failure;
  }

  Task<absl::Status> RunEmptyPopulationInitialization(
      std::shared_ptr<ClusterRebuildContext> context) {
    struct CoordinatorGuard {
      bool* running_;
      ~CoordinatorGuard() { *running_ = false; }
    } coordinator_guard{&coordinator_started_};

    std::uint64_t session_id = NextRedisFullSyncSessionId();
    bool root_started = false;
    bool promoted = false;
    const auto cancelled = [] {
      return absl::CancelledError(
          "empty population initialization was superseded");
    };
    auto current_or_cancelled = [&]() -> absl::Status {
      return EmptyPopulationCurrent(context) ? absl::OkStatus() : cancelled();
    };

    absl::Status prepared = current_or_cancelled();
    while (prepared.ok() && !CloseAllCommandDbGates()) {
      prepared = current_or_cancelled();
      if (!prepared.ok()) break;
      prepared = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                           std::chrono::milliseconds(1));
    }
    const bool gates_closed = prepared.ok();
    if (gates_closed) {
      while (CommandDbOperationsActive()) {
        prepared = current_or_cancelled();
        if (!prepared.ok()) break;
        prepared = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                             std::chrono::milliseconds(1));
        if (!prepared.ok()) break;
      }
    }
    if (prepared.ok()) {
      prepared = co_await storage_->QuiesceTombRaiderForReplica();
    }
    if (prepared.ok()) {
      prepared = co_await storage_->QuiesceExpiration();
      if (prepared.ok()) storage_->ResumeExpiration();
    }
    if (gates_closed) OpenAllCommandDbGates();
    if (!prepared.ok()) {
      co_return co_await FinishEmptyPopulationFailure(
          context, session_id, root_started, promoted, prepared);
    }

    absl::Status invalidated =
        co_await storage_->BeginReplicaFullSync(session_id);
    if (!invalidated.ok()) {
      co_return co_await FinishEmptyPopulationFailure(
          context, session_id, root_started, promoted, invalidated);
    }
    root_started = true;

    const unsigned worker_count = storage_->worker_count();
    for (unsigned owner = 0; owner < worker_count; ++owner) {
      if (absl::Status current = current_or_cancelled(); !current.ok()) {
        co_return co_await FinishEmptyPopulationFailure(
            context, session_id, root_started, promoted, current);
      }
      if (absl::Status authorized = cluster_group_->ValidateResetAuthorization(
              *context->authorization_);
          !authorized.ok()) {
        co_return co_await FinishEmptyPopulationFailure(
            context, session_id, root_started, promoted, authorized);
      }
      std::vector<storage::ReplicaPartitionReset> resets;
      resets.reserve((storage::kLogicalStorageShards + worker_count - 1) /
                     worker_count);
      for (std::uint32_t partition = owner;
           partition < storage::kLogicalStorageShards;
           partition += worker_count) {
        storage::ReplicaPartitionReset reset;
        reset.partition_id_ = static_cast<std::uint16_t>(partition);
        reset.db_epochs_.fill(1);
        resets.push_back(reset);
      }
      absl::StatusOr<std::vector<storage::ReplicaPartitionEpoch>> reset;
      if (owner == 0) {
        reset = co_await storage_->ResetReplicaPartitions(session_id, resets);
      } else {
        reset = co_await bycorf::SubmitTaskTo(
            owner, [this, session_id, resets = std::move(resets)]() mutable {
              return storage_->ResetReplicaPartitions(session_id, resets);
            });
      }
      if (!reset.ok()) {
        co_return co_await FinishEmptyPopulationFailure(
            context, session_id, root_started, promoted, reset.status());
      }
      if (ShouldInjectEmptyPopulationResetFailure(owner)) {
        co_return co_await FinishEmptyPopulationFailure(
            context, session_id, root_started, promoted,
            absl::InternalError(
                "injected empty-population reset result failure"));
      }
      for (const storage::ReplicaPartitionEpoch& partition : *reset) {
        absl::Status recorded = cluster_group_->RecordPartitionReset(
            context->directive_.identity_, partition.partition_id_,
            partition.replication_epoch_);
        if (!recorded.ok()) {
          co_return co_await FinishEmptyPopulationFailure(
              context, session_id, root_started, promoted, recorded);
        }
        absl::Status handed_off;
        if (owner == 0) {
          handed_off = co_await storage_->HandoffReplicaPartition(
              session_id, partition.partition_id_,
              partition.replication_epoch_);
        } else {
          handed_off = co_await bycorf::SubmitTaskTo(
              owner, [this, session_id, partition]() {
                return storage_->HandoffReplicaPartition(
                    session_id, partition.partition_id_,
                    partition.replication_epoch_);
              });
        }
        if (!handed_off.ok()) {
          co_return co_await FinishEmptyPopulationFailure(
              context, session_id, root_started, promoted, handed_off);
        }
        absl::Status recorded_handoff = cluster_group_->RecordPartitionHandoff(
            context->directive_.identity_, partition.partition_id_,
            context->manifest_->logical_epochs()[partition.partition_id_],
            partition.replication_epoch_);
        if (!recorded_handoff.ok()) {
          co_return co_await FinishEmptyPopulationFailure(
              context, session_id, root_started, promoted, recorded_handoff);
        }
      }
    }

    if (absl::Status current = current_or_cancelled(); !current.ok()) {
      co_return co_await FinishEmptyPopulationFailure(
          context, session_id, root_started, promoted, current);
    }
    const std::vector<std::string> empty_catalog;
    absl::Status catalog = co_await ReplaceLuaFunctionCatalog(empty_catalog);
    if (catalog.ok() && ShouldInjectEmptyPopulationCatalogFailure()) {
      catalog = absl::InternalError(
          "injected empty-population catalog result failure");
    }
    if (catalog.ok()) {
      catalog = cluster_group_->MarkFunctionCatalogComplete(
          context->directive_.identity_);
    }
    if (!catalog.ok()) {
      co_return co_await FinishEmptyPopulationFailure(
          context, session_id, root_started, promoted, catalog);
    }

    absl::Status promotion;
    if (ShouldInjectReplicaPromotionFailure()) {
      promotion = absl::InternalError("injected replica promotion failure");
    } else {
      // Keep co_await out of a conditional expression. GCC has historically
      // mis-lowered that shape in this coroutine-heavy translation unit.
      promotion = co_await storage_->PromoteReplicaRoot(session_id);
    }
    if (!promotion.ok()) {
      // PromoteReplicaRoot persists and publishes in several ordered steps;
      // any error is treated as unknowable, matching replicated full sync.
      promoted = true;
      co_return co_await FinishEmptyPopulationFailure(
          context, session_id, root_started, promoted, promotion);
    }
    promoted = true;

    const std::uint64_t child_log_epoch =
        role_epoch_.load(std::memory_order_acquire);
    for (unsigned worker = 0; worker < worker_count; ++worker) {
      const std::size_t flow_capacity = BacklogCapacityForFlow(
          worker, backlog_size_bytes_.load(std::memory_order_acquire));
      absl::Status enabled = co_await bycorf::SubmitTaskTo(
          worker,
          [this, child_log_epoch, flow_capacity]() -> Task<absl::Status> {
            co_return co_await storage_->EnableReplicationLog(child_log_epoch,
                                                              flow_capacity);
          });
      if (!enabled.ok()) {
        co_return co_await FinishEmptyPopulationFailure(
            context, session_id, root_started, promoted, enabled);
      }
    }
    absl::Status group_ready =
        cluster_group_->MarkStoragePromoted(context->directive_.identity_);
    auto ready =
        group_ready.ok()
            ? cluster_group_->PublishReady(context->directive_.identity_)
            : absl::StatusOr<ReadyToken>(group_ready);
    if (!ready.ok()) {
      co_return co_await FinishEmptyPopulationFailure(
          context, session_id, root_started, promoted, ready.status());
    }

    absl::Status recorded = co_await storage_->CommitPopulationIdentity(
        detail::EncodeRecoveredPopulation(ready->identity()));
    if (!recorded.ok()) {
      co_return co_await FinishEmptyPopulationFailure(
          context, session_id, root_started, promoted, recorded);
    }
    recovered_population_.reset();
    recovered_population_fenced_ = false;
    operator_recovery_active_ = false;

    bool installed = false;
    {
      AssertStateOwner();
      installed = cluster_rebuild_ == context &&
                  !replica_reconfiguration_running_ &&
                  !failed_stopped_.load(std::memory_order_relaxed);
      if (installed) {
        context->ready_token_ = *ready;
        context->state_.store(ReplicationGroupState::kReady,
                              std::memory_order_release);
        native_dataset_valid_.store(true, std::memory_order_release);
      }
    }
    if (!installed) {
      co_return co_await FinishEmptyPopulationFailure(
          context, session_id, root_started, promoted,
          absl::CancelledError(
              "empty population completed after supersession"));
    }
    owner_source_term_ = context->directive_.identity_.term_;
    StoreRole(ReplicationRole::kMaster, std::memory_order_release);
    storage_->SetReplicaLoading(false);
    // Population readiness does not convey a write lease. NodeControl enables
    // a finite expiration capability only after the matching FDS and lease
    // deadline pass their final activation recheck.
    context->completion_->Resolve(absl::OkStatus());
    co_return absl::OkStatus();
  }

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
    auto connected = co_await ConnectTcp(upstream.host_, upstream.port_,
                                         tls_context_, &outbound_sockets_);
    if (!connected.ok()) co_return connected.status();
    TcpStream stream = std::move(*connected);
    ScopedSocketSetMembership membership(&outbound_sockets_, stream.NativeFd());
    absl::Status status =
        co_await AuthenticateUpstream(stream, masteruser_, masterauth_);
    if (!status.ok()) {
      stream.Close().IgnoreError();
      co_return status;
    }
    const std::vector<std::string> command{"CLUSTER", "NODES"};
    const std::string encoded_command = EncodeRespCommand(command);
    status = co_await WriteText(stream, encoded_command);
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
    auto connected = co_await ConnectTcp(upstream.host_, upstream.port_,
                                         tls_context_, &outbound_sockets_);
    if (!connected.ok()) co_return connected.status();
    TcpStream stream = std::move(*connected);
    ScopedSocketSetMembership membership(&outbound_sockets_, stream.NativeFd());
    absl::Status status =
        co_await AuthenticateUpstream(stream, masteruser_, masterauth_);
    if (!status.ok()) {
      stream.Close().IgnoreError();
      co_return status;
    }
    const std::vector<std::string> sync_args{
        "LVPSYNC", std::string(kProtocolVersion), "?", "?", "?", "?", "?", "?"};
    const std::string encoded_sync = EncodeRespCommand(sync_args);
    status = co_await WriteText(stream, encoded_sync);
    if (!status.ok()) {
      stream.Close().IgnoreError();
      co_return status;
    }
    auto response = co_await ReadLine(stream);
    stream.Close().IgnoreError();
    if (!response.ok()) co_return response.status();
    if (response->starts_with("+LVFULLRESYNC ")) {
      UpstreamDiscovery result;
      result.protocol_ = UpstreamProtocol::kNative;
      co_return result;
    }
    if (response->starts_with('-') &&
        response->find("unknown command") != std::string::npos &&
        (response->find("LVPSYNC") != std::string::npos ||
         response->find("lvpsync") != std::string::npos)) {
      co_return co_await DiscoverRedis(upstream);
    }
    co_return absl::FailedPreconditionError(
        absl::StrCat("upstream rejected Lavik protocol probe: ", *response));
  }

  Task<absl::Status> WaitUntilStorageReady() {
    while (!StorageIsReady()) {
      if (replication_shutdown_requested_) {
        co_return absl::CancelledError(
            "replication startup stopped for process shutdown");
      }
      absl::Status slept = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!slept.ok()) co_return slept;
    }
    if (!replication_shutdown_requested_) StartCoordinator();
    co_return absl::OkStatus();
  }

  void StartCoordinator() {
    if (bycorf::ThisWorker().id_ != 0 || !StorageIsReady() ||
        replication_shutdown_requested_) {
      return;
    }
    if (failed_stopped_.load(std::memory_order_acquire)) return;
    {
      AssertStateOwner();
      if (replica_reconfiguration_running_ ||
          failed_stopped_.load(std::memory_order_relaxed)) {
        return;
      }
    }
    if (initial_protocol_probe_pending_) {
      if (coordinator_started_) return;
      coordinator_started_ = true;
      bycorf::ThisWorker().self_->SpawnRoot(ProbeInitialUpstream());
      return;
    }
    if (redis_psync_.load(std::memory_order_acquire)) {
      std::vector<std::shared_ptr<RedisSource>> sources;
      {
        AssertStateOwner();
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
      AssertStateOwner();
      if (!upstream_.has_value() || replica_reconfiguration_running_ ||
          failed_stopped_.load(std::memory_order_relaxed)) {
        return;
      }
    }
    coordinator_started_ = true;
    bycorf::ThisWorker().self_->Spawn(Coordinator());
  }

  Task<absl::Status> ProbeInitialUpstream() {
    while (true) {
      ReplicaOfConfig upstream;
      std::uint64_t role_epoch = 0;
      {
        AssertStateOwner();
        if (replication_shutdown_requested_ || !upstream_.has_value() ||
            !initial_protocol_probe_pending_) {
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
        absl::Status slept = co_await bycorf::SleepFor(
            *bycorf::ThisWorker().self_, kReconnectDelay);
        if (!slept.ok()) {
          coordinator_started_ = false;
          co_return slept;
        }
        continue;
      }
      {
        AssertStateOwner();
        if (replication_shutdown_requested_ || !upstream_.has_value() ||
            *upstream_ != upstream ||
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

  std::uint64_t NextRedisFullSyncSessionId() noexcept {
    for (;;) {
      const std::uint64_t candidate =
          next_redis_full_sync_session_id_.fetch_add(1,
                                                     std::memory_order_relaxed);
      if (candidate != 0) return candidate;
    }
  }

  Task<absl::StatusOr<std::uint64_t>> BeginRedisFullSyncAttempt(
      const std::shared_ptr<RedisSource>& source) {
    // Serialize the short durable invalidation/activation decisions. RDB
    // imports use the same mutex, but network receipt does not, so sources can
    // still download concurrently without allowing an old all-source snapshot
    // to activate across a newer FULLRESYNC.
    std::uint64_t session_id = 0;
    std::vector<std::shared_ptr<ReplicaSession>> replaced_sessions;
    absl::Status invalidated;
    {
      co_await redis_fullsync_mutex_.Lock();
      bycorf::UnlockGuard fullsync_unlock(&redis_fullsync_mutex_,
                                          bycorf::ThisWorker().self_);
      {
        AssertStateOwner();
        if (replication_shutdown_requested_ ||
            !redis_psync_.load(std::memory_order_relaxed) ||
            !RedisSourceRegistered(source) || redis_topology_fault_ ||
            source->role_epoch_ !=
                role_epoch_.load(std::memory_order_relaxed)) {
          co_return absl::CancelledError("Redis full sync source was replaced");
        }
        if (redis_full_sync_session_id_ == 0) {
          redis_full_sync_session_id_ = NextRedisFullSyncSessionId();
          for (const auto& current : redis_sources_) {
            current->dataset_valid_.store(false, std::memory_order_release);
            current->full_sync_session_id_ = 0;
            current->replid_.reset();
            if (current != source && current->session_ != nullptr) {
              replaced_sessions.push_back(current->session_);
            }
          }
        }
        session_id = redis_full_sync_session_id_;
        source->dataset_valid_.store(false, std::memory_order_release);
        source->full_sync_session_id_ = 0;
        source->replid_.reset();
      }
      RefreshRedisRole();
      invalidated = co_await storage_->BeginReplicaFullSync(session_id);
    }
    absl::Status stopped = absl::OkStatus();
    for (const auto& session : replaced_sessions) {
      absl::Status current = co_await CancelAndWaitForReplicaFlows(session);
      if (stopped.ok() && !current.ok()) stopped = current;
    }
    if (!invalidated.ok()) co_return invalidated;
    if (!stopped.ok()) co_return stopped;
    co_return session_id;
  }

  Task<absl::Status> WaitForRedisFullSyncActivation(
      const std::shared_ptr<RedisSource>& source) {
    while (storage_->ReplicaRecoveryFenced()) {
      {
        AssertStateOwner();
        if (replication_shutdown_requested_ || !RedisSourceRegistered(source) ||
            redis_topology_fault_ ||
            source->role_epoch_ !=
                role_epoch_.load(std::memory_order_relaxed)) {
          co_return absl::CancelledError(
              "Redis full sync was cancelled before population activation");
        }
      }
      absl::Status waited = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) co_return waited;
    }
    co_return absl::OkStatus();
  }

  void RefreshRedisRole() {
    bool ready = false;
    bool syncing = false;
    {
      AssertStateOwner();
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
    if (replication_shutdown_requested_ || redis_topology_monitor_started_ ||
        !redis_cluster_) {
      return;
    }
    redis_topology_monitor_started_ = true;
    const std::uint64_t epoch = role_epoch_.load(std::memory_order_acquire);
    bycorf::ThisWorker().self_->SpawnRoot(RedisTopologyMonitor(epoch));
  }

  void FaultRedisTopology(std::string_view reason) {
    std::vector<std::shared_ptr<ReplicaSession>> sessions;
    {
      AssertStateOwner();
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
        "Redis Cluster topology changed; replication stopped and Lavik "
        "entered LOADING: {}",
        reason);
    RefreshRedisRole();
  }

  void ApplyStableRedisTopology(RedisClusterTopology topology) {
    std::vector<std::shared_ptr<ReplicaSession>> replaced;
    {
      AssertStateOwner();
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
        SetDesiredUpstream(redis_sources_.front()->upstream_);
    }
    for (const auto& session : replaced) session->Cancel();
  }

  Task<absl::Status> RedisTopologyMonitor(std::uint64_t role_epoch) {
    unsigned incompatible_observations = 0;
    while (true) {
      absl::Status slept = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, kRedisTopologyPollInterval);
      if (!slept.ok()) co_return slept;
      std::vector<ReplicaOfConfig> endpoints;
      {
        AssertStateOwner();
        if (replication_shutdown_requested_ ||
            !redis_psync_.load(std::memory_order_relaxed) || !redis_cluster_ ||
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
        AssertStateOwner();
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
    if (replication_shutdown_requested_ || source->coordinator_started_) return;
    source->coordinator_started_ = true;
    spdlog::info("starting Redis replication coordinator for {}:{}",
                 source->upstream_.host_, source->upstream_.port_);
    bycorf::ThisWorker().self_->SpawnRoot(RedisCoordinator(source));
  }

  Task<absl::Status> RedisCoordinator(std::shared_ptr<RedisSource> source) {
    if (replication_shutdown_requested_) {
      source->coordinator_started_ = false;
      co_return absl::CancelledError(
          "Redis replication stopped for process shutdown");
    }
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
        AssertStateOwner();
        if (replication_shutdown_requested_ || !RedisSourceRegistered(source) ||
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
      auto session = std::make_shared<ReplicaSession>(&outbound_sockets_);
      {
        AssertStateOwner();
        if (replication_shutdown_requested_ ||
            !redis_psync_.load(std::memory_order_relaxed) ||
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
        AssertStateOwner();
        StoreRedisLink(source, false);
        source->syncing_ = false;
        if (source->session_ == session) source->session_.reset();
      }
      RefreshRedisRole();

      bool retry = false;
      {
        AssertStateOwner();
        retry =
            !replication_shutdown_requested_ &&
            redis_psync_.load(std::memory_order_relaxed) &&
            RedisSourceRegistered(source) && !redis_topology_fault_ &&
            source->role_epoch_ == role_epoch_.load(std::memory_order_relaxed);
      }
      if (!retry) break;
      spdlog::warn("Redis replication connection to {}:{} ended: {}",
                   source->upstream_.host_, source->upstream_.port_,
                   connected.message());
      absl::Status slept = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, kReconnectDelay);
      if (!slept.ok()) {
        source->coordinator_started_ = false;
        co_return slept;
      }
    }
    source->coordinator_started_ = false;
    co_return absl::OkStatus();
  }

  void LatchReplicationFailure(std::string reason) {
    std::string latched_reason;
    std::shared_ptr<detail::ClusterRebuildCompletionState> completion;
    std::shared_ptr<detail::ClusterPromotionPrepareCompletionState>
        promotion_completion;
    {
      AssertStateOwner();
      if (!failed_stopped_.load(std::memory_order_relaxed)) {
        failure_reason_ = std::move(reason);
        failed_stopped_.store(true, std::memory_order_release);
      }
      replica_reconfiguration_running_ = false;
      if (cluster_rebuild_ != nullptr) {
        cluster_rebuild_->ready_token_.reset();
        cluster_rebuild_->state_.store(ReplicationGroupState::kFailedStopped,
                                       std::memory_order_release);
        completion = cluster_rebuild_->completion_;
      }
      if (cluster_promotion_prepare_ != nullptr) {
        promotion_completion = cluster_promotion_prepare_->completion_;
      }
      latched_reason = failure_reason_;
    }
    if (completion != nullptr) {
      completion->Resolve(absl::InternalError(
          absl::StrCat("replication failed-stopped: ", latched_reason)));
    }
    if (promotion_completion != nullptr) {
      promotion_completion->Resolve(absl::InternalError(
          absl::StrCat("replication failed-stopped: ", latched_reason)));
    }
    // The role/generation close and storage write guard are independent
    // defenses: neither client dispatch nor a stale internal continuation may
    // turn an uncertain in-place root into a writable population.
    native_dataset_valid_.store(false, std::memory_order_release);
    storage_->SetReplicaLoading(true);
    storage_->SetExpirationAuthority(false);
    StoreRole(ReplicationRole::kConnecting, std::memory_order_release);
    if (cluster_enabled_) {
      bycorf::ThisWorker().self_->Spawn(
          RevokeClusterRebuildSourceAuthorizations());
    }
    spdlog::critical("replication failed-stopped until restart: {}",
                     latched_reason);
  }

  Task<absl::Status> Coordinator() {
    while (true) {
      ReplicaOfConfig upstream;
      std::uint64_t role_epoch = 0;
      std::shared_ptr<ReplicaSession> session;
      {
        AssertStateOwner();
        if (replication_shutdown_requested_ || !upstream_.has_value() ||
            replica_reconfiguration_running_ ||
            failed_stopped_.load(std::memory_order_relaxed)) {
          break;
        }
        upstream = *upstream_;
        role_epoch = role_epoch_.load(std::memory_order_relaxed);
        session = std::make_shared<ReplicaSession>(&outbound_sockets_);
        session->cluster_rebuild_ = cluster_rebuild_;
        if (cluster_follow_owner_ != nullptr &&
            cluster_follow_owner_->desired_.local_node_id_ !=
                cluster_follow_owner_->desired_.owner_node_id_) {
          session->cluster_follow_ = cluster_follow_owner_;
        }
        active_replica_session_ = session;
      }
      StoreRole(ReplicationRole::kConnecting, std::memory_order_release);
      absl::Status connected =
          co_await RunReplicaSession(upstream, role_epoch, session);
      {
        AssertStateOwner();
        if (active_replica_session_ != session) {
          // Explicit REPLICAOF reconfiguration moved this attempt to its own
          // node-level teardown. It owns abort and any failure latch; this
          // coordinator must neither race a second abort nor start a new
          // attempt until that transition commits.
          continue;
        }
        replica_session_teardown_running_ = true;
      }
      // Session teardown is one node-level action: cancel every flow and
      // rendezvous, join all detached apply work, then discard the in-place
      // partial root. Starting a retry before that sequence is proven complete
      // can mix attempts in the same physical indexes.
      absl::Status stopped = co_await CancelAndWaitForReplicaFlows(session);
      std::optional<std::string> fail_stop = session->FailStopReason();
      const std::shared_ptr<ClusterRebuildContext> cluster_context =
          session->cluster_rebuild_;
      const ReplicationGroupState cluster_state =
          cluster_context == nullptr
              ? ReplicationGroupState::kNotReady
              : cluster_context->state_.load(std::memory_order_acquire);
      const bool cluster_ready = cluster_state == ReplicationGroupState::kReady;
      const bool cluster_proof_invalidated =
          cluster_state == ReplicationGroupState::kNotReady ||
          cluster_state == ReplicationGroupState::kFailedStopped;
      if (!stopped.ok() && !fail_stop.has_value()) {
        fail_stop =
            absl::StrCat("replica cancellation/join outcome is uncertain: ",
                         stopped.message());
      }
      bool lease_admission_retry = false;
      {
        AssertStateOwner();
        // Only the explicit source lease-gate response is retryable. Pointer
        // identity proves the immutable rebuild directive/FDS attempt is still
        // current; role/upstream equality closes replacement races. Requiring
        // no published source session, no flow, and no BeginReplicaFullSync
        // boundary keeps every other connection/protocol failure terminal.
        lease_admission_retry =
            IsLeaseAdmissionSuspended(connected) && stopped.ok() &&
            !fail_stop.has_value() && cluster_context != nullptr &&
            cluster_state == ReplicationGroupState::kRebuilding &&
            cluster_group_->state() == ReplicationGroupState::kRebuilding &&
            cluster_rebuild_ == cluster_context &&
            active_replica_session_ == session &&
            session->cluster_follow_ == nullptr && session->session_id_ == 0 &&
            session->active_flows_.load(std::memory_order_acquire) == 0 &&
            session->connected_flows_.load(std::memory_order_acquire) == 0 &&
            !session->destructive_root_started_.load(
                std::memory_order_acquire) &&
            !replication_shutdown_requested_ && !cluster_control_stopping_ &&
            upstream_.has_value() && *upstream_ == upstream &&
            role_epoch_.load(std::memory_order_relaxed) == role_epoch &&
            cluster_context->lease_admission_pre_mutation_retries_ <
                kLeaseAdmissionPreMutationRetries;
        if (lease_admission_retry) {
          ++cluster_context->lease_admission_pre_mutation_retries_;
        }
      }
      const bool partial_root_must_abort =
          !lease_admission_retry &&
          (cluster_context == nullptr || !cluster_ready);
      if (stopped.ok() && !fail_stop.has_value() && partial_root_must_abort &&
          session->session_id_ != 0) {
        absl::Status discarded =
            co_await storage_->AbortReplicaRoot(session->session_id_);
        if (!discarded.ok() && !fail_stop.has_value()) {
          fail_stop = absl::StrCat("replica abort outcome is uncertain: ",
                                   discarded.message());
        }
      }

      const bool cluster_attempt_must_retire =
          !lease_admission_retry && cluster_context != nullptr &&
          (!cluster_ready || cluster_proof_invalidated ||
           fail_stop.has_value());
      if (cluster_context != nullptr && fail_stop.has_value()) {
        absl::Status latched =
            cluster_group_->FailStop(cluster_context->directive_.identity_);
        if (!latched.ok() &&
            cluster_group_->state() != ReplicationGroupState::kFailedStopped) {
          *fail_stop = absl::StrCat(
              *fail_stop,
              "; cluster failure latch rejected: ", latched.message());
        }
      } else if (cluster_attempt_must_retire) {
        absl::Status invalidated = cluster_group_->InvalidateProof(
            cluster_context->directive_.identity_);
        if (!invalidated.ok()) {
          fail_stop =
              absl::StrCat("cluster population proof invalidation failed: ",
                           invalidated.message());
          (void)cluster_group_->FailStop(cluster_context->directive_.identity_);
        }
      }

      bool retry = false;
      {
        AssertStateOwner();
        if (active_replica_session_ == session) {
          active_replica_session_.reset();
          replica_session_id_ = 0;
          source_worker_count_ = 0;
        }
        if (cluster_attempt_must_retire &&
            cluster_rebuild_ == cluster_context) {
          cluster_rebuild_.reset();
          if (session->cluster_follow_ == nullptr ||
              cluster_follow_owner_ != session->cluster_follow_) {
            SetDesiredUpstream(std::nullopt);
          }
          upstream_node_id_.reset();
          upstream_history_id_.reset();
        }
        replica_session_teardown_running_ = false;
        const bool current_follow =
            session->cluster_follow_ != nullptr &&
            cluster_follow_owner_ == session->cluster_follow_;
        retry = lease_admission_retry ||
                (!replication_shutdown_requested_ && upstream_.has_value() &&
                 role_epoch_.load(std::memory_order_relaxed) == role_epoch &&
                 !fail_stop.has_value() &&
                 (current_follow || cluster_context == nullptr ||
                  (cluster_ready && !cluster_proof_invalidated)));
      }
      if (cluster_attempt_must_retire) {
        absl::Status terminal = connected;
        if (fail_stop.has_value()) {
          terminal = absl::InternalError(
              absl::StrCat("replication failed-stopped: ", *fail_stop));
        } else if (cluster_control_stopping_) {
          terminal = absl::CancelledError(
              "cluster rebuild cancelled and retired for process shutdown");
        } else if (connected.ok()) {
          terminal = absl::AbortedError(
              "replication session ended before rebuild readiness");
        }
        cluster_context->completion_->Resolve(std::move(terminal));
      }
      if (fail_stop.has_value()) {
        LatchReplicationFailure(*fail_stop);
        break;
      }
      if (!retry) continue;
      StoreRole(ReplicationRole::kConnecting, std::memory_order_release);
      spdlog::warn("replication connection to {}:{} ended: {}", upstream.host_,
                   upstream.port_, connected.message());
      absl::Status slept = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, kReconnectDelay);
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
      auto next = reader->NextStreaming();
      if (!next.ok()) co_return next.status();
      if (!next->has_value()) break;
      auto drained = reader->DrainCollection();
      if (!drained.ok()) co_return drained;
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
    bycorf::UnlockGuard fullsync_unlock(&redis_fullsync_mutex_,
                                        bycorf::ThisWorker().self_);

    while (!CloseAllCommandDbGates()) {
      absl::Status waited = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) co_return waited;
    }
    struct CommandGateGuard {
      ~CommandGateGuard() { OpenAllCommandDbGates(); }
    } command_gate_guard;
    while (CommandDbOperationsActive()) {
      absl::Status waited = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) co_return waited;
    }
    {
      AssertStateOwner();
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
      auto next = reader->NextStreaming();
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
      auto result = co_await rdb::RestoreFileEntry(storage_, &*reader, entry);
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
    bycorf::UnlockGuard fullsync_unlock(&redis_fullsync_mutex_,
                                        bycorf::ThisWorker().self_);
    while (!CloseAllCommandDbGates()) {
      absl::Status waited = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) co_return waited;
    }
    struct CommandGateGuard {
      ~CommandGateGuard() { OpenAllCommandDbGates(); }
    } reopen;
    while (CommandDbOperationsActive()) {
      absl::Status waited = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) co_return waited;
    }
    {
      AssertStateOwner();
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
      if (source->role_epoch_ != role_epoch_.load(std::memory_order_acquire)) {
        co_return absl::CancelledError(
            "Redis replication source was detached before command apply");
      }
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
      AssertStateOwner();
      if (redis_full_sync_session_id_ == 0 &&
          source->dataset_valid_.load(std::memory_order_acquire)) {
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
    absl::Status activated = co_await WaitForRedisFullSyncActivation(source);
    if (!activated.ok()) co_return activated;
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
    auto full_sync_session = co_await BeginRedisFullSyncAttempt(source);
    if (!full_sync_session.ok()) co_return full_sync_session.status();
    auto rdb_path = co_await ReceiveRedisRdb(stream);
    if (!rdb_path.ok()) co_return rdb_path.status();
    absl::Status status = co_await ImportRedisRdb(*rdb_path, source);
    (void)::unlink(rdb_path->c_str());
    if (!status.ok()) co_return status;

    {
      co_await redis_fullsync_mutex_.Lock();
      bycorf::UnlockGuard fullsync_unlock(&redis_fullsync_mutex_,
                                          bycorf::ThisWorker().self_);
      source->offset_.store(offset, std::memory_order_release);
      source->dataset_valid_ = true;
      bool population_complete = true;
      std::string population_accumulator;
      {
        AssertStateOwner();
        if (redis_full_sync_session_id_ != *full_sync_session) {
          co_return absl::CancelledError(
              "Redis full sync attempt was superseded during import");
        }
        source->replid_ = std::move(replid);
        source->full_sync_session_id_ = *full_sync_session;
        if (redis_sources_.front() == source) {
          upstream_node_id_ = source->replid_;
          upstream_history_id_ = source->replid_;
        }
        source_worker_count_ = static_cast<unsigned>(redis_sources_.size());
        RedisSlotSet registered_slots;
        for (const auto& current : redis_sources_) {
          registered_slots |= current->slots_;
          const bool valid =
              current->dataset_valid_.load(std::memory_order_acquire) &&
              current->replid_.has_value() &&
              current->full_sync_session_id_ == *full_sync_session;
          population_complete &= valid;
          if (valid) {
            absl::StrAppend(&population_accumulator, *current->replid_, ":",
                            current->offset_.load(std::memory_order_acquire),
                            ";");
          }
        }
        if (redis_cluster_) {
          population_complete =
              population_complete && !redis_topology_fault_ &&
              expected_redis_topology_.has_value() &&
              registered_slots.count() == storage::kLogicalStorageShards &&
              redis_sources_.size() ==
                  expected_redis_topology_->masters_.size();
        }
      }
      if (population_complete) {
        const auto bytes = std::span<const std::byte>(
            reinterpret_cast<const std::byte*>(population_accumulator.data()),
            population_accumulator.size());
        status = co_await storage_->CompleteReplicaFullSync(
            *full_sync_session, storage::PopulationToken{
                                    .generation_ = *full_sync_session,
                                    .digest_ = storage::Crc64(bytes),
                                });
        if (!status.ok()) {
          AssertStateOwner();
          if (redis_full_sync_session_id_ == *full_sync_session) {
            source->dataset_valid_.store(false, std::memory_order_release);
            source->full_sync_session_id_ = 0;
            source->replid_.reset();
          }
          co_return status;
        }
        {
          AssertStateOwner();
          if (redis_full_sync_session_id_ != *full_sync_session) {
            co_return absl::CancelledError(
                "Redis full sync activation was superseded");
          }
          redis_full_sync_session_id_ = 0;
        }
      }
    }
    spdlog::info("Redis FULLRESYNC completed from {}:{} at offset {}",
                 source->upstream_.host_, source->upstream_.port_,
                 source->offset_.load(std::memory_order_acquire));
    co_return co_await RedisFollowerOnline(stream, source);
  }

  Task<absl::Status> RunRedisReplicaSession(
      const std::shared_ptr<RedisSource>& source,
      const std::shared_ptr<ReplicaSession>& session) {
    session->active_flows_.fetch_add(1, std::memory_order_acq_rel);
    ReplicaFlowActivityGuard activity(&session->active_flows_);
    auto connected =
        co_await ConnectTcp(source->upstream_.host_, source->upstream_.port_,
                            tls_context_, &session->sockets_);
    if (!connected.ok()) co_return connected.status();
    TcpStream stream = std::move(*connected);
    const int fd = stream.NativeFd();
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
      AssertStateOwner();
      valid_cursor = source->dataset_valid_.load(std::memory_order_acquire) &&
                     source->replid_.has_value();
    }
    if (!valid_cursor) {
      co_return absl::FailedPreconditionError(
          "Redis accepted partial sync without a valid local dataset");
    }
    if (reply->replid_.has_value()) {
      AssertStateOwner();
      source->replid_ = std::move(reply->replid_);
    }
    spdlog::info("Redis partial resynchronization continued from offset {}",
                 source->offset_.load(std::memory_order_acquire));
    co_return co_await RedisFollowerOnline(stream, source);
  }

  Task<absl::Status> RunReplicaSession(
      const ReplicaOfConfig& upstream, std::uint64_t role_epoch,
      const std::shared_ptr<ReplicaSession>& session) {
    auto connected = co_await ConnectTcp(upstream.host_, upstream.port_,
                                         tls_context_, &session->sockets_);
    if (!connected.ok()) co_return connected.status();
    TcpStream control = std::move(*connected);
    const int control_fd = control.NativeFd();
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
    // The control hello carries the complete resume context. Flow handshakes
    // merely bind one socket to one component of this whole-group vector.
    std::string requested_group;
    std::string requested_history;
    std::string applied_vector;
    std::vector<std::uint64_t> requested_next_lsns;
    unsigned requested_flow_count = 0;
    bool resume_proof_advertised = false;
    {
      AssertStateOwner();
      requested_group = group_id_;
      // A durable full-sync fence means the old population and resume proof
      // have already been invalidated. Even if this boot still remembers a
      // matching history and backlog cursor, advertising them could create a
      // transient ONLINE state over an incomplete replacement.
      const bool replacement_required =
          storage_->ReplicaRecoveryFenced() ||
          !native_dataset_valid_.load(std::memory_order_acquire);
      requested_history =
          replacement_required ? "?" : upstream_history_id_.value_or("?");
      if (!replacement_required && applied_frontier_ != nullptr) {
        auto snapshot = applied_frontier_->TrySnapshot();
        if (snapshot.ok()) {
          requested_next_lsns = std::move(*snapshot);
          applied_vector = EncodeAppliedVector(requested_next_lsns);
          requested_flow_count =
              static_cast<unsigned>(requested_next_lsns.size());
          resume_proof_advertised = true;
        } else {
          applied_vector = "?";
        }
      } else {
        applied_vector = "?";
      }
    }
    std::vector<std::string> sync_args{
        "LVPSYNC",
        std::string(kProtocolVersion),
        absl::StrCat("?", node_id_, ":", listen_port_),
        requested_group,
        requested_history,
        replica_incarnation_,
        boot_id_,
        applied_vector};
    if (session->cluster_follow_ != nullptr) {
      const DesiredClusterUpstream& desired =
          session->cluster_follow_->desired_;
      sync_args.insert(
          sync_args.end(),
          {"FOLLOW", desired.group_id_, desired.local_assignment_id_,
           desired.owner_assignment_id_, absl::StrCat(desired.group_term_),
           desired.owner_node_id_, absl::StrCat(desired.manifest_revision_),
           desired.manifest_id_.Hex(),
           absl::StrCat(desired.partition_replication_epoch_)});
    } else if (session->cluster_rebuild_ != nullptr) {
      const RebuildIdentity& identity =
          session->cluster_rebuild_->directive_.identity_;
      sync_args.insert(
          sync_args.end(),
          {"POPULATION", identity.group_id_, identity.assignment_id_,
           identity.source_assignment_id_, absl::StrCat(identity.term_),
           absl::StrCat(identity.directive_revision_), identity.authority_id_,
           identity.source_node_id_, identity.source_boot_id_,
           identity.source_history_id_, identity.target_node_id_,
           identity.target_boot_id_, identity.operation_id_,
           identity.directive_id_, identity.attempt_id_,
           absl::StrCat(identity.manifest_revision_),
           identity.manifest_id_.Hex(),
           absl::StrCat(identity.partition_replication_epoch_)});
    }
    const std::string encoded_sync = EncodeRespCommand(sync_args);
    absl::Status sent = co_await WriteText(control, encoded_sync);
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
    if (*response == kLeaseAdmissionSuspendedReply) {
      session->sockets_.Remove(control_fd);
      control.Close().IgnoreError();
      co_return absl::UnavailableError(kLeaseAdmissionSuspendedStatus);
    }
    const std::vector<std::string_view> words = SplitWords(*response);
    std::uint64_t session_id = 0;
    unsigned source_workers = 0;
    const bool scoped_cluster_handshake = session->cluster_follow_ != nullptr ||
                                          session->cluster_rebuild_ != nullptr;
    const bool source_group_valid =
        scoped_cluster_handshake
            ? words.size() == 8 && IsPopulationGroupToken(words[3])
            : words.size() == 8 && IsReplicationId(words[3]);
    if (words.size() != 8 || words[0] != "+LVFULLRESYNC" ||
        !ParseUnsigned(words[1], &session_id) || session_id == 0 ||
        !IsReplicationId(words[2]) || !source_group_valid ||
        !IsReplicationId(words[4]) || !IsReplicationId(words[5]) ||
        !ParseUnsigned(words[6], &source_workers) || source_workers == 0 ||
        !IsReplicationId(words[7])) {
      if (session->cluster_follow_ == nullptr &&
          session->cluster_rebuild_ != nullptr) {
        (void)co_await InvalidateReplicaContinuation(session, false);
      }
      session->sockets_.Remove(control_fd);
      control.Close().IgnoreError();
      co_return absl::InvalidArgumentError(
          "invalid LVPSYNC response from upstream");
    }
    if (role_epoch_.load(std::memory_order_acquire) != role_epoch) {
      session->sockets_.Remove(control_fd);
      control.Close().IgnoreError();
      co_return absl::CancelledError("replication role epoch was replaced");
    }
    if (session->cluster_follow_ != nullptr) {
      const DesiredClusterUpstream& desired =
          session->cluster_follow_->desired_;
      if (words[2] != desired.owner_node_id_ ||
          words[3] != PopulationGroupToken(desired.group_id_)) {
        session->sockets_.Remove(control_fd);
        control.Close().IgnoreError();
        co_return absl::FailedPreconditionError(
            "native source node/group does not match the steady Owner "
            "relationship");
      }
    } else if (session->cluster_rebuild_ != nullptr) {
      const RebuildDirective& directive = session->cluster_rebuild_->directive_;
      if (words[2] != directive.identity_.source_node_id_ ||
          words[3] != PopulationGroupToken(directive.identity_.group_id_) ||
          words[4] != directive.identity_.source_boot_id_ ||
          words[5] != directive.identity_.source_history_id_ ||
          source_workers != directive.flow_count_) {
        (void)co_await InvalidateReplicaContinuation(session, false);
        session->sockets_.Remove(control_fd);
        control.Close().IgnoreError();
        co_return absl::FailedPreconditionError(
            "native source node/group/boot/history/flow layout does not "
            "match the cluster rebuild directive");
      }
    }
    bool local_population_matches_response =
        resume_proof_advertised && requested_group == words[3] &&
        requested_history == words[5] && requested_flow_count == source_workers;
    if (session->cluster_follow_ != nullptr &&
        (!local_population_matches_response ||
         session->cluster_follow_->force_full_.load(
             std::memory_order_acquire))) {
      absl::Status admitted = BeginClusterFollowFullPopulation(
          session, std::string(words[4]), std::string(words[5]),
          source_workers);
      if (!admitted.ok()) {
        session->sockets_.Remove(control_fd);
        control.Close().IgnoreError();
        co_return admitted;
      }
      local_population_matches_response = false;
    }
    if (!local_population_matches_response) {
      // Publishing a new source context makes it eligible for the next
      // reconnect. Persist the destructive fence first, so a disconnect
      // before the first flow cannot turn an empty/old population into a
      // same-context CONTINUE proof.
      session->destructive_root_started_.store(true, std::memory_order_release);
      absl::Status invalidated =
          co_await storage_->BeginReplicaFullSync(session_id);
      if (!invalidated.ok()) {
        session->sockets_.Remove(control_fd);
        control.Close().IgnoreError();
        co_return invalidated;
      }
      native_dataset_valid_.store(false, std::memory_order_release);
      if (role_epoch_.load(std::memory_order_acquire) != role_epoch) {
        session->sockets_.Remove(control_fd);
        control.Close().IgnoreError();
        co_return absl::CancelledError("replication role epoch was replaced");
      }
    }
    session->session_id_ = session_id;
    session->flow_capability_ = std::string(words[7]);
    session->source_worker_count_ = source_workers;
    session->transaction_owners_.reserve(storage_->worker_count());
    for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
      session->transaction_owners_.push_back(
          std::make_unique<ReplicaTransactionOwner>());
    }
    session->fullsync_cut_ =
        std::make_unique<bycorf::CoroutineBarrier>(source_workers);
    session->promotion_complete_ =
        std::make_unique<bycorf::CoroutineBarrier>(source_workers);
    session->flow_modes_selected_ =
        std::make_unique<bycorf::CoroutineBarrier>(source_workers);
    session->fullsync_begin_complete_ =
        std::make_unique<bycorf::CoroutineBarrier>(source_workers);
    // Flow requests use one immutable control-handshake snapshot. A changed
    // layout never inherits an old prefix; it starts at the initial cursor and
    // the source selects FULL collectively.
    auto initial_next_lsns = detail::InitialAppliedNextLsnsForReconnect(
        source_workers, requested_next_lsns, local_population_matches_response);
    if (!initial_next_lsns.ok()) {
      session->sockets_.Remove(control_fd);
      control.Close().IgnoreError();
      co_return initial_next_lsns.status();
    }
    auto next_frontier = std::make_shared<detail::ReplicaAppliedFrontier>(
        source_workers, storage_->worker_count());
    absl::Status installed = next_frontier->InstallNextLsns(*initial_next_lsns);
    if (!installed.ok()) {
      session->sockets_.Remove(control_fd);
      control.Close().IgnoreError();
      co_return installed;
    }
    {
      AssertStateOwner();
      applied_frontier_ = next_frontier;
      session->applied_frontier_ = std::move(next_frontier);
      session->InitializeFullSyncState(*initial_next_lsns);
      if (active_replica_session_ == session) {
        upstream_node_id_ = std::string(words[2]);
        group_id_ = std::string(words[3]);
        upstream_history_id_ = std::string(words[5]);
        replica_session_id_ = session_id;
        source_worker_count_ = source_workers;
      }
    }

    if (ShouldInjectControlDropAfterResponse()) {
      session->sockets_.Remove(control_fd);
      control.Close().IgnoreError();
      co_return absl::UnavailableError(
          "injected replication control disconnect after response");
    }

    for (unsigned flow_id = 0; flow_id < source_workers; ++flow_id) {
      const unsigned owner = flow_id % storage_->worker_count();
      auto start = [this, upstream, session, flow_id]() {
        session->active_flows_.fetch_add(1, std::memory_order_acq_rel);
        bycorf::ThisWorker().self_->Spawn(
            RunReplicaFlow(upstream, session, flow_id));
        return absl::OkStatus();
      };
      absl::Status started;
      if (owner == bycorf::ThisWorker().id_) {
        started = start();
      } else {
        started = co_await bycorf::SubmitTo(owner, start);
      }
      if (!started.ok()) {
        session->sockets_.Remove(control_fd);
        control.Close().IgnoreError();
        (void)co_await CancelAndWaitForReplicaFlows(session);
        co_return started;
      }
    }

    auto online = co_await ReadLine(control);
    if (!online.ok() || *online != "+LVONLINE") {
      session->sockets_.Remove(control_fd);
      control.Close().IgnoreError();
      absl::Status failed =
          online.ok() ? absl::InvalidArgumentError(
                            "upstream did not complete flow handshake")
                      : online.status();
      absl::Status stopped = co_await CancelAndWaitForReplicaFlows(session);
      co_return stopped.ok() ? failed : stopped;
    }
    if (!session->ReadyForOnline(
            native_dataset_valid_.load(std::memory_order_acquire))) {
      session->sockets_.Remove(control_fd);
      control.Close().IgnoreError();
      absl::Status failed = absl::FailedPreconditionError(
          "upstream declared ONLINE before the local rebuild proof completed");
      absl::Status stopped = co_await CancelAndWaitForReplicaFlows(session);
      co_return stopped.ok() ? failed : stopped;
    }
    bool still_current = false;
    {
      AssertStateOwner();
      still_current =
          active_replica_session_ == session &&
          role_epoch_.load(std::memory_order_relaxed) == role_epoch &&
          !replica_reconfiguration_running_;
    }
    if (!still_current) {
      session->sockets_.Remove(control_fd);
      control.Close().IgnoreError();
      absl::Status stopped = co_await CancelAndWaitForReplicaFlows(session);
      const absl::Status replaced =
          absl::CancelledError("replication role epoch was replaced");
      co_return stopped.ok() ? replaced : stopped;
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
        auto& transactions = session->transaction_owners_[owner]->transactions_;
        std::vector<std::shared_ptr<ReplicaTransactionArrival>> arrivals;
        arrivals.reserve(transactions.size());
        for (auto it = transactions.begin(); it != transactions.end();) {
          auto& arrival = it->second;
          // A complete transaction that already entered apply owns an active
          // counter and must publish either its committed participant cursors
          // or its failure before role transition captures the frontier.
          if (arrival->applying_) {
            ++it;
            continue;
          }
          arrival->status_ =
              absl::CancelledError("replication session cancelled");
          arrivals.push_back(std::move(arrival));
          const auto discarded = it++;
          transactions.erase(discarded);
        }
        for (const auto& arrival : arrivals) {
          (void)arrival->completion_.ResolveOnce(
              storage::ReplicationTransactionResolution::kDiscard);
        }
        return absl::OkStatus();
      };
      absl::Status cancelled;
      if (owner == bycorf::ThisWorker().id_) {
        cancelled = cancel_owner();
      } else {
        cancelled = co_await bycorf::SubmitTo(owner, cancel_owner);
      }
      if (!cancelled.ok()) {
        session->RequireFailStop(
            absl::StrCat("replica cancellation could not resolve worker ",
                         owner, ": ", cancelled.message()));
        co_return cancelled;
      }
    }
    auto next_warning =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (session->active_flows_.load(std::memory_order_acquire) != 0 ||
           session->active_transaction_applies_.load(
               std::memory_order_acquire) != 0) {
      absl::Status slept = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!slept.ok()) {
        session->RequireFailStop(absl::StrCat(
            "replica cancellation could not join detached apply work: ",
            slept.message()));
        co_return slept;
      }
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

  Task<absl::Status> ValidateClusterResetBoundary(
      const std::shared_ptr<ClusterRebuildContext>& context) {
    if (context == nullptr) co_return absl::OkStatus();
    auto validate = [this, context] {
      return cluster_group_->ValidateResetAuthorization(
          *context->authorization_);
    };
    co_return bycorf::ThisWorker().id_ == 0
        ? validate()
        : co_await bycorf::SubmitTo(0, std::move(validate));
  }

  Task<absl::Status> RecordClusterResetProof(
      const std::shared_ptr<ClusterRebuildContext>& context,
      std::vector<storage::ReplicaPartitionEpoch> resets) {
    if (context == nullptr) co_return absl::OkStatus();
    auto record = [this, context, resets = std::move(resets)] {
      for (const storage::ReplicaPartitionEpoch& reset : resets) {
        absl::Status status = cluster_group_->RecordPartitionReset(
            context->directive_.identity_, reset.partition_id_,
            reset.replication_epoch_);
        if (!status.ok()) return status;
      }
      return absl::OkStatus();
    };
    co_return bycorf::ThisWorker().id_ == 0
        ? record()
        : co_await bycorf::SubmitTo(0, std::move(record));
  }

  Task<absl::Status> RecordClusterHandoffProof(
      const std::shared_ptr<ClusterRebuildContext>& context,
      std::uint16_t partition_id, std::uint64_t target_local_epoch) {
    if (context == nullptr) co_return absl::OkStatus();
    auto record = [this, context, partition_id, target_local_epoch] {
      return cluster_group_->RecordPartitionHandoff(
          context->directive_.identity_, partition_id,
          context->manifest_->logical_epochs()[partition_id],
          target_local_epoch);
    };
    co_return bycorf::ThisWorker().id_ == 0
        ? record()
        : co_await bycorf::SubmitTo(0, std::move(record));
  }

  Task<absl::Status> PrepareReplicaFlowMode(
      const std::shared_ptr<ReplicaSession>& session, unsigned flow_id,
      bool fullsync) {
    absl::Status agreed = co_await session->flow_modes_selected_->Wait(
        *bycorf::ThisWorker().self_);
    if (!agreed.ok()) co_return agreed;
    if (!fullsync) co_return absl::OkStatus();

    if (flow_id == 0 && session->cluster_follow_ != nullptr &&
        session->cluster_rebuild_ != nullptr &&
        session->cluster_rebuild_->state_.load(std::memory_order_acquire) ==
            ReplicationGroupState::kReady) {
      // The authenticated source matched our history but no longer retains
      // every requested cursor. Do not invalidate the usable population in a
      // flow worker. Fail this session before BeginReplicaFullSync and let the
      // fixed-delay coordinator retry install a fresh FULL authorization on
      // worker zero.
      session->cluster_follow_->force_full_.store(true,
                                                  std::memory_order_release);
      const absl::Status retry = absl::UnavailableError(
          "steady Owner no longer retains the requested continuation");
      session->fullsync_begin_complete_->Abort(retry);
      co_return retry;
    }

    if (flow_id == 0 && session->cluster_rebuild_ != nullptr &&
        session->cluster_rebuild_->state_.load(std::memory_order_acquire) ==
            ReplicationGroupState::kReady) {
      const absl::Status fresh = absl::FailedPreconditionError(
          "cluster history cannot continue; a fresh rebuild attempt is "
          "required before destructive reset");
      (void)co_await InvalidateReplicaContinuation(session);
      session->fullsync_begin_complete_->Abort(fresh);
      co_return fresh;
    }

    // A role-generation change prevents new external data commands, but work
    // admitted against the old population may already hold a database gate.
    // Flow zero establishes the destructive-reset boundary while all other
    // flows wait: close admission, drain that work, and quiesce background
    // mutators before reopening the gates for trusted replica apply.
    if (flow_id == 0) {
      absl::Status prepared = session->PrepareFullSyncCursors();
      while (prepared.ok() && !CloseAllCommandDbGates()) {
        if (session->cancelled()) {
          prepared = absl::CancelledError(
              "replication session ended before full-sync admission closed");
          break;
        }
        prepared = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                             std::chrono::milliseconds(1));
        if (!prepared.ok()) break;
      }
      const bool gates_closed = prepared.ok();
      if (gates_closed) {
        while (CommandDbOperationsActive()) {
          if (session->cancelled()) {
            prepared = absl::CancelledError(
                "replication session ended while draining old commands");
            break;
          }
          prepared = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                               std::chrono::milliseconds(1));
          if (!prepared.ok()) break;
        }
      }
      if (prepared.ok()) {
        prepared = co_await storage_->QuiesceTombRaiderForReplica();
      }
      if (prepared.ok()) {
        prepared = co_await storage_->QuiesceExpiration();
        if (prepared.ok()) storage_->ResumeExpiration();
      }
      if (gates_closed) OpenAllCommandDbGates();
      if (!prepared.ok()) {
        session->fullsync_begin_complete_->Abort(prepared);
        co_return prepared;
      }
    }

    co_return co_await session->fullsync_begin_complete_->Wait(
        *bycorf::ThisWorker().self_);
  }

  Task<absl::Status> RunReplicaFlow(ReplicaOfConfig upstream,
                                    std::shared_ptr<ReplicaSession> session,
                                    unsigned flow_id) {
    ReplicaFlowActivityGuard activity(&session->active_flows_);
    auto connected = co_await ConnectTcp(upstream.host_, upstream.port_,
                                         tls_context_, &session->sockets_);
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
    const auto cursor = session->RequestedCursor(flow_id);
    spdlog::info(
        "replication target session {} flow {} requesting cursor={}:{}",
        session->session_id_, flow_id, cursor.lsn_, cursor.fragment_index_);
    const std::vector<std::string> flow_args{
        "LVFLOW",
        std::string(kProtocolVersion),
        std::to_string(session->session_id_),
        std::to_string(flow_id),
        std::to_string(cursor.lsn_),
        std::to_string(cursor.fragment_index_),
        session->flow_capability_};
    const std::string encoded_flow = EncodeRespCommand(flow_args);
    absl::Status sent = co_await WriteText(stream, encoded_flow);
    if (!sent.ok()) {
      session->sockets_.Remove(fd);
      stream.Close().IgnoreError();
      session->Cancel();
      co_return sent;
    }
    auto response = co_await ReadLine(stream);
    std::uint64_t response_session_id = 0;
    unsigned response_flow_id = 0;
    const std::vector<std::string_view> response_words =
        response.ok() ? SplitWords(*response) : std::vector<std::string_view>{};
    const bool fullsync =
        response_words.size() == 4 && response_words[3] == "FULL";
    const bool continue_mode =
        response_words.size() == 4 && response_words[3] == "CONTINUE";
    if (!response.ok() || response_words.size() != 4 ||
        response_words[0] != "+LVFLOW" ||
        !ParseUnsigned(response_words[1], &response_session_id) ||
        response_session_id != session->session_id_ ||
        !ParseUnsigned(response_words[2], &response_flow_id) ||
        response_flow_id != flow_id || (!fullsync && !continue_mode)) {
      session->sockets_.Remove(fd);
      stream.Close().IgnoreError();
      session->Cancel();
      co_return response.ok()
          ? absl::InvalidArgumentError("invalid LVFLOW response")
          : response.status();
    }
    absl::Status selected = session->SelectFlowMode(flow_id, fullsync);
    if (!selected.ok()) {
      session->sockets_.Remove(fd);
      stream.Close().IgnoreError();
      session->Cancel();
      co_return selected;
    }
    absl::Status mode_ready =
        co_await PrepareReplicaFlowMode(session, flow_id, fullsync);
    if (!mode_ready.ok()) {
      session->sockets_.Remove(fd);
      stream.Close().IgnoreError();
      session->Cancel();
      co_return mode_ready;
    }
    if (fullsync) {
      // Every flow performs this idempotent call before reading its first data
      // frame. The single system-state writer makes all of them wait for the
      // same durable invalidation, so no partition reset can outrun it.
      session->destructive_root_started_.store(true, std::memory_order_release);
      absl::Status invalidated =
          co_await storage_->BeginReplicaFullSync(session->session_id_);
      if (!invalidated.ok()) {
        session->sockets_.Remove(fd);
        stream.Close().IgnoreError();
        session->Cancel();
        co_return invalidated;
      }
      // ResetReplicaPartitions destructively detaches the previous population.
      // Once FULL is selected, the old root is not a promotion candidate even
      // if this attempt disconnects before receiving its first reset frame.
      native_dataset_valid_.store(false, std::memory_order_release);
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
        co_await arrival->completion_.Wait(*bycorf::ThisWorker().self_);
    if (resolution == storage::ReplicationTransactionResolution::kDiscard &&
        arrival->status_.ok()) {
      co_return absl::CancelledError(
          "replicated transaction was discarded before completion");
    }
    co_return arrival->status_;
  }

  Task<absl::Status> ApplyReadyReplicaTransaction(
      std::shared_ptr<ReplicaSession> session, unsigned owner,
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
    std::vector<detail::ReplicaAppliedFrontier::FlowApplied> frontier_updates;
    frontier_updates.reserve(arrival->participants_.size());
    absl::Status status = absl::OkStatus();
    for (unsigned participant : arrival->participants_) {
      const std::uint64_t lsn = arrival->lsns_[participant];
      if (lsn == std::numeric_limits<std::uint64_t>::max()) {
        status = absl::OutOfRangeError(
            "replicated transaction LSN cannot advance past UINT64_MAX");
        break;
      }
      frontier_updates.push_back(
          {.flow_id_ = participant, .applied_lsn_ = lsn});
    }

    if (status.ok()) {
      for (const auto& predecessor : predecessors) {
        status = co_await WaitForReplicaTransaction(predecessor);
        if (!status.ok()) break;
      }
    }
    // Once every participant is registered, promotion owns this apply through
    // active_transaction_applies_. Transport cancellation discards only
    // incomplete arrivals; this task must publish its cursor into the frozen
    // frontier before the role transition can continue.
    LAVIK_FAULT_INJECT(if (status.ok()) status =
                           co_await MaybePauseBeforeReplicaTransactionApply(););
    if (status.ok()) status = co_await ApplyReplicatedCommand(command);

    if (status.ok()) {
      // Cursor publication is part of the apply completion, not network ACK.
      // A role change may close the socket while this transaction is inside
      // storage; a successful commit must still enter the frozen frontier.
      status = session->applied_frontier_->AdvanceBatchAfterApply(
          owner, frontier_updates);
    }
    arrival->status_ = std::move(status);
    auto& transactions = session->transaction_owners_[owner]->transactions_;
    auto found = transactions.find(arrival->id_);
    if (found != transactions.end() && found->second == arrival) {
      // All declared participants arrived before apply started. Their flow
      // queues retain the shared arrival until ACK, so the owner table no
      // longer needs to extend its lifetime after publishing the result.
      transactions.erase(found);
    }
    (void)arrival->completion_.ResolveOnce(
        arrival->status_.ok()
            ? storage::ReplicationTransactionResolution::kPublish
            : storage::ReplicationTransactionResolution::kDiscard);
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
    assert(owner == bycorf::ThisWorker().id_);
    if (session->cancelled()) {
      return absl::CancelledError(
          "replication session ended before transaction arrival");
    }
    auto& transactions = session->transaction_owners_[owner]->transactions_;
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
    if (prepared.predecessor_ != nullptr && prepared.predecessor_ != arrival &&
        std::find(arrival->predecessors_.begin(), arrival->predecessors_.end(),
                  prepared.predecessor_) == arrival->predecessors_.end()) {
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
      bycorf::ThisWorker().self_->Spawn(
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
    auto register_on_owner = [this, session, owner,
                              prepared = std::move(*prepared)]() mutable {
      return RegisterReplicaTransactionOnOwner(session, owner,
                                               std::move(prepared));
    };
    co_return co_await bycorf::SubmitTo(owner, std::move(register_on_owner));
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
    std::vector<detail::ReplicaAppliedFrontier::FlowApplied> frontier_updates;
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
        frontier_updates.reserve(session->source_worker_count_);
        for (unsigned participant = 0;
             participant < session->source_worker_count_; ++participant) {
          frontier_updates.push_back(
              {.flow_id_ = participant,
               .applied_lsn_ = arrival->lsns_[participant]});
        }
      }
    }

    if (apply_here) {
      absl::Status status = absl::OkStatus();
      LAVIK_FAULT_INJECT(status =
                             co_await MaybePauseBeforeReplicaControlApply(););
      if (status.ok()) {
        if (std::ranges::any_of(frontier_updates, [](const auto& update) {
              return update.applied_lsn_ ==
                     std::numeric_limits<std::uint64_t>::max();
            })) {
          status = absl::OutOfRangeError(
              "replicated control LSN cannot advance past UINT64_MAX");
        }
      }
      if (status.ok()) {
        status = co_await ApplyReplicatedCommand(apply_command);
      }
      if (status.ok()) {
        status = session->applied_frontier_->AdvanceBatchAfterApply(
            bycorf::ThisWorker().id_, frontier_updates);
      }
      std::lock_guard lock(session->control_mutex_);
      arrival->status_ = std::move(status);
    }

    absl::Status completed =
        co_await arrival->completion_.Wait(*bycorf::ThisWorker().self_);
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
    bycorf::AsyncNotification command_ready_;
    bycorf::AsyncNotification capacity_ready_;
    bycorf::AsyncNotification completion_ready_;
    bycorf::AsyncNotification completion_capacity_ready_;
    bycorf::AsyncNotification stage_done_ready_;
    bycorf::AsyncNotification ack_done_ready_;
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

#if LAVIK_FAULTS_ENABLED
  void InjectPeerFlowCancelAfterCommandApply(
      const std::shared_ptr<ReplicaSession>& session, unsigned flow_id,
      const ReplicatedCommand& command) {
    const char* configured = std::getenv(
        "LAVIK_REPLICATION_CANCEL_PEER_FLOW_AFTER_COMMAND_APPLY_ONCE");
    if (configured == nullptr || command.args_.size() < 2 ||
        command.args_[1] != configured ||
        replication_peer_flow_cancel_fault_used_.exchange(
            true, std::memory_order_acq_rel)) {
      return;
    }

    // Mark the whole session at the exact post-commit boundary. This models a
    // sibling flow failure without scheduling test-only cross-worker work.
    session->Cancel();
    spdlog::warn(
        "injected peer-flow session cancellation after command apply on flow "
        "{}",
        flow_id);
  }
#endif

  Task<absl::Status> StageReplicaOnlineCommands(
      const std::shared_ptr<ReplicaSession>& session, unsigned flow_id,
      const std::shared_ptr<ReplicaOnlineApplyState>& state) {
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
      state->capacity_ready_.NotifyAll(*bycorf::ThisWorker().self_);
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
        if (applied.ok() && session->cancelled()) {
          co_return absl::CancelledError(
              "replication session ended before command apply");
        }
        if (applied.ok() && control) {
          applied = co_await ApplyReplicaControl(session, flow_id, pending.lsn_,
                                                 std::move(pending.command_));
        } else if (applied.ok()) {
          if (pending.lsn_ == std::numeric_limits<std::uint64_t>::max()) {
            applied = absl::OutOfRangeError(
                "replicated command LSN cannot advance past UINT64_MAX");
          }
          LAVIK_FAULT_INJECT(if (applied.ok()) {
            applied = co_await MaybePauseBeforeReplicaCommandApply();
          });
          if (applied.ok()) {
            applied = co_await ApplyReplicatedCommand(pending.command_);
          }
          LAVIK_FAULT_INJECT(if (applied.ok()) {
            InjectPeerFlowCancelAfterCommandApply(session, flow_id,
                                                  pending.command_);
          });
        }
      }
      if (!applied.ok()) {
        (void)co_await InvalidateReplicaContinuation(session);
        co_return applied;
      }
      if (!transaction && !control) {
        // Applied is a storage boundary. Publish the cursor here so promotion
        // can close the transport and still capture every command whose local
        // mutation completed; ACK delivery is not part of that proof.
        applied = session->applied_frontier_->AdvanceAfterApply(flow_id,
                                                                pending.lsn_);
        if (!applied.ok()) {
          (void)co_await InvalidateReplicaContinuation(session);
          co_return applied;
        }
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
      state->completion_ready_.NotifyAll(*bycorf::ThisWorker().self_);
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

      ReplicaOnlineCompletion pending = std::move(state->completions_.front());
      state->completions_.pop_front();
      state->completion_capacity_ready_.NotifyAll(*bycorf::ThisWorker().self_);
      const bool transaction = pending.transaction_ != nullptr;
      absl::Status applied = absl::OkStatus();
      if (transaction) {
        applied = co_await WaitForReplicaTransaction(pending.transaction_);
      }
      if (!applied.ok()) {
        (void)co_await InvalidateReplicaContinuation(session);
        co_return applied;
      }

      // Every event publishes its cursor at apply completion. ACK is transport
      // feedback only and must not overwrite a newer cursor after staging has
      // advanced farther on this flow.
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
    state->stage_done_ready_.NotifyAll(*bycorf::ThisWorker().self_);
    state->completion_ready_.NotifyAll(*bycorf::ThisWorker().self_);
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
    state->ack_done_ready_.NotifyAll(*bycorf::ThisWorker().self_);
    state->completion_capacity_ready_.NotifyAll(*bycorf::ThisWorker().self_);
    if (!state->ack_status_.ok()) {
      (void)::shutdown(stream.NativeFd(), SHUT_RDWR);
    }
    co_return state->ack_status_;
  }

  Task<absl::Status> RunReplicaOnlineFlowData(
      TcpStream& stream, const std::shared_ptr<ReplicaSession>& session,
      unsigned flow_id, std::uint64_t first_expected_lsn,
      std::pair<DataFrameKind, std::string> first_frame) {
    auto state = std::make_shared<ReplicaOnlineApplyState>();
    bycorf::ThisWorker().self_->Spawn(
        TrackReplicaOnlineStage(stream, session, flow_id, state));
    bycorf::ThisWorker().self_->Spawn(
        TrackReplicaOnlineAcks(stream, session, flow_id, state));

    std::uint64_t staged_command_lsn = 0;
    std::uint64_t expected_command_lsn = first_expected_lsn;
    std::uint32_t next_command_fragment = 0;
    std::string staged_command;
    std::size_t received_commands = 0;
    std::optional<std::pair<DataFrameKind, std::string>> pending_frame(
        std::move(first_frame));
    absl::Status receiver_status = absl::OkStatus();
    constexpr std::size_t kOnlineQueueCommands = 256;
    while (stream.IsOpen()) {
      if (state->stage_done_ || state->ack_done_) {
        receiver_status =
            state->stage_done_ ? state->stage_status_ : state->ack_status_;
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
      if (!reader.U64(&lsn) || !reader.U32(&fragment) || !reader.U8(&flags) ||
          reader.remaining() == 0) {
        receiver_status =
            absl::InvalidArgumentError("malformed replication command frame");
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
        if (fragment != 0 || staged_command_lsn != 0 ||
            lsn != expected_command_lsn) {
          receiver_status = absl::InvalidArgumentError(
              "replication command fragments overlap or skip history");
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
        receiver_status =
            state->stage_done_ ? state->stage_status_ : state->ack_status_;
        break;
      }
      state->commands_.push_back(ReplicaOnlineCommand{
          .lsn_ = lsn,
          .command_ = std::move(*command),
      });
      state->command_ready_.NotifyAll(*bycorf::ThisWorker().self_);
      staged_command_lsn = 0;
      next_command_fragment = 0;
      staged_command.clear();
      if (expected_command_lsn == std::numeric_limits<std::uint64_t>::max()) {
        receiver_status =
            absl::OutOfRangeError("replication command LSN exhausted");
        break;
      }
      ++expected_command_lsn;
      // Loopback and fast LAN reads can remain immediately-ready for hundreds
      // of megabytes. Give the owner-local FIFO consumer a bounded scheduling
      // opportunity even when ingress never naturally suspends.
      if ((++received_commands % kFullSyncSchedulingItems) == 0) {
        co_await bycorf::Yield(*bycorf::ThisWorker().self_);
      }
    }

    if (receiver_status.ok()) {
      receiver_status = absl::UnavailableError("replication flow closed");
    }
    if (receiver_status.code() == absl::StatusCode::kInvalidArgument ||
        receiver_status.code() == absl::StatusCode::kFailedPrecondition ||
        receiver_status.code() == absl::StatusCode::kDataLoss ||
        receiver_status.code() == absl::StatusCode::kOutOfRange) {
      // A malformed, gapped, or divergent tail cannot be retried from the
      // last cursor: an unacknowledged prefix may already have mutated the
      // in-place dataset. Invalidate the whole continuation domain so every
      // flow in the replacement session selects FULL together.
      (void)co_await InvalidateReplicaContinuation(session);
    }
    state->receiver_status_ = receiver_status;
    state->receiver_done_ = true;
    state->command_ready_.NotifyAll(*bycorf::ThisWorker().self_);
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
    std::optional<std::uint64_t> online_next_lsn;
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
      absl::Status phase =
          session->ValidateDataFramePhase(flow_id, frame->first);
      if (!phase.ok()) co_return phase;
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
        absl::Status authorized =
            co_await ValidateClusterResetBoundary(session->cluster_rebuild_);
        if (!authorized.ok()) co_return authorized;
        std::vector<storage::ReplicaPartitionEpoch> reset_proof;
        reset_proof.reserve(reset_count);
        for (unsigned owner = 0; owner < by_owner.size(); ++owner) {
          if (by_owner[owner].empty()) continue;
          if (owner == bycorf::ThisWorker().id_) {
            auto reset = co_await storage_->ResetReplicaPartitions(
                session->session_id_, by_owner[owner]);
            if (!reset.ok()) co_return reset.status();
            for (const storage::ReplicaPartitionEpoch& result : *reset) {
              epochs[result.partition_id_] = result.replication_epoch_;
              reset_proof.push_back(result);
            }
          } else {
            auto reset = co_await bycorf::SubmitTaskTo(
                owner,
                [this, session, resets = std::move(by_owner[owner])]() mutable {
                  return storage_->ResetReplicaPartitions(session->session_id_,
                                                          resets);
                });
            if (!reset.ok()) co_return reset.status();
            for (const storage::ReplicaPartitionEpoch& result : *reset) {
              epochs[result.partition_id_] = result.replication_epoch_;
              reset_proof.push_back(result);
            }
          }
        }
        absl::Status recorded = co_await RecordClusterResetProof(
            session->cluster_rebuild_, std::move(reset_proof));
        if (!recorded.ok()) co_return recorded;
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
        absl::Status handed_off = co_await bycorf::SubmitTaskTo(
            owner, [this, session, partition_id, epoch]() {
              return storage_->HandoffReplicaPartition(session->session_id_,
                                                       partition_id, epoch);
            });
        if (!handed_off.ok()) co_return handed_off;
        absl::Status handoff_recorded = co_await RecordClusterHandoffProof(
            session->cluster_rebuild_, partition_id, epoch);
        if (!handoff_recorded.ok()) co_return handoff_recorded;
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
          const bool partitionless =
              publish || (command.ok() && !command->args_.empty() &&
                          EqualCaseInsensitive(command->args_[0], "FUNCTION"));
          const bool desired_partition =
              session->cluster_rebuild_ == nullptr || partitionless ||
              session->cluster_rebuild_->manifest_
                      ->logical_epochs()[partition_id] != 0;
          if (!ephemeral && desired_partition) {
            absl::Status begun = co_await bycorf::SubmitTaskTo(
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
          } else if (desired_partition) {
            applied = co_await ApplyReplicatedCommand(*command);
          } else {
            // Destructive reset covers every physical partition, but a sparse
            // cluster manifest installs records only for member slots. Source
            // clusters are expected to be exact too; this target-side filter
            // is the final guard against out-of-manifest snapshot/tail data.
            applied = absl::OkStatus();
          }
          absl::Status ended = absl::OkStatus();
          if (!ephemeral && desired_partition) {
            ended = co_await bycorf::SubmitTaskTo(
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
        if (staged_command_lsn != 0 || next_command_fragment != 0 ||
            !staged_command.empty()) {
          co_return absl::InvalidArgumentError(
              "full-sync cut arrived with an incomplete command fragment");
        }
        DataReader reader(frame->second);
        std::uint64_t sequence = 0;
        std::uint64_t stable_next_lsn = 0;
        if (!reader.U64(&sequence) || !reader.U64(&stable_next_lsn) ||
            reader.remaining() != 0 || sequence != expected_fullsync_sequence ||
            stable_next_lsn == 0 || session->fullsync_cut_ == nullptr ||
            session->promotion_complete_ == nullptr) {
          co_return absl::InvalidArgumentError("malformed full-sync cut frame");
        }
        absl::Status recorded =
            session->RecordFullSyncCut(flow_id, stable_next_lsn);
        if (!recorded.ok()) co_return recorded;
        absl::Status cut =
            co_await session->fullsync_cut_->Wait(*bycorf::ThisWorker().self_);
        if (!cut.ok()) co_return cut;
        if (flow_id == 0) {
          auto cut_vector = session->FullSyncCutVector();
          if (!cut_vector.ok()) {
            session->promotion_complete_->Abort(cut_vector.status());
            co_return cut_vector.status();
          }
          if (session->cluster_rebuild_ != nullptr) {
            absl::Status cluster_recorded = cluster_group_->RecordFlowCutVector(
                session->cluster_rebuild_->directive_.identity_, *cut_vector);
            if (cluster_recorded.ok()) {
              cluster_recorded = cluster_group_->MarkFunctionCatalogComplete(
                  session->cluster_rebuild_->directive_.identity_);
            }
            if (!cluster_recorded.ok()) {
              session->promotion_complete_->Abort(cluster_recorded);
              co_return cluster_recorded;
            }
          }
          while (!CloseAllCommandDbGates()) {
            absl::Status waited = co_await bycorf::SleepFor(
                *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
            if (!waited.ok()) {
              session->promotion_complete_->Abort(waited);
              co_return waited;
            }
          }
          struct PromotionGateGuard {
            ~PromotionGateGuard() { OpenAllCommandDbGates(); }
          } promotion_gate;
          while (CommandDbOperationsActive()) {
            absl::Status waited = co_await bycorf::SleepFor(
                *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
            if (!waited.ok()) {
              session->promotion_complete_->Abort(waited);
              co_return waited;
            }
          }
          absl::Status promoted;
          if (ShouldInjectReplicaPromotionFailure()) {
            promoted =
                absl::InternalError("injected replica promotion failure");
          } else {
            promoted =
                co_await storage_->PromoteReplicaRoot(session->session_id_);
          }
          if (!promoted.ok()) {
            if (session->cluster_rebuild_ != nullptr) {
              (void)cluster_group_->FailStop(
                  session->cluster_rebuild_->directive_.identity_);
            }
            session->RequireFailStop(
                absl::StrCat("replica promotion outcome is uncertain: ",
                             promoted.message()));
            spdlog::warn(
                "replica session entered fail-stop before coordinator latch");
            LAVIK_FAULT_INJECT(
                if (const char* configured = std::getenv(
                        "LAVIK_REPLICATION_PAUSE_AFTER_FAIL_STOP_MS");
                    configured != nullptr) {
                  std::uint64_t pause_ms = 0;
                  const std::size_t length = std::strlen(configured);
                  const auto parsed = std::from_chars(
                      configured, configured + length, pause_ms);
                  if (parsed.ec == std::errc{} &&
                      parsed.ptr == configured + length && pause_ms != 0) {
                    (void)co_await bycorf::SleepFor(
                        *bycorf::ThisWorker().self_,
                        std::chrono::milliseconds(pause_ms));
                  }
                });
            session->promotion_complete_->Abort(promoted);
            co_return promoted;
          }
          bool still_current = false;
          {
            AssertStateOwner();
            still_current = active_replica_session_ == session &&
                            !replica_reconfiguration_running_;
          }
          if (!still_current || session->cancelled()) {
            const absl::Status replaced = absl::CancelledError(
                "replica promotion completed after session supersession");
            session->promotion_complete_->Abort(replaced);
            co_return replaced;
          }
          if (session->cluster_rebuild_ != nullptr) {
            absl::Status group_promoted = cluster_group_->MarkStoragePromoted(
                session->cluster_rebuild_->directive_.identity_);
            if (!group_promoted.ok()) {
              const std::string reason = absl::StrCat(
                  "promoted storage did not match the cluster cut proof: ",
                  group_promoted.message());
              (void)cluster_group_->FailStop(
                  session->cluster_rebuild_->directive_.identity_);
              session->RequireFailStop(reason);
              session->promotion_complete_->Abort(group_promoted);
              co_return group_promoted;
            }
          }
          // Promotion makes the rebuilt root durable and visible. Install the
          // complete all-flow continuation vector before releasing peers or
          // ACKing any cut: a disconnect in that interval must reconnect at
          // the stable cut rather than replaying an old-history cursor.
          absl::Status installed = session->InstallFullSyncCutVector();
          if (!installed.ok()) {
            if (session->cluster_rebuild_ != nullptr) {
              (void)cluster_group_->FailStop(
                  session->cluster_rebuild_->directive_.identity_);
            }
            session->RequireFailStop(
                absl::StrCat("promoted replica cut installation failed: ",
                             installed.message()));
            session->promotion_complete_->Abort(installed);
            co_return installed;
          }
          if (session->cluster_rebuild_ != nullptr) {
            auto ready = cluster_group_->PublishReady(
                session->cluster_rebuild_->directive_.identity_);
            if (!ready.ok()) {
              const std::string reason = absl::StrCat(
                  "promoted population readiness publication failed: ",
                  ready.status().message());
              (void)cluster_group_->FailStop(
                  session->cluster_rebuild_->directive_.identity_);
              session->RequireFailStop(reason);
              session->promotion_complete_->Abort(ready.status());
              co_return ready.status();
            }
            absl::Status recorded = co_await storage_->CommitPopulationIdentity(
                detail::EncodeRecoveredPopulation(ready->identity()));
            if (!recorded.ok()) {
              session->RequireFailStop(recorded.ToString());
              session->promotion_complete_->Abort(recorded);
              co_return recorded;
            }
            recovered_population_.reset();
            recovered_population_fenced_ = false;
            operator_recovery_active_ = false;
            {
              AssertStateOwner();
              if (active_replica_session_ == session &&
                  cluster_rebuild_ == session->cluster_rebuild_ &&
                  !replica_reconfiguration_running_ &&
                  session->cluster_rebuild_->state_.load(
                      std::memory_order_relaxed) ==
                      ReplicationGroupState::kRebuilding) {
                session->cluster_rebuild_->ready_token_ = *ready;
                session->cluster_rebuild_->state_.store(
                    ReplicationGroupState::kReady, std::memory_order_release);
                native_dataset_valid_.store(true, std::memory_order_release);
                if (session->cluster_follow_ != nullptr) {
                  // A backlog-gap latch is needed only until one fresh FULL
                  // publishes a complete replacement population. Clearing it
                  // here, rather than at admission, keeps retries destructive
                  // until success while allowing later reconnects to resume.
                  session->cluster_follow_->force_full_.store(
                      false, std::memory_order_release);
                }
              } else {
                const absl::Status replaced = absl::CancelledError(
                    "cluster readiness completed after session supersession");
                session->promotion_complete_->Abort(replaced);
                co_return replaced;
              }
            }
            session->cluster_rebuild_->completion_->Resolve(absl::OkStatus());
          } else {
            native_dataset_valid_.store(true, std::memory_order_release);
          }
        }
        absl::Status finalized = co_await session->promotion_complete_->Wait(
            *bycorf::ThisWorker().self_);
        if (!finalized.ok()) co_return finalized;
        absl::Status acknowledged =
            co_await send_ack(kResetBatchAckPartition, sequence);
        if (!acknowledged.ok()) co_return acknowledged;
        if (ShouldInjectFullSyncCutDrop(flow_id)) {
          spdlog::warn(
              "injected disconnect after full-sync cut acknowledgement");
          co_return absl::UnavailableError(
              "injected disconnect after full-sync cut acknowledgement");
        }
        ++expected_fullsync_sequence;
      } else if (frame->first == DataFrameKind::kCommand) {
        if (!online_next_lsn.has_value()) {
          co_return absl::FailedPreconditionError(
              "replication command arrived without an ONLINE cursor");
        }
        co_return co_await RunReplicaOnlineFlowData(
            stream, session, flow_id, *online_next_lsn, std::move(*frame));
      } else if (frame->first == DataFrameKind::kCursor) {
        DataReader reader(frame->second);
        std::uint64_t lsn = 0;
        std::uint32_t fragment = 0;
        if (!reader.U64(&lsn) || !reader.U32(&fragment) ||
            reader.remaining() != 0) {
          co_return absl::InvalidArgumentError("malformed replication cursor");
        }
        absl::Status accepted =
            session->AcceptBacklogCursor(flow_id, lsn, fragment);
        if (!accepted.ok()) co_return accepted;
        online_next_lsn = lsn;
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
        const bool desired_partition =
            session->cluster_rebuild_ == nullptr ||
            session->cluster_rebuild_->manifest_
                    ->logical_epochs()[partition_id] != 0;
        absl::Status applied = absl::OkStatus();
        if (desired_partition) {
          applied = co_await bycorf::SubmitTaskTo(
              owner, [this, session, partition_id, epoch,
                      records = std::move(records->second)]() mutable {
                return storage_->ApplyReplicaRecords(
                    session->session_id_, partition_id, epoch, records);
              });
        }
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
    LAVIK_FAULT_INJECT({
      const char* configured = replication_drop_flow_after_command_;
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
    });
    (void)flow_id;
    return false;
  }

  bool ShouldInjectControlDropAfterResponse() {
    LAVIK_FAULT_INJECT({
      const char* configured = replication_drop_after_control_response_once_;
      if (configured == nullptr || std::string_view(configured) != "1") {
        return false;
      }
      bool expected = false;
      return replication_control_response_fault_drop_used_
          .compare_exchange_strong(expected, true, std::memory_order_acq_rel);
    });
    return false;
  }

  bool ShouldInjectFullSyncCutDrop(unsigned flow_id) {
    LAVIK_FAULT_INJECT({
      if (flow_id != 0) return false;
      const char* configured =
          std::getenv("LAVIK_REPLICATION_DROP_AFTER_FULLSYNC_CUT");
      if (configured == nullptr) return false;
      unsigned occurrence = 0;
      const std::size_t length = std::strlen(configured);
      const auto parsed =
          std::from_chars(configured, configured + length, occurrence);
      if (parsed.ec != std::errc{} || parsed.ptr != configured + length ||
          occurrence == 0) {
        return false;
      }
      return replication_fullsync_cut_ack_count_.fetch_add(
                 1, std::memory_order_acq_rel) +
                 1 ==
             occurrence;
    });
    (void)flow_id;
    return false;
  }

  bool ShouldInjectReplicaPromotionFailure() {
    LAVIK_FAULT_INJECT({
      const char* configured =
          std::getenv("LAVIK_REPLICATION_FAIL_PROMOTE_ONCE");
      if (configured == nullptr || std::string_view(configured) != "1") {
        return false;
      }
      return !replication_promotion_fault_used_.exchange(
          true, std::memory_order_acq_rel);
    });
    return false;
  }

  bool ShouldInjectEmptyPopulationResetFailure(unsigned owner) {
    LAVIK_FAULT_INJECT({
      if (owner != 0) return false;
      const char* configured =
          std::getenv("LAVIK_REPLICATION_FAIL_EMPTY_RESET_ONCE");
      if (configured == nullptr || std::string_view(configured) != "1") {
        return false;
      }
      return !empty_population_reset_fault_used_.exchange(
          true, std::memory_order_acq_rel);
    });
    (void)owner;
    return false;
  }

  bool ShouldInjectEmptyPopulationCatalogFailure() {
    LAVIK_FAULT_INJECT({
      const char* configured =
          std::getenv("LAVIK_REPLICATION_FAIL_EMPTY_CATALOG_ONCE");
      if (configured == nullptr || std::string_view(configured) != "1") {
        return false;
      }
      return !empty_population_catalog_fault_used_.exchange(
          true, std::memory_order_acq_rel);
    });
    return false;
  }

  bool ShouldInjectPromotionPrepareFailure(std::string_view stage) {
    LAVIK_FAULT_INJECT({
      if (!LAVIK_FAULT_MATCHES("LAVIK_REPLICATION_FAIL_PROMOTION_PREPARE_AT",
                               stage)) {
        return false;
      }
      return !promotion_prepare_fault_used_.exchange(true,
                                                     std::memory_order_acq_rel);
    });
    (void)stage;
    return false;
  }

  bool ShouldInjectEarlyOnline() const {
    return LAVIK_FAULT_MATCHES("LAVIK_REPLICATION_EARLY_ONLINE", "1");
  }

  bool ShouldInjectPostCutReset(unsigned flow_id) {
    LAVIK_FAULT_INJECT({
      if (flow_id != 0) return false;
      const char* configured =
          std::getenv("LAVIK_REPLICATION_POST_CUT_RESET_ONCE");
      if (configured == nullptr || std::string_view(configured) != "1") {
        return false;
      }
      return !replication_post_cut_reset_fault_used_.exchange(
          true, std::memory_order_acq_rel);
    });
    (void)flow_id;
    return false;
  }

  bool ShouldInjectDivergentTail(unsigned flow_id) {
    LAVIK_FAULT_INJECT({
      unsigned target_flow = 0;
      if (const char* target =
              std::getenv("LAVIK_REPLICATION_DIVERGENT_TAIL_FLOW");
          target != nullptr && !ParseUnsigned(target, &target_flow)) {
        return false;
      }
      if (flow_id != target_flow) return false;
      const char* configured =
          std::getenv("LAVIK_REPLICATION_DIVERGENT_TAIL_ONCE");
      if (configured == nullptr || std::string_view(configured) != "1") {
        return false;
      }
      return !replication_divergent_tail_fault_used_.exchange(
          true, std::memory_order_acq_rel);
    });
    (void)flow_id;
    return false;
  }

  Task<absl::Status> InvalidateReplicaContinuation(
      const std::shared_ptr<ReplicaSession>& session,
      bool require_installed_cursor = true) {
    if (bycorf::ThisWorker().id_ != 0) {
      // This is an error path, never an online per-command owner hop. Join
      // the owner's exact-session invalidation before allowing flow teardown
      // to finish; otherwise promotion/reconnect could reuse the bad cursor.
      co_return co_await bycorf::SubmitTaskTo(0, [this, session,
                                                  require_installed_cursor] {
        return InvalidateReplicaContinuation(session, require_installed_cursor);
      });
    }
    bool continuation_invalidated = false;
    bool cluster_population_invalidated = false;
    {
      AssertStateOwner();
      if (active_replica_session_ != session ||
          (require_installed_cursor &&
           applied_frontier_ != session->applied_frontier_)) {
        co_return absl::OkStatus();
      }
      applied_frontier_.reset();
      upstream_history_id_.reset();
      continuation_invalidated = true;
      if (session->cluster_rebuild_ != nullptr) {
        session->cluster_rebuild_->ready_token_.reset();
        if (!failed_stopped_.load(std::memory_order_relaxed)) {
          session->cluster_rebuild_->state_.store(
              ReplicationGroupState::kNotReady, std::memory_order_release);
        }
        cluster_population_invalidated = true;
      }
    }
    if (continuation_invalidated) {
      spdlog::warn(
          "invalidated native replication continuation; replacement session "
          "requires FULL");
    }
    // Replay may already have committed a successful non-idempotent prefix
    // before a later strict EXEC child failed. Dropping the shared cursor
    // state makes every flow request LSN 1 in the replacement session, which
    // forces one coordinated full sync instead of retrying that prefix.
    if (cluster_population_invalidated) {
      // The owner closes serving before acknowledging invalidation to the
      // detecting flow. Proof retirement follows whole-session join; stale
      // failures from a replaced session cannot fence its successor.
      native_dataset_valid_.store(false, std::memory_order_release);
      storage_->SetReplicaLoading(true);
      StoreRole(ReplicationRole::kConnecting, std::memory_order_release);
      bycorf::ThisWorker().self_->Spawn(
          RevokeClusterRebuildSourceAuthorizations());
    }
    co_return absl::OkStatus();
  }

  bool ShouldInjectFlowDropAfterTransaction(unsigned flow_id) {
    LAVIK_FAULT_INJECT({
      const char* configured = replication_drop_flow_after_transaction_apply_;
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
    });
    (void)flow_id;
    return false;
  }

  bool ShouldInjectFlowDropAfterCommandApply(unsigned flow_id) {
    LAVIK_FAULT_INJECT({
      const char* configured = replication_drop_flow_after_command_apply_;
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
    });
    (void)flow_id;
    return false;
  }

#if LAVIK_FAULTS_ENABLED
  // These coroutines and their call sites are test-only, so ordinary builds
  // do not allocate an empty pause task on each ONLINE mutation or barrier.
  Task<absl::Status> MaybePauseBeforeReplicaCommandApply() {
    const char* configured = replication_pause_before_command_apply_ms_;
    if (configured == nullptr) co_return absl::OkStatus();
    std::uint64_t milliseconds = 0;
    const std::size_t length = std::strlen(configured);
    const auto parsed =
        std::from_chars(configured, configured + length, milliseconds);
    if (parsed.ec != std::errc{} || parsed.ptr != configured + length ||
        milliseconds == 0 || milliseconds > 60000 ||
        replication_command_apply_pause_used_.exchange(
            true, std::memory_order_acq_rel)) {
      co_return absl::OkStatus();
    }
    spdlog::info(
        "replication command admitted; pausing before database admission");
    co_return co_await bycorf::SleepFor(
        *bycorf::ThisWorker().self_, std::chrono::milliseconds(milliseconds));
  }

  Task<absl::Status> MaybePauseBeforeReplicaTransactionApply() {
    const char* configured = replication_pause_before_transaction_apply_ms_;
    if (configured == nullptr) co_return absl::OkStatus();
    std::uint64_t milliseconds = 0;
    const std::size_t length = std::strlen(configured);
    const auto parsed =
        std::from_chars(configured, configured + length, milliseconds);
    if (parsed.ec != std::errc{} || parsed.ptr != configured + length ||
        milliseconds == 0 || milliseconds > 60000 ||
        replication_transaction_apply_pause_used_.exchange(
            true, std::memory_order_acq_rel)) {
      co_return absl::OkStatus();
    }
    spdlog::info(
        "replication transaction complete; pausing before database apply");
    co_return co_await bycorf::SleepFor(
        *bycorf::ThisWorker().self_, std::chrono::milliseconds(milliseconds));
  }

  Task<absl::Status> MaybePauseBeforeReplicaControlApply() {
    const char* configured = replication_pause_before_control_apply_ms_;
    if (configured == nullptr) co_return absl::OkStatus();
    std::uint64_t milliseconds = 0;
    const std::size_t length = std::strlen(configured);
    const auto parsed =
        std::from_chars(configured, configured + length, milliseconds);
    if (parsed.ec != std::errc{} || parsed.ptr != configured + length ||
        milliseconds == 0 || milliseconds > 60000 ||
        replication_control_apply_pause_used_.exchange(
            true, std::memory_order_acq_rel)) {
      co_return absl::OkStatus();
    }
    spdlog::info(
        "replication control barrier complete; pausing before database apply");
    co_return co_await bycorf::SleepFor(
        *bycorf::ThisWorker().self_, std::chrono::milliseconds(milliseconds));
  }

  Task<absl::Status> MaybePauseBeforeRedisExportDbAdmission() {
    const char* configured =
        std::getenv("LAVIK_REPLICATION_PAUSE_REDIS_EXPORT_BEFORE_GATES_MS");
    if (configured != nullptr) {
      std::uint64_t milliseconds = 0;
      const std::size_t length = std::strlen(configured);
      const auto parsed =
          std::from_chars(configured, configured + length, milliseconds);
      if (parsed.ec == std::errc{} && parsed.ptr == configured + length &&
          milliseconds != 0 && milliseconds <= 60000 &&
          !redis_export_gate_pause_used_.exchange(true,
                                                  std::memory_order_acq_rel)) {
        spdlog::info("Redis export active; pausing before database admission");
        co_return co_await bycorf::SleepFor(
            *bycorf::ThisWorker().self_,
            std::chrono::milliseconds(milliseconds));
      }
    }
    co_return absl::OkStatus();
  }
#endif

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
            absl::Status frame_header = AppendDataFrameHeader(
                &frame_headers, DataFrameKind::kFullSyncCommand, payload.size(),
                DataFrameCrc32c(payload));
            if (!frame_header.ok()) co_return frame_header;
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
      LAVIK_FAULT_INJECT(
          if (const char* configured = std::getenv(
                  "LAVIK_REPLICATION_PAUSE_FULLSYNC_BEFORE_CATALOG_ACK_MS");
              configured != nullptr &&
              !replication_fullsync_catalog_pause_used_.exchange(
                  true, std::memory_order_acq_rel)) {
            std::uint64_t pause_ms = 0;
            const std::size_t length = std::strlen(configured);
            const auto parsed =
                std::from_chars(configured, configured + length, pause_ms);
            if (parsed.ec == std::errc{} && parsed.ptr == configured + length &&
                pause_ms != 0 && pause_ms <= 60000) {
              spdlog::info(
                  "native full sync holds command gates before catalog "
                  "acknowledgement");
              const auto deadline = std::chrono::steady_clock::now() +
                                    std::chrono::milliseconds(pause_ms);
              while (!session->cancelled() &&
                     std::chrono::steady_clock::now() < deadline) {
                absl::Status paused = co_await bycorf::SleepFor(
                    *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
                if (!paused.ok()) co_return paused;
              }
              if (session->cancelled()) {
                co_return absl::CancelledError(
                    "replication session ended before catalog acknowledgement");
              }
            }
          });
      auto catalog_operation = co_await AcquireFunctionCatalogOperation();
      std::vector<std::string> args{"FUNCTION", "RESTORE",
                                    GlobalFunctionCatalog().SnapshotDump(),
                                    "FLUSH"};
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
          kBacklogBatchBytes - kDataFrameHeaderBytes - 8 - kCommandHeaderBytes;
      const std::size_t total = static_cast<std::size_t>(source->size());
      std::size_t offset = 0;
      std::uint32_t fragment = 0;
      do {
        const std::size_t count = std::min(kFragmentBytes, total - offset);
        const bool first = offset == 0;
        const bool last = offset + count == total;
        std::uint8_t flags = 0;
        if (first) {
          flags |=
              static_cast<std::uint8_t>(storage::ReplicationFrameFlag::kFirst);
        }
        if (last) {
          flags |=
              static_cast<std::uint8_t>(storage::ReplicationFrameFlag::kLast);
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
            offset, std::span(reinterpret_cast<std::byte*>(payload.data() +
                                                           payload_offset),
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

    auto send_partition_handoff =
        [&](std::uint16_t partition_id) -> Task<absl::Status> {
      std::string handoff_body;
      PutU16(handoff_body, partition_id);
      // Partition handoff no longer identifies a shared-backlog cursor. It is
      // only a target-side completion marker; the final cut supplies the
      // stable ONLINE cursor for the whole flow.
      PutU64(handoff_body, 1);
      absl::Status sent = co_await WriteFullSyncFrameAndWaitAck(
          stream, DataFrameKind::kPartitionHandoff, handoff_body, partition_id,
          fullsync_sequence);
      if (!sent.ok()) co_return sent;
      ++fullsync_sequence;
      LAVIK_FAULT_INJECT(if (const char* configured =
                                 std::getenv("LAVIK_REPLICATION_PAUSE_FULLSYNC_"
                                             "AFTER_HANDOFF_MS");
                             configured != nullptr &&
                             !replication_fullsync_handoff_pause_used_.exchange(
                                 true, std::memory_order_acq_rel)) {
        std::uint64_t pause_ms = 0;
        const std::size_t length = std::strlen(configured);
        const auto parsed =
            std::from_chars(configured, configured + length, pause_ms);
        if (parsed.ec == std::errc{} && parsed.ptr == configured + length &&
            pause_ms != 0) {
          spdlog::info(
              "paused full sync after acknowledged handoff partition {} for "
              "{} ms",
              partition_id, pause_ms);
          sent = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                           std::chrono::milliseconds(pause_ms));
        }
      });
      co_return sent;
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
        absl::Status waited = co_await bycorf::SleepFor(
            *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
        if (!waited.ok()) co_return waited;
      }
      gate_reopen.active_ = true;
      while (SnapshotTransactionsActive()) {
        if (session->cancelled()) {
          co_return absl::CancelledError(
              "replication session ended while draining transactions");
        }
        absl::Status waited = co_await bycorf::SleepFor(
            *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
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
      LAVIK_FAULT_INJECT(
          if (const char* configured = std::getenv(
                  "LAVIK_REPLICATION_PAUSE_FULLSYNC_AFTER_RESET_MS");
              configured != nullptr &&
              !replication_fullsync_pause_used_.exchange(
                  true, std::memory_order_acq_rel)) {
            std::uint64_t pause_ms = 0;
            const std::size_t length = std::strlen(configured);
            const auto parsed =
                std::from_chars(configured, configured + length, pause_ms);
            if (parsed.ec == std::errc{} && parsed.ptr == configured + length &&
                pause_ms != 0) {
              absl::Status paused = co_await bycorf::SleepFor(
                  *bycorf::ThisWorker().self_,
                  std::chrono::milliseconds(pause_ms));
              if (!paused.ok()) {
                cleanup();
                co_return paused;
              }
            }
          });

      for (const std::uint16_t partition_id : reset_partitions) {
        // Reset and handoff cover every physical partition so stale keys cannot
        // survive a sparse rebuild. Only manifest members open snapshot capture
        // and cross the wire with records; non-members prove their empty reset
        // directly through the same handoff protocol.
        if (!session->IncludesPopulationPartition(partition_id)) {
          session->SetProgress(flow_id, ReplicationPhase::kOverrideCatchup,
                               fullsync_backlog_cursor.lsn_,
                               fullsync_backlog_cursor.fragment_index_,
                               partition_id, 0);
          sent = co_await send_partition_handoff(partition_id);
          if (!sent.ok()) {
            cleanup();
            co_return sent;
          }
          if ((++processed_partitions & 63U) == 0) {
            absl::Status yielded = co_await bycorf::SleepFor(
                *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
            if (!yielded.ok()) {
              cleanup();
              co_return yielded;
            }
          }
          continue;
        }
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
            co_await bycorf::Yield(*bycorf::ThisWorker().self_);
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
          absl::Status yielded = co_await bycorf::SleepFor(
              *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
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
        sent = co_await send_partition_handoff(partition_id);
        if (!sent.ok()) {
          cleanup();
          co_return sent;
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
      absl::Status waited = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
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
        absl::Status waited = co_await bycorf::SleepFor(
            *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
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
        absl::Status waited = co_await bycorf::SleepFor(
            *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
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
    // acknowledged by the target; later retention pressure follows the
    // configured wait-or-full-sync policy instead of silently losing history.
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
      LAVIK_FAULT_INJECT(
          if (const char* configured =
                  std::getenv("LAVIK_REPLICATION_PAUSE_FULLSYNC_BEFORE_CUT_MS");
              configured != nullptr) {
            std::uint64_t pause_ms = 0;
            const std::size_t length = std::strlen(configured);
            const auto parsed =
                std::from_chars(configured, configured + length, pause_ms);
            if (parsed.ec == std::errc{} && parsed.ptr == configured + length &&
                pause_ms != 0) {
              absl::Status paused = co_await bycorf::SleepFor(
                  *bycorf::ThisWorker().self_,
                  std::chrono::milliseconds(pause_ms));
              if (!paused.ok()) co_return paused;
            }
          });
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
    if (ShouldInjectPostCutReset(flow_id)) {
      // A valid but phase-invalid reset after promotion catches targets that
      // validate frame shape without binding it to the rebuild lifecycle.
      // Cover the physical domain so accepting it would erase the promoted
      // population before this injected disconnect.
      std::string reset;
      reset.reserve(4 + storage::kLogicalStorageShards *
                            (2 + 8 * storage::kLogicalDatabaseCount));
      PutU32(reset, storage::kLogicalStorageShards);
      for (std::uint32_t partition = 0;
           partition < storage::kLogicalStorageShards; ++partition) {
        PutU16(reset, static_cast<std::uint16_t>(partition));
        for (std::uint64_t epoch : source_db_epochs) PutU64(reset, epoch);
      }
      absl::Status injected =
          co_await WriteDataFrame(stream, DataFrameKind::kReset, reset);
      co_return injected.ok()
          ? absl::UnavailableError("injected post-cut reset and disconnected")
          : injected;
    }
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
    session->SetHighestSentNextLsn(flow_id, cursor.lsn_);
    bycorf::ThisWorker().self_->Spawn(
        MonitorBacklogStall(session, flow_id, stream.NativeFd(),
                            session->ProgressGeneration(flow_id)));
    co_return co_await RunMasterFlowBacklog(stream, session, flow_id, cursor);
  }

  struct MasterBacklogDuplexState {
    std::deque<std::uint64_t> expected_acks_;
    bycorf::AsyncNotification expected_ack_ready_;
    bycorf::AsyncNotification receiver_done_ready_;
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
            : absl::UnavailableError("replication backlog flow closed");
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
      const storage::ReplicationLogCursor acknowledged{.lsn_ = expected_lsn + 1,
                                                       .fragment_index_ = 0};
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
    duplex->receiver_done_ready_.NotifyAll(*bycorf::ThisWorker().self_);
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
      absl::Status slept = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::seconds(1));
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
          "10 minutes; disconnecting it to release retained backlog pressure",
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
    bycorf::ThisWorker().self_->Spawn(
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
      constexpr std::size_t kOnlinePayloadHeaderBytes = 8 + 4 + 1;
      constexpr std::size_t kOnlineWireHeaderBytes =
          kDataFrameHeaderBytes + kOnlinePayloadHeaderBytes;
      std::string frame_headers;
      frame_headers.reserve(batch->frames_.size() * kOnlineWireHeaderBytes);
      std::vector<iovec> wire_batch;
      wire_batch.reserve(batch->frames_.size() * 2);
      std::vector<std::uint64_t> pending_acks;
      pending_acks.reserve(batch->frames_.size());
      for (const auto& frame : batch->frames_) {
        if (frame.payload_.size() > kMaxDataFrame - kOnlinePayloadHeaderBytes) {
          co_return absl::ResourceExhaustedError(
              "replication data frame exceeds configured limit");
        }
        std::string prelude;
        prelude.reserve(kOnlinePayloadHeaderBytes);
        std::uint64_t wire_lsn = frame.header_.lsn_;
        if (ShouldInjectDivergentTail(flow_id)) {
          wire_lsn = wire_lsn == std::numeric_limits<std::uint64_t>::max()
                         ? 0
                         : wire_lsn + 1;
          spdlog::warn("injected divergent replication tail on flow {}",
                       flow_id);
        }
        PutU64(prelude, wire_lsn);
        PutU32(prelude, frame.header_.fragment_index_);
        PutU8(prelude, frame.header_.flags_);
        absl::Status frame_header = AppendDataFrameHeader(
            &frame_headers, DataFrameKind::kCommand,
            kOnlinePayloadHeaderBytes + frame.payload_.size(),
            DataFrameCrc32c(prelude, frame.payload_));
        if (!frame_header.ok()) co_return frame_header;
        frame_headers.append(prelude);
      }
      for (std::size_t index = 0; index < batch->frames_.size(); ++index) {
        const auto& frame = batch->frames_[index];
        wire_batch.push_back(iovec{
            .iov_base = frame_headers.data() + index * kOnlineWireHeaderBytes,
            .iov_len = kOnlineWireHeaderBytes});
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
                                      pending_acks.begin(), pending_acks.end());
        duplex->expected_ack_ready_.NotifyAll(*bycorf::ThisWorker().self_);
        absl::Status sent = co_await stream.WriteAllV(wire_batch);
        if (!sent.ok()) {
          sender_status = sent;
          break;
        }
        // A successful vectored write is the furthest the peer could possibly
        // have consumed even if its corresponding ACK is lost on disconnect.
        session->SetHighestSentNextLsn(flow_id, batch->next_.lsn_);
      }
      // Keep sending independently from the durable cursor. In particular,
      // never stop a source flow at an arbitrary batch boundary waiting for a
      // cross-worker transaction ACK: another participant's envelope may be
      // in that flow's next batch. The concurrent receiver advances retained
      // history as ACKs arrive while socket backpressure bounds wire output.
      cursor = batch->next_;
      if (batch->at_tail_) {
        absl::Status slept = co_await bycorf::SleepFor(
            *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
        if (!slept.ok()) {
          sender_status = slept;
          break;
        }
        continue;
      }
      cursor = batch->next_;
    }
    if (sender_status.ok()) {
      sender_status = absl::UnavailableError("replication backlog flow closed");
    }
    duplex->sender_done_ = true;
    duplex->expected_ack_ready_.NotifyAll(*bycorf::ThisWorker().self_);
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
    if (connection->state_ == bycorf::ConnectionState::kActive &&
        !connection->closing_) {
      bycorf::ThisWorker().self_->BeginClose(
          connection, status,
          status.ok() ? bycorf::CloseMode::kLocalClose
                      : bycorf::CloseMode::kLocalError);
    }
    if (!status.ok()) {
      spdlog::warn("replication native handshake failed: {}", status.message());
    }
    co_return status;
  }

  Task<absl::Status> ServeOwnedNativeConnection(TcpStream& stream,
                                                std::vector<std::string> args,
                                                std::uint64_t client_id) {
    if (!source_sockets_.Add(stream.NativeFd())) {
      co_return absl::CancelledError(
          "native replication source stopped for process shutdown");
    }
    ScopedSocketSetMembership source_socket(&source_sockets_,
                                            stream.NativeFd());
    ReplicationConnectionMetricGuard connection_metric(
        EqualCaseInsensitive(args.front(), "LVPSYNC")
            ? ReplicationConnectionKind::kControl
            : ReplicationConnectionKind::kFlow);
    if (EqualCaseInsensitive(args.front(), "LVPSYNC")) {
      absl::Status result =
          co_await ServeMasterControl(stream, std::move(args), client_id);
      if (IsLeaseAdmissionSuspended(result)) {
        const std::string reply =
            absl::StrCat(kLeaseAdmissionSuspendedReply, "\r\n");
        const absl::Status sent = co_await WriteText(stream, reply);
        if (!sent.ok()) co_return sent;
      }
      co_return result;
    }
    co_return co_await ServeMasterFlow(stream, std::move(args));
  }

  Task<absl::Status> ServeMasterControl(TcpStream& stream,
                                        std::vector<std::string> args,
                                        std::uint64_t client_id) {
    const bool population_handshake =
        args.size() == 26 && args[8] == "POPULATION";
    const bool follow_handshake = args.size() == 17 && args[8] == "FOLLOW";
    const bool requested_group_valid =
        args.size() > 4 && (args[3] == "?" || IsReplicationId(args[3]) ||
                            ((population_handshake || follow_handshake) &&
                             IsPopulationGroupToken(args[3])));
    if ((!cluster_enabled_ && args.size() != 8) ||
        (cluster_enabled_ && !population_handshake && !follow_handshake) ||
        args[1] != kProtocolVersion || !requested_group_valid ||
        (args[4] != "?" && !IsReplicationId(args[4])) ||
        (args[5] != "?" && !IsReplicationId(args[5])) ||
        (args[6] != "?" && !IsReplicationId(args[6]))) {
      co_return absl::InvalidArgumentError("invalid LVPSYNC handshake");
    }
    active_master_controls_.fetch_add(1, std::memory_order_acq_rel);
    active_unpublished_master_controls_.fetch_add(1, std::memory_order_acq_rel);
    struct ControlGuard {
      std::atomic<unsigned>* active_;
      std::atomic<unsigned>* unpublished_;
      std::shared_ptr<MasterSession> session_;
      ~ControlGuard() {
        if (session_ != nullptr) {
          session_->MarkControlComplete();
        } else {
          unpublished_->fetch_sub(1, std::memory_order_acq_rel);
        }
        active_->fetch_sub(1, std::memory_order_acq_rel);
      }
      void MarkPublished(const std::shared_ptr<MasterSession>& session) {
        assert(session_ == nullptr);
        session_ = session;
        unpublished_->fetch_sub(1, std::memory_order_acq_rel);
      }
    } control_guard{&active_master_controls_,
                    &active_unpublished_master_controls_, nullptr};
    // Increment-before-check closes admission against DrainSourceEgress: the
    // barrier either observes this handler or this handler observes shutdown
    // before its first await or storage mutation.
    if (replication_shutdown_requested_.load(std::memory_order_acquire)) {
      co_return absl::CancelledError(
          "replication source stopped for process shutdown");
    }
    std::optional<RebuildIdentity> requested_population;
    std::optional<ClusterSteadyExport> requested_follow;
    if (population_handshake) {
      RebuildIdentity identity;
      identity.group_id_ = args[9];
      identity.assignment_id_ = args[10];
      identity.source_assignment_id_ = args[11];
      identity.authority_id_ = args[14];
      identity.source_node_id_ = args[15];
      identity.source_boot_id_ = args[16];
      identity.source_history_id_ = args[17];
      identity.target_node_id_ = args[18];
      identity.target_boot_id_ = args[19];
      identity.operation_id_ = args[20];
      identity.directive_id_ = args[21];
      identity.attempt_id_ = args[22];
      auto manifest = ParsePopulationManifestId(args[24]);
      if (!ParseUnsigned(args[12], &identity.term_) || identity.term_ == 0 ||
          !ParseUnsigned(args[13], &identity.directive_revision_) ||
          identity.directive_revision_ == 0 ||
          !ParseUnsigned(args[23], &identity.manifest_revision_) ||
          identity.manifest_revision_ == 0 || !manifest.ok() ||
          !ParseUnsigned(args[25], &identity.partition_replication_epoch_)) {
        co_return absl::InvalidArgumentError(
            "invalid population identity in LVPSYNC handshake");
      }
      identity.manifest_id_ = *manifest;
      requested_population = std::move(identity);
    }
    if (follow_handshake) {
      ClusterSteadyExport relationship;
      relationship.group_id_ = args[9];
      relationship.target_assignment_id_ = args[10];
      relationship.source_assignment_id_ = args[11];
      relationship.source_node_id_ = args[13];
      auto manifest = ParsePopulationManifestId(args[15]);
      if (!ParseUnsigned(args[12], &relationship.group_term_) ||
          relationship.group_term_ == 0 || relationship.group_id_.empty() ||
          relationship.target_assignment_id_.empty() ||
          relationship.source_assignment_id_.empty() ||
          relationship.source_node_id_.empty() ||
          !ParseUnsigned(args[14], &relationship.manifest_revision_) ||
          relationship.manifest_revision_ == 0 || !manifest.ok() ||
          !ParseUnsigned(args[16],
                         &relationship.partition_replication_epoch_) ||
          relationship.partition_replication_epoch_ == 0) {
        co_return absl::InvalidArgumentError(
            "invalid steady FOLLOW identity in LVPSYNC handshake");
      }
      relationship.manifest_id_ = *manifest;
      requested_follow = std::move(relationship);
    }
    if (population_handshake || follow_handshake) {
      AssertStateOwner();
      // This is the admission side of the revoke barrier. If this handler
      // observes an open gate, a later revoker sees active_master_controls_
      // and joins it; if the revoker closed the gate first, reject before
      // starting the history monitor or awaiting storage setup. Keeping the
      // gate closed for the whole revoke join prevents connection churn from
      // starving FenceAck/FDS publication.
      if (cluster_source_revocations_in_flight_ != 0) {
        if (population_handshake) {
          // Source and target receive FDS independently. A target can present
          // its current rebuild while this source is between clearing the old
          // projection and publishing replay-pending state. Classify that
          // bounded fail-closed window with the same pre-mutation retry marker
          // as a closed lease, rather than turning it into a terminal peer
          // close.
          co_return absl::UnavailableError(kLeaseAdmissionSuspendedStatus);
        }
        co_return absl::FailedPreconditionError(
            "cluster source authorization is being revoked");
      }
    }
    if (is_replica() || is_loading()) {
      co_return absl::FailedPreconditionError(
          "node lost valid source state during the native handshake");
    }
    StartIdleReplicationHistoryMonitor();
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
            "invalid LVPSYNC replica identity");
      }
      replica_node_id = std::string(identity.substr(1, 40));
      replica_host = PeerHost(stream.NativeFd());
      if (replica_host.empty()) {
        co_return absl::InternalError(
            "failed to identify LVPSYNC replica endpoint");
      }
    }
    if (requested_follow.has_value()) {
      requested_follow->target_node_id_ = replica_node_id;
    }
    const std::uint64_t session_id =
        next_master_session_id_.fetch_add(1, std::memory_order_relaxed);
    SetClientReplicationSession(client_id, session_id);
    std::string source_history_id;
    std::string source_group_id;
    std::shared_ptr<MasterSession> session;
    auto applied = DecodeAppliedVector(args[7]);
    if (!applied.ok()) co_return applied.status();
#if LAVIK_FAULTS_ENABLED
    if (population_handshake) {
      // Deterministically exercise the only suspension cut between optimistic
      // POPULATION admission and the master_mutex_-guarded classification /
      // publication transition. A concurrent revoker must close the gate and
      // make the second check below reject this unpublished control.
      absl::Status barrier = co_await WaitAtSourceAdmissionFaultBarrier(
          replication_shutdown_requested_);
      if (!barrier.ok()) co_return barrier;
    }
#endif
    {
      co_await master_mutex_.Lock(*bycorf::ThisWorker().self_);
      bycorf::CrossWorkerMutex::Guard master_lock(&master_mutex_);
      {
        AssertStateOwner();
        // Pair this check with publication under master_mutex_. A revoker can
        // close the gate after the early admission check while this handler is
        // awaiting history or storage. If publication wins this lock, the
        // revoker captures and joins the new session; if revocation wins, no
        // old FOLLOW relationship or population capability may publish after
        // its captured session set.
        if (cluster_source_revocations_in_flight_ != 0) {
          if (population_handshake) {
            co_return absl::UnavailableError(kLeaseAdmissionSuspendedStatus);
          }
          co_return absl::FailedPreconditionError(
              "cluster source authorization is being revoked");
        }
      }
      source_history_id = history_id_;
      {
        AssertStateOwner();
        source_group_id = group_id_;
      }
      if (replication_shutdown_requested_.load(std::memory_order_acquire) ||
          is_replica() || is_loading()) {
        co_return absl::FailedPreconditionError(
            "node lost valid source state during the native handshake");
      }

      std::shared_ptr<const ClusterRebuildContext> authorized_population;
      std::optional<ClusterSteadyExport> authorized_follow;
      if (requested_population.has_value()) {
        const RebuildIdentity& requested = *requested_population;
        AssertStateOwner();
        const bool source_matches =
            cluster_rebuild_ != nullptr &&
            cluster_rebuild_->state_.load(std::memory_order_relaxed) ==
                ReplicationGroupState::kReady &&
            cluster_rebuild_->ready_token_.has_value() &&
            cluster_rebuild_->ready_token_->identity().group_id_ ==
                requested.group_id_ &&
            cluster_rebuild_->ready_token_->identity().assignment_id_ ==
                requested.source_assignment_id_ &&
            cluster_rebuild_->ready_token_->identity().manifest_revision_ ==
                requested.manifest_revision_ &&
            cluster_rebuild_->ready_token_->identity().manifest_id_ ==
                requested.manifest_id_ &&
            cluster_rebuild_->ready_token_->identity()
                    .partition_replication_epoch_ ==
                requested.partition_replication_epoch_ &&
            requested.source_node_id_ == node_id_ &&
            requested.source_boot_id_ == boot_id_ &&
            requested.source_history_id_ == source_history_id &&
            requested.target_node_id_ == replica_node_id;
        const detail::SourceAuthorizationDisposition disposition =
            source_matches
                ? source_authorizations_.ClassifyAuthorizedRebuild(
                      requested, storage_->worker_count(), true,
                      cluster::LeaseClockNow().time_since_epoch())
                : detail::SourceAuthorizationDisposition::kNotAuthorized;
        if (disposition ==
            detail::SourceAuthorizationDisposition::kLeaseSuspended) {
          co_return absl::UnavailableError(kLeaseAdmissionSuspendedStatus);
        }
        if (disposition !=
            detail::SourceAuthorizationDisposition::kAuthorized) {
          co_return absl::PermissionDeniedError(
              "cluster population export is not authorized for this exact "
              "rebuild identity");
        }
        authorized_population = cluster_rebuild_;
        // Meta-managed population identity supersedes the legacy process-local
        // replication Group id. The target compares this response with the
        // same authorized directive before accepting any source bytes.
        source_group_id = PopulationGroupToken(requested.group_id_);
      } else if (requested_follow.has_value()) {
        const ClusterSteadyExport& requested = *requested_follow;
        AssertStateOwner();
        const std::shared_ptr<ClusterFollowOwnerContext> relationship =
            cluster_follow_owner_;
        const bool target_allowed =
            relationship != nullptr &&
            std::any_of(relationship->desired_.members_.begin(),
                        relationship->desired_.members_.end(),
                        [&](const ClusterReplicationMember& member) {
                          return member.node_id_ == requested.target_node_id_ &&
                                 member.assignment_id_ ==
                                     requested.target_assignment_id_;
                        });
        const bool authorized =
            relationship != nullptr && target_allowed &&
            relationship->desired_.local_node_id_ == node_id_ &&
            relationship->desired_.owner_node_id_ == node_id_ &&
            relationship->desired_.local_assignment_id_ ==
                requested.source_assignment_id_ &&
            relationship->desired_.owner_assignment_id_ ==
                requested.source_assignment_id_ &&
            relationship->desired_.group_id_ == requested.group_id_ &&
            relationship->desired_.group_term_ == requested.group_term_ &&
            relationship->desired_.manifest_revision_ ==
                requested.manifest_revision_ &&
            relationship->desired_.manifest_id_ == requested.manifest_id_ &&
            relationship->desired_.partition_replication_epoch_ ==
                requested.partition_replication_epoch_ &&
            requested.source_node_id_ == node_id_ &&
            requested.target_node_id_ != node_id_ && args[6] != "?" &&
            IsReplicationId(args[6]) && cluster_rebuild_ != nullptr &&
            cluster_rebuild_->state_.load(std::memory_order_relaxed) ==
                ReplicationGroupState::kReady &&
            cluster_rebuild_->ready_token_.has_value() &&
            ClusterFollowReadyPopulationMatches(relationship->desired_);
        if (!authorized) {
          co_return absl::PermissionDeniedError(
              "steady cluster export is not authorized by the exact current "
              "Owner/member relationship");
        }
        authorized_population = cluster_rebuild_;
        authorized_follow = requested;
        source_group_id = PopulationGroupToken(requested.group_id_);
      }

      const bool allow_continue =
          !population_handshake && args[3] == source_group_id &&
          args[4] == source_history_id && IsReplicationId(args[5]) &&
          IsReplicationId(args[6]) &&
          applied->size() == storage_->worker_count();
      session = std::make_shared<MasterSession>(
          session_id, storage_->worker_count(), std::move(replica_node_id),
          std::move(replica_host), replica_port, source_history_id,
          allow_continue, std::move(*applied), std::move(authorized_population),
          std::move(authorized_follow));
      if (!session->SetControl(stream.NativeFd())) {
        co_return absl::InternalError("failed to register control connection");
      }
      // Authorization validation and registry publication are one transition.
      // Revocation holds the same registry mutex after clearing capabilities,
      // so it either rejects this handshake above or observes and cancels the
      // published session before any awaited setup can export data.
      master_sessions_[session_id] = session;
      control_guard.MarkPublished(session);
    }
    for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
      const std::size_t flow_capacity = BacklogCapacityForFlow(
          worker, backlog_size_bytes_.load(std::memory_order_acquire));
      absl::Status enabled = co_await bycorf::SubmitTaskTo(
          worker, [this, session_id, flow_capacity]() -> Task<absl::Status> {
            // Runtime backlog is bounded process memory and intentionally
            // disappears when the source history id changes.
            co_return co_await storage_->EnableReplicationLog(session_id,
                                                              flow_capacity);
          });
      if (!enabled.ok()) {
        (void)co_await RemoveMasterSession(session);
        co_return enabled;
      }
      if (session->cancelled()) {
        (void)co_await RemoveMasterSession(session);
        co_return absl::CancelledError(
            "cluster population export authorization was revoked");
      }
    }
    const std::string resync_reply = absl::StrCat(
        "+LVFULLRESYNC ", session_id, " ", node_id_, " ", source_group_id, " ",
        boot_id_, " ", source_history_id, " ", storage_->worker_count(), " ",
        session->flow_capability_, "\r\n");
    absl::Status sent = co_await WriteText(stream, resync_reply);
    if (!sent.ok()) {
      (void)co_await RemoveMasterSession(session);
      co_return sent;
    }
    // Anonymous LVPSYNC is reserved for protocol detection. It deliberately
    // avoids leaving a ten-minute flow-stall session or appearing in INFO as
    // a downstream replica. Real protocol-v1 replicas always advertise their
    // node id and listening port.
    if (protocol_probe) {
      (void)co_await RemoveMasterSession(session);
      co_return absl::OkStatus();
    }
    if (ShouldInjectEarlyOnline()) {
      sent = co_await WriteText(stream, "+LVONLINE\r\n");
      if (sent.ok()) sent = co_await WaitForClose(stream);
      (void)co_await RemoveMasterSession(session);
      co_return sent.ok() ? absl::FailedPreconditionError(
                                "injected ONLINE before local flow readiness")
                          : sent;
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
      absl::Status slept = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!slept.ok()) {
        (void)co_await RemoveMasterSession(session);
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
      (void)co_await RemoveMasterSession(session);
      co_return absl::DeadlineExceededError(
          stalled ? "replica flow made no full-sync progress for 10 minutes"
                  : "replica flows did not complete full synchronization");
    }
    sent = co_await WriteText(stream, "+LVONLINE\r\n");
    if (!sent.ok()) {
      (void)co_await RemoveMasterSession(session);
      co_return sent;
    }
    session->MarkOnline();
    {
      co_await master_mutex_.Lock(*bycorf::ThisWorker().self_);
      bycorf::CrossWorkerMutex::Guard lock(&master_mutex_);
      const auto lease = disconnected_replica_leases_.find(session->node_id_);
      if (lease != disconnected_replica_leases_.end() &&
          lease->second.session_id_ < session->id_) {
        disconnected_replica_leases_.erase(lease);
      }
    }
    spdlog::info("accepted replication session {} with {} data flows",
                 session_id, session->worker_count());
    absl::Status waited = co_await WaitForClose(stream);
    (void)co_await RemoveMasterSession(session);
    co_return waited;
  }

  Task<absl::Status> ServeMasterFlow(TcpStream& stream,
                                     std::vector<std::string> args) {
    std::uint64_t session_id = 0;
    unsigned flow_id = 0;
    std::uint64_t next_lsn = 0;
    std::uint32_t fragment_index = 0;
    if (args.size() != 7 || args[1] != kProtocolVersion ||
        !ParseUnsigned(args[2], &session_id) || session_id == 0 ||
        !ParseUnsigned(args[3], &flow_id) ||
        flow_id != bycorf::ThisWorker().id_ ||
        !ParseUnsigned(args[4], &next_lsn) || next_lsn == 0 ||
        !ParseUnsigned(args[5], &fragment_index) || !IsReplicationId(args[6])) {
      co_return absl::InvalidArgumentError("invalid LVFLOW handshake");
    }
    std::shared_ptr<MasterSession> session;
    {
      co_await master_mutex_.Lock(*bycorf::ThisWorker().self_);
      bycorf::CrossWorkerMutex::Guard lock(&master_mutex_);
      const auto found = master_sessions_.find(session_id);
      if (found != master_sessions_.end()) session = found->second;
    }
    if (session == nullptr ||
        !session->SetFlow(flow_id, args[6], stream.NativeFd())) {
      co_return absl::FailedPreconditionError(
          "LVFLOW references an unavailable session");
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
      absl::Status waited = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
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
    const std::string flow_reply =
        absl::StrCat("+LVFLOW ", session_id, " ", flow_id, " ",
                     selected_continue_mode ? "CONTINUE" : "FULL", "\r\n");
    absl::Status sent = co_await WriteText(stream, flow_reply);
    if (!sent.ok()) {
      session->ClearFlow(flow_id, stream.NativeFd());
      session->Cancel();
      co_return sent;
    }
    // Keep the protocol branch outside a conditional expression containing
    // two co_await operands. GCC 13 can mis-lower that expression in this
    // large coroutine and resume the backlog awaiter for a selected FULL flow,
    // which sends its ONLINE cursor before the required full-sync cut.
    absl::Status waited;
    if (selected_continue_mode) {
      waited = co_await EnterMasterFlowBacklog(stream, session, flow_id,
                                               next_lsn, fragment_index);
    } else {
      waited = co_await RunMasterFlowData(stream, session, flow_id);
    }
    if (!waited.ok()) {
      spdlog::warn("replication source flow {} ended: {}", flow_id,
                   waited.message());
      // A disconnected session must stop pinning history before any cleanup
      // that may need the replication-log mutex. This also immediately wakes
      // any publisher backpressured on that replica.
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

  Task<absl::Status> RemoveMasterSession(
      const std::shared_ptr<MasterSession>& session) {
    {
      co_await master_mutex_.Lock(*bycorf::ThisWorker().self_);
      bycorf::CrossWorkerMutex::Guard lock(&master_mutex_);
      const auto found = master_sessions_.find(session->id_);
      if (found != master_sessions_.end() && found->second == session) {
        master_sessions_.erase(found);
        // Flow coroutines are owner-worker tasks and can outlive the control
        // socket. Keep the session until every flow has observed cancellation;
        // only then is its highest-sent reconnect bound stable.
        retired_master_sessions_.push_back(session);
      }
    }
    session->Cancel();
    co_return absl::OkStatus();
  }

  Task<absl::Status> DrainSourceEgress() {
    assert(bycorf::ThisWorker().id_ == 0);
    // Demotion has already made the role non-master. Process shutdown closes
    // every registered source socket before request drain so retained history
    // cannot deadlock an admitted publisher; this coroutine performs the
    // worker-affine registry join before source history is disabled.
    std::vector<std::shared_ptr<MasterSession>> sessions;
    {
      co_await master_mutex_.Lock(*bycorf::ThisWorker().self_);
      bycorf::CrossWorkerMutex::Guard lock(&master_mutex_);
      sessions.reserve(master_sessions_.size() +
                       retired_master_sessions_.size());
      for (auto& [session_id, session] : master_sessions_) {
        (void)session_id;
        sessions.push_back(std::move(session));
      }
      for (auto& session : retired_master_sessions_) {
        sessions.push_back(std::move(session));
      }
      master_sessions_.clear();
      retired_master_sessions_.clear();
      disconnected_replica_leases_.clear();
      history_id_ = NewReplicationId();
    }
    for (const auto& session : sessions) session->Cancel();
    auto source_flows_active = [&] {
      return std::any_of(
          sessions.begin(), sessions.end(),
          [](const auto& item) { return item->connected_flows() != 0; });
    };
    while (active_master_controls_.load(std::memory_order_acquire) != 0 ||
           redis_export_active_.load(std::memory_order_acquire) ||
           idle_history_monitor_running_ || history_reset_running_ ||
           source_flows_active()) {
      const int redis_export_fd =
          redis_export_fd_.load(std::memory_order_acquire);
      if (redis_export_fd >= 0) (void)::shutdown(redis_export_fd, SHUT_RDWR);
      absl::Status waited = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) co_return waited;
    }
    // Flow teardown performs any history reset before ClearFlow drops the
    // final connected-flow count. Together with the monitor/reset flags, this
    // joins every old source task before its history can be disabled.
    co_return absl::OkStatus();
  }

  Task<absl::Status> DisableSourceHistory() {
    for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
      absl::Status disabled = co_await bycorf::SubmitTaskTo(
          worker, [this]() { return storage_->DisableReplicationLog(); });
      if (!disabled.ok()) co_return disabled;
    }
    co_return absl::OkStatus();
  }

  Task<absl::Status> RetireSourceHistory() {
    absl::Status drained = co_await DrainSourceEgress();
    if (!drained.ok()) co_return drained;
    co_return co_await DisableSourceHistory();
  }

  void FinalizeRetiredMasterSessionsLocked() {
    auto retired = retired_master_sessions_.begin();
    while (retired != retired_master_sessions_.end()) {
      const std::shared_ptr<MasterSession>& session = *retired;
      if (session->control_active() || session->connected_flows() != 0) {
        ++retired;
        continue;
      }
      if (session->ever_online() && !session->node_id_.empty()) {
        std::vector<std::uint64_t> upper = session->HighestSentNextLsns();
        const bool reconnectable =
            upper.size() == storage_->worker_count() &&
            std::all_of(upper.begin(), upper.end(),
                        [](std::uint64_t lsn) { return lsn != 0; });
        if (reconnectable) {
          DisconnectedReplicaLease lease{
              .session_id_ = session->id_,
              .history_id_ = session->source_history_id_,
              .highest_sent_next_lsns_ = std::move(upper),
          };
          auto existing = disconnected_replica_leases_.find(session->node_id_);
          if (existing == disconnected_replica_leases_.end()) {
            disconnected_replica_leases_.emplace(session->node_id_,
                                                 std::move(lease));
          } else if (existing->second.session_id_ < session->id_) {
            existing->second = std::move(lease);
          }
        }
      }
      retired = retired_master_sessions_.erase(retired);
    }
  }

  bool MasterHistoryHasConsumersLocked() const {
    return !master_sessions_.empty() || !retired_master_sessions_.empty() ||
           source_authorizations_.RetainsSourceHistory() ||
           active_master_controls_.load(std::memory_order_acquire) != 0 ||
           redis_export_active_.load(std::memory_order_acquire);
  }

  void StartIdleReplicationHistoryMonitor() {
    assert(bycorf::ThisWorker().id_ == 0);
    if (idle_history_monitor_running_) return;
    idle_history_monitor_running_ = true;
    bycorf::ThisWorker().self_->Spawn(MonitorIdleReplicationHistory());
  }

  Task<absl::Status> MonitorIdleReplicationHistory() {
    assert(bycorf::ThisWorker().id_ == 0);
    struct MonitorGuard {
      bool* running_;
      ~MonitorGuard() { *running_ = false; }
    } monitor_guard{&idle_history_monitor_running_};

    for (;;) {
      absl::Status slept = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(10));
      if (!slept.ok()) co_return slept;
      if (is_replica()) co_return absl::OkStatus();

      std::string history_id;
      std::vector<storage::ReplicationLogInfo> logs;
      {
        co_await master_mutex_.Lock(*bycorf::ThisWorker().self_);
        bycorf::CrossWorkerMutex::Guard lock(&master_mutex_);
        FinalizeRetiredMasterSessionsLocked();
        if (MasterHistoryHasConsumersLocked()) continue;
        if (cluster_enabled_) {
          // Meta binds source authorizations and population proofs to this
          // history. Finish retiring disconnected sessions, but leave history
          // retirement to explicit cluster role/population transitions. The
          // backlog stays bounded independently; standalone reconnect leases
          // are unnecessary while the population owns the history lifetime.
          disconnected_replica_leases_.clear();
          co_return absl::OkStatus();
        }
        history_id = history_id_;
      }

      logs.resize(storage_->worker_count());
      for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
        if (worker == 0) {
          logs[worker] = storage_->LocalReplicationLogInfo();
        } else {
          logs[worker] = co_await bycorf::SubmitTo(
              worker, [this] { return storage_->LocalReplicationLogInfo(); });
        }
      }

      bool no_reconnectable_replica = false;
      {
        co_await master_mutex_.Lock(*bycorf::ThisWorker().self_);
        bycorf::CrossWorkerMutex::Guard lock(&master_mutex_);
        FinalizeRetiredMasterSessionsLocked();
        if (MasterHistoryHasConsumersLocked()) continue;
        for (auto lease = disconnected_replica_leases_.begin();
             lease != disconnected_replica_leases_.end();) {
          const DisconnectedReplicaLease& candidate = lease->second;
          bool expired =
              candidate.history_id_ != history_id_ ||
              candidate.highest_sent_next_lsns_.size() != logs.size();
          for (std::size_t worker = 0; !expired && worker < logs.size();
               ++worker) {
            // Native continuation is all-flow: one flow whose oldest retained
            // LSN is beyond the furthest frame possibly sent makes the entire
            // disconnected replica require full synchronization.
            expired =
                logs[worker].state_ != storage::ReplicationLogState::kActive ||
                logs[worker].floor_lsn_ >
                    candidate.highest_sent_next_lsns_[worker];
          }
          if (expired) {
            spdlog::info(
                "replication reconnect lease expired node={} session={}",
                lease->first, candidate.session_id_);
            const auto expired_lease = lease++;
            disconnected_replica_leases_.erase(expired_lease);
          } else {
            ++lease;
          }
        }
        no_reconnectable_replica = disconnected_replica_leases_.empty();
      }
      if (!no_reconnectable_replica || history_reset_running_) continue;

      // A Meta-managed Owner needs a continuous source cursor even when no
      // replica can reconnect: its clean-shutdown proof must cover subsequent
      // accepted writes. With no consumer pins the bounded log evicts complete
      // events without ACK backpressure, so keeping the sequence alive does
      // not retain an unbounded history. Standalone idle retirement is
      // unchanged.
      if (cluster_enabled_) continue;

      // Serialize with history reset and handshake setup, then let every
      // command admitted against this history finish publishing before the
      // log is cleared. In particular, a durable Function inside EXEC must
      // cross its participant log fences rather than being failed by idle
      // history cleanup.
      history_reset_running_ = true;
      bool command_gates_closed = false;
      struct IdleResetGuard {
        bool* reset_running_;
        bool* command_gates_closed_;
        ~IdleResetGuard() {
          if (*command_gates_closed_) OpenAllCommandDbGates();
          *reset_running_ = false;
        }
      } idle_reset_guard{&history_reset_running_, &command_gates_closed};
      while (!CloseAllCommandDbGates()) {
        absl::Status waited = co_await bycorf::SleepFor(
            *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
        if (!waited.ok()) co_return waited;
      }
      command_gates_closed = true;
      while (CommandDbOperationsActive()) {
        absl::Status waited = co_await bycorf::SleepFor(
            *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
        if (!waited.ok()) co_return waited;
      }

      // A handshake that arrives after the reset flag was set waits and sees
      // the new history. One that became active before it is caught here.
      bool disable = false;
      {
        co_await master_mutex_.Lock(*bycorf::ThisWorker().self_);
        bycorf::CrossWorkerMutex::Guard lock(&master_mutex_);
        FinalizeRetiredMasterSessionsLocked();
        disable = !is_replica() && !MasterHistoryHasConsumersLocked() &&
                  disconnected_replica_leases_.empty();
        if (disable) history_id_ = NewReplicationId();
      }
      if (!disable) {
        continue;
      }

      absl::Status disabled = absl::OkStatus();
      for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
        disabled = co_await bycorf::SubmitTaskTo(
            worker, [this]() { return storage_->DisableReplicationLog(); });
        if (!disabled.ok()) break;
      }
      if (!disabled.ok()) co_return disabled;
      spdlog::info(
          "disabled replication history after all disconnected replicas "
          "fell behind the backlog");
      co_return absl::OkStatus();
    }
  }

  Task<absl::Status> ResetInvalidReplicationHistory() {
    if (bycorf::ThisWorker().id_ != 0) {
      co_return co_await bycorf::SubmitTaskTo(
          0, [this]() { return EnsureReplicationHistoryReady(); });
    }
    co_return co_await EnsureReplicationHistoryReady();
  }

  Task<absl::Status> EnsureReplicationHistoryReady() {
    // Control handshakes are owned by worker 0. Keep reset ownership local and
    // let another handshake yield while the first one performs storage IO;
    // there is no process-global lock on the write path.
    while (history_reset_running_) {
      absl::Status waited = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
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
      if (worker == bycorf::ThisWorker().id_) {
        info = storage_->LocalReplicationLogInfo();
      } else {
        info = co_await bycorf::SubmitTo(
            worker, [this] { return storage_->LocalReplicationLogInfo(); });
      }
      invalid |= info.state_ == storage::ReplicationLogState::kInvalid;
    }
    if (!invalid) co_return absl::OkStatus();
    std::vector<std::shared_ptr<MasterSession>> cancelled;
    {
      co_await master_mutex_.Lock(*bycorf::ThisWorker().self_);
      bycorf::CrossWorkerMutex::Guard lock(&master_mutex_);
      cancelled.reserve(master_sessions_.size());
      for (auto& [id, session] : master_sessions_) {
        (void)id;
        cancelled.push_back(session);
      }
      master_sessions_.clear();
      disconnected_replica_leases_.clear();
      history_id_ = NewReplicationId();
    }
    for (const auto& session : cancelled) session->Cancel();
    for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
      absl::Status disabled =
          co_await bycorf::SubmitTaskTo(worker, [this]() -> Task<absl::Status> {
            co_return co_await storage_->DisableReplicationLog();
          });
      if (!disabled.ok()) co_return disabled;
    }
    co_return absl::OkStatus();
  }

  storage::StorageEngine* storage_;
  // The packed atomic is owned by the enclosing manager so external commands
  // reach it without following this pImpl. Bit zero is serving-open and the
  // remaining bits are a monotonic dataset generation.
  std::atomic<std::uint64_t>* const serving_generation_;
  const bool cluster_enabled_;
  // The existing discovery cancellation set also covers target-session
  // sockets, including connect/TLS. It is declared before their shared owners
  // so it outlives them. Only socket lifecycle/shutdown touches this registry;
  // heartbeat and progress never acquire its descriptor-lifetime mutex.
  SocketSet outbound_sockets_;
  // All mutable role/population/session state below is owned by worker zero.
  // Foreign workers query or change it with Bycorf messages, never a native
  // thread lock. Flow-owned progress and command admission stay independent.
  std::optional<ReplicaOfConfig> upstream_;
  struct UpstreamSnapshot {
    std::uint64_t version_ = 0;
    std::optional<ReplicaOfConfig> endpoint_;
  };
  std::atomic<std::shared_ptr<const UpstreamSnapshot>> published_upstream_;
  std::atomic<std::uint64_t> upstream_version_{0};
  // Each cache is accessed only by its indexed worker, never by the writer.
  // Keeping it on the manager also avoids TLS cache ABA across manager reuse.
  std::unique_ptr<UpstreamSnapshot[]> upstream_caches_;
  // Owner-local. A failed promotion remains fenced, but REPLICAOF
  // NO ONE may retry the frozen proof without reconstructing retired flows.
  std::optional<storage::PromotionBase> pending_promotion_;
  std::shared_ptr<ReplicaSession> active_replica_session_;
  std::shared_ptr<ClusterRebuildContext> cluster_rebuild_;
  std::optional<detail::RecoveredPopulation> recovered_population_;
  bool recovered_population_fenced_ = false;
  std::uint64_t owner_source_term_ = 0;
  bool operator_recovery_active_ = false;
  // Retained through control-session replacement so exact directive replay
  // returns the original terminal evidence without repeating storage effects.
  std::shared_ptr<ClusterPromotionPrepareContext> cluster_promotion_prepare_;
  // Controlled failover owns one nestable active-expiration pause across FDS
  // replay/replacement. The stable frontier is observable only while every
  // source and population anchor remains exact.
  std::shared_ptr<ClusterSourcePauseContext> cluster_source_pause_;
  // Current committed action projected by FDS. Its runner and every public
  // observation are owner-local, so desired-state replacement can withdraw
  // stale progress synchronously before joining asynchronous preparation.
  std::shared_ptr<ClusterFailoverActionContext> cluster_failover_action_;
  // Cutover removes the transition before its finite lease arrives. Only the
  // exact action copied into the committed grant may keep the private prepared
  // context across that desired-state replacement.
  std::optional<ClusterFailoverActionId>
      retained_failover_activation_action_id_;
  std::optional<ClusterFailoverPreparedContext>
      retained_failover_prepared_context_;
  std::optional<DesiredClusterFailoverAction> retained_failover_desired_action_;
  // Successful activation remains boot-local so exact lease/FDS replay cannot
  // repeat promotion. It is retired with the physical population.
  std::optional<ClusterFailoverActivation> activated_failover_activation_;
  std::optional<ClusterFailoverPreparedContext>
      activated_failover_prepared_context_;
  // Terminal action failure suppresses the same boot/assignment/population
  // domain even after Meta replaces the action id, preventing hot reselection.
  std::optional<DesiredClusterFailoverAction> failed_failover_candidate_;
  // One steady post-Cutover relationship serves both roles. On the Owner it
  // is the FDS-derived source admission set; on a non-Owner it pins the
  // reconnecting coordinator and its exact target scope.
  std::shared_ptr<ClusterFollowOwnerContext> cluster_follow_owner_;
  detail::SourceAuthorizationLedger source_authorizations_;
  // Source authorization and handshake publication run on worker zero under
  // master_mutex_. A revoke uses the same registry gate; this count keeps
  // grants closed across the asynchronous flow join that follows registry
  // removal.
  unsigned cluster_source_revocations_in_flight_ = 0;
  std::atomic<ReplicationRole> role_{ReplicationRole::kMaster};
  std::atomic<std::uint64_t> link_state_changed_nanos_{SteadyNanos()};
  std::atomic<std::uint64_t> role_epoch_{0};
  // Redis sources replace one node-wide population together. The active
  // session is owned by worker zero; the allocator is boot-scoped so a
  // restarted process cannot mistake an old durable fence for its attempt.
  std::uint64_t redis_full_sync_session_id_ = 0;
  std::atomic<std::uint64_t> next_redis_full_sync_session_id_{1};
  std::atomic<bool> native_dataset_valid_{false};
  std::atomic<bool> failed_stopped_{false};
  std::atomic<unsigned> ready_workers_{0};
  std::uint64_t replica_session_id_ = 0;
  unsigned source_worker_count_ = 0;
  bool ready_waiter_started_ = false;             // worker 0 only
  bool coordinator_started_ = false;              // worker 0 only
  bool replica_reconfiguration_running_ = false;  // worker 0 only
  // Distinguishes the coordinator's automatic cancel/join/abort window from
  // an ordinary disconnected session. A control-plane supersession waits for
  // this owner to finish instead of racing a second abort of the same root.
  bool replica_session_teardown_running_ = false;  // worker 0 only
  // Worker-zero lifecycle bit. Shutdown sets it before cancelling the native
  // target session so no directive can recreate work behind the drain barrier.
  bool cluster_control_stopping_ = false;
  // Process main may set this before worker zero joins native/Redis target
  // roots. Every coordinator treats it as terminal for this process boot.
  std::atomic<bool> replication_shutdown_requested_{false};
  bool initial_protocol_probe_pending_ = false;  // worker 0 only
  std::shared_ptr<detail::ReplicaAppliedFrontier> applied_frontier_;
  std::optional<std::string> upstream_node_id_;
  std::optional<std::string> upstream_history_id_;
  std::string failure_reason_;  // worker 0 only
#if LAVIK_FAULTS_ENABLED
  // Fault-injection settings are process-startup inputs in fault-enabled
  // binaries only. Ordinary releases neither read them nor keep fault state.
  // Cache their pointers before workers launch so ONLINE/ACK paths do not enter
  // libc getenv for every replicated mutation. Runtime setenv is unsupported;
  // the environment owns these strings for the process lifetime.
  const char* const replication_drop_flow_after_command_ =
      std::getenv("LAVIK_REPLICATION_DROP_FLOW_AFTER_COMMAND");
  const char* const replication_drop_after_control_response_once_ =
      std::getenv("LAVIK_REPLICATION_DROP_AFTER_CONTROL_RESPONSE_ONCE");
  const char* const replication_drop_flow_after_transaction_apply_ =
      std::getenv("LAVIK_REPLICATION_DROP_FLOW_AFTER_TRANSACTION_APPLY");
  const char* const replication_drop_flow_after_command_apply_ =
      std::getenv("LAVIK_REPLICATION_DROP_FLOW_AFTER_COMMAND_APPLY");
  const char* const replication_pause_before_command_apply_ms_ =
      std::getenv("LAVIK_REPLICATION_PAUSE_BEFORE_COMMAND_APPLY_MS");
  const char* const replication_pause_before_transaction_apply_ms_ =
      std::getenv("LAVIK_REPLICATION_PAUSE_BEFORE_TRANSACTION_APPLY_MS");
  const char* const replication_pause_before_control_apply_ms_ =
      std::getenv("LAVIK_REPLICATION_PAUSE_BEFORE_CONTROL_APPLY_MS");
  std::atomic<bool> replication_fault_drop_used_{false};
  std::atomic<bool> replication_control_response_fault_drop_used_{false};
  std::atomic<bool> replication_transaction_fault_drop_used_{false};
  std::atomic<bool> replication_command_apply_fault_drop_used_{false};
  std::atomic<bool> replication_command_apply_pause_used_{false};
  std::atomic<bool> replication_transaction_apply_pause_used_{false};
  std::atomic<bool> replication_control_apply_pause_used_{false};
  std::atomic<bool> redis_export_gate_pause_used_{false};
  std::atomic<bool> replication_peer_flow_cancel_fault_used_{false};
  std::atomic<unsigned> replication_fullsync_cut_ack_count_{0};
  std::atomic<bool> replication_promotion_fault_used_{false};
  std::atomic<bool> empty_population_reset_fault_used_{false};
  std::atomic<bool> empty_population_catalog_fault_used_{false};
  std::atomic<bool> promotion_prepare_fault_used_{false};
  std::atomic<bool> replication_post_cut_reset_fault_used_{false};
  std::atomic<bool> replication_divergent_tail_fault_used_{false};
  std::atomic<bool> replication_fullsync_pause_used_{false};
  std::atomic<bool> replication_fullsync_handoff_pause_used_{false};
  std::atomic<bool> replication_fullsync_catalog_pause_used_{false};
#endif
  std::atomic<unsigned> snapshot_read_concurrency_{
      kDefaultReplicationSnapshotReadConcurrency};
  std::atomic<std::size_t> snapshot_batch_size_{kSnapshotKeysPerBatch};
  std::atomic<std::size_t> backlog_size_bytes_{0};
  // Mirrors the fully installed storage policy for CONFIG GET; publisher
  // rollover reads the storage-owned atomic directly.
  std::atomic<bool> backlog_backpressure_{true};
  std::atomic<std::size_t> publish_queue_bytes_per_worker_{0};
  std::atomic<unsigned> replica_priority_{100};
  bool history_reset_running_ = false;  // worker 0 only

  const std::string node_id_;
  std::string group_id_;  // worker 0 only
  const std::string boot_id_;
  const std::string replica_incarnation_;
  // Owning the single boot-scoped group here fixes the
  // one-process/one-dataset seam without allowing the control client to create
  // a second local group or making recovered bytes implicitly ready.
  std::unique_ptr<lavik::ReplicationGroup> cluster_group_;
  std::string history_id_;  // guarded by master_mutex_
  const std::uint16_t listen_port_;
  const std::shared_ptr<bycorf::TlsContext> tls_context_;
  const std::string masteruser_;
  const std::string masterauth_;
  std::atomic<bool> redis_psync_{false};
  std::atomic<bool> redis_export_backpressure_{false};
  std::atomic<bool> redis_export_active_{false};
  // Valid only while redis_export_active_ is true; demotion shuts this socket
  // down before disabling the source history it consumes.
  std::atomic<int> redis_export_fd_{-1};
  // Each Redis source owns an independent PSYNC cursor. Cursors intentionally
  // remain process-local until storage and offsets can share a crash-atomic
  // commit record; process restart therefore requests a fresh RDB per source.
  std::vector<std::shared_ptr<RedisSource>> redis_sources_;
  std::optional<RedisClusterTopology> expected_redis_topology_;
  bool redis_cluster_ = false;
  bool redis_topology_fault_ = false;
  bool redis_topology_monitor_started_ = false;  // worker 0 only
  // Covers native control/flow and Redis PSYNC export sockets independently
  // of the worker-affine source session registry. Process shutdown cancels it
  // before request drain so transport teardown releases backlog retention.
  SocketSet source_sockets_;
  bycorf::AsyncMutex redis_fullsync_mutex_;  // worker 0 only
  std::atomic<std::uint64_t> next_master_session_id_{1};
  std::atomic<unsigned> active_master_controls_{0};
  // Controls that passed initial syntax validation but have not yet published
  // a MasterSession. Source retirement joins this exact set; preserved
  // sessions cannot mask it by disconnecting while the barrier waits.
  std::atomic<unsigned> active_unpublished_master_controls_{0};
  mutable bycorf::CrossWorkerMutex master_mutex_;
  absl::flat_hash_map<std::uint64_t, std::shared_ptr<MasterSession>>
      master_sessions_;
  std::vector<std::shared_ptr<MasterSession>> retired_master_sessions_;
  absl::flat_hash_map<std::string, DisconnectedReplicaLease>
      disconnected_replica_leases_;
  bool idle_history_monitor_running_ = false;  // worker 0 only
};

ReplicationManager::ReplicationManager(
    storage::StorageEngine* storage, ReplicationOptions options,
    std::optional<ReplicaOfConfig> initial_upstream)
    : group_(std::make_unique<ReplicationGroup>(
          storage, std::move(initial_upstream), options, &serving_generation_)),
      options_(std::move(options)) {}

ReplicationManager::~ReplicationManager() = default;

void ReplicationManager::StorageReady(bycorf::Worker& worker) {
  group_->StorageReady(worker);
}

Task<absl::Status> ReplicationManager::ApplyDirective(
    ReplicationDirective directive) {
  switch (directive.kind_) {
    case ReplicationDirective::Kind::kSetUpstream:
      co_return co_await group_->SetUpstream(std::move(directive.upstream_));
    case ReplicationDirective::Kind::kAddUpstream:
      if (!directive.upstream_.has_value()) {
        co_return absl::InvalidArgumentError(
            "add-upstream directive has no endpoint");
      }
      co_return co_await group_->AddUpstream(std::move(*directive.upstream_));
    case ReplicationDirective::Kind::kBacklogBytes:
      co_return co_await group_->SetBacklogSizeBytes(directive.value_);
    case ReplicationDirective::Kind::kBacklogBackpressure:
      co_return co_await group_->SetBacklogBackpressure(directive.value_ != 0);
    case ReplicationDirective::Kind::kPublishQueueBytes:
      co_return co_await group_->SetPublishQueueBytesPerWorker(
          directive.value_);
    case ReplicationDirective::Kind::kSnapshotReadConcurrency:
      co_return group_->SetSnapshotReadConcurrency(
          static_cast<unsigned>(directive.value_));
    case ReplicationDirective::Kind::kSnapshotBatchSize:
      co_return group_->SetSnapshotBatchSize(directive.value_);
    case ReplicationDirective::Kind::kReplicaPriority:
      co_return group_->SetReplicaPriority(
          static_cast<unsigned>(directive.value_));
  }
  co_return absl::InvalidArgumentError("unknown replication directive");
}

Task<ReplicationStatus> ReplicationManager::Observe() const {
  return group_->status();
}

Task<ReplicationIdentity> ReplicationManager::ObserveIdentity() const {
  return group_->identity();
}

std::optional<ReplicaOfConfig> ReplicationManager::upstream() const {
  return group_->upstream();
}

Task<absl::StatusOr<ClusterRebuildCompletion>>
ReplicationManager::StartClusterRebuildDirective(ReplicaOfConfig upstream,
                                                 RebuildDirective directive,
                                                 PopulationManifest manifest) {
  auto started = co_await group_->StartClusterRebuildDirective(
      std::move(upstream), std::move(directive), std::move(manifest));
  if (!started.ok()) co_return started.status();
  co_return ClusterRebuildCompletion(std::move(*started));
}

Task<absl::StatusOr<ClusterRebuildCompletion>>
ReplicationManager::StartEmptyPopulationInitialization(
    RebuildIdentity identity, PopulationManifest manifest) {
  auto started = co_await group_->StartEmptyPopulationInitialization(
      std::move(identity), std::move(manifest));
  if (!started.ok()) co_return started.status();
  co_return ClusterRebuildCompletion(std::move(*started));
}

Task<absl::StatusOr<ClusterPromotionPrepareCompletion>>
ReplicationManager::StartClusterPromotionPrepareDirective(
    ClusterPromotionPrepareDirective directive) {
  auto started = co_await group_->StartClusterPromotionPrepareDirective(
      std::move(directive));
  if (!started.ok()) co_return started.status();
  co_return ClusterPromotionPrepareCompletion(std::move(*started));
}

Task<absl::Status> ReplicationManager::ReconcileClusterSourcePause(
    std::optional<DesiredClusterSourcePause> desired) {
  return group_->ReconcileClusterSourcePause(std::move(desired));
}

Task<ClusterSourcePauseStatus> ReplicationManager::cluster_source_pause_status()
    const {
  return group_->cluster_source_pause_status();
}

Task<absl::Status> ReplicationManager::ReconcileClusterFailoverAction(
    std::optional<DesiredClusterFailoverAction> desired,
    std::optional<ClusterFailoverActionId> pending_activation_action_id) {
  return group_->ReconcileClusterFailoverAction(std::move(desired),
                                                pending_activation_action_id);
}

Task<ClusterFailoverActionStatus>
ReplicationManager::cluster_failover_action_status() const {
  return group_->cluster_failover_action_status();
}

Task<std::optional<ClusterFailoverPreparedContext>>
ReplicationManager::FindClusterFailoverPreparedContext(
    const ClusterFailoverActionId& action_id) const {
  return group_->FindClusterFailoverPreparedContext(action_id);
}

Task<absl::Status> ReplicationManager::ActivateClusterPreparedPromotion(
    ClusterFailoverActivation activation) {
  return group_->ActivateClusterPreparedPromotion(std::move(activation));
}

Task<absl::Status> ReplicationManager::EnableClusterExpirationAuthorityUntil(
    std::chrono::nanoseconds deadline_since_boot) {
  return group_->EnableClusterExpirationAuthorityUntil(deadline_since_boot);
}

Task<absl::Status> ReplicationManager::RevokeClusterExpirationAuthority() {
  return group_->RevokeClusterExpirationAuthority();
}

Task<absl::Status> ReplicationManager::ReconcileClusterFollowOwner(
    std::optional<DesiredClusterUpstream> desired) {
  return group_->ReconcileClusterFollowOwner(std::move(desired));
}

Task<absl::Status> ReplicationManager::ApplyClusterRebuildDirective(
    ReplicaOfConfig upstream, RebuildDirective directive,
    PopulationManifest manifest) {
  return group_->ApplyClusterRebuildDirective(
      std::move(upstream), std::move(directive), std::move(manifest));
}

Task<absl::Status> ReplicationManager::CancelClusterRebuildForShutdown() {
  return group_->CancelClusterRebuildForShutdown();
}

Task<absl::Status> ReplicationManager::RecoverClusterPopulation() {
  return group_->RecoverClusterPopulation();
}

void ReplicationManager::RequestShutdown() noexcept {
  group_->RequestShutdown();
}

Task<absl::Status> ReplicationManager::QuiesceForShutdown() {
  return group_->QuiesceForShutdown();
}

Task<absl::Status> ReplicationManager::ReconcileClusterPopulation(
    std::optional<DesiredClusterPopulation> desired) {
  return group_->ReconcileClusterPopulation(std::move(desired));
}

Task<absl::Status> ReplicationManager::CancelInProgressClusterPopulation(
    bool preserve_current_follow_attempt) {
  return group_->CancelInProgressClusterPopulation(
      preserve_current_follow_attempt);
}

std::optional<ClusterRebuildCompletion>
ReplicationManager::FindCompletedClusterPopulation(
    const RebuildDirective& directive) const {
  auto completion = group_->FindCompletedClusterPopulation(directive);
  if (completion == nullptr) return std::nullopt;
  return ClusterRebuildCompletion(std::move(completion));
}

Task<ClusterPopulationStatus> ReplicationManager::cluster_population_status()
    const {
  return group_->cluster_population_status();
}

Task<absl::Status> ReplicationManager::AuthorizeClusterRebuildSource(
    RebuildDirective directive) {
  return group_->AuthorizeClusterRebuildSource(std::move(directive));
}

Task<absl::Status>
ReplicationManager::RevokeClusterRebuildSourceAuthorizations() {
  return group_->RevokeClusterRebuildSourceAuthorizations();
}

Task<absl::Status> ReplicationManager::EnableClusterRebuildSourceAdmissionUntil(
    std::chrono::nanoseconds deadline_since_boot) {
  return group_->EnableClusterRebuildSourceAdmissionUntil(deadline_since_boot);
}

Task<absl::Status> ClusterRebuildCompletion::Await() const {
  if (state_ == nullptr) {
    co_return absl::FailedPreconditionError(
        "cluster rebuild completion handle is empty");
  }
  co_return co_await state_->Await();
}

std::optional<absl::Status> ClusterRebuildCompletion::result() const {
  if (state_ == nullptr) {
    return absl::FailedPreconditionError(
        "cluster rebuild completion handle is empty");
  }
  return state_->result();
}

Task<ClusterPromotionPrepareCompletion::Result>
ClusterPromotionPrepareCompletion::Await() const {
  if (state_ == nullptr) {
    co_return absl::FailedPreconditionError(
        "cluster promotion completion handle is empty");
  }
  co_return co_await state_->Await();
}

std::optional<ClusterPromotionPrepareCompletion::Result>
ClusterPromotionPrepareCompletion::result() const {
  if (state_ == nullptr) {
    return Result(absl::FailedPreconditionError(
        "cluster promotion completion handle is empty"));
  }
  return state_->result();
}

Task<absl::Status> ReplicationManager::
    ClearClusterRebuildSourceAuthorizationsForSessionReplacement(
        bool preserve_established_exports) {
  return group_->ClearClusterRebuildSourceAuthorizationsForSessionReplacement(
      preserve_established_exports);
}

Task<absl::Status>
ReplicationManager::RefreshClusterRebuildSourceAuthorizationsForFdsReplacement(
    bool preserve_established_exports,
    std::size_t expected_authorization_replays) {
  return group_->RefreshClusterRebuildSourceAuthorizationsForFdsReplacement(
      preserve_established_exports, expected_authorization_replays);
}

unsigned ReplicationManager::snapshot_read_concurrency() const noexcept {
  return group_->snapshot_read_concurrency();
}

std::size_t ReplicationManager::snapshot_batch_size() const noexcept {
  return group_->snapshot_batch_size();
}

std::size_t ReplicationManager::backlog_size_bytes() const noexcept {
  return group_->backlog_size_bytes();
}

bool ReplicationManager::backlog_backpressure() const noexcept {
  return group_->backlog_backpressure();
}

std::size_t ReplicationManager::publish_queue_bytes_per_worker()
    const noexcept {
  return group_->publish_queue_bytes_per_worker();
}

unsigned ReplicationManager::replica_priority() const noexcept {
  return group_->replica_priority();
}

bool ReplicationManager::IsNativeHandshake(
    std::span<const std::string> args) noexcept {
  return !args.empty() && (EqualCaseInsensitive(args.front(), "LVPSYNC") ||
                           EqualCaseInsensitive(args.front(), "LVFLOW"));
}

Task<absl::Status> ReplicationManager::ServeNativeConnection(
    TcpStream& stream, std::vector<std::string> args, std::uint64_t client_id,
    std::string client_address, bool tls) {
  if (!IsNativeHandshake(args)) {
    co_return absl::InvalidArgumentError(
        "connection did not start with a replication handshake");
  }
  co_return co_await group_->ServeNativeConnection(
      stream, std::move(args), client_id, std::move(client_address), tls);
}

Task<absl::Status> ReplicationManager::ServeRedisExportConnection(
    TcpStream& stream, std::vector<std::string> args, std::uint64_t client_id,
    std::string client_address, bool tls, bool eof_capable) {
  return group_->ServeRedisExportConnection(stream, std::move(args), client_id,
                                            std::move(client_address), tls,
                                            eof_capable);
}

Task<absl::StatusOr<std::optional<NativeReplicationWatermark>>>
ReplicationManager::CaptureNativeReplicationWatermark() {
  return group_->CaptureNativeReplicationWatermark();
}

Task<std::optional<std::uint64_t>>
ReplicationManager::CountAcknowledgedNativeReplicas(
    const NativeReplicationWatermark& watermark) const {
  return group_->CountAcknowledgedNativeReplicas(watermark);
}

Task<std::uint64_t> ReplicationManager::CountOnlineNativeReplicas() const {
  return group_->CountOnlineNativeReplicas();
}

bool ReplicationManager::is_replica() const noexcept {
  return group_->is_replica();
}

bool ReplicationManager::is_loading() const noexcept {
  return group_->is_loading();
}

bool ReplicationManager::reject_writes() const noexcept {
  return is_loading() || group_->is_redis_follower() ||
         (options_.replica_read_only_ && is_replica());
}

std::uint64_t ReplicationManager::role_epoch() const noexcept {
  return group_->role_epoch();
}

std::uint64_t ReplicationManager::CaptureServingGeneration() const noexcept {
  constexpr std::uint64_t kServingOpen = 1;
  const std::uint64_t generation =
      serving_generation_.load(std::memory_order_acquire);
  return (generation & kServingOpen) != 0 ? generation : 0;
}

bool ReplicationManager::ServingGenerationMatches(
    std::uint64_t generation) const noexcept {
  return generation != 0 &&
         serving_generation_.load(std::memory_order_acquire) == generation;
}

bool ReplicationManager::redirects_clients_to_upstream() const noexcept {
  return !group_->is_redis_follower();
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

}  // namespace lavik
