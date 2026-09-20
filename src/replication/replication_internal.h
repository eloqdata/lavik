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

#pragma once

// Private declarations shared by the native and Redis replication translation
// units. ReplicationGroup remains the single worker-owned lifecycle
// coordinator; splitting protocol implementations does not add another owner or
// lock.

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
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
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
#include "full_sync_handoff.h"
#include "lavik/cluster/control_protocol.h"
#include "lavik/cluster/lease_clock.h"
#include "lavik/command.h"
#include "lavik/command_table.h"
#include "lavik/fault_injection.h"
#include "lavik/memory.h"
#include "lavik/metrics.h"
#include "lavik/rdb.h"
#include "lavik/rdb_collection.h"
#include "lavik/replication.h"
#include "lavik/replication_command.h"
#include "lavik/replication_group.h"
#include "lavik/replication_history.h"
#include "lavik/resp.h"
#include "lavik/storage/engine.h"
#include "native_recovery.h"
#include "native_reparent.h"
#include "native_replay.h"
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

namespace replication_internal {

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

bool IsLeaseAdmissionSuspended(const absl::Status& status);

std::uint64_t SteadyNanos() noexcept;

std::uint64_t SecondsSince(std::uint64_t started_nanos) noexcept;

#if LAVIK_FAULTS_ENABLED
// Signal that a test-only coroutine barrier has been reached. The caller
// owns the release condition (for example action replacement or marker removal)
// so tests need not infer progress from elapsed time.
absl::Status SignalFaultBarrier(const char* variable,
                                std::string_view barrier_name);

Task<absl::Status> WaitAtSourceAdmissionFaultBarrier(
    const std::atomic<bool>& shutdown_requested);
#endif
// A disk-backed full sync of a multi-terabyte dataset can legitimately run
// for hours. Only a flow that stops making protocol progress is timed out;
// there is deliberately no wall-clock limit on the whole synchronization.
constexpr auto kFullSyncStallTimeout = std::chrono::minutes(10);
constexpr auto kReconnectDelay = std::chrono::seconds(1);
// A new Owner may still be opening admission when replicas first reconnect.
// Keep that transient denial from adding a full second to convergence.
constexpr auto kNativeReconnectDelay = std::chrono::milliseconds(100);
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
// group in one target fdatasync, then scan one source partition at a time
// while handoffs complete independently. This keeps the scan memory bound
// without issuing one metadata durability round-trip for every empty partition.
constexpr std::size_t kFullSyncResetBatch = 64;
constexpr std::size_t kFullSyncHandoffWindow = 64;
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
  // Inclusive, completed ONLINE flow LSNs. FULL and cursor ACKs remain kAck.
  kAckRange = 9,
};

void PutU8(std::string& output, std::uint8_t value);

void PutU16(std::string& output, std::uint16_t value);

void PutU32(std::string& output, std::uint32_t value);

void PutU64(std::string& output, std::uint64_t value);

void PutString(std::string& output, std::string_view value);

absl::Status ReplicationMemoryExhausted(std::string_view operation);

absl::Status ReserveReplicationString(std::string* output, std::size_t desired);

absl::Status AppendReplicationString(std::string* output,
                                     std::string_view value);

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

std::size_t EncodedRecordBytes(const SnapshotRecord& record);

absl::Status EncodeRecords(std::uint16_t partition_id,
                           std::span<const SnapshotRecord> records,
                           std::string* output);

// The returned records own every key and value copied from the frame. Catch at
// this ownership boundary so reserve, per-record strings, and vector growth
// share one failure path.
absl::StatusOr<std::pair<std::uint16_t, std::vector<SnapshotRecord>>>
DecodeRecords(std::string_view payload);

bool EqualCaseInsensitive(std::string_view left,
                          std::string_view right) noexcept;

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

std::vector<std::string_view> SplitWords(std::string_view line);

std::string EncodeRespCommand(std::span<const std::string> args);

Task<absl::Status> WriteText(TcpStream& stream, std::string_view text);

Task<absl::Status> WriteText(TcpStream& stream, const char* text);

// A Task starts after the call expression has finished. Force temporary
// command/reply buffers into a caller-owned local instead of allowing a
// string_view to outlive them.
Task<absl::Status> WriteText(TcpStream&, std::string&&) = delete;

std::uint32_t DataFrameCrc32c(std::string_view first,
                              std::string_view second = {}) noexcept;

absl::Status AppendDataFrameHeader(std::string* output, DataFrameKind kind,
                                   std::size_t payload_bytes,
                                   std::uint32_t payload_crc32c);

absl::Status AppendDataFrame(std::string* output, DataFrameKind kind,
                             std::string_view payload);

Task<absl::Status> WriteDataFrame(TcpStream& stream, DataFrameKind kind,
                                  std::string_view payload);

Task<absl::StatusOr<std::string>> ReadExact(TcpStream& stream,
                                            std::size_t size);

Task<absl::StatusOr<std::pair<DataFrameKind, std::string>>> ReadDataFrame(
    TcpStream& stream);

// Optional progress distinguishes an unsupported-handshake EOF from a
// truncated reply, which must never trigger capability fallback.
Task<absl::StatusOr<std::string>> ReadLine(TcpStream& stream,
                                           bool* received_any = nullptr);

Task<absl::StatusOr<std::string>> ReadRedisBulkReply(TcpStream& stream);

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

bool HasCommaFlag(std::string_view flags, std::string_view wanted);

absl::StatusOr<ReplicaOfConfig> ParseRedisClusterAddress(
    std::string_view address);

absl::StatusOr<RedisClusterTopology> ParseRedisClusterNodes(
    std::string_view body);

std::string FormatRedisSlots(const RedisSlotSet& slots);

std::vector<std::uint16_t> RedisSlotsVector(const RedisSlotSet& slots);

bool SameRedisSlotLayout(const RedisClusterTopology& left,
                         const RedisClusterTopology& right);

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

absl::Status WriteFileAll(int fd, std::span<const std::byte> bytes);

Task<absl::StatusOr<std::string>> ReceiveRedisRdb(TcpStream& stream);

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

struct RedisPsyncReply {
  bool full_ = false;
  std::optional<std::string> replid_;
  std::uint64_t offset_ = 0;
};

bool IsReplicationId(std::string_view value);

absl::StatusOr<RedisPsyncReply> ParseRedisPsyncReply(std::string_view line);

Task<absl::Status> AuthenticateUpstream(TcpStream& stream,
                                        std::string_view username,
                                        std::string_view password);

Task<absl::Status> WaitForClose(TcpStream& stream);

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

absl::Status ConfigureConnectedFd(int fd);

// libc name resolution is blocking and cannot be cancelled safely. Recovery
// gives it detached ownership of only bounded resolver inputs/results, never
// the group or a coroutine. A process-wide cap prevents repeated replacements
// from accumulating resolver work; the candidate can leave at its cutoff.
struct RecoveryResolvedAddress {
  ~RecoveryResolvedAddress() {
    if (addresses_ != nullptr) ::freeaddrinfo(addresses_);
  }
  std::string host_;
  std::string service_;
  addrinfo hints_{};
  addrinfo* addresses_ = nullptr;
  int result_ = EAI_AGAIN;
  std::atomic<bool> done_{false};
};
extern std::atomic<unsigned> recovery_resolvers_in_flight;

Task<absl::StatusOr<std::shared_ptr<RecoveryResolvedAddress>>>
ResolveRecoveryAddress(std::string_view host, std::uint16_t port,
                       SocketSet* sockets);

Task<absl::StatusOr<TcpStream>> ConnectTcp(
    std::string_view host, std::uint16_t port,
    const std::shared_ptr<bycorf::TlsContext>& tls_context, SocketSet* sockets,
    bool cancellable_dns = false);

std::string NewReplicationId();

bool IsReplicationId(std::string_view value);

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
    const DesiredClusterFailoverAction& rhs);

bool IsRetainedControlledDegrade(
    const DesiredClusterFailoverAction& current,
    const DesiredClusterFailoverAction& replacement);

ClusterPromotionPrepareDirective BuildFailoverPrepareDirective(
    const DesiredClusterFailoverAction& desired,
    std::vector<std::uint64_t> current_frontier);

std::string PopulationGroupToken(std::string_view group_id);

bool IsPopulationGroupToken(std::string_view value);

absl::StatusOr<PopulationManifestId> ParsePopulationManifestId(
    std::string_view value);

std::string PeerHost(int fd);

std::string EncodeAppliedVector(std::span<const std::uint64_t> next_lsns);

absl::StatusOr<std::vector<std::uint64_t>> DecodeAppliedVector(
    std::string_view encoded);

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

// Optional raw-byte ownership is bounded independently of decoded online
// commands. Memory pressure drops retention only, including all participants
// of an effect, without delaying ordinary replica application.
struct CanonicalReceiveCharge {
  std::shared_ptr<std::atomic<std::size_t>> budget_;
  std::size_t bytes_ = 0;
  RetainedMemoryCharge memory_;
  ~CanonicalReceiveCharge() {
    if (budget_ != nullptr)
      budget_->fetch_sub(bytes_, std::memory_order_relaxed);
  }
};

std::shared_ptr<CanonicalReceiveCharge> TryReserveCanonical(
    const std::shared_ptr<std::atomic<std::size_t>>& budget, std::size_t bytes);

struct ReplicaTransactionArrival {
  std::uint64_t id_ = 0;
  std::uint8_t db_id_ = 0;
  std::vector<unsigned> participants_;
  std::vector<std::string> command_args_;
  std::vector<NativeHistoryRecord> canonical_records_;
  std::vector<std::shared_ptr<CanonicalReceiveCharge>> canonical_charges_;
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
  std::string canonical_;
  std::shared_ptr<CanonicalReceiveCharge> canonical_charge_;
};

struct ReplicaControlArrival {
  explicit ReplicaControlArrival(unsigned flow_count)
      : completion_(flow_count) {}

  ReplicatedCommand command_;
  std::vector<NativeHistoryRecord> canonical_records_;
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

  ClusterRebuildContext(RebuildDirective directive, PopulationManifest manifest,
                        ReadyToken ready)
      : directive_(std::move(directive)),
        manifest_(std::move(manifest)),
        state_(ReplicationGroupState::kReady),
        ready_token_(std::move(ready)),
        completion_(std::make_shared<detail::ClusterRebuildCompletionState>()) {
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

std::uint64_t RecoveryUnixMs();

struct ClusterRecoveryContext {
  ClusterRecoveryContext(DesiredClusterRecovery desired, SocketSet* shutdown)
      : desired_(std::move(desired)),
        sockets_(shutdown),
        lease_deadline_(
            BootDeadline(*desired_.action_.recovery_deadline_unix_ms_)) {}
  static cluster::LeaseTime BootDeadline(std::uint64_t deadline) {
    const auto now = RecoveryUnixMs();
    const auto remaining = deadline > now ? deadline - now : 0;
    return cluster::LeaseClockNow() +
           std::chrono::milliseconds(
               std::min<std::uint64_t>(remaining, 86400000));
  }
  bool Expired() const {
    // The absolute committed cutoff also accounts for suspend/dispatch. The
    // boot clock prevents a backward wall-clock adjustment from extending it.
    return RecoveryUnixMs() >= *desired_.action_.recovery_deadline_unix_ms_ ||
           cluster::LeaseClockNow() >= lease_deadline_;
  }
  const DesiredClusterRecovery desired_;
  SocketSet sockets_;
  const cluster::LeaseTime lease_deadline_;
  unsigned active_exports_ = 0;  // Coordinator worker only.
  bool watcher_finished_ = false;
#if LAVIK_FAULTS_ENABLED
  bool test_rejected_first_request_ = false;
#endif
};

struct TimedSocketContext {
  explicit TimedSocketContext(SocketSet* parent)
      : sockets_(parent),
        deadline_(cluster::LeaseClockNow() + kHandshakeTimeout) {}
  SocketSet sockets_;
  const cluster::LeaseTime deadline_;
  bool finished_ = false;
  bool watcher_finished_ = false;
};
struct NativeContinuationProof {
  std::string target_node_id_;
  std::string target_assignment_id_;
  std::string target_boot_id_;
  std::string child_history_id_;
  std::string capability_;
  std::vector<std::uint64_t> child_origin_;
};

struct RecoveryReceiveBudget {
  std::size_t bytes_ = 0;
};
struct RecoveryReceivedEffect {
  RecoveryReceivedEffect(std::shared_ptr<RecoveryReceiveBudget> budget,
                         std::size_t bytes)
      : budget_(std::move(budget)), bytes_(bytes) {
    budget_->bytes_ += bytes_;
  }
  ~RecoveryReceivedEffect() { budget_->bytes_ -= bytes_; }
  const std::shared_ptr<RecoveryReceiveBudget> budget_;
  const std::size_t bytes_;
  RetainedMemoryCharge charge_;
  std::vector<NativeHistoryRecord> records_;
};
struct RecoveryPeerSession {
  RecoveryPeerSession(ClusterRecoveryPeer peer, SocketSet* parent)
      : peer_(std::move(peer)), sockets_(parent) {}
  RetainedMemoryCharge metadata_charge_;
  const ClusterRecoveryPeer peer_;
  SocketSet sockets_;
  std::optional<detail::NativeRecoveryAdvertisement> report_;
  std::optional<std::pair<unsigned, std::uint64_t>> request_;
  std::unique_ptr<RecoveryReceivedEffect> received_;
  bool connecting_ = true;
  bool busy_ = false;
  bool finished_ = false;
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
  bool recovery_started_ = false;
  bool recovery_running_ = false;
  std::optional<ClusterCandidateRecoveryResult> recovery_;
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
  std::vector<std::shared_ptr<ReplicationHistory>> retained_histories_;
  std::string source_history_id_;
  std::shared_ptr<detail::NativeReplay> replay_;
  bool allow_initial_cursor_ = false;
  const std::shared_ptr<std::atomic<std::size_t>> canonical_receive_bytes_ =
      std::make_shared<std::atomic<std::size_t>>(0);
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
    if (!fullsync && !allow_initial_cursor_ &&
        requested_cursors_[flow_id].lsn_ == 1 &&
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

std::string_view ReplicationPhaseName(ReplicationPhase phase) noexcept;

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
  // Immutable first complete READY boundary. In particular a completed empty
  // FULL proves LSN one; that position must not be confused with an unready
  // target merely because a membership refresh closes its control socket.
  std::atomic<std::uint64_t> proven_origin_lsn_{0};
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
    if (phase == ReplicationPhase::kReady && cursor.fragment_index_ == 0) {
      std::uint64_t unset = 0;
      (void)progress.proven_origin_lsn_.compare_exchange_strong(
          unset, cursor.lsn_, std::memory_order_release);
    }
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

  std::vector<std::uint64_t> ProvenOrigin() const {
    std::vector<std::uint64_t> result;
    result.reserve(flows_.size());
    for (const auto& flow : flows_)
      result.push_back(flow.proven_origin_lsn_.load(std::memory_order_acquire));
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
  bool allow_initial_cursor_ =
      false;  // Published under master_mutex_ before flow admission.
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

// Own the authenticated PSYNC socket until role transition has drained the old
// source. Keeping this connection avoids a second handshake and bounds pending
// RDB input by transport backpressure. Abandoned preparations close
// immediately.
struct PreparedRedisConnection {
  PreparedRedisConnection(TcpStream stream,
                          std::shared_ptr<TimedSocketContext> transport)
      : stream_(std::move(stream)),
        transport_(std::move(transport)),
        sockets_(&transport_->sockets_) {}
  ~PreparedRedisConnection() {
    if (stream_.IsOpen()) {
      sockets_->Remove(stream_.NativeFd());
      stream_.Close().IgnoreError();
    }
  }
  bool TransferTo(SocketSet* sockets) {
    sockets_->Remove(stream_.NativeFd());
    sockets_ = sockets;
    return sockets_->Add(stream_.NativeFd());
  }
  TcpStream TakeStream() {
    sockets_->Remove(stream_.NativeFd());
    return std::exchange(stream_, TcpStream{});
  }
  TcpStream stream_;
  std::shared_ptr<TimedSocketContext> transport_;
  SocketSet* sockets_;
  RedisPsyncReply reply_;
};

struct RedisSource {
  std::shared_ptr<PreparedRedisConnection> prepared_;
  ReplicaOfConfig upstream_;
  std::string node_id_;
  RedisSlotSet slots_;
  std::shared_ptr<ReplicaSession> session_;
  std::optional<std::string> replid_;
  std::atomic<std::uint64_t> offset_{0};
  // Coordinator-owned SELECT context at offset_. A partial reconnect resumes
  // this context; SELECT inside MULTI is committed only with the whole EXEC.
  std::uint8_t selected_db_ = 0;
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

struct UpstreamDiscovery {
  std::shared_ptr<PreparedRedisConnection> prepared_;
  bool redis_cluster_ = false;
  std::optional<RedisClusterTopology> topology_;
  std::optional<RedisClusterMaster> self_;
};

}  // namespace replication_internal

using namespace replication_internal;

class ReplicationManager::ReplicationGroup {
 public:
  ReplicationGroup(storage::StorageEngine* storage,
                   std::optional<ReplicaOfConfig> initial_upstream,
                   const ReplicationOptions& options,
                   std::atomic<std::uint64_t>* serving_generation);

  void StorageReady(bycorf::Worker& worker);

  Task<absl::StatusOr<std::shared_ptr<detail::ClusterRebuildCompletionState>>>
  StartClusterRebuildDirective(ReplicaOfConfig upstream,
                               RebuildDirective directive,
                               PopulationManifest manifest);

  Task<absl::Status> ApplyClusterRebuildDirective(ReplicaOfConfig upstream,
                                                  RebuildDirective directive,
                                                  PopulationManifest manifest);

  Task<absl::StatusOr<std::shared_ptr<detail::ClusterRebuildCompletionState>>>
  StartEmptyPopulationInitialization(RebuildIdentity identity,
                                     PopulationManifest manifest);

#if LAVIK_FAULTS_ENABLED
  // Fault builds can synthesize the already-proven candidate boundary so the
  // promotion kernel can be exercised without a second process implementing
  // the full destructive-rebuild protocol. The selector is attempt-scoped,
  // and this entire path is erased from ordinary release binaries.
  Task<absl::Status> SeedReadyPromotionCandidateForFaultTest(
      const ClusterPromotionPrepareDirective& directive,
      bool recovered = false);
#endif

#if LAVIK_FAULTS_ENABLED
  Task<absl::StatusOr<bool>> WaitAtPromotionFaultBarrier(
      const std::shared_ptr<ClusterPromotionPrepareContext>& context,
      const char* signal_variable);
#endif

  absl::Status InstallPromotionHistoryBridge(
      const ClusterPromotionPrepareDirective& directive,
      const ReadyToken& population, const ClusterPromotionPrepared& prepared);

  absl::Status AdoptLocalOwnerHistory();

  Task<absl::Status> RunClusterPromotionPrepare(
      std::shared_ptr<ClusterPromotionPrepareContext> context,
      std::shared_ptr<ClusterRebuildContext> population,
      std::shared_ptr<detail::ReplicaAppliedFrontier> frontier,
      std::shared_ptr<ReplicaSession> session, bool native_population);

  Task<absl::StatusOr<
      std::shared_ptr<detail::ClusterPromotionPrepareCompletionState>>>
  StartClusterPromotionPrepareDirective(
      ClusterPromotionPrepareDirective directive);

  Task<absl::StatusOr<
      std::shared_ptr<detail::ClusterPromotionPrepareCompletionState>>>
  StartNativeClusterFailoverPromotionPrepare(
      ClusterPromotionPrepareDirective directive);

  absl::Status ValidateClusterSourcePause(
      const DesiredClusterSourcePause& desired) const;

  bool SourcePausePopulationMatches(
      const DesiredClusterSourcePause& desired) const;

  Task<absl::Status> CaptureClusterSourcePause(
      const std::shared_ptr<ClusterSourcePauseContext>& context);

  Task<absl::Status> ReconcileClusterSourcePause(
      std::optional<DesiredClusterSourcePause> desired);

  Task<ClusterSourcePauseStatus> cluster_source_pause_status() const;

  bool FailoverPopulationMatches(
      const DesiredClusterFailoverAction& desired) const;

  bool FailoverReplicaDomainMatches(
      const DesiredClusterFailoverAction& desired) const;

  bool IsNativeSelfOriginDomain(
      const DesiredClusterFailoverAction& desired) const;

  bool ReadyPopulationIsSuppressed(
      const DesiredClusterFailoverAction& failed) const;

  void PublishFailoverActionFailure(
      const std::shared_ptr<ClusterFailoverActionContext>& context,
      std::string failure_class, std::string failure_detail);

  std::chrono::milliseconds FailoverActionWatchdog() const;

  void RequestFailoverPromotionCancellation(
      const std::shared_ptr<ClusterFailoverActionContext>& action);

  Task<absl::Status> WaitForFailoverActionRetry(
      const std::shared_ptr<ClusterFailoverActionContext>& context,
      std::chrono::steady_clock::time_point watchdog_deadline);

  std::vector<std::string> RecoveryHandshake(
      const DesiredClusterFailoverAction& action,
      const ClusterReplicationMember& donor) const;

  Task<absl::Status> WriteRecoveryFrame(TcpStream& stream,
                                        std::string_view payload);

  Task<absl::StatusOr<std::string>> ReadRecoveryFrame(TcpStream& stream,
                                                      std::size_t bound);

  Task<absl::Status> WatchRecoveryDeadline(
      std::shared_ptr<ClusterRecoveryContext> scope);

  Task<absl::Status> ReconcileClusterRecovery(
      std::optional<DesiredClusterRecovery> desired);

  Task<absl::Status> ResetRetainedHistory(std::string history,
                                          unsigned flow_count);

  Task<std::vector<std::vector<NativeHistoryRange>>> RetainedCoverage(
      std::string history);

  Task<absl::Status> SendRetainedEffect(
      TcpStream& stream, std::string_view history,
      const detail::NativeRecoveryAdvertisement& report, unsigned flow,
      std::uint64_t lsn, std::function<bool()> current);

  std::vector<std::string> ParentHandshake(
      const DesiredClusterUpstream& desired,
      const ClusterFailoverCompatibilityDomain& parent,
      std::span<const std::uint64_t> cursor) const;

  bool CurrentPartialOwner(
      const std::shared_ptr<ClusterFollowOwnerContext>& relationship) const;

  Task<absl::Status> WatchTimedSockets(
      std::shared_ptr<TimedSocketContext> transfer,
      std::function<bool()> current);

  Task<absl::Status> CancelPartialExports();

  Task<bool> ChildOriginAvailable(
      const std::shared_ptr<const detail::NativeHistoryBridge>& bridge);

  Task<absl::Status> RunParentExport(
      TcpStream& stream, const std::vector<std::string>& args,
      const std::shared_ptr<TimedSocketContext>& transfer,
      const std::shared_ptr<ClusterFollowOwnerContext>& relationship);

  Task<absl::Status> ServeParentExport(TcpStream& stream,
                                       std::vector<std::string> args);

  Task<absl::Status> ServeRecoveryDonor(TcpStream& stream,
                                        std::vector<std::string> args);

  Task<absl::StatusOr<std::unique_ptr<RecoveryReceivedEffect>>>
  FetchRetainedEffect(TcpStream& stream,
                      const detail::NativeRecoveryAdvertisement& report,
                      const std::shared_ptr<RecoveryReceiveBudget>& budget,
                      unsigned flow, std::uint64_t lsn);

  Task<absl::Status> RunRecoveryPeer(
      std::shared_ptr<ClusterRecoveryContext> scope,
      std::shared_ptr<RecoveryPeerSession> peer,
      std::shared_ptr<RecoveryReceiveBudget> budget);

  Task<absl::Status> RunRecoveryPeerConnection(
      const std::shared_ptr<ClusterRecoveryContext>& scope,
      const std::shared_ptr<RecoveryPeerSession>& peer,
      const std::shared_ptr<RecoveryReceiveBudget>& budget);

  void MaybeStartCandidateRecovery();

  Task<absl::Status> RunCandidateRecovery(
      std::shared_ptr<ClusterFailoverActionContext> action,
      std::shared_ptr<ClusterRecoveryContext> scope);

  Task<absl::Status> RunClusterFailoverAction(
      std::shared_ptr<ClusterFailoverActionContext> context);

  absl::Status ValidateClusterFailoverAction(
      const DesiredClusterFailoverAction& desired) const;

  Task<absl::Status> ReconcileClusterFailoverAction(
      std::optional<DesiredClusterFailoverAction> desired,
      std::optional<ClusterFailoverActionId> pending_activation_action_id);

  Task<ClusterFailoverActionStatus> cluster_failover_action_status() const;

  Task<std::optional<ClusterFailoverPreparedContext>>
  FindClusterFailoverPreparedContext(
      const ClusterFailoverActionId& action_id) const;

  absl::Status ValidateClusterFailoverActivation(
      const ClusterFailoverActivation& activation) const;

  bool ClusterActivationPopulationMatches(
      const ClusterFailoverActivation& activation) const;

  Task<absl::Status> ActivateClusterPreparedPromotion(
      ClusterFailoverActivation activation);

  Task<absl::Status> EnableClusterExpirationAuthorityUntil(
      std::chrono::nanoseconds deadline_since_boot);

  Task<absl::Status> RevokeClusterExpirationAuthority();

  absl::StatusOr<std::pair<DesiredClusterUpstream, PopulationManifest>>
  NormalizeClusterFollowOwner(DesiredClusterUpstream desired) const;

  bool ClusterFollowReadyPopulationMatches(
      const DesiredClusterUpstream& desired) const;

  Task<absl::Status> StopClusterFollowIngress(
      const std::shared_ptr<ClusterFollowOwnerContext>& previous);

  absl::Status BeginClusterFollowFullPopulation(
      const std::shared_ptr<ReplicaSession>& session,
      std::string source_boot_id, std::string source_history_id,
      std::uint32_t source_flow_count);

  Task<absl::Status> FreezeFormerOwnerPopulation(std::uint64_t source_term);

  Task<absl::Status> ReconcileClusterFollowOwner(
      std::optional<DesiredClusterUpstream> desired);

  Task<absl::Status> RetireClusterPopulation(
      std::optional<DesiredClusterPopulation> desired, bool preserve_any_ready,
      bool preserve_current_follow_attempt, std::string_view reason);

  Task<absl::Status> ReconcileClusterPopulation(
      std::optional<DesiredClusterPopulation> desired);

  Task<absl::Status> CancelInProgressClusterPopulation(
      bool preserve_current_follow_attempt);

  Task<absl::Status> InstallRecoveredPopulation(
      detail::RecoveredPopulation population);

  Task<absl::Status> RecoverClusterPopulation();

  Task<absl::Status> CancelClusterRebuildForShutdown();

  void RequestShutdown() noexcept;

  Task<absl::Status> QuiesceForShutdown();

  std::shared_ptr<detail::ClusterRebuildCompletionState>
  FindCompletedClusterPopulation(const RebuildDirective& directive) const;

  Task<ClusterPopulationStatus> cluster_population_status() const;

  Task<absl::Status> AuthorizeClusterRebuildSource(RebuildDirective directive);

  Task<absl::Status> RevokeClusterRebuildSourceAuthorizations();

  Task<absl::Status> EnableClusterRebuildSourceAdmissionUntil(
      std::chrono::nanoseconds deadline_since_boot);

  Task<absl::Status>
  ClearClusterRebuildSourceAuthorizationsForSessionReplacement(
      bool preserve_established_exports);

  Task<absl::Status> RefreshClusterRebuildSourceAuthorizationsForFdsReplacement(
      bool preserve_current_population_exports,
      std::size_t expected_authorization_replays);

  enum class SourceAuthorizationRetirementMode {
    kStrongRevoke,
    kSessionReplacement,
    kFdsReplacement,
  };

  Task<absl::Status> RetireClusterRebuildSourceAuthorizations(
      SourceAuthorizationRetirementMode mode,
      bool preserve_current_population_exports,
      std::size_t expected_authorization_replays = 0);

  // Shared durability/history kernel for native Cluster promotion and
  // standalone/Sentinel REPLICAOF NO ONE. The caller owns admission drain,
  // Function catalog exclusion, and expiration quiescence. Success creates a
  // child publisher but deliberately does not open serving or expiration.
  Task<absl::StatusOr<ClusterPromotionPrepared>> PreparePromotion(
      storage::PromotionBase promotion_base);

  // Opens the prepared publisher/storage role without assuming ownership of
  // any expiration pause or authority capability. Cluster uses this kernel
  // between provisional lease validation and NodeControl's final lease/FDS
  // recheck; standalone adds its own expiration pairing below.
  void ActivatePreparedPromotionRole();

  // Standalone/Sentinel has no external authority commit between prepare and
  // activation, so its synchronous command also consumes the expiration pause
  // that SetUpstream acquired and restores permanent expiration authority.
  void ActivatePreparedPromotion();

  Task<absl::Status> SetUpstream(std::optional<ReplicaOfConfig> upstream);

  Task<absl::Status> AddUpstream(ReplicaOfConfig upstream);

  Task<absl::StatusOr<std::optional<NativeReplicationWatermark>>>
  CaptureNativeReplicationWatermark();

  Task<std::optional<std::uint64_t>> CountAcknowledgedNativeReplicas(
      const NativeReplicationWatermark& watermark) const;

  Task<std::uint64_t> CountOnlineNativeReplicas() const;

  Task<ReplicationIdentity> identity() const;

  std::optional<ReplicaOfConfig> upstream() const;

  Task<ReplicationStatus> status() const;

  void StoreRole(ReplicationRole next, std::memory_order order) noexcept;

  void StoreRedisLink(const std::shared_ptr<RedisSource>& source,
                      bool up) noexcept;

  bool is_replica() const noexcept;

  std::uint64_t role_epoch() const noexcept;

  bool is_redis_follower() const noexcept;

  bool is_loading() const noexcept;
  DatasetReadState dataset_read_state(
      const std::atomic<bool>& serve_stale) const noexcept;

  absl::Status SetSnapshotReadConcurrency(unsigned concurrency) noexcept;

  unsigned snapshot_read_concurrency() const noexcept;

  absl::Status SetSnapshotBatchSize(std::size_t count) noexcept;

  std::size_t snapshot_batch_size() const noexcept;

  absl::Status SetReplicaPriority(unsigned priority) noexcept;

  unsigned replica_priority() const noexcept;

  std::size_t BacklogCapacityForFlow(unsigned flow_id,
                                     std::size_t global_bytes) const noexcept;

  Task<absl::Status> SetBacklogSizeBytes(std::size_t bytes);

  std::size_t backlog_size_bytes() const noexcept;

  Task<absl::Status> SetBacklogBackpressure(bool enabled);

  bool backlog_backpressure() const noexcept;

  Task<absl::Status> SetPublishQueueBytesPerWorker(std::size_t bytes);

  std::size_t publish_queue_bytes_per_worker() const noexcept;

  Task<absl::Status> ServeNativeConnection(TcpStream& stream,
                                           std::vector<std::string> args,
                                           std::uint64_t client_id,
                                           std::string client_address,
                                           bool tls);

 private:
  static void AssertStateOwner() noexcept;

  void PublishUpstreamSnapshot();

  void SetDesiredUpstream(std::optional<ReplicaOfConfig> upstream);

  bool EmptyPopulationCurrent(
      const std::shared_ptr<ClusterRebuildContext>& context);

  Task<absl::Status> FinishEmptyPopulationFailure(
      const std::shared_ptr<ClusterRebuildContext>& context,
      std::uint64_t session_id, bool root_started, bool promoted,
      absl::Status failure);

  Task<absl::Status> RunEmptyPopulationInitialization(
      std::shared_ptr<ClusterRebuildContext> context);

  bool StorageIsReady() const noexcept;

  Task<absl::StatusOr<std::optional<RedisClusterTopology>>>
  QueryRedisClusterTopology(const ReplicaOfConfig& upstream);

  Task<absl::StatusOr<std::optional<RedisClusterTopology>>>
  QueryRedisClusterTopology(TcpStream& stream);

  Task<absl::StatusOr<UpstreamDiscovery>> DiscoverRedis(TcpStream& stream);

  Task<absl::StatusOr<UpstreamDiscovery>> PrepareRedisUpstreamConnection(
      const ReplicaOfConfig& upstream,
      const std::shared_ptr<TimedSocketContext>& transport);

  Task<absl::StatusOr<UpstreamDiscovery>> PrepareRedisUpstream(
      const ReplicaOfConfig& upstream);

  Task<absl::Status> WaitUntilStorageReady();

  void StartCoordinator();

  Task<absl::Status> ConnectInitialRedisUpstream();

  bool RedisSourceRegistered(const std::shared_ptr<RedisSource>& source) const;

  std::uint64_t NextRedisFullSyncSessionId() noexcept;

  Task<absl::StatusOr<std::uint64_t>> BeginRedisFullSyncAttempt(
      const std::shared_ptr<RedisSource>& source);

  Task<absl::Status> WaitForRedisFullSyncActivation(
      const std::shared_ptr<RedisSource>& source);

  void RefreshRedisRole();

  void StartRedisTopologyMonitor();

  void FaultRedisTopology(std::string_view reason);

  void ApplyStableRedisTopology(RedisClusterTopology topology);

  Task<absl::Status> RedisTopologyMonitor(std::uint64_t role_epoch);

  void StartRedisCoordinator(const std::shared_ptr<RedisSource>& source);

  Task<absl::Status> RedisCoordinator(std::shared_ptr<RedisSource> source);

  void LatchReplicationFailure(std::string reason);

  Task<absl::Status> Coordinator();

  Task<absl::Status> ImportRedisRdb(const std::string& path,
                                    const std::shared_ptr<RedisSource>& source);

  Task<absl::Status> ExpectRedisReply(TcpStream& stream,
                                      std::vector<std::string> command,
                                      std::string_view expected);

  Task<absl::Status> SendRedisAck(TcpStream& stream,
                                  const std::shared_ptr<RedisSource>& source);

  absl::Status ValidateRedisSourceCommand(
      const ReplicatedCommand& command,
      const std::shared_ptr<RedisSource>& source);

  Task<absl::Status> ResetRedisSourceSlots(
      const std::shared_ptr<RedisSource>& source);

  Task<absl::Status> ConsumeRedisCommandStream(
      TcpStream& stream, const std::shared_ptr<RedisSource>& source);

  Task<absl::StatusOr<RedisPsyncReply>> StartRedisPsync(
      TcpStream& stream, const std::shared_ptr<RedisSource>& source);

  Task<absl::Status> RedisFollowerOnline(
      TcpStream& stream, const std::shared_ptr<RedisSource>& source);

  Task<absl::Status> CompleteRedisFullSync(
      TcpStream& stream, const std::shared_ptr<RedisSource>& source,
      std::string replid, std::uint64_t offset);

  Task<absl::Status> RunRedisReplicaSession(
      const std::shared_ptr<RedisSource>& source,
      const std::shared_ptr<ReplicaSession>& session);

  Task<absl::Status> RunRedisConnectedSession(
      const std::shared_ptr<RedisSource>& source, TcpStream& stream,
      std::optional<RedisPsyncReply> prepared_reply);

  Task<absl::StatusOr<bool>> ReplayAndSwitchParent(
      const std::shared_ptr<ReplicaSession>& session,
      const std::shared_ptr<TimedSocketContext>& transfer);

  Task<absl::Status> TryPartialReparent(
      const std::shared_ptr<ReplicaSession>& session);

  Task<absl::Status> RunReplicaSession(
      const ReplicaOfConfig& upstream, std::uint64_t role_epoch,
      const std::shared_ptr<ReplicaSession>& session);

  Task<absl::Status> CancelAndWaitForReplicaFlows(
      const std::shared_ptr<ReplicaSession>& session);

  Task<absl::Status> ValidateClusterResetBoundary(
      const std::shared_ptr<ClusterRebuildContext>& context);

  Task<absl::Status> RecordClusterResetProof(
      const std::shared_ptr<ClusterRebuildContext>& context,
      std::vector<storage::ReplicaPartitionEpoch> resets);

  Task<absl::Status> RecordClusterHandoffProof(
      const std::shared_ptr<ClusterRebuildContext>& context,
      std::uint16_t partition_id, std::uint64_t target_local_epoch);

  Task<absl::Status> PrepareReplicaFlowMode(
      const std::shared_ptr<ReplicaSession>& session, unsigned flow_id,
      bool fullsync);

  Task<absl::Status> RunReplicaFlow(ReplicaOfConfig upstream,
                                    std::shared_ptr<ReplicaSession> session,
                                    unsigned flow_id);

  Task<absl::Status> WaitForReplicaTransaction(
      const std::shared_ptr<ReplicaTransactionArrival>& arrival);

  Task<absl::Status> ApplyReadyReplicaTransaction(
      std::shared_ptr<ReplicaSession> session, unsigned owner,
      std::shared_ptr<ReplicaTransactionArrival> arrival);

  absl::StatusOr<PreparedReplicaTransactionArrival>
  PrepareReplicaTransactionArrival(
      const std::shared_ptr<ReplicaSession>& session, unsigned flow_id,
      std::uint64_t lsn, ReplicatedCommand envelope,
      std::shared_ptr<ReplicaTransactionArrival> predecessor);

  absl::StatusOr<std::shared_ptr<ReplicaTransactionArrival>>
  RegisterReplicaTransactionOnOwner(
      const std::shared_ptr<ReplicaSession>& session, unsigned owner,
      PreparedReplicaTransactionArrival prepared);

  Task<absl::StatusOr<std::shared_ptr<ReplicaTransactionArrival>>>
  RegisterReplicaTransaction(
      const std::shared_ptr<ReplicaSession>& session, unsigned flow_id,
      std::uint64_t lsn, ReplicatedCommand envelope,
      std::shared_ptr<ReplicaTransactionArrival> predecessor,
      std::string canonical,
      std::shared_ptr<CanonicalReceiveCharge> canonical_charge);

  Task<absl::Status> ApplyReplicaControl(
      const std::shared_ptr<ReplicaSession>& session, unsigned flow_id,
      std::uint64_t lsn, ReplicatedCommand command, std::string canonical);

  struct ReplicaOnlineCommand {
    std::uint64_t lsn_ = 0;
    ReplicatedCommand command_;
    std::string canonical_;
    std::shared_ptr<CanonicalReceiveCharge> canonical_charge_;
  };

  struct ReplicaOnlineCompletion {
    std::uint64_t lsn_ = 0;
    std::shared_ptr<ReplicaTransactionArrival> transaction_;
  };

  struct ReplicaOnlineApplyState {
    // Immutable negotiation result, owned by this flow worker.
    bool ack_ranges_ = false;
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
      const ReplicatedCommand& command);
#endif

  Task<absl::Status> StageReplicaOnlineCommands(
      const std::shared_ptr<ReplicaSession>& session, unsigned flow_id,
      const std::shared_ptr<ReplicaOnlineApplyState>& state);

  Task<absl::Status> AckReplicaOnlineCommands(
      TcpStream& stream, const std::shared_ptr<ReplicaSession>& session,
      unsigned flow_id, const std::shared_ptr<ReplicaOnlineApplyState>& state);

  Task<absl::Status> TrackReplicaOnlineStage(
      TcpStream& stream, const std::shared_ptr<ReplicaSession>& session,
      unsigned flow_id, const std::shared_ptr<ReplicaOnlineApplyState>& state);

  Task<absl::Status> TrackReplicaOnlineAcks(
      TcpStream& stream, const std::shared_ptr<ReplicaSession>& session,
      unsigned flow_id, const std::shared_ptr<ReplicaOnlineApplyState>& state);

  Task<absl::Status> RunReplicaOnlineFlowData(
      TcpStream& stream, const std::shared_ptr<ReplicaSession>& session,
      unsigned flow_id, std::uint64_t first_expected_lsn,
      std::pair<DataFrameKind, std::string> first_frame, bool ack_ranges);

  struct ReplicaHandoffState {
    // Flow-local lifetime extends until every handoff task has joined. The
    // ACK mutex serializes frame bytes, not partition completion order.
    std::array<bool, storage::kLogicalStorageShards> pending_{};
    bycorf::AsyncMutex ack_mutex_;
    bycorf::AsyncNotification changed_;
    std::size_t active_ = 0;
    std::size_t completed_ = 0;
    absl::Status status_;
    bool stopping_ = false;
  };

  Task<absl::Status> SendReplicaFullSyncAck(
      TcpStream& stream, const std::shared_ptr<ReplicaHandoffState>& state,
      std::uint16_t partition, std::uint64_t sequence);

  Task<absl::Status> ApplyReplicaHandoff(
      const std::shared_ptr<ReplicaSession>& session,
      const std::shared_ptr<ReplicaHandoffState>& state,
      std::uint16_t partition, std::uint64_t epoch);

  Task<absl::Status> TrackReplicaHandoff(
      TcpStream& stream, const std::shared_ptr<ReplicaSession>& session,
      const std::shared_ptr<ReplicaHandoffState>& state,
      std::uint16_t partition, std::uint64_t epoch, std::uint64_t sequence);

  Task<absl::Status> RunReplicaFlowData(
      TcpStream& stream, const std::shared_ptr<ReplicaSession>& session,
      unsigned flow_id, bool ack_ranges);

  Task<absl::Status> WaitReplicaHandoffs(
      const std::shared_ptr<ReplicaSession>& session,
      const std::shared_ptr<ReplicaHandoffState>& state,
      std::optional<std::uint16_t> partition = std::nullopt,
      std::size_t active_limit = 0);

  Task<absl::Status> ReceiveReplicaFlowData(
      TcpStream& stream, const std::shared_ptr<ReplicaSession>& session,
      unsigned flow_id, const std::shared_ptr<ReplicaHandoffState>& state,
      bool ack_ranges);

  bool ShouldInjectFlowDrop(unsigned flow_id);

  bool ShouldInjectControlDropAfterResponse();

  bool ShouldInjectFullSyncCutDrop(unsigned flow_id);

  bool ShouldInjectReplicaPromotionFailure();

  bool ShouldInjectEmptyPopulationResetFailure(unsigned owner);

  bool ShouldInjectEmptyPopulationCatalogFailure();

  bool ShouldInjectPromotionPrepareFailure(std::string_view stage);

  bool ShouldInjectEarlyOnline() const;

  bool ShouldInjectPostCutReset(unsigned flow_id);

  bool ShouldInjectDivergentTail(unsigned flow_id);

  Task<absl::Status> InvalidateReplicaContinuation(
      const std::shared_ptr<ReplicaSession>& session,
      bool require_installed_cursor = true);

  bool ShouldInjectFlowDropAfterTransaction(unsigned flow_id);

  bool ShouldInjectFlowDropAfterCommandApply(unsigned flow_id);

#if LAVIK_FAULTS_ENABLED
  // These coroutines and their call sites are test-only, so ordinary builds
  // do not allocate an empty pause task on each ONLINE mutation or barrier.
  Task<absl::Status> MaybePauseBeforeReplicaCommandApply();

  Task<absl::Status> MaybePauseBeforeReplicaTransactionApply();

  Task<absl::Status> MaybePauseBeforeReplicaControlApply();

#endif

  struct FullSyncAckState {
    FullSyncAckState(unsigned flow, unsigned flows)
        : handoffs_(storage::kLogicalStorageShards, flow, flows) {}
    detail::FullSyncHandoffProgress handoffs_;
    // Non-handoff ACKs share the same reader. Handoff ownership lives only in
    // the partition ledger; these entries belong to records, commands and cut.
    absl::flat_hash_map<std::uint64_t, std::uint16_t> expected_;
    bycorf::AsyncNotification changed_;
    absl::Status status_;
    bool stopping_ = false;
    bool receiver_done_ = false;
  };

  Task<absl::Status> ReceiveFullSyncAcks(
      TcpStream& stream, const std::shared_ptr<MasterSession>& session,
      unsigned flow, const std::shared_ptr<FullSyncAckState>& state);

  Task<absl::Status> WaitFullSyncRequest(
      const std::shared_ptr<FullSyncAckState>& state, std::uint64_t sequence);

  Task<absl::Status> SendFullSyncRequest(
      TcpStream& stream, const std::shared_ptr<FullSyncAckState>& state,
      DataFrameKind kind, std::string_view body, std::uint16_t partition,
      std::uint64_t sequence);

  Task<absl::Status> RunMasterFlowData(
      TcpStream& stream, const std::shared_ptr<MasterSession>& session,
      unsigned flow_id, bool ack_ranges);

  Task<absl::StatusOr<std::uint64_t>> RunMasterFullSync(
      TcpStream& stream, const std::shared_ptr<MasterSession>& session,
      unsigned flow_id, const std::shared_ptr<FullSyncAckState>& ack_state);

  Task<absl::Status> EnterMasterFlowBacklog(
      TcpStream& stream, const std::shared_ptr<MasterSession>& session,
      unsigned flow_id, std::uint64_t next_lsn, std::uint32_t fragment_index,
      bool ack_ranges);

  struct MasterBacklogDuplexState {
    bool ack_ranges_ = false;
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
      const std::shared_ptr<MasterBacklogDuplexState>& duplex);

  Task<absl::Status> TrackMasterFlowBacklogAcks(
      TcpStream& stream, const std::shared_ptr<MasterSession>& session,
      unsigned flow_id,
      const std::shared_ptr<MasterBacklogDuplexState>& duplex);

  Task<absl::Status> MonitorBacklogStall(std::shared_ptr<MasterSession> session,
                                         unsigned flow_id, int fd,
                                         std::uint64_t observed_generation);

  Task<absl::Status> RunMasterFlowBacklog(
      TcpStream& stream, const std::shared_ptr<MasterSession>& session,
      unsigned flow_id, storage::ReplicationLogCursor cursor, bool ack_ranges);

  Task<absl::Status> RunAdoptedConnection(Connection* connection,
                                          std::vector<std::string> args,
                                          std::uint64_t client_id,
                                          std::string client_address, bool tls,
                                          std::uint64_t replication_session_id);

  Task<absl::Status> ServeOwnedNativeConnection(TcpStream& stream,
                                                std::vector<std::string> args,
                                                std::uint64_t client_id);

  Task<absl::Status> ServeMasterControl(TcpStream& stream,
                                        std::vector<std::string> args,
                                        std::uint64_t client_id);

  Task<absl::Status> ServeMasterFlow(TcpStream& stream,
                                     std::vector<std::string> args);

  Task<absl::Status> RemoveMasterSession(
      const std::shared_ptr<MasterSession>& session);

  Task<absl::Status> DrainSourceEgress();

  Task<absl::Status> DisableSourceHistory();

  Task<absl::Status> RetireSourceHistory();

  void FinalizeRetiredMasterSessionsLocked();

  bool MasterHistoryHasConsumersLocked() const;

  void StartIdleReplicationHistoryMonitor();

  Task<absl::Status> MonitorIdleReplicationHistory();

  Task<absl::Status> ResetInvalidReplicationHistory();

  Task<absl::Status> EnsureReplicationHistoryReady();

  storage::StorageEngine* storage_;
  // The packed atomic is owned by the enclosing manager so external commands
  // reach it without following this pImpl. Bit zero is serving-open and the
  // remaining bits are a monotonic dataset generation.
  std::atomic<std::uint64_t>* const serving_generation_;
  const bool meta_managed_;
  const bool single_client_mode_;
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
  std::shared_ptr<ClusterRecoveryContext> cluster_recovery_;
  std::shared_ptr<const detail::NativeHistoryBridge> history_bridge_;
  std::vector<std::shared_ptr<TimedSocketContext>> partial_exports_;
  absl::flat_hash_map<std::string, NativeContinuationProof>
      continuation_proofs_;
  std::optional<NativeContinuationProof> upstream_continuation_proof_;
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
  bool initial_redis_connection_pending_ = false;
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
  bool replication_idle_history_pause_used_ = false;  // worker 0 only
#endif
  std::atomic<unsigned> snapshot_read_concurrency_{
      kDefaultReplicationSnapshotReadConcurrency};
  std::atomic<std::size_t> snapshot_batch_size_{kSnapshotKeysPerBatch};
  std::vector<std::shared_ptr<ReplicationHistory>> retained_histories_;
  // Controller revision is worker-0-owned; each element is touched only on
  // its corresponding worker. Fan-outs may overlap across a suspension.
  std::uint64_t retained_reset_revision_ = 0;
  std::vector<std::uint64_t> retained_reset_revisions_;
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
  // Each Redis source owns an independent PSYNC cursor. Cursors intentionally
  // remain process-local until storage and offsets can share a crash-atomic
  // commit record; process restart therefore requests a fresh RDB per source.
  std::vector<std::shared_ptr<RedisSource>> redis_sources_;
  std::optional<RedisClusterTopology> expected_redis_topology_;
  bool redis_cluster_ = false;
  bool redis_topology_fault_ = false;
  bool redis_topology_monitor_started_ = false;  // worker 0 only
  // Covers native control/flow sockets independently
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

}  // namespace lavik
