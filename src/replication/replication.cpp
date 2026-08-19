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
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <limits>
#include <memory>
#include <mutex>
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
#include "celer/runtime/cross_core.h"
#include "celer/runtime/sync.h"
#include "celer/runtime/worker.h"
#include "keylane/command.h"
#include "keylane/metrics.h"
#include "keylane/replication_command.h"
#include "keylane/storage/engine.h"
#include "spdlog/spdlog.h"

namespace keylane {
namespace {

using celer::Connection;
using celer::Task;
using celer::TcpStream;
using storage::PartitionDeltaBatch;
using storage::PartitionReplicationStart;
using storage::PartitionSnapshotBatch;
using storage::SnapshotRecord;

constexpr std::string_view kProtocolVersion = "1";
constexpr auto kHandshakeTimeout = std::chrono::seconds(10);
constexpr auto kFullSyncTimeout = std::chrono::minutes(10);
constexpr auto kReconnectDelay = std::chrono::seconds(1);
constexpr std::size_t kSnapshotKeysPerBatch = 16;
constexpr std::size_t kDeltaRecordsPerBatch = 256;
constexpr std::size_t kMaxDataFrame = 12U * 1024U * 1024U;
constexpr std::size_t kBacklogBatchBytes = 2U * 1024U * 1024U;
constexpr std::size_t kBacklogBatchFrames = 128;
constexpr std::uint16_t kResetBatchAckPartition =
    std::numeric_limits<std::uint16_t>::max();
constexpr std::string_view kReplicationTransactionEnvelope =
    "__KEYLANE_TX_V1";

enum class DataFrameKind : std::uint8_t {
  kReset = 1,
  kRecords = 2,
  kAck = 3,
  kCommand = 4,
  kCursor = 5,
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
  bool String(std::uint32_t size, std::string* value) {
    if (remaining() < size) return false;
    value->assign(input_.data() + position_, size);
    position_ += size;
    return true;
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

bool EncodeRecords(std::uint16_t partition_id,
                   std::span<const SnapshotRecord> records,
                   std::string* output) {
  std::size_t bytes = 2 + 4;
  for (const SnapshotRecord& record : records) {
    if (record.key_.size() > std::numeric_limits<std::uint32_t>::max() ||
        record.value_.size() > std::numeric_limits<std::uint32_t>::max() ||
        bytes > kMaxDataFrame - EncodedRecordBytes(record)) {
      return false;
    }
    bytes += EncodedRecordBytes(record);
  }
  output->clear();
  output->reserve(bytes);
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
  return true;
}

absl::StatusOr<std::pair<std::uint16_t, std::vector<SnapshotRecord>>>
DecodeRecords(std::string_view payload) {
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
        !reader.U64(&record.logical_size_) || !reader.U32(&record.chunk_index_) ||
        !reader.U32(&record.chunk_count_) || !reader.U32(&key_size) ||
        !reader.U32(&value_size) ||
        kind < static_cast<std::uint8_t>(SnapshotRecord::Kind::kValue) ||
        kind > static_cast<std::uint8_t>(SnapshotRecord::Kind::kValueCommit) ||
        record.db_id_ >= storage::kLogicalDatabaseCount ||
        value_type > static_cast<std::uint8_t>(storage::ValueType::kStream) ||
        !reader.String(key_size, &record.key_) ||
        !reader.String(value_size, &record.value_)) {
      return absl::InvalidArgumentError(
          "malformed replication record payload");
    }
    record.kind_ = static_cast<SnapshotRecord::Kind>(kind);
    record.value_type_ = static_cast<storage::ValueType>(value_type);
    records.push_back(std::move(record));
  }
  if (reader.remaining() != 0) {
    return absl::InvalidArgumentError("trailing replication record payload");
  }
  return std::make_pair(partition_id, std::move(records));
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
  co_return co_await stream.WriteAll(std::span<const std::byte>(
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
  frame.reserve(5 + payload.size());
  absl::Status appended = AppendDataFrame(&frame, kind, payload);
  if (!appended.ok()) co_return appended;
  co_return co_await WriteText(stream, frame);
}

Task<absl::StatusOr<std::string>> ReadExact(TcpStream& stream,
                                             std::size_t size) {
  std::string result(size, '\0');
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
      length - 1 > kMaxDataFrame || kind < 1 || kind > 5) {
    co_return absl::InvalidArgumentError("malformed replication frame header");
  }
  auto payload = co_await ReadExact(stream, length - 1);
  if (!payload.ok()) co_return payload.status();
  co_return std::make_pair(static_cast<DataFrameKind>(kind), std::move(*payload));
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
  }

  storage::ReplicationLogCursor Load(unsigned flow_id) const noexcept {
    if (flow_id >= cursors_.size()) return {};
    return cursors_[flow_id];
  }

  void Store(unsigned flow_id, std::uint64_t lsn,
             std::uint32_t fragment) noexcept {
    if (flow_id >= cursors_.size()) return;
    cursors_[flow_id] = {.lsn_ = lsn, .fragment_index_ = fragment};
  }

  std::size_t size() const noexcept { return cursors_.size(); }

  std::vector<storage::ReplicationLogCursor> cursors_;
};

struct ReplicaTransactionArrival {
  explicit ReplicaTransactionArrival(unsigned participants)
      : completion_(participants) {}

  std::uint8_t db_id_ = 0;
  std::vector<unsigned> participants_;
  std::vector<std::string> command_args_;
  std::vector<bool> arrived_;
  std::vector<std::uint64_t> lsns_;
  std::size_t arrival_count_ = 0;
  std::size_t departure_count_ = 0;
  bool applying_ = false;
  absl::Status status_ = absl::UnknownError(
      "replicated transaction has not completed");
  celer::CoroutineBarrier completion_;
};

struct ReplicaSession {
  explicit ReplicaSession(std::uint64_t requested_generation)
      : generation_(requested_generation) {}

  std::uint64_t generation_ = 0;
  std::uint64_t session_id_ = 0;
  unsigned source_worker_count_ = 0;
  std::shared_ptr<ReplicaCursorState> cursors_;
  // Flow coroutines are detached onto their owner workers. Track their whole
  // lifetime, including connect/handshake and storage apply, so a failed
  // session cannot start a replacement while old flows are still mutating
  // replica storage or holding network buffers.
  std::atomic<unsigned> active_flows_{0};
  std::atomic<unsigned> connected_flows_{0};
  SocketSet sockets_;
  std::atomic<bool> cancelled_{false};
  std::mutex transaction_mutex_;
  absl::flat_hash_map<std::uint64_t,
                      std::shared_ptr<ReplicaTransactionArrival>>
      transactions_;

  void Cancel() {
    if (cancelled_.exchange(true, std::memory_order_acq_rel)) return;
    sockets_.Cancel();
    std::vector<std::shared_ptr<ReplicaTransactionArrival>> arrivals;
    {
      std::lock_guard lock(transaction_mutex_);
      arrivals.reserve(transactions_.size());
      for (auto& [_, arrival] : transactions_) {
        arrivals.push_back(std::move(arrival));
      }
      transactions_.clear();
    }
    const absl::Status cancelled =
        absl::CancelledError("replication session cancelled");
    for (const auto& arrival : arrivals) {
      arrival->completion_.Abort(cancelled);
    }
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
  ReplicaFlowActivityGuard& operator=(const ReplicaFlowActivityGuard&) =
      delete;
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
  kDeltaCatchup,
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
    case ReplicationPhase::kDeltaCatchup:
      return "delta_catchup";
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
                std::string host, std::uint16_t port)
      : id_(id),
        node_id_(std::move(node_id)),
        host_(std::move(host)),
        port_(port),
        flow_fds_(worker_count, -1),
        flows_(worker_count),
        flow_resume_possible_(worker_count, -1),
        snapshot_ready_(worker_count),
        snapshot_gate_closed_(worker_count),
        snapshot_fenced_(worker_count) {}

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

  Task<absl::Status> WaitSnapshotReady() {
    co_return co_await snapshot_ready_.Wait(*celer::ThisWorker().self_);
  }

  Task<absl::Status> WaitSnapshotGateClosed() {
    co_return co_await snapshot_gate_closed_.Wait(
        *celer::ThisWorker().self_);
  }

  Task<absl::Status> WaitSnapshotFenced() {
    co_return co_await snapshot_fenced_.Wait(*celer::ThisWorker().self_);
  }

  void AbortSnapshotCut(const absl::Status& status) {
    snapshot_ready_.Abort(status);
    snapshot_gate_closed_.Abort(status);
    snapshot_fenced_.Abort(status);
  }

  unsigned connected_flows() const {
    return connected_flows_.load(std::memory_order_acquire);
  }

  void SetProgress(unsigned flow_id, ReplicationPhase phase,
                   std::uint64_t lsn, std::uint32_t fragment_index,
                   std::uint16_t partition_id,
                   std::uint64_t partition_sequence) {
    if (cancelled_.load(std::memory_order_acquire) ||
        flow_id >= flows_.size()) {
      return;
    }
    ReplicaFlowProgress& progress = flows_[flow_id];
    progress.lsn_.store(lsn, std::memory_order_relaxed);
    progress.fragment_index_.store(fragment_index, std::memory_order_relaxed);
    progress.current_partition_.store(partition_id,
                                      std::memory_order_relaxed);
    progress.partition_sequence_.store(partition_sequence,
                                       std::memory_order_relaxed);
    progress.phase_.store(phase, std::memory_order_release);
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
  }

  void MarkFailed(unsigned flow_id) {
    if (flow_id < flows_.size()) {
      flows_[flow_id].phase_.store(ReplicationPhase::kFailed,
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
    if (phase == ReplicationPhase::kConnecting ||
        phase == ReplicationPhase::kFailed) {
      return std::nullopt;
    }
    return flows_[flow_id].lsn_.load(std::memory_order_acquire);
  }

  std::optional<ReplicaFlowProgressSnapshot> Progress(
      unsigned flow_id) const {
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

  void MarkOnline() noexcept {
    online_.store(true, std::memory_order_release);
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

  bool cancelled() const {
    return cancelled_.load(std::memory_order_acquire);
  }

  std::uint64_t id_ = 0;
  const std::string node_id_;
  const std::string host_;
  const std::uint16_t port_ = 0;

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
  std::atomic<unsigned> connected_flows_{0};
  std::atomic<bool> online_{false};
  std::atomic<bool> cancelled_{false};
};

}  // namespace

class ReplicationManager::Impl {
 public:
  Impl(storage::StorageEngine* storage,
       std::optional<ReplicaOfConfig> initial_upstream,
       const ReplicationOptions& options)
      : storage_(storage),
        upstream_(std::move(initial_upstream)),
        cursor_state_(std::make_shared<ReplicaCursorState>(storage->worker_count())),
        replid_(NewReplicationId()),
        listen_port_(options.listen_port_),
        tls_context_(options.use_tls_ ? options.tls_context_ : nullptr),
        masteruser_(options.masteruser_),
        masterauth_(options.masterauth_) {
    if (upstream_.has_value()) {
      role_.store(ReplicationRole::kConnecting, std::memory_order_relaxed);
      generation_.store(1, std::memory_order_relaxed);
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

    std::shared_ptr<ReplicaSession> cancelled;
    {
      std::lock_guard lock(state_mutex_);
      if (upstream_ == upstream) co_return absl::OkStatus();
      upstream_ = std::move(upstream);
      cancelled = std::move(active_replica_session_);
      replica_session_id_ = 0;
      source_worker_count_ = 0;
      upstream_replid_.reset();
      // An explicit topology change is not an automatic reconnect. Local
      // writes may have occurred while promoted or while following another
      // source, so none of the old per-flow cursors are safe for CONTINUE.
      cursor_state_.reset();
      generation_.fetch_add(1, std::memory_order_acq_rel);
      role_.store(upstream_.has_value() ? ReplicationRole::kConnecting
                                        : ReplicationRole::kMaster,
                  std::memory_order_release);
    }
    if (cancelled != nullptr) cancelled->Cancel();
    if (upstream_.has_value() && StorageIsReady()) StartCoordinator();
    co_return absl::OkStatus();
  }

  ReplicationStatus status() const {
    ReplicationStatus result;
    result.role_ = role_.load(std::memory_order_acquire);
    result.generation_ = generation_.load(std::memory_order_acquire);
    result.local_node_id_ = replid_;
    {
      std::lock_guard lock(state_mutex_);
      result.upstream_ = upstream_;
      result.upstream_node_id_ = upstream_replid_;
      result.session_id_ = replica_session_id_;
      result.source_worker_count_ = source_worker_count_;
      if (active_replica_session_ != nullptr) {
        result.connected_flows_ =
            active_replica_session_->connected_flows_.load(
                std::memory_order_acquire);
      }
    }
    {
      std::lock_guard lock(master_mutex_);
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
      }
    }
    std::sort(result.downstream_replicas_.begin(),
              result.downstream_replicas_.end(),
              [](const auto& left, const auto& right) {
                return left.node_id_ < right.node_id_;
              });
    return result;
  }

  bool is_replica() const noexcept {
    return role_.load(std::memory_order_acquire) != ReplicationRole::kMaster;
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

  Task<absl::Status> ServeNativeConnection(TcpStream& stream,
                                           std::vector<std::string> args) {
    // Accepted Redis sockets are normally optimized for batched replies, but
    // replication is an ACK-driven stream whose frame header and payload are
    // written separately. Without TCP_NODELAY on the source endpoint, Nagle
    // can hold every payload behind the tiny header until the peer's delayed
    // ACK fires (about 40 ms per sparse snapshot partition on Linux).
    absl::Status accepted_config = ConfigureConnectedFd(stream.NativeFd());
    if (!accepted_config.ok()) co_return accepted_config;
    unsigned owner = 0;
    if (EqualCaseInsensitive(args.front(), "KLFLOW")) {
      unsigned flow_id = 0;
      if (args.size() != 6 || !ParseUnsigned(args[3], &flow_id)) {
        co_return absl::InvalidArgumentError("invalid KLFLOW handshake");
      }
      // flow_id belongs to the upstream worker set. Multiple upstream flows
      // may share one local worker when the worker counts differ.
      owner = flow_id % storage_->worker_count();
    }
    if (owner == celer::ThisWorker().id_) {
      co_return co_await ServeOwnedNativeConnection(stream, std::move(args));
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
    stream.Close().IgnoreError();
    co_return co_await celer::SubmitTo(
        owner, [this, duplicate, tls_state = std::move(tls_state),
                args = std::move(args)]() mutable {
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
          celer::ThisWorker().self_->Spawn(
              RunAdoptedConnection(registered, std::move(args)));
          return absl::OkStatus();
        });
  }

 private:
  bool StorageIsReady() const noexcept {
    return ready_workers_.load(std::memory_order_acquire) ==
           storage_->worker_count();
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
    if (celer::ThisWorker().id_ != 0 || coordinator_started_ ||
        !StorageIsReady()) {
      return;
    }
    {
      std::lock_guard lock(state_mutex_);
      if (!upstream_.has_value()) return;
    }
    coordinator_started_ = true;
    celer::ThisWorker().self_->Spawn(Coordinator());
  }

  Task<absl::Status> Coordinator() {
    while (true) {
      ReplicaOfConfig upstream;
      std::uint64_t generation = 0;
      std::shared_ptr<ReplicaSession> session;
      {
        std::lock_guard lock(state_mutex_);
        if (!upstream_.has_value()) break;
        upstream = *upstream_;
        generation = generation_.load(std::memory_order_relaxed);
        session = std::make_shared<ReplicaSession>(generation);
        active_replica_session_ = session;
      }
      role_.store(ReplicationRole::kConnecting, std::memory_order_release);
      absl::Status connected =
          co_await RunReplicaSession(upstream, generation, session);
      session->Cancel();

      bool retry = false;
      {
        std::lock_guard lock(state_mutex_);
        if (active_replica_session_ == session) {
          active_replica_session_.reset();
          replica_session_id_ = 0;
          source_worker_count_ = 0;
        }
        retry = upstream_.has_value() &&
                generation_.load(std::memory_order_relaxed) == generation;
      }
      if (!retry) continue;
      role_.store(ReplicationRole::kConnecting, std::memory_order_release);
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

  Task<absl::Status> RunReplicaSession(
      const ReplicaOfConfig& upstream, std::uint64_t generation,
      const std::shared_ptr<ReplicaSession>& session) {
    auto connected =
        co_await ConnectTcp(upstream.host_, upstream.port_, tls_context_);
    if (!connected.ok()) co_return connected.status();
    TcpStream control = std::move(*connected);
    const int control_fd = control.NativeFd();
    if (!session->sockets_.Add(control_fd)) {
      control.Close().IgnoreError();
      co_return absl::CancelledError("replication generation was replaced");
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

    role_.store(ReplicationRole::kHandshake, std::memory_order_release);
    // Protocol version and argument count remain 1 and 3. Older sources ignore
    // the third token, while newer sources decode the optional identity after
    // '?', which keeps rolling upgrades compatible in both directions.
    const std::vector<std::string> sync_args{
        "KLPSYNC", std::string(kProtocolVersion),
        absl::StrCat("?", replid_, ":", listen_port_)};
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
    if (words.size() != 4 || words[0] != "+KLFULLRESYNC" ||
        !ParseUnsigned(words[1], &session_id) || session_id == 0 ||
        words[2].size() != 40 || !ParseUnsigned(words[3], &source_workers) ||
        source_workers == 0) {
      session->sockets_.Remove(control_fd);
      control.Close().IgnoreError();
      co_return absl::InvalidArgumentError(
          "invalid KLPSYNC response from upstream");
    }
    if (generation_.load(std::memory_order_acquire) != generation) {
      session->sockets_.Remove(control_fd);
      control.Close().IgnoreError();
      co_return absl::CancelledError("replication generation was replaced");
    }
    session->session_id_ = session_id;
    session->source_worker_count_ = source_workers;
    // Flow count is defined by the upstream, not by this node's worker count.
    // Build the cursor state before spawning any flow so every flow_id has a
    // valid (lsn, fragment) pair, including when worker counts differ.
    auto next_cursors = std::make_shared<ReplicaCursorState>(source_workers);
    {
      std::lock_guard lock(state_mutex_);
      if (cursor_state_ != nullptr) {
        const unsigned copied = static_cast<unsigned>(std::min(
            cursor_state_->size(), next_cursors->size()));
        for (unsigned i = 0; i < copied; ++i) {
          const auto cursor = cursor_state_->Load(i);
          next_cursors->Store(i, cursor.lsn_, cursor.fragment_index_);
        }
      }
      cursor_state_ = next_cursors;
      session->cursors_ = std::move(next_cursors);
      if (active_replica_session_ == session) {
        upstream_replid_ = std::string(words[2]);
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
          online.ok()
              ? absl::InvalidArgumentError(
                    "upstream did not complete flow handshake")
              : online.status();
      absl::Status stopped = co_await CancelAndWaitForReplicaFlows(session);
      co_return stopped.ok() ? failed : stopped;
    }
    role_.store(ReplicationRole::kOnline, std::memory_order_release);
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
    auto next_warning =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (session->active_flows_.load(std::memory_order_acquire) != 0) {
      absl::Status slept = co_await celer::SleepFor(
          *celer::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!slept.ok()) co_return slept;
      if (std::chrono::steady_clock::now() >= next_warning) {
        spdlog::warn(
            "waiting for {} cancelled replication flow(s) to finish",
            session->active_flows_.load(std::memory_order_acquire));
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
    const std::vector<std::string> flow_args{
        "KLFLOW", std::string(kProtocolVersion),
        std::to_string(session->session_id_), std::to_string(flow_id),
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

  Task<absl::Status> ApplyReplicaTransaction(
      const std::shared_ptr<ReplicaSession>& session, unsigned flow_id,
      std::uint64_t lsn, ReplicatedCommand envelope) {
    const auto& args = envelope.args_;
    if (args.size() < 5 || args[0] != kReplicationTransactionEnvelope) {
      co_return absl::InvalidArgumentError(
          "malformed replicated transaction envelope");
    }
    auto parse_uint = [](std::string_view text,
                         std::uint64_t* output) noexcept {
      const char* begin = text.data();
      const char* end = begin + text.size();
      const auto parsed = std::from_chars(begin, end, *output);
      return parsed.ec == std::errc{} && parsed.ptr == end;
    };
    std::uint64_t txid = 0;
    std::uint64_t participant_count = 0;
    if (!parse_uint(args[1], &txid) || txid == 0 ||
        !parse_uint(args[2], &participant_count) || participant_count == 0 ||
        participant_count > session->source_worker_count_ ||
        participant_count > args.size() - 4) {
      co_return absl::InvalidArgumentError(
          "invalid replicated transaction identity");
    }
    const std::size_t command_begin = 3 + participant_count;
    if (command_begin >= args.size()) {
      co_return absl::InvalidArgumentError(
          "replicated transaction has no command");
    }
    std::vector<unsigned> participants;
    participants.reserve(participant_count);
    bool current_flow_participates = false;
    for (std::size_t index = 0; index < participant_count; ++index) {
      std::uint64_t participant = 0;
      if (!parse_uint(args[3 + index], &participant) ||
          participant >= session->source_worker_count_ ||
          std::find(participants.begin(), participants.end(), participant) !=
              participants.end()) {
        co_return absl::InvalidArgumentError(
            "invalid replicated transaction participant");
      }
      participants.push_back(static_cast<unsigned>(participant));
      current_flow_participates |= participant == flow_id;
    }
    if (!current_flow_participates) {
      co_return absl::InvalidArgumentError(
          "replicated transaction arrived on a non-participant flow");
    }
    // `envelope` is owned by this arrival coroutine. Move its command tail
    // before taking the session mutex so the first arrival only transfers a
    // vector allocation while locked; a large MSET must not copy all of its
    // key/value payload into ReplicaTransactionArrival under the mutex.
    std::vector<std::string> command_args;
    command_args.reserve(args.size() - command_begin);
    std::move(envelope.args_.begin() + command_begin, envelope.args_.end(),
              std::back_inserter(command_args));

    std::shared_ptr<ReplicaTransactionArrival> arrival;
    bool apply_here = false;
    std::vector<std::string> apply_command_args;
    {
      std::lock_guard lock(session->transaction_mutex_);
      if (session->cancelled()) {
        co_return absl::CancelledError(
            "replication session ended before transaction arrival");
      }
      auto [it, inserted] = session->transactions_.try_emplace(txid);
      if (inserted) {
        it->second = std::make_shared<ReplicaTransactionArrival>(
            static_cast<unsigned>(participants.size()));
        it->second->db_id_ = envelope.db_id_;
        it->second->participants_ = participants;
        it->second->command_args_ = std::move(command_args);
        it->second->arrived_.resize(session->source_worker_count_);
        it->second->lsns_.resize(session->source_worker_count_);
      }
      arrival = it->second;
      if (arrival->db_id_ != envelope.db_id_ ||
          arrival->participants_ != participants ||
          (!inserted && arrival->command_args_ != command_args) ||
          arrival->arrived_[flow_id]) {
        co_return absl::InvalidArgumentError(
            "conflicting replicated transaction envelope");
      }
      arrival->arrived_[flow_id] = true;
      arrival->lsns_[flow_id] = lsn;
      ++arrival->arrival_count_;
      if (arrival->arrival_count_ == arrival->participants_.size() &&
          !arrival->applying_) {
        arrival->applying_ = true;
        apply_here = true;
        apply_command_args = std::move(arrival->command_args_);
      }
    }

    if (apply_here) {
      ReplicatedCommand command{.db_id_ = envelope.db_id_,
                                .args_ = std::move(apply_command_args)};
      absl::Status status = co_await ApplyReplicatedCommand(command);
      std::lock_guard lock(session->transaction_mutex_);
      arrival->status_ = std::move(status);
      if (arrival->status_.ok()) {
        // Advance every participant together before any flow sends its ACK.
        // After an asymmetric disconnect the replacement session therefore
        // either requests all copies again or skips all of them; it can never
        // replay a transaction that was already applied once.
        for (unsigned participant : arrival->participants_) {
          session->cursors_->Store(participant,
                                   arrival->lsns_[participant] + 1, 0);
        }
      }
    }

    // Every participant arrives exactly once. Non-executing flows suspend
    // here immediately; the elected flow only arrives after apply and cursor
    // publication complete. CoroutineBarrier resumes each waiter on its
    // original worker, without polling the session mutex.
    absl::Status completed =
        co_await arrival->completion_.Wait(*celer::ThisWorker().self_);
    if (!completed.ok()) co_return completed;

    absl::Status result;
    {
      std::lock_guard lock(session->transaction_mutex_);
      result = arrival->status_;
      ++arrival->departure_count_;
      if (arrival->departure_count_ == arrival->participants_.size()) {
        session->transactions_.erase(txid);
      }
    }
    co_return result;
  }

  Task<absl::Status> RunReplicaFlowData(
      TcpStream& stream, const std::shared_ptr<ReplicaSession>& session,
      unsigned flow_id) {
    absl::flat_hash_map<std::uint16_t, std::uint64_t> epochs;
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
            auto reset =
                co_await storage_->ResetReplicaPartitions(by_owner[owner]);
            if (!reset.ok()) co_return reset.status();
            for (const storage::ReplicaPartitionEpoch& result : *reset) {
              epochs[result.partition_id_] = result.replication_epoch_;
            }
          } else {
            auto reset = co_await celer::SubmitTaskTo(
                owner,
                [this, resets = std::move(by_owner[owner])]() mutable {
                  return storage_->ResetReplicaPartitions(resets);
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
      } else if (frame->first == DataFrameKind::kCommand) {
        if (ShouldInjectFlowDrop(flow_id)) {
          co_return absl::UnavailableError(
              "injected replication flow disconnect");
        }
        DataReader reader(frame->second);
        std::uint64_t lsn = 0;
        std::uint32_t fragment = 0;
        std::uint8_t flags = 0;
        if (!reader.U64(&lsn) || !reader.U32(&fragment) ||
            !reader.U8(&flags) || reader.remaining() == 0) {
          co_return absl::InvalidArgumentError(
              "malformed replication command frame");
        }
        const auto first_flag = static_cast<std::uint8_t>(
            storage::ReplicationFrameFlag::kFirst);
        const auto last_flag = static_cast<std::uint8_t>(
            storage::ReplicationFrameFlag::kLast);
        if ((flags & ~(first_flag | last_flag)) != 0) {
          co_return absl::InvalidArgumentError(
              "invalid replication command flags");
        }
        const bool first = (flags & first_flag) != 0;
        const bool last = (flags & last_flag) != 0;
        if (first) {
          if (fragment != 0 || staged_command_lsn != 0) {
            co_return absl::InvalidArgumentError(
                "replication command fragments overlap");
          }
          staged_command_lsn = lsn;
          next_command_fragment = 0;
          staged_command.clear();
        }
        if (staged_command_lsn != lsn || fragment != next_command_fragment) {
          co_return absl::InvalidArgumentError(
              "replication command fragment is out of order");
        }
        staged_command.append(frame->second.data() + 13, reader.remaining());
        ++next_command_fragment;
        if (!last) continue;
        auto command = DecodeReplicationCommand(staged_command);
        if (!command.ok()) co_return command.status();
        absl::Status applied;
        const bool transaction =
            !command->args_.empty() &&
            command->args_[0] == kReplicationTransactionEnvelope;
        if (transaction) {
          applied = co_await ApplyReplicaTransaction(
              session, flow_id, lsn, std::move(*command));
        } else {
          applied = co_await ApplyReplicatedCommand(*command);
        }
        if (!applied.ok()) {
          InvalidateReplicaContinuation(session);
          co_return applied;
        }
        // Publish progress before the ACK write. A peer disconnect is only
        // observed by that write; retaining the old cursor until afterwards
        // would replay non-idempotent commands such as APPEND on reconnect.
        session->cursors_->Store(flow_id, lsn + 1, 0);
        if (transaction && ShouldInjectFlowDropAfterTransaction(flow_id)) {
          co_return absl::UnavailableError(
              "injected replication flow disconnect after transaction");
        }
        if (!transaction && ShouldInjectFlowDropAfterCommandApply(flow_id)) {
          co_return absl::UnavailableError(
              "injected replication flow disconnect after command apply");
        }
        absl::Status acknowledged = co_await send_ack(0, lsn);
        if (!acknowledged.ok()) co_return acknowledged;
        staged_command_lsn = 0;
        next_command_fragment = 0;
        staged_command.clear();
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
      } else {
        auto records = DecodeRecords(frame->second);
        if (!records.ok()) co_return records.status();
        const auto found = epochs.find(records->first);
        if (found == epochs.end()) {
          co_return absl::FailedPreconditionError(
              "replication records arrived before reset");
        }
        const unsigned owner = records->first % storage_->worker_count();
        const std::uint16_t partition_id = records->first;
        const std::uint64_t epoch = found->second;
        const std::uint64_t sequence = records->second.empty()
                                           ? 0
                                           : records->second.back()
                                                 .mutation_sequence_;
        absl::Status applied = co_await celer::SubmitTaskTo(
            owner, [this, partition_id, epoch,
                    records = std::move(records->second)]() mutable {
              return storage_->ApplyReplicaRecords(partition_id, epoch,
                                                    records);
            });
        if (!applied.ok()) co_return applied;
        absl::Status acknowledged =
            co_await send_ack(records->first, sequence);
        if (!acknowledged.ok()) co_return acknowledged;
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
    const auto parsed = std::from_chars(configured, configured + length, target);
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
    upstream_replid_.reset();
  }

  bool ShouldInjectFlowDropAfterTransaction(unsigned flow_id) {
    const char* configured = std::getenv(
        "KEYLANE_REPLICATION_DROP_FLOW_AFTER_TRANSACTION_APPLY");
    if (configured == nullptr) return false;
    unsigned target = 0;
    const std::size_t length = std::strlen(configured);
    const auto parsed = std::from_chars(configured, configured + length, target);
    if (parsed.ec != std::errc{} || parsed.ptr != configured + length ||
        target != flow_id) {
      return false;
    }
    return !replication_transaction_fault_drop_used_.exchange(
        true, std::memory_order_acq_rel);
  }

  bool ShouldInjectFlowDropAfterCommandApply(unsigned flow_id) {
    const char* configured = std::getenv(
        "KEYLANE_REPLICATION_DROP_FLOW_AFTER_COMMAND_APPLY");
    if (configured == nullptr) return false;
    unsigned target = 0;
    const std::size_t length = std::strlen(configured);
    const auto parsed = std::from_chars(configured, configured + length, target);
    if (parsed.ec != std::errc{} || parsed.ptr != configured + length ||
        target != flow_id) {
      return false;
    }
    return !replication_command_apply_fault_drop_used_.exchange(
        true, std::memory_order_acq_rel);
  }

  Task<absl::Status> TrimBacklogForFlow(unsigned flow_id) {
    std::optional<std::uint64_t> minimum;
    {
      std::lock_guard lock(master_mutex_);
      for (const auto& [id, session] : master_sessions_) {
        (void)id;
        const auto retained = session->RetainedLsn(flow_id);
        if (retained.has_value()) {
          if (!minimum.has_value() || *retained < *minimum) {
            minimum = *retained;
          }
        }
      }
    }
    // With no active consumer, retain the bounded backlog for a reconnecting
    // replica. Capacity eviction remains the upper bound; a future partial
    // sync can continue while its cursor is still above the resulting floor.
    if (!minimum.has_value()) co_return absl::OkStatus();
    co_return co_await storage_->TrimReplicationLog(*minimum);
  }

  Task<absl::Status> RunMasterFlowData(
      TcpStream& stream, const std::shared_ptr<MasterSession>& session,
      unsigned flow_id) {
    // Retain the command log while the snapshot is running. The actual
    // handoff cursor is established by an ordered publisher fence after the
    // final delta pass: commands before that fence are represented by the
    // snapshot/deltas, and commands after it are represented only by the log.
    const auto initial_log = storage_->LocalReplicationLogInfo();
    const std::uint64_t backlog_start_lsn =
        initial_log.tail_lsn_ == 0 ? 1 : initial_log.tail_lsn_ + 1;
    session->SetProgress(flow_id, ReplicationPhase::kReset,
                         backlog_start_lsn, 0, 0, 0);
    struct CapturedPartition {
      std::uint16_t partition_id_ = 0;
      PartitionReplicationStart start_;
    };
    std::vector<CapturedPartition> captured;
    captured.reserve((storage::kLogicalStorageShards +
                      storage_->worker_count() - 1) /
                     storage_->worker_count());
    absl::flat_hash_map<std::uint16_t, std::uint64_t> next_sequence;
    auto cleanup = [&]() {
      for (const CapturedPartition& partition : captured) {
        storage_->EndPartitionReplication(partition.partition_id_);
      }
    };

    // Fence every partition owned by this source flow first, then reset the
    // target with one frame. This removes one network round trip and one
    // metadata fdatasync boundary per partition while preserving the
    // per-partition snapshot/delta sequence captured below.
    for (std::uint32_t value = flow_id; value < storage::kLogicalStorageShards;
         value += storage_->worker_count()) {
      const auto partition_id = static_cast<std::uint16_t>(value);
      PartitionReplicationStart start =
          storage_->BeginPartitionReplication(partition_id);
      next_sequence[partition_id] = start.snapshot_sequence_;
      session->SetProgress(flow_id, ReplicationPhase::kReset,
                           backlog_start_lsn, 0, partition_id,
                           start.snapshot_sequence_);
      captured.push_back(CapturedPartition{
          .partition_id_ = partition_id,
          .start_ = std::move(start),
      });
    }
    std::string reset;
    reset.reserve(4 + captured.size() *
                          (2 + 8 * storage::kLogicalDatabaseCount));
    PutU32(reset, static_cast<std::uint32_t>(captured.size()));
    for (const CapturedPartition& partition : captured) {
      PutU16(reset, partition.partition_id_);
      for (std::uint64_t epoch : partition.start_.db_epochs_) {
        PutU64(reset, epoch);
      }
    }
    absl::Status sent = co_await WriteFrameAndWaitAck(
        stream, DataFrameKind::kReset, reset, kResetBatchAckPartition);
    if (!sent.ok()) {
      cleanup();
      co_return sent;
    }

    std::size_t processed_partitions = 0;
    for (const CapturedPartition& partition : captured) {
      const std::uint16_t partition_id = partition.partition_id_;
      const PartitionReplicationStart& start = partition.start_;
      session->SetProgress(flow_id, ReplicationPhase::kSnapshot,
                           backlog_start_lsn, 0, partition_id,
                           start.snapshot_sequence_);
      for (std::uint8_t db_id = 0; db_id < storage::kLogicalDatabaseCount;
           ++db_id) {
        if ((start.nonempty_db_mask_ & (std::uint16_t{1} << db_id)) == 0)
          continue;
        std::uint64_t cursor = 0;
        do {
          auto batch = co_await storage_->SnapshotPartition(
              partition_id, db_id, cursor, kSnapshotKeysPerBatch,
              snapshot_read_concurrency());
          if (!batch.ok()) {
            cleanup();
            co_return batch.status();
          }
          if (!batch->records_.empty()) {
            std::string payload;
            if (!EncodeRecords(partition_id, batch->records_, &payload)) {
              cleanup();
              co_return absl::ResourceExhaustedError(
                  "replication snapshot batch exceeds frame limit");
            }
            sent = co_await WriteFrameAndWaitAck(
                stream, DataFrameKind::kRecords, payload, partition_id);
            if (!sent.ok()) {
              cleanup();
              co_return sent;
            }
          }
          cursor = batch->cursor_;
        } while (cursor != 0);
      }
      session->SetProgress(flow_id, ReplicationPhase::kDeltaCatchup,
                           backlog_start_lsn, 0, partition_id,
                           next_sequence[partition_id]);
      while (true) {
        PartitionDeltaBatch batch = storage_->ReadPartitionDeltas(
            partition_id, next_sequence[partition_id], kDeltaRecordsPerBatch);
        if (batch.overflow_) {
          cleanup();
          co_return absl::AbortedError("partition delta retention overflow");
        }
        if (!batch.records_.empty()) {
          std::string payload;
          if (!EncodeRecords(partition_id, batch.records_, &payload)) {
            cleanup();
            co_return absl::ResourceExhaustedError(
                "replication delta batch exceeds frame limit");
          }
          sent = co_await WriteFrameAndWaitAck(
              stream, DataFrameKind::kRecords, payload, partition_id);
          if (!sent.ok()) {
            cleanup();
            co_return sent;
          }
          next_sequence[partition_id] =
              batch.records_.back().mutation_sequence_;
          session->SetProgress(flow_id, ReplicationPhase::kDeltaCatchup,
                               backlog_start_lsn, 0, partition_id,
                               next_sequence[partition_id]);
          storage_->AcknowledgePartitionDeltas(
              partition_id, next_sequence[partition_id]);
        } else {
          next_sequence[partition_id] = batch.watermark_;
          session->SetProgress(flow_id, ReplicationPhase::kDeltaCatchup,
                               backlog_start_lsn, 0, partition_id,
                               next_sequence[partition_id]);
          storage_->AcknowledgePartitionDeltas(partition_id,
                                                next_sequence[partition_id]);
          break;
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
    }

    auto drain_deltas = [&]() -> Task<absl::Status> {
      while (true) {
        bool sent_any = false;
        for (const auto& [partition_id, sequence] : next_sequence) {
          PartitionDeltaBatch batch = storage_->ReadPartitionDeltas(
              partition_id, sequence, kDeltaRecordsPerBatch);
          if (batch.overflow_) {
            co_return absl::AbortedError(
                "partition delta retention overflow");
          }
          if (batch.records_.empty()) continue;
          std::string payload;
          if (!EncodeRecords(partition_id, batch.records_, &payload)) {
            co_return absl::ResourceExhaustedError(
                "replication delta batch exceeds frame limit");
          }
          absl::Status delta_sent = co_await WriteFrameAndWaitAck(
              stream, DataFrameKind::kRecords, payload, partition_id);
          if (!delta_sent.ok()) co_return delta_sent;
          next_sequence[partition_id] =
              batch.records_.back().mutation_sequence_;
          session->SetProgress(flow_id, ReplicationPhase::kDeltaCatchup,
                               backlog_start_lsn, 0, partition_id,
                               next_sequence[partition_id]);
          storage_->AcknowledgePartitionDeltas(
              partition_id, next_sequence[partition_id]);
          sent_any = true;
        }
        if (!sent_any) co_return absl::OkStatus();
      }
    };

    // First reduce the tail while transactions can still enter. This keeps
    // the actual exclusion window below to only the small tail accumulated
    // while all source flows rendezvous.
    absl::Status drained = co_await drain_deltas();
    if (!drained.ok()) {
      cleanup();
      co_return drained;
    }

    absl::Status cut_ready = co_await session->WaitSnapshotReady();
    if (!cut_ready.ok()) {
      cleanup();
      co_return cut_ready;
    }

    struct SnapshotGateReopen {
      bool active_ = false;
      ~SnapshotGateReopen() {
        if (active_) OpenSnapshotTransactionGate();
      }
    } gate_reopen;

    if (flow_id == 0) {
      while (!CloseSnapshotTransactionGate()) {
        if (session->cancelled()) {
          cleanup();
          co_return absl::CancelledError(
              "replication session ended while waiting for snapshot cut");
        }
        absl::Status waited = co_await celer::SleepFor(
            *celer::ThisWorker().self_, std::chrono::milliseconds(1));
        if (!waited.ok()) {
          cleanup();
          co_return waited;
        }
      }
      gate_reopen.active_ = true;
      while (SnapshotTransactionsActive()) {
        if (session->cancelled()) {
          cleanup();
          co_return absl::CancelledError(
              "replication session ended while draining transactions");
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
    // after-image deltas; transactions admitted after reopen will be behind
    // every flow's publisher fence and therefore represented by the backlog.
    drained = co_await drain_deltas();
    if (!drained.ok()) {
      cleanup();
      co_return drained;
    }

    // The publisher fence is ordered with mutation command enqueue on this
    // worker. It prevents non-idempotent mutations (INCR, LPUSH, HINCRBY, ...)
    // already represented by deltas from being executed a second time. A
    // mutation that commits while this awaits is queued after the fence and
    // is therefore preserved by the command backlog when cleanup drops its
    // now-unneeded delta.
    auto backlog_cursor = co_await storage_->FenceReplicationLog();
    if (!backlog_cursor.ok()) {
      cleanup();
      co_return backlog_cursor.status();
    }
    absl::Status fenced = co_await session->WaitSnapshotFenced();
    if (!fenced.ok()) {
      cleanup();
      co_return fenced;
    }
    if (flow_id == 0) {
      OpenSnapshotTransactionGate();
      gate_reopen.active_ = false;
    }
    cleanup();
    co_return co_await EnterMasterFlowBacklog(
        stream, session, flow_id, *backlog_cursor, 0);
  }

  Task<absl::Status> EnterMasterFlowBacklog(
      TcpStream& stream, const std::shared_ptr<MasterSession>& session,
      unsigned flow_id, std::uint64_t next_lsn,
      std::uint32_t fragment_index) {
    storage::ReplicationLogCursor cursor{.lsn_ = next_lsn,
                                         .fragment_index_ = fragment_index};
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
    co_return co_await RunMasterFlowBacklog(stream, session, flow_id, cursor);
  }

  Task<absl::Status> RunMasterFlowBacklog(
      TcpStream& stream, const std::shared_ptr<MasterSession>& session,
      unsigned flow_id, storage::ReplicationLogCursor cursor) {
    while (stream.IsOpen()) {
      auto batch = co_await storage_->ReadReplicationLog(
          cursor, kBacklogBatchBytes, kBacklogBatchFrames);
      if (!batch.ok()) co_return batch.status();
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
        wire_batch.push_back(
            iovec{.iov_base = frame_headers.data() + index * 18,
                  .iov_len = 18});
        wire_batch.push_back(iovec{
            .iov_base = const_cast<char*>(frame.payload_.data()),
            .iov_len = frame.payload_.size()});
        const bool last =
            (frame.header_.flags_ &
             static_cast<std::uint8_t>(storage::ReplicationFrameFlag::kLast)) !=
            0;
        if (last) pending_acks.push_back(frame.header_.lsn_);
      }
      if (!wire_batch.empty()) {
        absl::Status sent = co_await stream.WriteAllV(wire_batch);
        if (!sent.ok()) co_return sent;
      }
      // Keep the storage reader moving independently from the durable cursor.
      // A batch may end in the middle of a fragmented command, so its next
      // cursor can be ahead of the last command the replica has acknowledged.
      cursor = batch->next_;
      for (const std::uint64_t expected_lsn : pending_acks) {
        auto ack = co_await ReadDataFrame(stream);
        if (!ack.ok()) co_return ack.status();
        if (ack->first != DataFrameKind::kAck) {
          co_return absl::InvalidArgumentError("replication command ACK expected");
        }
        DataReader ack_reader(ack->second);
        std::uint16_t ignored_partition = 0;
        std::uint64_t acknowledged_lsn = 0;
        if (!ack_reader.U16(&ignored_partition) ||
            !ack_reader.U64(&acknowledged_lsn) ||
            acknowledged_lsn != expected_lsn) {
          co_return absl::InvalidArgumentError("malformed replication command ACK");
        }
        session->SetBacklogCursor(
            flow_id, ReplicationPhase::kReady,
            storage::ReplicationLogCursor{.lsn_ = expected_lsn + 1,
                                          .fragment_index_ = 0});
      }
      if (!pending_acks.empty()) {
        absl::Status trimmed = co_await TrimBacklogForFlow(flow_id);
        if (!trimmed.ok()) co_return trimmed;
      }
      if (batch->at_tail_) {
        absl::Status slept = co_await celer::SleepFor(
            *celer::ThisWorker().self_, std::chrono::milliseconds(1));
        if (!slept.ok()) co_return slept;
        continue;
      }
      cursor = batch->next_;
    }
    co_return absl::UnavailableError("replication backlog flow closed");
  }

  Task<absl::Status> RunAdoptedConnection(Connection* connection,
                                          std::vector<std::string> args) {
    TcpStream stream(connection);
    absl::Status status =
        co_await ServeOwnedNativeConnection(stream, std::move(args));
    if (connection->state_ == celer::ConnectionState::kActive &&
        !connection->closing_) {
      celer::ThisWorker().self_->BeginClose(
          connection, status,
          status.ok() ? celer::CloseMode::kLocalClose
                      : celer::CloseMode::kLocalError);
    }
    if (!status.ok()) {
      spdlog::warn("replication native handshake failed: {}",
                   status.message());
    }
    co_return status;
  }

  Task<absl::Status> ServeOwnedNativeConnection(TcpStream& stream,
                                                std::vector<std::string> args) {
    ReplicationConnectionMetricGuard connection_metric(
        EqualCaseInsensitive(args.front(), "KLPSYNC")
            ? ReplicationConnectionKind::kControl
            : ReplicationConnectionKind::kFlow);
    if (EqualCaseInsensitive(args.front(), "KLPSYNC")) {
      co_return co_await ServeMasterControl(stream, std::move(args));
    }
    co_return co_await ServeMasterFlow(stream, std::move(args));
  }

  Task<absl::Status> ServeMasterControl(TcpStream& stream,
                                        std::vector<std::string> args) {
    if (args.size() != 3 || args[1] != kProtocolVersion) {
      co_return absl::InvalidArgumentError("invalid KLPSYNC handshake");
    }
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
    auto session = std::make_shared<MasterSession>(
        session_id, storage_->worker_count(), std::move(replica_node_id),
        std::move(replica_host), replica_port);
    for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
      absl::Status enabled = co_await celer::SubmitTaskTo(
          worker, [this, session_id]() -> Task<absl::Status> {
            // Runtime backlog is bounded and intentionally not fsynced. A
            // later durable-ACK mode will use a separate policy.
            co_return co_await storage_->EnableReplicationLog(
                session_id, 64ULL * 1024 * 1024);
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
        stream, absl::StrCat("+KLFULLRESYNC ", session_id, " ", replid_, " ",
                             storage_->worker_count(), "\r\n"));
    if (!sent.ok()) {
      RemoveMasterSession(session);
      co_return sent;
    }

    const auto deadline = std::chrono::steady_clock::now() + kFullSyncTimeout;
    auto next_progress_log =
        std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (!session->cancelled() &&
           !session->all_flows_ready() &&
           std::chrono::steady_clock::now() < deadline) {
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
          "replica flows did not complete full synchronization");
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
        (next_lsn > 1 || fragment_index != 0) &&
        log_info.state_ == storage::ReplicationLogState::kActive &&
        next_lsn >= log_info.floor_lsn_ && next_lsn <= log_info.tail_lsn_ + 1;
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
        stream, absl::StrCat("+KLFLOW ", session_id, " ", flow_id, " ",
                             selected_continue_mode ? "CONTINUE" : "FULL",
                             "\r\n"));
    if (!sent.ok()) {
      session->ClearFlow(flow_id, stream.NativeFd());
      session->Cancel();
      co_return sent;
    }
    absl::Status waited = selected_continue_mode
                              ? co_await EnterMasterFlowBacklog(
                                    stream, session, flow_id, next_lsn,
                                    fragment_index)
                              : co_await RunMasterFlowData(stream, session,
                                                           flow_id);
    if (!waited.ok()) {
      spdlog::warn("replication source flow {} ended: {}", flow_id,
                   waited.message());
    }
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

  storage::StorageEngine* storage_;
  mutable std::mutex state_mutex_;
  std::optional<ReplicaOfConfig> upstream_;
  std::shared_ptr<ReplicaSession> active_replica_session_;
  std::atomic<ReplicationRole> role_{ReplicationRole::kMaster};
  std::atomic<std::uint64_t> generation_{0};
  std::atomic<unsigned> ready_workers_{0};
  std::uint64_t replica_session_id_ = 0;
  unsigned source_worker_count_ = 0;
  bool ready_waiter_started_ = false;  // worker 0 only
  bool coordinator_started_ = false;   // worker 0 only
  std::shared_ptr<ReplicaCursorState> cursor_state_;
  std::optional<std::string> upstream_replid_;
  std::atomic<bool> replication_fault_drop_used_{false};
  std::atomic<bool> replication_transaction_fault_drop_used_{false};
  std::atomic<bool> replication_command_apply_fault_drop_used_{false};
  std::atomic<unsigned> snapshot_read_concurrency_{1};

  const std::string replid_;
  const std::uint16_t listen_port_;
  const std::shared_ptr<celer::TlsContext> tls_context_;
  const std::string masteruser_;
  const std::string masterauth_;
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
  co_return co_await impl_->SetUpstream(std::move(upstream));
}

absl::Status ReplicationManager::SetSnapshotReadConcurrency(
    unsigned concurrency) noexcept {
  return impl_->SetSnapshotReadConcurrency(concurrency);
}

unsigned ReplicationManager::snapshot_read_concurrency() const noexcept {
  return impl_->snapshot_read_concurrency();
}

bool ReplicationManager::IsNativeHandshake(
    std::span<const std::string> args) noexcept {
  return !args.empty() && (EqualCaseInsensitive(args.front(), "KLPSYNC") ||
                           EqualCaseInsensitive(args.front(), "KLFLOW"));
}

Task<absl::Status> ReplicationManager::ServeNativeConnection(
    TcpStream& stream, std::vector<std::string> args) {
  if (!IsNativeHandshake(args)) {
    co_return absl::InvalidArgumentError(
        "connection did not start with a replication handshake");
  }
  co_return co_await impl_->ServeNativeConnection(stream, std::move(args));
}

ReplicationStatus ReplicationManager::status() const { return impl_->status(); }

bool ReplicationManager::is_replica() const noexcept {
  return impl_->is_replica();
}

bool ReplicationManager::reject_writes() const noexcept {
  return options_.replica_read_only_ && is_replica();
}

std::string_view ReplicationRoleName(ReplicationRole role) noexcept {
  switch (role) {
    case ReplicationRole::kMaster:
      return "master";
    case ReplicationRole::kConnecting:
      return "connecting";
    case ReplicationRole::kHandshake:
      return "handshake";
    case ReplicationRole::kOnline:
      return "online";
  }
  return "unknown";
}

}  // namespace keylane
