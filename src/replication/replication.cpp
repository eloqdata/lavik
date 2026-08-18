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
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "celer/net/connection.h"
#include "celer/runtime/cross_core.h"
#include "celer/runtime/worker.h"
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
constexpr auto kReconnectDelay = std::chrono::seconds(1);
constexpr std::size_t kSnapshotKeysPerBatch = 16;
constexpr std::size_t kDeltaRecordsPerBatch = 256;
constexpr std::size_t kMaxDataFrame = 12U * 1024U * 1024U;

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

Task<absl::Status> WriteDataFrame(TcpStream& stream, DataFrameKind kind,
                                  std::string_view payload) {
  if (payload.size() > kMaxDataFrame) {
    co_return absl::ResourceExhaustedError(
        "replication data frame exceeds configured limit");
  }
  std::string header;
  header.reserve(5);
  PutU32(header, static_cast<std::uint32_t>(payload.size() + 1));
  PutU8(header, static_cast<std::uint8_t>(kind));
  absl::Status status = co_await WriteText(stream, header);
  if (!status.ok()) co_return status;
  if (!payload.empty()) status = co_await WriteText(stream, payload);
  co_return status;
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

Task<absl::StatusOr<TcpStream>> ConnectTcp(std::string_view host,
                                           std::uint16_t port) {
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
  co_return TcpStream(registered);
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

struct ReplicaSession {
  explicit ReplicaSession(std::uint64_t requested_generation)
      : generation_(requested_generation) {}

  std::uint64_t generation_ = 0;
  std::uint64_t session_id_ = 0;
  unsigned source_worker_count_ = 0;
  std::shared_ptr<ReplicaCursorState> cursors_;
  std::atomic<unsigned> connected_flows_{0};
  SocketSet sockets_;
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
  MasterSession(std::uint64_t id, unsigned worker_count)
      : id_(id), flow_fds_(worker_count, -1) {}

  bool SetControl(int fd) {
    std::lock_guard lock(mutex_);
    if (cancelled_ || control_fd_ >= 0) return false;
    control_fd_ = fd;
    return true;
  }

  bool SetFlow(unsigned flow_id, int fd) {
    std::lock_guard lock(mutex_);
    if (cancelled_ || flow_id >= flow_fds_.size() || flow_fds_[flow_id] >= 0) {
      return false;
    }
    flow_fds_[flow_id] = fd;
    ++connected_flows_;
    return true;
  }

  void ClearFlow(unsigned flow_id, int fd) {
    std::lock_guard lock(mutex_);
    if (flow_id < flow_fds_.size() && flow_fds_[flow_id] == fd) {
      flow_fds_[flow_id] = -1;
      if (connected_flows_ != 0) --connected_flows_;
    }
  }

  unsigned connected_flows() const {
    std::lock_guard lock(mutex_);
    return connected_flows_;
  }

  unsigned worker_count() const noexcept {
    return static_cast<unsigned>(flow_fds_.size());
  }

  void Cancel() {
    std::lock_guard lock(mutex_);
    if (cancelled_) return;
    cancelled_ = true;
    if (control_fd_ >= 0) ::shutdown(control_fd_, SHUT_RDWR);
    for (int fd : flow_fds_) {
      if (fd >= 0) ::shutdown(fd, SHUT_RDWR);
    }
  }

  bool cancelled() const {
    std::lock_guard lock(mutex_);
    return cancelled_;
  }

  std::uint64_t id_ = 0;

 private:
  mutable std::mutex mutex_;
  int control_fd_ = -1;
  std::vector<int> flow_fds_;
  unsigned connected_flows_ = 0;
  bool cancelled_ = false;
};

}  // namespace

class ReplicationManager::Impl {
 public:
  Impl(storage::StorageEngine* storage,
       std::optional<ReplicaOfConfig> initial_upstream)
      : storage_(storage),
        upstream_(std::move(initial_upstream)),
        cursor_state_(std::make_shared<ReplicaCursorState>(storage->worker_count())),
        replid_(NewReplicationId()) {
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
      generation_.fetch_add(1, std::memory_order_acq_rel);
      role_.store(upstream_.has_value() ? ReplicationRole::kConnecting
                                        : ReplicationRole::kMaster,
                  std::memory_order_release);
    }
    if (cancelled != nullptr) cancelled->sockets_.Cancel();
    if (upstream_.has_value() && StorageIsReady()) StartCoordinator();
    co_return absl::OkStatus();
  }

  ReplicationStatus status() const {
    ReplicationStatus result;
    result.role_ = role_.load(std::memory_order_acquire);
    result.generation_ = generation_.load(std::memory_order_acquire);
    std::lock_guard lock(state_mutex_);
    result.upstream_ = upstream_;
    result.session_id_ = replica_session_id_;
    result.source_worker_count_ = source_worker_count_;
    if (active_replica_session_ != nullptr) {
      result.connected_flows_ = active_replica_session_->connected_flows_.load(
          std::memory_order_acquire);
    }
    return result;
  }

  bool is_replica() const noexcept {
    return role_.load(std::memory_order_acquire) != ReplicationRole::kMaster;
  }

  Task<absl::Status> ServeNativeConnection(TcpStream& stream,
                                           std::vector<std::string> args) {
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
        owner, [this, duplicate, args = std::move(args)]() mutable {
          Connection connection;
          connection.worker_ = celer::ThisWorker().self_;
          connection.file_.fd_ = duplicate;
          connection.closed_ = false;
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
      session->sockets_.Cancel();

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
    auto connected = co_await ConnectTcp(upstream.host_, upstream.port_);
    if (!connected.ok()) co_return connected.status();
    TcpStream control = std::move(*connected);
    const int control_fd = control.NativeFd();
    if (!session->sockets_.Add(control_fd)) {
      control.Close().IgnoreError();
      co_return absl::CancelledError("replication generation was replaced");
    }
    ReplicationConnectionMetricGuard connection_metric(
        ReplicationConnectionKind::kControl);

    role_.store(ReplicationRole::kHandshake, std::memory_order_release);
    const std::vector<std::string> sync_args{
        "KLPSYNC", std::string(kProtocolVersion), "?"};
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
        replica_session_id_ = session_id;
        source_worker_count_ = source_workers;
      }
    }

    for (unsigned flow_id = 0; flow_id < source_workers; ++flow_id) {
      const unsigned owner = flow_id % storage_->worker_count();
      auto start = [this, upstream, session, flow_id]() {
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
        session->sockets_.Cancel();
        session->sockets_.Remove(control_fd);
        control.Close().IgnoreError();
        co_return started;
      }
    }

    auto online = co_await ReadLine(control);
    if (!online.ok() || *online != "+KLONLINE") {
      session->sockets_.Cancel();
      session->sockets_.Remove(control_fd);
      control.Close().IgnoreError();
      co_return online.ok() ? absl::InvalidArgumentError(
                                  "upstream did not complete flow handshake")
                            : online.status();
    }
    role_.store(ReplicationRole::kOnline, std::memory_order_release);
    spdlog::info(
        "replication session {} online with {}:{} using 1+{} connections",
        session_id, upstream.host_, upstream.port_, source_workers);
    absl::Status waited = co_await WaitForClose(control);
    session->sockets_.Remove(control_fd);
    control.Close().IgnoreError();
    session->sockets_.Cancel();
    // Cursor state is flow-owned and intentionally non-atomic. Wait for all
    // old flow coroutines to leave before a replacement session copies it.
    const auto flow_deadline =
        std::chrono::steady_clock::now() + kHandshakeTimeout;
    while (session->connected_flows_.load(std::memory_order_acquire) != 0 &&
           std::chrono::steady_clock::now() < flow_deadline) {
      absl::Status slept = co_await celer::SleepFor(
          *celer::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!slept.ok()) break;
    }
    co_return waited;
  }

  Task<absl::Status> RunReplicaFlow(ReplicaOfConfig upstream,
                                    std::shared_ptr<ReplicaSession> session,
                                    unsigned flow_id) {
    auto connected = co_await ConnectTcp(upstream.host_, upstream.port_);
    if (!connected.ok()) {
      session->sockets_.Cancel();
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
      session->sockets_.Cancel();
      co_return sent;
    }
    auto response = co_await ReadLine(stream);
    const std::string expected =
        absl::StrCat("+KLFLOW ", session->session_id_, " ", flow_id);
    if (!response.ok() || response->substr(0, expected.size()) != expected) {
      session->sockets_.Remove(fd);
      stream.Close().IgnoreError();
      session->sockets_.Cancel();
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
    session->sockets_.Cancel();
    co_return data_status;
  }

  Task<absl::Status> RunReplicaFlowData(
      TcpStream& stream, const std::shared_ptr<ReplicaSession>& session,
      unsigned flow_id) {
    std::unordered_map<std::uint16_t, std::uint64_t> epochs;
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
        std::uint16_t partition_id = 0;
        std::array<std::uint64_t, storage::kLogicalDatabaseCount> db_epochs{};
        if (!reader.U16(&partition_id)) {
          co_return absl::InvalidArgumentError("malformed replication reset");
        }
        for (std::uint64_t& epoch : db_epochs) {
          if (!reader.U64(&epoch)) {
            co_return absl::InvalidArgumentError("malformed replication reset");
          }
        }
        if (reader.remaining() != 0) {
          co_return absl::InvalidArgumentError("trailing replication reset");
        }
        const unsigned owner = partition_id % storage_->worker_count();
        auto reset = co_await celer::SubmitTaskTo(
            owner, [this, partition_id, db_epochs]()
                -> Task<absl::StatusOr<std::uint64_t>> {
              co_return co_await storage_->ResetReplicaPartition(
                  partition_id, db_epochs);
            });
        if (!reset.ok()) co_return reset.status();
        epochs[partition_id] = *reset;
        absl::Status acknowledged = co_await send_ack(partition_id, 0);
        if (!acknowledged.ok()) co_return acknowledged;
      } else if (frame->first == DataFrameKind::kCommand) {
        if (ShouldInjectFlowDrop(flow_id)) {
          co_return absl::UnavailableError(
              "injected replication flow disconnect");
        }
        DataReader reader(frame->second);
        std::uint64_t lsn = 0;
        std::uint32_t fragment = 0;
        if (!reader.U64(&lsn) || !reader.U32(&fragment) ||
            reader.remaining() == 0) {
          co_return absl::InvalidArgumentError("malformed replication command frame");
        }
        const std::string encoded(frame->second.data() + 12,
                                  reader.remaining());
        auto command = DecodeReplicationCommand(encoded);
        if (!command.ok()) co_return command.status();
        absl::Status applied = co_await ApplyReplicatedCommand(*command);
        if (!applied.ok()) co_return applied;
        absl::Status acknowledged = co_await send_ack(0, lsn);
        if (!acknowledged.ok()) co_return acknowledged;
        session->cursors_->Store(flow_id, lsn, fragment + 1);
      } else if (frame->first == DataFrameKind::kCursor) {
        DataReader reader(frame->second);
        std::uint64_t lsn = 0;
        std::uint32_t fragment = 0;
        if (!reader.U64(&lsn) || !reader.U32(&fragment) ||
            reader.remaining() != 0) {
          co_return absl::InvalidArgumentError("malformed replication cursor");
        }
        session->cursors_->Store(flow_id, lsn, fragment);
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

  Task<absl::Status> RunMasterFlowData(TcpStream& stream, unsigned flow_id) {
    // Keep the command-log fence from before the snapshot. Commands in this
    // range may also have been applied as partition deltas, but replaying the
    // canonical commands is idempotent and closes the race between the final
    // delta pass and the live backlog handoff.
    const auto initial_log = storage_->LocalReplicationLogInfo();
    const std::uint64_t backlog_start_lsn =
        initial_log.tail_lsn_ == 0 ? 1 : initial_log.tail_lsn_ + 1;
    std::vector<std::uint16_t> captured;
    std::unordered_map<std::uint16_t, std::uint64_t> next_sequence;
    auto cleanup = [&]() {
      for (std::uint16_t partition_id : captured) {
        storage_->EndPartitionReplication(partition_id);
      }
    };

    std::size_t processed_partitions = 0;
    for (std::uint32_t value = flow_id; value < storage::kLogicalStorageShards;
         value += storage_->worker_count()) {
      const auto partition_id = static_cast<std::uint16_t>(value);
      const PartitionReplicationStart start =
          storage_->BeginPartitionReplication(partition_id);
      captured.push_back(partition_id);
      next_sequence[partition_id] = start.snapshot_sequence_;
      std::string reset;
      reset.reserve(2 + 8 * storage::kLogicalDatabaseCount);
      PutU16(reset, partition_id);
      for (std::uint64_t epoch : start.db_epochs_) PutU64(reset, epoch);
      absl::Status sent = co_await WriteFrameAndWaitAck(
          stream, DataFrameKind::kReset, reset, partition_id);
      if (!sent.ok()) {
        cleanup();
        co_return sent;
      }
      for (std::uint8_t db_id = 0; db_id < storage::kLogicalDatabaseCount;
           ++db_id) {
        if ((start.nonempty_db_mask_ & (std::uint16_t{1} << db_id)) == 0)
          continue;
        std::uint64_t cursor = 0;
        do {
          auto batch = co_await storage_->SnapshotPartition(
              partition_id, db_id, cursor, kSnapshotKeysPerBatch);
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
          storage_->AcknowledgePartitionDeltas(
              partition_id, next_sequence[partition_id]);
        } else {
          next_sequence[partition_id] = batch.watermark_;
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

    // Drain one final pass after all partitions have been snapshotted. Writes
    // that raced with an earlier partition pass are therefore applied before
    // the flow switches to the command backlog.
    while (true) {
      bool sent_any = false;
      for (const auto& [partition_id, sequence] : next_sequence) {
        PartitionDeltaBatch batch = storage_->ReadPartitionDeltas(
            partition_id, sequence, kDeltaRecordsPerBatch);
        if (batch.overflow_) {
          cleanup();
          co_return absl::AbortedError("partition delta retention overflow");
        }
        if (batch.records_.empty()) continue;
        std::string payload;
        if (!EncodeRecords(partition_id, batch.records_, &payload)) {
          cleanup();
          co_return absl::ResourceExhaustedError(
              "replication delta batch exceeds frame limit");
        }
        absl::Status sent = co_await WriteFrameAndWaitAck(
            stream, DataFrameKind::kRecords, payload, partition_id);
        if (!sent.ok()) {
          cleanup();
          co_return sent;
        }
        next_sequence[partition_id] =
            batch.records_.back().mutation_sequence_;
        storage_->AcknowledgePartitionDeltas(
            partition_id, next_sequence[partition_id]);
        sent_any = true;
      }
      if (!sent_any) break;
    }

    // The snapshot/delta fence is complete. From this point on, canonical
    // commands in the per-worker backlog are the live replication stream.
    std::string cursor_payload;
    PutU64(cursor_payload, backlog_start_lsn);
    PutU32(cursor_payload, 0);
    absl::Status cursor_sent =
        co_await WriteDataFrame(stream, DataFrameKind::kCursor, cursor_payload);
    if (!cursor_sent.ok()) {
      cleanup();
      co_return cursor_sent;
    }
    cleanup();
    co_return co_await RunMasterFlowBacklog(
        stream, backlog_start_lsn, 0);
  }

  Task<absl::Status> RunMasterFlowBacklog(TcpStream& stream,
                                          std::uint64_t next_lsn,
                                          std::uint32_t fragment_index) {
    storage::ReplicationLogCursor cursor{.lsn_ = next_lsn,
                                         .fragment_index_ = fragment_index};
    while (stream.IsOpen()) {
      auto batch = co_await storage_->ReadReplicationLog(
          cursor, 4 * 1024 * 1024, 128);
      if (!batch.ok()) co_return batch.status();
      for (const auto& frame : batch->frames_) {
        std::string payload;
        payload.reserve(12 + frame.payload_.size());
        PutU64(payload, frame.header_.lsn_);
        PutU32(payload, frame.header_.fragment_index_);
        payload.append(frame.payload_);
        absl::Status sent = co_await WriteDataFrame(
            stream, DataFrameKind::kCommand, payload);
        if (!sent.ok()) co_return sent;
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
            acknowledged_lsn != frame.header_.lsn_) {
          co_return absl::InvalidArgumentError("malformed replication command ACK");
        }
        cursor = storage::ReplicationLogCursor{
            .lsn_ = frame.header_.lsn_,
            .fragment_index_ = frame.header_.fragment_index_ + 1};
        if ((frame.header_.flags_ &
             static_cast<std::uint8_t>(storage::ReplicationFrameFlag::kLast)) !=
            0) {
          cursor = storage::ReplicationLogCursor{
              .lsn_ = frame.header_.lsn_ + 1, .fragment_index_ = 0};
        }
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
    const std::uint64_t session_id =
        next_master_session_id_.fetch_add(1, std::memory_order_relaxed);
    auto session =
        std::make_shared<MasterSession>(session_id, storage_->worker_count());
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

    const auto deadline = std::chrono::steady_clock::now() + kHandshakeTimeout;
    while (!session->cancelled() &&
           session->connected_flows() != session->worker_count() &&
           std::chrono::steady_clock::now() < deadline) {
      absl::Status slept = co_await celer::SleepFor(
          *celer::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!slept.ok()) {
        RemoveMasterSession(session);
        co_return slept;
      }
    }
    if (session->cancelled() ||
        session->connected_flows() != session->worker_count()) {
      RemoveMasterSession(session);
      co_return absl::DeadlineExceededError(
          "replica did not open every source-worker flow");
    }
    sent = co_await WriteText(stream, "+KLONLINE\r\n");
    if (!sent.ok()) {
      RemoveMasterSession(session);
      co_return sent;
    }
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
    absl::Status sent = co_await WriteText(
        stream, absl::StrCat("+KLFLOW ", session_id, " ", flow_id, " ",
                             continue_mode ? "CONTINUE" : "FULL", "\r\n"));
    if (!sent.ok()) {
      session->ClearFlow(flow_id, stream.NativeFd());
      session->Cancel();
      co_return sent;
    }
    absl::Status waited = continue_mode
                              ? co_await RunMasterFlowBacklog(
                                    stream, next_lsn, fragment_index)
                              : co_await RunMasterFlowData(stream, flow_id);
    if (!waited.ok()) {
      spdlog::warn("replication source flow {} ended: {}", flow_id,
                   waited.message());
    }
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
  std::atomic<bool> replication_fault_drop_used_{false};

  const std::string replid_;
  std::atomic<std::uint64_t> next_master_session_id_{1};
  std::mutex master_mutex_;
  std::unordered_map<std::uint64_t, std::shared_ptr<MasterSession>>
      master_sessions_;
};

ReplicationManager::ReplicationManager(
    storage::StorageEngine* storage, ReplicationOptions options,
    std::optional<ReplicaOfConfig> initial_upstream)
    : impl_(std::make_unique<Impl>(storage, std::move(initial_upstream))),
      options_(options) {}

ReplicationManager::~ReplicationManager() = default;

void ReplicationManager::StorageReady(celer::Worker& worker) {
  impl_->StorageReady(worker);
}

Task<absl::Status> ReplicationManager::SetUpstream(
    std::optional<ReplicaOfConfig> upstream) {
  co_return co_await impl_->SetUpstream(std::move(upstream));
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
