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

#include "replication_internal.h"

namespace lavik {
namespace replication_internal {

std::atomic<unsigned> recovery_resolvers_in_flight{0};

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

std::uint64_t RecoveryUnixMs() {
  const auto value = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
  return value > 0 ? static_cast<std::uint64_t>(value)
                   : std::numeric_limits<std::uint64_t>::max();
}

std::shared_ptr<CanonicalReceiveCharge> TryReserveCanonical(
    const std::shared_ptr<std::atomic<std::size_t>>& budget,
    std::size_t bytes) {
  constexpr std::size_t kCanonicalReceiveBytes = 8 * 1024 * 1024;
  if (bytes > kCanonicalReceiveBytes - sizeof(CanonicalReceiveCharge) - 64)
    return nullptr;
  bytes += sizeof(CanonicalReceiveCharge) + 64;
  auto used = budget->load(std::memory_order_relaxed);
  do {
    if (used > kCanonicalReceiveBytes - bytes) return nullptr;
  } while (!budget->compare_exchange_weak(used, used + bytes,
                                          std::memory_order_relaxed));
  auto reservation = TryReserveMemory(bytes);
  if (!reservation.has_value()) {
    budget->fetch_sub(bytes, std::memory_order_relaxed);
    return nullptr;
  }
  auto owner = std::make_shared<CanonicalReceiveCharge>();
  owner->budget_ = budget;
  owner->bytes_ = bytes;
  owner->memory_.Adopt(&*reservation, bytes);
  return owner;
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

std::string EncodeAppliedVector(std::span<const std::uint64_t> next_lsns) {
  std::string result;
  for (std::uint64_t next_lsn : next_lsns) {
    if (!result.empty()) result.push_back(',');
    absl::StrAppend(&result, next_lsn);
  }
  return result.empty() ? "?" : result;
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

bool IsPopulationGroupToken(std::string_view value) {
  return !value.empty() && value.size() <= 128 && value.size() % 2 == 0 &&
         std::all_of(value.begin(), value.end(), [](unsigned char digit) {
           return (digit >= '0' && digit <= '9') ||
                  (digit >= 'a' && digit <= 'f');
         });
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

bool SameFailoverActionExceptAuthorization(
    const DesiredClusterFailoverAction& lhs,
    const DesiredClusterFailoverAction& rhs) {
  DesiredClusterFailoverAction lhs_copy = lhs;
  DesiredClusterFailoverAction rhs_copy = rhs;
  lhs_copy.transition_revision_ = rhs_copy.transition_revision_ = 0;
  lhs_copy.authorized_revision_.reset();
  rhs_copy.authorized_revision_.reset();
  lhs_copy.recovery_deadline_unix_ms_.reset();
  rhs_copy.recovery_deadline_unix_ms_.reset();
  return lhs_copy == rhs_copy;
}

bool IsReplicationId(std::string_view value) {
  return value.size() == 40 &&
         std::all_of(value.begin(), value.end(), [](unsigned char digit) {
           return (digit >= '0' && digit <= '9') ||
                  (digit >= 'a' && digit <= 'f');
         });
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

Task<absl::StatusOr<TcpStream>> ConnectTcp(
    std::string_view host, std::uint16_t port,
    const std::shared_ptr<bycorf::TlsContext>& tls_context, SocketSet* sockets,
    bool cancellable_dns) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  addrinfo* addresses = nullptr;
  const std::string service = std::to_string(port);
  if (cancellable_dns) {
    auto resolved = co_await ResolveRecoveryAddress(host, port, sockets);
    if (!resolved.ok()) co_return resolved.status();
    addresses = std::exchange((*resolved)->addresses_, nullptr);
  } else {
    const int resolved = ::getaddrinfo(std::string(host).c_str(),
                                       service.c_str(), &hints, &addresses);
    if (resolved != 0)
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
        const int polled = ::poll(&pending, 1, 0);
        if (polled < 0 && errno == EINTR) continue;
        if (polled == 0) {
          const auto waited = co_await bycorf::SleepFor(
              *bycorf::ThisWorker().self_, std::chrono::milliseconds(5));
          if (!waited.ok()) {
            connect_result = -1;
            break;
          }
          continue;
        }
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

Task<absl::StatusOr<std::shared_ptr<RecoveryResolvedAddress>>>
ResolveRecoveryAddress(std::string_view host, std::uint16_t port,
                       SocketSet* sockets) {
  if (host.empty() || host.size() > 255)
    co_return absl::InvalidArgumentError("invalid recovery donor host");
  auto resolved = std::make_shared<RecoveryResolvedAddress>();
  resolved->host_ = host;
  resolved->service_ = std::to_string(port);
  resolved->hints_.ai_family = AF_UNSPEC;
  resolved->hints_.ai_socktype = SOCK_STREAM;
  resolved->hints_.ai_protocol = IPPROTO_TCP;
  resolved->hints_.ai_flags = AI_NUMERICHOST;
  resolved->result_ =
      ::getaddrinfo(resolved->host_.c_str(), resolved->service_.c_str(),
                    &resolved->hints_, &resolved->addresses_);
  if (resolved->result_ == 0) co_return resolved;
  unsigned active =
      recovery_resolvers_in_flight.load(std::memory_order_relaxed);
  do {
    if (active >= 32)
      co_return absl::ResourceExhaustedError(
          "recovery resolver work bound reached");
  } while (!recovery_resolvers_in_flight.compare_exchange_weak(
      active, active + 1, std::memory_order_relaxed));
  resolved->hints_.ai_flags = 0;
  try {
    std::thread([resolved] {
      resolved->result_ =
          ::getaddrinfo(resolved->host_.c_str(), resolved->service_.c_str(),
                        &resolved->hints_, &resolved->addresses_);
      resolved->done_.store(true, std::memory_order_release);
      recovery_resolvers_in_flight.fetch_sub(1, std::memory_order_relaxed);
    }).detach();
  } catch (const std::system_error&) {
    recovery_resolvers_in_flight.fetch_sub(1, std::memory_order_relaxed);
    co_return absl::ResourceExhaustedError(
        "cannot start recovery donor resolver");
  }
  while (!resolved->done_.load(std::memory_order_acquire)) {
    if (sockets->cancelled())
      co_return absl::CancelledError("recovery donor resolution cancelled");
    auto waited = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                            std::chrono::milliseconds(1));
    if (!waited.ok()) co_return waited;
  }
  if (resolved->result_ != 0)
    co_return absl::UnavailableError("cannot resolve recovery donor");
  co_return resolved;
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

bool HasCommaFlag(std::string_view flags, std::string_view wanted) {
  while (!flags.empty()) {
    const std::size_t comma = flags.find(',');
    if (flags.substr(0, comma) == wanted) return true;
    if (comma == std::string_view::npos) break;
    flags.remove_prefix(comma + 1);
  }
  return false;
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

absl::Status AppendDataFrame(std::string* output, DataFrameKind kind,
                             std::string_view payload) {
  absl::Status header = AppendDataFrameHeader(output, kind, payload.size(),
                                              DataFrameCrc32c(payload));
  if (!header.ok()) return header;
  output->append(payload);
  return absl::OkStatus();
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

std::uint32_t DataFrameCrc32c(std::string_view first,
                              std::string_view second) noexcept {
  absl::crc32c_t crc = absl::ComputeCrc32c(first);
  crc = absl::ExtendCrc32c(crc, second);
  return static_cast<std::uint32_t>(crc);
}

Task<absl::Status> WriteText(TcpStream& stream, const char* text) {
  return WriteText(stream, std::string_view(text));
}

Task<absl::Status> WriteText(TcpStream& stream, std::string_view text) {
  return stream.WriteAll(std::span<const std::byte>(
      reinterpret_cast<const std::byte*>(text.data()), text.size()));
}

std::string EncodeRespCommand(std::span<const std::string> args) {
  std::string encoded = absl::StrCat("*", args.size(), "\r\n");
  for (const std::string& arg : args) {
    absl::StrAppend(&encoded, "$", arg.size(), "\r\n", arg, "\r\n");
  }
  return encoded;
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

std::size_t EncodedRecordBytes(const SnapshotRecord& record) {
  return 1 + 1 + 1 + 8 + 8 + 8 + 8 + 4 + 4 + 4 + 4 + record.key_.size() +
         record.value_.size();
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

absl::Status ReplicationMemoryExhausted(std::string_view operation) {
  RecordMemoryRejection();
  return absl::ResourceExhaustedError(
      absl::StrCat("insufficient memory for ", operation));
}

void PutString(std::string& output, std::string_view value) {
  output.append(value.data(), value.size());
}

void PutU64(std::string& output, std::uint64_t value) {
  for (unsigned i = 0; i < 8; ++i) PutU8(output, value >> (i * 8));
}

void PutU32(std::string& output, std::uint32_t value) {
  for (unsigned i = 0; i < 4; ++i) PutU8(output, value >> (i * 8));
}

void PutU16(std::string& output, std::uint16_t value) {
  PutU8(output, static_cast<std::uint8_t>(value));
  PutU8(output, static_cast<std::uint8_t>(value >> 8));
}

void PutU8(std::string& output, std::uint8_t value) {
  output.push_back(static_cast<char>(value));
}

#if LAVIK_FAULTS_ENABLED
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

#if LAVIK_FAULTS_ENABLED
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
#endif

std::uint64_t SecondsSince(std::uint64_t started_nanos) noexcept {
  const std::uint64_t now = SteadyNanos();
  return now > started_nanos ? (now - started_nanos) / 1'000'000'000 : 0;
}

std::uint64_t SteadyNanos() noexcept {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

bool IsLeaseAdmissionSuspended(const absl::Status& status) {
  return status.code() == absl::StatusCode::kUnavailable &&
         status.message() == kLeaseAdmissionSuspendedStatus;
}

}  // namespace replication_internal

ReplicationManager::ReplicationGroup::ReplicationGroup(
    storage::StorageEngine* storage,
    std::optional<ReplicaOfConfig> initial_upstream,
    const ReplicationOptions& options,
    std::atomic<std::uint64_t>* serving_generation)
    : storage_(storage),
      serving_generation_(serving_generation),
      meta_managed_(options.meta_managed_),
      upstream_(meta_managed_ ? std::nullopt : std::move(initial_upstream)),
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
      redis_psync_(meta_managed_ ? false : options.redis_psync_) {
  if (meta_managed_) {
    cluster_group_ =
        std::make_unique<lavik::ReplicationGroup>(node_id_, boot_id_);
  }
  const auto boot_bytes = std::span<const std::byte>(
      reinterpret_cast<const std::byte*>(boot_id_.data()), boot_id_.size());
  next_redis_full_sync_session_id_.store(storage::Crc64(boot_bytes),
                                         std::memory_order_relaxed);
  if (meta_managed_ && initial_upstream.has_value()) {
    // Server config rejects this combination, but ReplicationManager is also
    // a public embedding seam. Ignore the standalone source here so a direct
    // caller cannot bypass cluster replication policy.
    spdlog::warn("ignoring external initial upstream in Meta-managed mode");
  }
  const std::size_t minimum_blocks = storage_->worker_count();
  const std::size_t configured_blocks =
      options.backlog_size_bytes_ / storage::kStorageBlockBytes;
  backlog_size_bytes_.store(
      std::max(minimum_blocks, configured_blocks) * storage::kStorageBlockBytes,
      std::memory_order_relaxed);
  if (meta_managed_) {
    for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
      auto history = std::make_shared<ReplicationHistory>(
          BacklogCapacityForFlow(worker, backlog_size_bytes_.load()));
      retained_histories_.push_back(history);
      storage_->SetReplicationHistory(worker, std::move(history));
    }
    retained_reset_revisions_.resize(storage_->worker_count());
  }
  backlog_backpressure_.store(options.backlog_backpressure_,
                              std::memory_order_relaxed);
  publish_queue_bytes_per_worker_.store(options.publish_queue_bytes_per_worker_,
                                        std::memory_order_relaxed);
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
  if (upstream_.has_value() || meta_managed_) {
    StoreRole(ReplicationRole::kConnecting, std::memory_order_relaxed);
    role_epoch_.store(1, std::memory_order_relaxed);
  }
  // Every external upstream uses the Redis PSYNC connection path.
  initial_redis_connection_pending_ = upstream_.has_value();
  PublishUpstreamSnapshot();
}

auto ReplicationManager::ReplicationGroup::StorageReady(bycorf::Worker& worker)
    -> void {
  ready_workers_.fetch_add(1, std::memory_order_acq_rel);
  if (worker.id() == 0 && !ready_waiter_started_) {
    ready_waiter_started_ = true;
    worker.Spawn(WaitUntilStorageReady());
  }
}

auto ReplicationManager::ReplicationGroup::StartClusterRebuildDirective(
    ReplicaOfConfig upstream, RebuildDirective directive,
    PopulationManifest manifest)
    -> Task<absl::StatusOr<
        std::shared_ptr<detail::ClusterRebuildCompletionState>>> {
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(
        0,
        [this, upstream = std::move(upstream), directive = std::move(directive),
         manifest = std::move(manifest)]() mutable {
          return StartClusterRebuildDirective(
              std::move(upstream), std::move(directive), std::move(manifest));
        });
  }
  if (!meta_managed_ || cluster_group_ == nullptr) {
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

  absl::Status validated = cluster_group_->ValidateRebuild(directive, manifest);

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
    absl::Status revoked = co_await RevokeClusterRebuildSourceAuthorizations();
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
      std::optional<std::string> fail_stop = previous_session->FailStopReason();
      if (!stopped.ok() && !fail_stop.has_value()) {
        fail_stop =
            absl::StrCat("replica cancellation/join outcome is uncertain: ",
                         stopped.message());
      }
      if (stopped.ok() && !fail_stop.has_value() &&
          previous_session->session_id_ != 0) {
        absl::Status discarded =
            co_await storage_->AbortReplicaRoot(previous_session->session_id_);
        if (!discarded.ok()) {
          fail_stop = absl::StrCat("replica abort outcome is uncertain: ",
                                   discarded.message());
        }
      }
      if (fail_stop.has_value()) {
        (void)cluster_group_->FailStop(previous_context->directive_.identity_);
        LatchReplicationFailure(*fail_stop);
        co_return absl::FailedPreconditionError(absl::StrCat(
            "replication is failed-stopped until restart: ", *fail_stop));
      }
    }

    absl::Status retired =
        cluster_group_->InvalidateProof(previous_context->directive_.identity_);
    if (!retired.ok()) {
      const std::string reason = absl::StrCat(
          "superseded cluster proof could not be retired: ", retired.message());
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
                active_replica_session_ == nullptr && !upstream_.has_value() &&
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

auto ReplicationManager::ReplicationGroup::ApplyClusterRebuildDirective(
    ReplicaOfConfig upstream, RebuildDirective directive,
    PopulationManifest manifest) -> Task<absl::Status> {
  auto started = co_await StartClusterRebuildDirective(
      std::move(upstream), std::move(directive), std::move(manifest));
  if (!started.ok()) co_return started.status();
  co_return co_await (*started)->Await();
}

auto ReplicationManager::ReplicationGroup::StartEmptyPopulationInitialization(
    RebuildIdentity identity, PopulationManifest manifest)
    -> Task<absl::StatusOr<
        std::shared_ptr<detail::ClusterRebuildCompletionState>>> {
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(
        0, [this, identity = std::move(identity),
            manifest = std::move(manifest)]() mutable {
          return StartEmptyPopulationInitialization(std::move(identity),
                                                    std::move(manifest));
        });
  }
  if (!meta_managed_ || cluster_group_ == nullptr) {
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
  bycorf::ThisWorker().self_->Spawn(RunEmptyPopulationInitialization(context));
  co_return context->completion_;
}

#if LAVIK_FAULTS_ENABLED
auto ReplicationManager::ReplicationGroup::
    SeedReadyPromotionCandidateForFaultTest(
        const ClusterPromotionPrepareDirective& directive, bool recovered)
        -> Task<absl::Status> {
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
  storage_ready = co_await GlobalFunctionCatalog().ReplaceFromLibraryCodes({});
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
  if (recovered) {
    // Exercise the production verified-recovery boundary without repeating
    // the storage certificate/crash protocol covered by process tests.
    co_return co_await InstallRecoveredPopulation(
        {std::move(ready_identity), directive.required_applied_next_lsns_});
  }
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
auto ReplicationManager::ReplicationGroup::WaitAtPromotionFaultBarrier(
    const std::shared_ptr<ClusterPromotionPrepareContext>& context,
    const char* signal_variable) -> Task<absl::StatusOr<bool>> {
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

auto ReplicationManager::ReplicationGroup::InstallPromotionHistoryBridge(
    const ClusterPromotionPrepareDirective& directive,
    const ReadyToken& population, const ClusterPromotionPrepared& prepared)
    -> absl::Status {
  AssertStateOwner();
  auto bridge = std::make_shared<detail::NativeHistoryBridge>();
  bridge->id_ = NewReplicationId();
  bridge->group_id_ = directive.identity_.group_id_;
  bridge->parent_ = {
      population.identity().term_,
      directive.identity_.source_node_id_,
      directive.identity_.source_assignment_id_,
      directive.identity_.source_boot_id_,
      prepared.parent_history_id_,
      static_cast<unsigned>(prepared.frozen_applied_next_lsns_.size())};
  if (cluster_failover_action_ != nullptr &&
      !cluster_failover_action_->desired_.operator_recovery_ &&
      HexBytes(cluster_failover_action_->desired_.action_id_) ==
          directive.identity_.attempt_id_) {
    bridge->parent_ = cluster_failover_action_->desired_.domain_;
  }
  if (bridge->parent_.source_history_id_ != prepared.parent_history_id_ ||
      bridge->parent_.flow_count_ !=
          prepared.frozen_applied_next_lsns_.size()) {
    return absl::FailedPreconditionError(
        "promotion parent descriptor differs from its frozen Applied domain");
  }
  bridge->child_ = {
      directive.identity_.term_,          node_id_,
      directive.identity_.assignment_id_, boot_id_,
      prepared.child_history_id_,         storage_->worker_count()};
  bridge->manifest_revision_ = directive.identity_.manifest_revision_;
  bridge->manifest_id_ = directive.identity_.manifest_id_;
  bridge->partition_replication_epoch_ =
      directive.identity_.partition_replication_epoch_;
  bridge->promotion_ = prepared;
  // PreparePromotion just created every child publisher while serving and
  // expiration remain fenced. Every child LSN starts at one; this boundary
  // is immutable even after Owner writes advance its current tail.
  bridge->child_origin_.assign(storage_->worker_count(), 1);
  history_bridge_ = std::move(bridge);
  return absl::OkStatus();
}

auto ReplicationManager::ReplicationGroup::AdoptLocalOwnerHistory()
    -> absl::Status {
  AssertStateOwner();
  if (history_bridge_ == nullptr || cluster_rebuild_ == nullptr ||
      !cluster_rebuild_->ready_token_.has_value() ||
      !cluster_rebuild_->manifest_.has_value()) {
    return absl::FailedPreconditionError(
        "child activation has no parent boundary descriptor");
  }
  const auto& bridge = *history_bridge_;
  auto revision =
      cluster_group_->NextDirectiveRevision(bridge.child_.source_group_term_);
  if (!revision.ok()) return revision.status();
  RebuildDirective child = cluster_rebuild_->directive_;
  auto& identity = child.identity_;
  identity.term_ = bridge.child_.source_group_term_;
  identity.directive_revision_ = *revision;
  identity.authority_id_ = "history-switch:" + bridge.id_;
  identity.source_node_id_ = node_id_;
  identity.source_assignment_id_ = bridge.child_.source_assignment_id_;
  identity.source_boot_id_ = boot_id_;
  identity.source_history_id_ = bridge.child_.source_history_id_;
  identity.target_history_id_.clear();
  identity.operation_id_ = identity.directive_id_ = identity.attempt_id_ =
      "history-switch:" + bridge.id_;
  child.flow_count_ = bridge.child_.flow_count_;
  child.safe_source_active_ = true;
  auto adopted = std::make_shared<ClusterRebuildContext>(
      child, *cluster_rebuild_->manifest_, *cluster_rebuild_->ready_token_);
  // Operator recovery creates a fresh local base in the target term and
  // has no comparable historical parent. Bind that complete base to its
  // first publisher; ordinary promotion crosses into a successor term.
  auto ready =
      bridge.parent_.source_group_term_ == bridge.child_.source_group_term_
          ? cluster_group_->BindLocalSourceHistory(
                *cluster_rebuild_->ready_token_, child,
                *cluster_rebuild_->manifest_, bridge.child_origin_)
          : cluster_group_->SwitchHistory(
                *cluster_rebuild_->ready_token_, child,
                *cluster_rebuild_->manifest_,
                bridge.promotion_.frozen_applied_next_lsns_,
                bridge.promotion_.frozen_applied_next_lsns_,
                bridge.child_origin_);
  if (!ready.ok()) return ready.status();
  adopted->ready_token_ = std::move(*ready);
  cluster_rebuild_ = std::move(adopted);
  // Native Owner progress is sampled from its live source logs. Retaining a
  // fixed replica frontier here would advertise a stale child cursor.
  applied_frontier_.reset();
  return absl::OkStatus();
}

auto ReplicationManager::ReplicationGroup::RunClusterPromotionPrepare(
    std::shared_ptr<ClusterPromotionPrepareContext> context,
    std::shared_ptr<ClusterRebuildContext> population,
    std::shared_ptr<detail::ReplicaAppliedFrontier> frontier,
    std::shared_ptr<ReplicaSession> session, bool native_population)
    -> Task<absl::Status> {
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
        context, "LAVIK_REPLICATION_PROMOTION_PRE_DURABILITY_BARRIER_ACK_PATH");
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
  if (frozen.size() != context->directive_.required_applied_next_lsns_.size()) {
    co_return fail_stop(absl::FailedPreconditionError(
                            "joined candidate frontier changed flow layout"),
                        "frontier freeze");
  }
  for (std::size_t flow = 0; flow < frozen.size(); ++flow) {
    if (frozen[flow] < context->directive_.required_applied_next_lsns_[flow]) {
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
      .storage_accumulator_ =
          absl::StrCat("failover:", context->directive_.identity_.operation_id_,
                       ":", context->directive_.identity_.directive_id_, ":",
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
    context_changed =
        cluster_promotion_prepare_ != context || cluster_rebuild_ != population;
    if (!context_changed) {
      auto installed = InstallPromotionHistoryBridge(
          context->directive_, *population->ready_token_, *prepared);
      if (!installed.ok())
        co_return fail_stop(installed, "parent descriptor installation");
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

auto ReplicationManager::ReplicationGroup::
    StartClusterPromotionPrepareDirective(
        ClusterPromotionPrepareDirective directive)
        -> Task<absl::StatusOr<
            std::shared_ptr<detail::ClusterPromotionPrepareCompletionState>>> {
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(
        0, [this, directive = std::move(directive)]() mutable {
          return StartClusterPromotionPrepareDirective(std::move(directive));
        });
  }
  if (!meta_managed_ || cluster_group_ == nullptr) {
    co_return absl::FailedPreconditionError(
        "promotion prepare requires Meta-managed population mode");
  }
  const RebuildIdentity& identity = directive.identity_;
  if (identity.group_id_.empty() || identity.assignment_id_.empty() ||
      identity.term_ == 0 || identity.directive_revision_ == 0 ||
      identity.authority_id_.empty() || identity.source_node_id_.empty() ||
      identity.source_assignment_id_.empty() ||
      identity.source_boot_id_.empty() || identity.source_history_id_.empty() ||
      identity.target_node_id_ != node_id_ ||
      identity.target_boot_id_ != boot_id_ || identity.operation_id_.empty() ||
      identity.directive_id_.empty() || identity.attempt_id_.empty() ||
      identity.manifest_revision_ == 0 ||
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

    context =
        std::make_shared<ClusterPromotionPrepareContext>(std::move(directive));
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

auto ReplicationManager::ReplicationGroup::
    StartNativeClusterFailoverPromotionPrepare(
        ClusterPromotionPrepareDirective directive)
        -> Task<absl::StatusOr<
            std::shared_ptr<detail::ClusterPromotionPrepareCompletionState>>> {
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
  if (replica_reconfiguration_running_ || active_replica_session_ != nullptr ||
      upstream_.has_value() ||
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

auto ReplicationManager::ReplicationGroup::ValidateClusterSourcePause(
    const DesiredClusterSourcePause& desired) const -> absl::Status {
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

auto ReplicationManager::ReplicationGroup::SourcePausePopulationMatches(
    const DesiredClusterSourcePause& desired) const -> bool {
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

auto ReplicationManager::ReplicationGroup::CaptureClusterSourcePause(
    const std::shared_ptr<ClusterSourcePauseContext>& context)
    -> Task<absl::Status> {
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

auto ReplicationManager::ReplicationGroup::ReconcileClusterSourcePause(
    std::optional<DesiredClusterSourcePause> desired) -> Task<absl::Status> {
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(
        0, [this, desired = std::move(desired)]() mutable {
          return ReconcileClusterSourcePause(std::move(desired));
        });
  }
  if (!meta_managed_ || cluster_group_ == nullptr) {
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
  if (absl::Status valid = ValidateClusterSourcePause(*desired); !valid.ok()) {
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

  auto next = std::make_shared<ClusterSourcePauseContext>(std::move(*desired));
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

auto ReplicationManager::ReplicationGroup::cluster_source_pause_status() const
    -> Task<ClusterSourcePauseStatus> {
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

auto ReplicationManager::ReplicationGroup::FailoverPopulationMatches(
    const DesiredClusterFailoverAction& desired) const -> bool {
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

auto ReplicationManager::ReplicationGroup::FailoverReplicaDomainMatches(
    const DesiredClusterFailoverAction& desired) const -> bool {
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

auto ReplicationManager::ReplicationGroup::IsNativeSelfOriginDomain(
    const DesiredClusterFailoverAction& desired) const -> bool {
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

auto ReplicationManager::ReplicationGroup::ReadyPopulationIsSuppressed(
    const DesiredClusterFailoverAction& failed) const -> bool {
  AssertStateOwner();
  if (!FailoverPopulationMatches(failed)) return false;
  if (failed.operator_recovery_) return operator_recovery_active_;
  const ReadyToken& ready = *cluster_rebuild_->ready_token_;
  const RebuildIdentity& identity = ready.identity();
  const bool replica_domain =
      identity.term_ == failed.domain_.source_group_term_ &&
      identity.source_node_id_ == failed.domain_.source_node_id_ &&
      identity.source_assignment_id_ == failed.domain_.source_assignment_id_ &&
      identity.source_boot_id_ == failed.domain_.source_boot_id_ &&
      identity.source_history_id_ == failed.domain_.source_history_id_ &&
      ready.cut_vector().size() == failed.domain_.flow_count_;
  const bool self_origin_domain = IsNativeSelfOriginDomain(failed);
  return replica_domain || self_origin_domain;
}

auto ReplicationManager::ReplicationGroup::PublishFailoverActionFailure(
    const std::shared_ptr<ClusterFailoverActionContext>& context,
    std::string failure_class, std::string failure_detail) -> void {
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

auto ReplicationManager::ReplicationGroup::FailoverActionWatchdog() const
    -> std::chrono::milliseconds {
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

auto ReplicationManager::ReplicationGroup::RequestFailoverPromotionCancellation(
    const std::shared_ptr<ClusterFailoverActionContext>& action) -> void {
  AssertStateOwner();
  if (!action->prepare_directive_.has_value() ||
      cluster_promotion_prepare_ == nullptr ||
      cluster_promotion_prepare_->directive_ != *action->prepare_directive_ ||
      cluster_promotion_prepare_->durability_mutation_started_) {
    return;
  }
  cluster_promotion_prepare_->cancellation_requested_ = true;
}

auto ReplicationManager::ReplicationGroup::WaitForFailoverActionRetry(
    const std::shared_ptr<ClusterFailoverActionContext>& context,
    std::chrono::steady_clock::time_point watchdog_deadline)
    -> Task<absl::Status> {
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

auto ReplicationManager::ReplicationGroup::RecoveryHandshake(
    const DesiredClusterFailoverAction& action,
    const ClusterReplicationMember& donor) const -> std::vector<std::string> {
  return {"LVRECOVER",
          "1",
          action.group_id_,
          std::to_string(action.target_term_),
          HexBytes(action.transition_id_),
          HexBytes(action.action_id_),
          action.candidate_node_id_,
          action.candidate_assignment_id_,
          action.candidate_boot_id_,
          donor.node_id_,
          donor.assignment_id_,
          std::to_string(action.domain_.source_group_term_),
          action.domain_.source_node_id_,
          action.domain_.source_assignment_id_,
          action.domain_.source_boot_id_,
          action.domain_.source_history_id_,
          std::to_string(action.domain_.flow_count_),
          std::to_string(action.manifest_revision_),
          action.manifest_id_.Hex(),
          std::to_string(action.partition_replication_epoch_),
          std::to_string(*action.recovery_deadline_unix_ms_)};
}

auto ReplicationManager::ReplicationGroup::WriteRecoveryFrame(
    TcpStream& stream, std::string_view payload) -> Task<absl::Status> {
  const std::string header = absl::StrCat("+LVR ", payload.size(), " ",
                                          DataFrameCrc32c(payload), "\r\n");
  auto status = co_await WriteText(stream, header);
  if (status.ok()) status = co_await WriteText(stream, payload);
  co_return status;
}

auto ReplicationManager::ReplicationGroup::ReadRecoveryFrame(TcpStream& stream,
                                                             std::size_t bound)
    -> Task<absl::StatusOr<std::string>> {
  auto line = co_await ReadLine(stream);
  if (!line.ok()) co_return line.status();
  const auto words = SplitWords(*line);
  std::uint64_t size = 0;
  std::uint32_t crc = 0;
  if (words.size() != 3 || words[0] != "+LVR" ||
      !ParseUnsigned(words[1], &size) || size == 0 || size > bound ||
      !ParseUnsigned(words[2], &crc)) {
    co_return absl::InvalidArgumentError(
        "invalid recovery frame header or byte bound");
  }
  auto payload = co_await ReadExact(stream, size);
  if (!payload.ok()) co_return payload.status();
  if (DataFrameCrc32c(*payload) != crc)
    co_return absl::DataLossError("recovery frame checksum mismatch");
  co_return std::move(*payload);
}

auto ReplicationManager::ReplicationGroup::WatchRecoveryDeadline(
    std::shared_ptr<ClusterRecoveryContext> scope) -> Task<absl::Status> {
  while (!scope->sockets_.cancelled() && !scope->Expired()) {
    auto waited = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                            std::chrono::milliseconds(1));
    if (!waited.ok()) break;
  }
  scope->sockets_.Cancel();
  scope->watcher_finished_ = true;
  co_return absl::OkStatus();
}

auto ReplicationManager::ReplicationGroup::ReconcileClusterRecovery(
    std::optional<DesiredClusterRecovery> desired) -> Task<absl::Status> {
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(
        0, [this, desired = std::move(desired)]() mutable {
          return ReconcileClusterRecovery(std::move(desired));
        });
  }
  AssertStateOwner();
  if (!meta_managed_)
    co_return absl::FailedPreconditionError(
        "recovery requires Meta-managed replication");
  if (desired.has_value()) {
    const auto& action = desired->action_;
    const auto member = [&](std::string_view node,
                            std::string_view assignment) {
      return std::ranges::any_of(desired->members_, [&](const auto& peer) {
        return peer.member_.node_id_ == node &&
               peer.member_.assignment_id_ == assignment;
      });
    };
    if (desired->local_node_id_ != node_id_ ||
        desired->local_boot_id_ != boot_id_ ||
        !member(node_id_, desired->local_assignment_id_) ||
        !member(action.candidate_node_id_, action.candidate_assignment_id_) ||
        action.mode_ != ClusterFailoverMode::kUncontrolled ||
        action.authorized_revision_.has_value() ||
        action.committed_group_term_ != action.target_term_ ||
        action.committed_grant_active_ ||
        !action.recovery_deadline_unix_ms_.has_value() ||
        *action.recovery_deadline_unix_ms_ == 0 ||
        *action.recovery_deadline_unix_ms_ >
            std::numeric_limits<std::int64_t>::max() ||
        action.domain_.flow_count_ == 0 || action.domain_.flow_count_ > 1024 ||
        IsZeroBytes(action.transition_id_) || IsZeroBytes(action.action_id_)) {
      co_return absl::FailedPreconditionError(
          "recovery scope does not bind a grantless current action and "
          "membership");
    }
  }
  if (cluster_recovery_ != nullptr && desired.has_value() &&
      cluster_recovery_->desired_ == *desired)
    co_return absl::OkStatus();
  const auto previous = std::exchange(cluster_recovery_, nullptr);
  if (previous != nullptr) previous->sockets_.Cancel();
  // Withdraw old scope before joining. Late donor responses cannot enter a
  // replacement population, while an already admitted complete apply joins.
  const auto action = cluster_failover_action_;
  while ((previous != nullptr &&
          (previous->active_exports_ != 0 || !previous->watcher_finished_)) ||
         (action != nullptr && action->recovery_running_)) {
    auto waited = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                            std::chrono::milliseconds(1));
    if (!waited.ok()) co_return waited;
  }
  if (action != nullptr && !action->recovery_.has_value())
    action->recovery_started_ = false;
  if (desired.has_value() && !cluster_control_stopping_) {
    cluster_recovery_ = std::make_shared<ClusterRecoveryContext>(
        std::move(*desired), &outbound_sockets_);
    bycorf::ThisWorker().self_->Spawn(WatchRecoveryDeadline(cluster_recovery_));
  }
  co_return absl::OkStatus();
}

auto ReplicationManager::ReplicationGroup::ResetRetainedHistory(
    std::string history, unsigned flow_count) -> Task<absl::Status> {
  AssertStateOwner();
  const auto revision = ++retained_reset_revision_;
  for (unsigned worker = 0; worker < retained_histories_.size(); ++worker) {
    auto status = co_await bycorf::SubmitTo(
        worker, [this, worker, revision, history, flow_count] {
          auto& installed = retained_reset_revisions_[worker];
          if (revision < installed)
            return absl::CancelledError("retained history reset superseded");
          auto reset = retained_histories_[worker]->Reset(history, flow_count);
          if (reset.ok()) installed = revision;
          return reset;
        });
    if (!status.ok()) co_return status;
    if (revision != retained_reset_revision_)
      co_return absl::CancelledError("retained history reset superseded");
  }
  co_return absl::OkStatus();
}

auto ReplicationManager::ReplicationGroup::RetainedCoverage(std::string history)
    -> Task<std::vector<std::vector<NativeHistoryRange>>> {
  AssertStateOwner();
  const auto revision = retained_reset_revision_;
  struct Sample {
    RetainedMemoryCharge charge_;
    std::vector<std::vector<NativeHistoryRange>> ranges_;
  };
  std::vector<RetainedMemoryCharge> charges;
  std::vector<std::vector<NativeHistoryRange>> coverage;
  for (unsigned worker = 0; worker < retained_histories_.size(); ++worker) {
    // Alternating apply owners can fragment local coverage into one range
    // per record. Reserve a conservative copy/merge budget on that owner:
    // Four times the block capacity covers both the sampled ranges and
    // vector growth while merging, even for tiny transaction participants.
    // The fixed term covers both origin-flow vector headers. Admission
    // failure simply omits optional coverage. Keep the charge until merge
    // finishes; the bounded wire result uses the export's transport budget.
    auto local = co_await bycorf::SubmitTo(worker, [this, worker, history] {
      constexpr auto kHeaders =
          2 * 1024 * sizeof(std::vector<NativeHistoryRange>);
      const auto& cache = retained_histories_[worker];
      const auto bytes = cache->secondary_bytes();
      if (bytes > (std::numeric_limits<std::size_t>::max() - kHeaders) / 4)
        return Sample{};
      auto reservation = TryReserveMemory(4 * bytes + kHeaders);
      if (!reservation.has_value()) return Sample{};
      Sample sample;
      sample.charge_.Adopt(&*reservation, 4 * bytes + kHeaders);
      sample.ranges_ =
          cache->Coverage(history, std::numeric_limits<std::size_t>::max());
      return sample;
    });
    if (revision != retained_reset_revision_) co_return decltype(coverage){};
    if (coverage.size() < local.ranges_.size())
      coverage.resize(local.ranges_.size());
    for (std::size_t flow = 0; flow < local.ranges_.size(); ++flow)
      coverage[flow].insert(coverage[flow].end(), local.ranges_[flow].begin(),
                            local.ranges_[flow].end());
    charges.push_back(std::move(local.charge_));
  }
  co_return detail::MergeWorkerHistoryCoverage(std::move(coverage));
}

auto ReplicationManager::ReplicationGroup::SendRetainedEffect(
    TcpStream& stream, std::string_view history,
    const detail::NativeRecoveryAdvertisement& report, unsigned flow,
    std::uint64_t lsn, std::function<bool()> current) -> Task<absl::Status> {
  if (!current()) co_return absl::CancelledError("retained export was revoked");
  absl::StatusOr<std::vector<NativeHistoryRecordInfo>> effect =
      absl::NotFoundError("retained history has a gap");
  unsigned owner = 0;
  // Ordinary events live with their flow; complete transactions live with
  // their apply owner. Discover that owner once, then fetch every chunk
  // there.
  for (unsigned probe = 0; probe < retained_histories_.size(); ++probe) {
    owner = (flow + probe) % retained_histories_.size();
    effect =
        co_await bycorf::SubmitTo(owner, [this, owner, history, flow, lsn] {
          return retained_histories_[owner]->DescribeEffect(history, flow, lsn);
        });
    if (!current())
      co_return absl::CancelledError("retained export was revoked");
    if (effect.ok()) break;
  }
  if (!effect.ok()) co_return effect.status();
  for (const auto& record : *effect) {
    if (!detail::RecoveryCovers(report, record.flow_id_, record.lsn_))
      co_return absl::NotFoundError(
          "recovery effect has unavailable participants");
  }
  auto manifest = detail::EncodeRecoveryEffectManifest(*effect);
  if (!manifest.ok()) co_return manifest.status();
  auto status = co_await WriteRecoveryFrame(stream, *manifest);
  if (!status.ok()) co_return status;
  for (const auto& record : *effect) {
    for (std::size_t offset = 0; offset < record.bytes_;) {
      if (!current())
        co_return absl::CancelledError("recovery export was revoked");
      auto chunk = co_await bycorf::SubmitTo(
          owner, [this, owner, history, record, offset] {
            return retained_histories_[owner]->Read(
                history, record.flow_id_, record.lsn_, offset, 64 * 1024);
          });
      if (!current())
        co_return absl::CancelledError("retained export was revoked");
      if (!chunk.ok()) co_return chunk.status();
      if (chunk->total_bytes_ != record.bytes_)
        co_return absl::DataLossError("retained event changed during copy-out");
      status = co_await WriteRecoveryFrame(stream, chunk->bytes_);
      if (!status.ok()) co_return status;
      offset += chunk->bytes_.size();
    }
  }
  co_return absl::OkStatus();
}

auto ReplicationManager::ReplicationGroup::ParentHandshake(
    const DesiredClusterUpstream& desired,
    const ClusterFailoverCompatibilityDomain& parent,
    std::span<const std::uint64_t> cursor) const -> std::vector<std::string> {
  return {"LVPARENT",
          "1",
          desired.group_id_,
          std::to_string(desired.group_term_),
          desired.local_node_id_,
          desired.local_assignment_id_,
          desired.local_boot_id_,
          desired.owner_node_id_,
          desired.owner_assignment_id_,
          std::to_string(desired.manifest_revision_),
          desired.manifest_id_.Hex(),
          std::to_string(desired.partition_replication_epoch_),
          std::to_string(parent.source_group_term_),
          parent.source_node_id_,
          parent.source_assignment_id_,
          parent.source_boot_id_,
          parent.source_history_id_,
          std::to_string(parent.flow_count_),
          EncodeAppliedVector(cursor)};
}

auto ReplicationManager::ReplicationGroup::CurrentPartialOwner(
    const std::shared_ptr<ClusterFollowOwnerContext>& relationship) const
    -> bool {
  AssertStateOwner();
  return relationship != nullptr && cluster_follow_owner_ == relationship &&
         relationship->desired_.local_node_id_ == node_id_ &&
         relationship->desired_.owner_node_id_ == node_id_ &&
         !cluster_control_stopping_ && !is_replica() && !is_loading() &&
         native_dataset_valid_.load(std::memory_order_acquire) &&
         ClusterFollowReadyPopulationMatches(relationship->desired_) &&
         source_authorizations_.LeaseAdmissionOpen(
             cluster::LeaseClockNow().time_since_epoch());
}

auto ReplicationManager::ReplicationGroup::WatchTimedSockets(
    std::shared_ptr<TimedSocketContext> transfer, std::function<bool()> current)
    -> Task<absl::Status> {
  while (!transfer->finished_ && !transfer->sockets_.cancelled() &&
         cluster::LeaseClockNow() < transfer->deadline_ && current()) {
    auto waited = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                            std::chrono::milliseconds(1));
    if (!waited.ok()) break;
  }
  transfer->sockets_.Cancel();
  transfer->watcher_finished_ = true;
  co_return absl::OkStatus();
}

auto ReplicationManager::ReplicationGroup::CancelPartialExports()
    -> Task<absl::Status> {
  AssertStateOwner();
  const auto exports = partial_exports_;
  for (const auto& transfer : exports) transfer->sockets_.Cancel();
  while (std::ranges::any_of(exports, [](const auto& transfer) {
    return !transfer->finished_ || !transfer->watcher_finished_;
  })) {
    auto waited = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                            std::chrono::milliseconds(1));
    if (!waited.ok()) co_return waited;
  }
  co_return absl::OkStatus();
}

auto ReplicationManager::ReplicationGroup::ChildOriginAvailable(
    const std::shared_ptr<const detail::NativeHistoryBridge>& bridge)
    -> Task<bool> {
  if (bridge == nullptr || bridge->child_.source_history_id_ != history_id_ ||
      bridge->child_origin_.size() != storage_->worker_count())
    co_return false;
  for (unsigned flow = 0; flow < storage_->worker_count(); ++flow) {
    const auto info = co_await bycorf::SubmitTo(
        flow, [this] { return storage_->LocalReplicationLogInfo(); });
    if (info.state_ != storage::ReplicationLogState::kActive ||
        info.floor_lsn_ > bridge->child_origin_[flow] ||
        info.tail_lsn_ == std::numeric_limits<std::uint64_t>::max() ||
        info.tail_lsn_ + 1 < bridge->child_origin_[flow])
      co_return false;
  }
  co_return history_bridge_ == bridge &&
      bridge->child_.source_history_id_ == history_id_;
}

auto ReplicationManager::ReplicationGroup::RunParentExport(
    TcpStream& stream, const std::vector<std::string>& args,
    const std::shared_ptr<TimedSocketContext>& transfer,
    const std::shared_ptr<ClusterFollowOwnerContext>& relationship)
    -> Task<absl::Status> {
  auto parent = ClusterFailoverCompatibilityDomain{};
  if (!ParseUnsigned(args[12], &parent.source_group_term_) ||
      parent.source_group_term_ == 0 ||
      !ParseUnsigned(args[17], &parent.flow_count_) ||
      parent.flow_count_ == 0 || parent.flow_count_ > 1024 ||
      !IsReplicationId(args[13]) || !IsReplicationId(args[15]) ||
      !IsReplicationId(args[16])) {
    co_return absl::InvalidArgumentError("invalid partial parent domain");
  }
  parent.source_node_id_ = args[13];
  parent.source_assignment_id_ = args[14];
  parent.source_boot_id_ = args[15];
  parent.source_history_id_ = args[16];
  auto cursor = DecodeAppliedVector(args[18]);
  if (!cursor.ok() || cursor->size() != parent.flow_count_)
    co_return absl::InvalidArgumentError(
        "partial parent cursor layout mismatch");
  const auto bridge = history_bridge_;
  auto coverage = co_await RetainedCoverage(parent.source_history_id_);
  const bool child_available = co_await ChildOriginAvailable(bridge);
  const auto current = [&] {
    return !transfer->sockets_.cancelled() &&
           cluster::LeaseClockNow() < transfer->deadline_ &&
           CurrentPartialOwner(relationship);
  };
  if (!current())
    co_return absl::CancelledError("partial Owner authorization changed");
  if (bridge == nullptr ||
      bridge->child_.source_group_term_ != relationship->desired_.group_term_ ||
      detail::PlanNativeReparent(*bridge, parent, *cursor, coverage,
                                 child_available) ==
          detail::NativeReparentPlan::kFull) {
    const auto full = absl::StrCat("-LVPARENTFULL ", node_id_, " ", boot_id_,
                                   " ", history_id_, "\r\n");
    co_return co_await WriteText(stream, full);
  }
  const auto& boundary = bridge->promotion_.frozen_applied_next_lsns_;
  if (coverage.empty()) coverage.resize(parent.flow_count_);
  for (unsigned flow = 0; flow < coverage.size(); ++flow) {
    for (auto& range : coverage[flow])
      range.end_lsn_ = std::min(range.end_lsn_, boundary[flow]);
    std::erase_if(coverage[flow], [](const auto& range) {
      return range.first_lsn_ >= range.end_lsn_;
    });
  }
  detail::NativeRecoveryAdvertisement report{boot_id_, boundary,
                                             std::move(coverage)};
  const auto hello = absl::StrCat(
      "+LVPARENT ", bridge->id_, " ", node_id_, " ", boot_id_, " ", history_id_,
      " ", parent.source_history_id_, " ", EncodeAppliedVector(boundary), " ",
      EncodeAppliedVector(bridge->child_origin_), "\r\n");
  auto status = co_await WriteText(stream, hello);
  if (!status.ok()) co_return status;
  auto encoded = detail::EncodeRecoveryAdvertisement(report);
  if (!encoded.ok()) co_return encoded.status();
  status = co_await WriteRecoveryFrame(stream, *encoded);
  if (!status.ok()) co_return status;
  while (current() && history_bridge_ == bridge) {
    auto line = co_await ReadLine(stream);
    if (!line.ok()) co_return line.status();
    const auto words = SplitWords(*line);
    if (words.size() == 2 && words[0] == "SWITCH") {
      auto final = DecodeAppliedVector(words[1]);
      if (!final.ok() || *final != boundary)
        co_return absl::FailedPreconditionError(
            "HistorySwitch did not reach the complete parent boundary");
      const bool child_still_available = co_await ChildOriginAvailable(bridge);
      if (!current() || history_bridge_ != bridge || !child_still_available)
        co_return absl::NotFoundError(
            "child origin is no longer available for HistorySwitch");
      NativeContinuationProof continuation{args[4],
                                           args[5],
                                           args[6],
                                           bridge->child_.source_history_id_,
                                           NewReplicationId(),
                                           bridge->child_origin_};
      const auto reply =
          absl::StrCat("+LVSWITCH ", continuation.capability_, "\r\n");
      // Publish the bounded boot-local continuation capability before its
      // reply. A target that atomically adopts the child can reconnect even
      // when its final ACK disappears; normal all-flow coverage still gates
      // it.
      continuation_proofs_[args[4]] = std::move(continuation);
      status = co_await WriteText(stream, reply);
      if (!status.ok()) co_return status;
      auto ack = co_await ReadLine(stream);
      if (!ack.ok()) co_return ack.status();
      co_return *ack == "ACK"
          ? absl::OkStatus()
          : absl::InvalidArgumentError("invalid HistorySwitch ACK");
    }
    unsigned flow = 0;
    std::uint64_t lsn = 0;
    if (words.size() != 2 || !ParseUnsigned(words[0], &flow) ||
        !ParseUnsigned(words[1], &lsn) ||
        !detail::RecoveryCovers(report, flow, lsn))
      co_return absl::PermissionDeniedError(
          "partial replay exceeds its complete parent ranges");
    const auto effect_current = [&] {
      return current() && history_bridge_ == bridge;
    };
    status = co_await SendRetainedEffect(stream, parent.source_history_id_,
                                         report, flow, lsn, effect_current);
    if (!status.ok()) co_return status;
  }
  co_return absl::CancelledError("partial export was superseded");
}

auto ReplicationManager::ReplicationGroup::ServeParentExport(
    TcpStream& stream, std::vector<std::string> args) -> Task<absl::Status> {
  AssertStateOwner();
  const auto relationship = cluster_follow_owner_;
  if (args.size() != 19 || args[1] != "1" ||
      cluster_source_revocations_in_flight_ != 0 ||
      !CurrentPartialOwner(relationship) || !IsReplicationId(args[6])) {
    co_return absl::PermissionDeniedError(
        "partial export requires a current export-ready Owner");
  }
  const auto& desired = relationship->desired_;
  const bool member =
      args[4] != node_id_ &&
      std::ranges::any_of(desired.members_, [&](const auto& item) {
        return item.node_id_ == args[4] && item.assignment_id_ == args[5];
      });
  if (!member || args[2] != desired.group_id_ ||
      args[3] != std::to_string(desired.group_term_) || args[7] != node_id_ ||
      args[8] != desired.local_assignment_id_ ||
      args[9] != std::to_string(desired.manifest_revision_) ||
      args[10] != desired.manifest_id_.Hex() ||
      args[11] != std::to_string(desired.partition_replication_epoch_))
    co_return absl::PermissionDeniedError(
        "partial export membership or population scope is stale");
  if (partial_exports_.size() >= 32)
    co_return absl::ResourceExhaustedError(
        "partial export connection bound reached");
  constexpr std::size_t kExportBytes = 3 * detail::kRecoveryMetadataBytes;
  auto reservation = TryReserveMemory(kExportBytes);
  if (!reservation.has_value())
    co_return absl::ResourceExhaustedError(
        "partial export cannot reserve transport memory");
  RetainedMemoryCharge charge;
  charge.Adopt(&*reservation, kExportBytes);
  const auto transfer =
      std::make_shared<TimedSocketContext>(&outbound_sockets_);
  if (!transfer->sockets_.Add(stream.NativeFd()))
    co_return absl::CancelledError("partial export was revoked");
  ScopedSocketSetMembership membership(&transfer->sockets_, stream.NativeFd());
  partial_exports_.push_back(transfer);
  bycorf::ThisWorker().self_->Spawn(WatchTimedSockets(
      transfer,
      [this, relationship] { return CurrentPartialOwner(relationship); }));
  auto result = co_await RunParentExport(stream, args, transfer, relationship);
  transfer->finished_ = true;
  transfer->sockets_.Cancel();
  while (!transfer->watcher_finished_) {
    auto waited = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                            std::chrono::milliseconds(1));
    if (!waited.ok()) {
      result = waited;
      break;
    }
  }
  std::erase(partial_exports_, transfer);
  co_return result;
}

auto ReplicationManager::ReplicationGroup::ServeRecoveryDonor(
    TcpStream& stream, std::vector<std::string> args) -> Task<absl::Status> {
  AssertStateOwner();
  const auto scope = cluster_recovery_;
  if (scope == nullptr || scope->sockets_.cancelled() || scope->Expired() ||
      args !=
          RecoveryHandshake(scope->desired_.action_,
                            {node_id_, scope->desired_.local_assignment_id_})) {
    co_return absl::PermissionDeniedError(
        "native recovery export has no current FDS scope");
  }
  if (scope->active_exports_ >= 32)
    co_return absl::ResourceExhaustedError(
        "recovery donor connection bound reached");
  constexpr std::size_t kExportBytes = 3 * detail::kRecoveryMetadataBytes;
  auto export_reservation = TryReserveMemory(kExportBytes);
  if (!export_reservation.has_value())
    co_return absl::ResourceExhaustedError(
        "recovery donor cannot reserve transport memory");
  RetainedMemoryCharge export_charge;
  export_charge.Adopt(&*export_reservation, kExportBytes);
  // Each connection is bound to both transport membership incarnations and
  // the origin lineage. The donor's boot comes from this live authenticated
  // response, and never substitutes for the original source boot/layout.
  if (!scope->sockets_.Add(stream.NativeFd()))
    co_return absl::CancelledError("recovery export was revoked");
  ScopedSocketSetMembership membership(&scope->sockets_, stream.NativeFd());
  ++scope->active_exports_;
  struct ExportGuard {
    unsigned& active_;
    ~ExportGuard() { --active_; }
  } guard{scope->active_exports_};
  auto local = scope->desired_.action_;
  local.candidate_node_id_ = node_id_;
  local.candidate_assignment_id_ = scope->desired_.local_assignment_id_;
  local.candidate_boot_id_ = boot_id_;
  if (!FailoverReplicaDomainMatches(local))
    co_return absl::FailedPreconditionError(
        "donor has no complete same-domain replica population");
  auto cut = applied_frontier_->TrySnapshot();
  if (!cut.ok()) co_return cut.status();
  detail::NativeRecoveryAdvertisement report{
      boot_id_, std::move(*cut),
      co_await RetainedCoverage(local.domain_.source_history_id_)};
  if (cluster_recovery_ != scope || scope->sockets_.cancelled() ||
      scope->Expired() || !FailoverReplicaDomainMatches(local))
    co_return absl::CancelledError("recovery export was revoked");
  if (report.coverage_.empty())
    report.coverage_.resize(local.domain_.flow_count_);
  for (unsigned flow = 0; flow < report.coverage_.size(); ++flow) {
    auto& ranges = report.coverage_[flow];
    for (auto& range : ranges)
      range.end_lsn_ = std::min(range.end_lsn_, report.applied_[flow]);
    std::erase_if(ranges, [](const auto& range) {
      return range.first_lsn_ >= range.end_lsn_;
    });
  }
  auto encoded = detail::EncodeRecoveryAdvertisement(report);
  if (!encoded.ok()) co_return encoded.status();
  auto status = co_await WriteRecoveryFrame(stream, *encoded);
  if (!status.ok()) co_return status;
  while (cluster_recovery_ == scope && !scope->sockets_.cancelled() &&
         !scope->Expired()) {
    auto request = co_await ReadLine(stream);
    if (!request.ok()) co_return request.status();
    const auto words = SplitWords(*request);
    unsigned flow = 0;
    std::uint64_t lsn = 0;
    if (words.size() != 2 || !ParseUnsigned(words[0], &flow) ||
        !ParseUnsigned(words[1], &lsn) ||
        !detail::RecoveryCovers(report, flow, lsn))
      co_return absl::PermissionDeniedError(
          "recovery request exceeds advertised complete coverage");
    if (cluster_recovery_ != scope || scope->sockets_.cancelled() ||
        scope->Expired() || !FailoverReplicaDomainMatches(local)) {
      co_return absl::CancelledError(
          "recovery donor identity or population changed");
    }
    const auto current = [&] {
      return cluster_recovery_ == scope && !scope->sockets_.cancelled() &&
             !scope->Expired() && FailoverReplicaDomainMatches(local);
    };
    status = co_await SendRetainedEffect(
        stream, local.domain_.source_history_id_, report, flow, lsn, current);
    if (!status.ok()) co_return status;
  }
  co_return absl::OkStatus();
}

auto ReplicationManager::ReplicationGroup::FetchRetainedEffect(
    TcpStream& stream, const detail::NativeRecoveryAdvertisement& report,
    const std::shared_ptr<RecoveryReceiveBudget>& budget, unsigned flow,
    std::uint64_t lsn)
    -> Task<absl::StatusOr<std::unique_ptr<RecoveryReceivedEffect>>> {
  const std::string request = absl::StrCat(flow, " ", lsn, "\r\n");
  auto status = co_await WriteText(stream, request);
  if (!status.ok()) co_return status;
  auto response =
      co_await ReadRecoveryFrame(stream, detail::kRecoveryMetadataBytes);
  if (!response.ok()) co_return response.status();
  auto manifest = detail::DecodeRecoveryEffectManifest(*response);
  if (!manifest.ok()) co_return manifest.status();
  if (!std::ranges::any_of(*manifest, [&](const auto& record) {
        return record.flow_id_ == flow && record.lsn_ == lsn;
      }))
    co_return absl::InvalidArgumentError(
        "donor returned an unrelated logical effect");
  // Admission includes canonical bytes and semantic decoding. The 12x
  // bound covers even many empty arguments: each four-byte length can
  // allocate a string object plus vector capacity. This one group budget
  // is independent of history; oversized optional replay falls back.
  std::size_t bytes = manifest->size() * (sizeof(NativeHistoryRecord) + 128);
  for (const auto& record : *manifest) {
    if (!detail::RecoveryCovers(report, record.flow_id_, record.lsn_))
      co_return absl::InvalidArgumentError(
          "donor effect exceeds its complete report");
    bytes += 12 * record.bytes_;
  }
  if (bytes > detail::kRecoveryReceiveBytes - budget->bytes_)
    co_return absl::ResourceExhaustedError(
        "candidate recovery receive budget exhausted");
  auto reservation = TryReserveMemory(bytes);
  if (!reservation.has_value())
    co_return absl::ResourceExhaustedError(
        "candidate recovery cannot reserve receive memory");
  auto received = std::make_unique<RecoveryReceivedEffect>(budget, bytes);
  received->charge_.Adopt(&*reservation, bytes);
  for (const auto& record : *manifest) {
    NativeHistoryRecord value{record.flow_id_, record.lsn_, {}};
    value.canonical_.reserve(record.bytes_);
    while (value.canonical_.size() < record.bytes_) {
      response = co_await ReadRecoveryFrame(
          stream, std::min<std::uint64_t>(
                      64 * 1024, record.bytes_ - value.canonical_.size()));
      if (!response.ok()) co_return response.status();
      value.canonical_.append(*response);
    }
    received->records_.push_back(std::move(value));
  }
  co_return std::move(received);
}

auto ReplicationManager::ReplicationGroup::RunRecoveryPeer(
    std::shared_ptr<ClusterRecoveryContext> scope,
    std::shared_ptr<RecoveryPeerSession> peer,
    std::shared_ptr<RecoveryReceiveBudget> budget) -> Task<absl::Status> {
  AssertStateOwner();
  struct PeerGuard {
    RecoveryPeerSession& peer_;
    ~PeerGuard() {
      peer_.connecting_ = false;
      peer_.busy_ = false;
      peer_.finished_ = true;
    }
  } guard{*peer};
  constexpr std::size_t kPeerMetadataBytes = 3 * detail::kRecoveryMetadataBytes;
  auto metadata_reservation = TryReserveMemory(kPeerMetadataBytes);
  if (!metadata_reservation.has_value())
    co_return absl::ResourceExhaustedError(
        "recovery peer cannot reserve metadata memory");
  peer->metadata_charge_.Adopt(&*metadata_reservation, kPeerMetadataBytes);
  auto connected = co_await ConnectTcp(
      peer->peer_.endpoint_.host_, peer->peer_.endpoint_.port_, tls_context_,
      &peer->sockets_, /*cancellable_dns=*/true);
  if (!connected.ok()) co_return connected.status();
  TcpStream stream = std::move(*connected);
  ScopedSocketSetMembership membership(&peer->sockets_, stream.NativeFd());
  auto status = co_await AuthenticateUpstream(stream, masteruser_, masterauth_);
  if (!status.ok()) co_return status;
  const auto handshake = EncodeRespCommand(
      RecoveryHandshake(scope->desired_.action_, peer->peer_.member_));
  status = co_await WriteText(stream, handshake);
  if (!status.ok()) co_return status;
  auto response =
      co_await ReadRecoveryFrame(stream, detail::kRecoveryMetadataBytes);
  if (!response.ok()) co_return response.status();
  auto report = detail::DecodeRecoveryAdvertisement(*response);
  if (!report.ok()) co_return report.status();
  if (report->applied_.size() != scope->desired_.action_.domain_.flow_count_)
    co_return absl::InvalidArgumentError(
        "donor report has a different origin layout");
  peer->report_ = std::move(*report);
  peer->connecting_ = false;
  while (cluster_recovery_ == scope && !peer->sockets_.cancelled() &&
         !scope->Expired()) {
    if (!peer->request_.has_value()) {
      status = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                         std::chrono::milliseconds(1));
      if (!status.ok()) co_return status;
      continue;
    }
    const auto [flow, lsn] = *peer->request_;
    auto received =
        co_await FetchRetainedEffect(stream, *peer->report_, budget, flow, lsn);
    if (!received.ok()) co_return received.status();
    // A fully received bundle no longer depends on donor liveness. Only the
    // coordinator admits it into NativeReplay under the current population.
    peer->received_ = std::move(*received);
    peer->request_.reset();
    peer->busy_ = false;
    while (peer->received_ != nullptr && !peer->sockets_.cancelled()) {
      status = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                         std::chrono::milliseconds(1));
      if (!status.ok()) co_return status;
    }
  }
  co_return absl::OkStatus();
}

auto ReplicationManager::ReplicationGroup::MaybeStartCandidateRecovery()
    -> void {
  AssertStateOwner();
  const auto action = cluster_failover_action_;
  const auto scope = cluster_recovery_;
  if (action == nullptr || scope == nullptr || action->cancelled_ ||
      action->failure_published_ || action->recovery_started_ ||
      action->desired_.authorized_revision_.has_value() ||
      action->desired_.action_id_ != scope->desired_.action_.action_id_ ||
      action->desired_.transition_id_ !=
          scope->desired_.action_.transition_id_ ||
      action->desired_.recovery_deadline_unix_ms_ !=
          scope->desired_.action_.recovery_deadline_unix_ms_)
    return;
  action->recovery_started_ = true;
  action->recovery_running_ = true;
  action->state_ = ClusterFailoverActionState::kRecovering;
  bycorf::ThisWorker().self_->Spawn(RunCandidateRecovery(action, scope));
}

auto ReplicationManager::ReplicationGroup::RunCandidateRecovery(
    std::shared_ptr<ClusterFailoverActionContext> action,
    std::shared_ptr<ClusterRecoveryContext> scope) -> Task<absl::Status> {
  AssertStateOwner();
  struct RunnerGuard {
    ClusterFailoverActionContext& action_;
    ~RunnerGuard() { action_.recovery_running_ = false; }
  } guard{*action};
  const auto current = [&] {
    return cluster_failover_action_ == action && cluster_recovery_ == scope &&
           !action->cancelled_ && !cluster_control_stopping_;
  };
#if LAVIK_FAULTS_ENABLED
  if (LAVIK_FAULT_MATCHES("LAVIK_REPLICATION_SEED_READY_RECOVERY_CANDIDATE",
                          HexBytes(action->desired_.action_id_))) {
    auto seeded_action = action->desired_;
    seeded_action.authorized_revision_ = seeded_action.transition_revision_;
    auto directive = BuildFailoverPrepareDirective(
        seeded_action,
        std::vector<std::uint64_t>(seeded_action.domain_.flow_count_, 1));
    const bool recovered =
        LAVIK_FAULT_MATCHES("LAVIK_REPLICATION_SEED_CLEAN_RECOVERED_CANDIDATE",
                            HexBytes(action->desired_.action_id_));
    auto seeded =
        co_await SeedReadyPromotionCandidateForFaultTest(directive, recovered);
    if (!seeded.ok()) co_return seeded;
    auto retained = recovered ? absl::OkStatus()
                              : co_await ResetRetainedHistory(
                                    action->desired_.domain_.source_history_id_,
                                    action->desired_.domain_.flow_count_);
    if (!retained.ok()) co_return retained;
  }
#endif
  auto stopped = co_await StopClusterFollowIngress(cluster_follow_owner_);
  if (!stopped.ok()) {
    LatchReplicationFailure(absl::StrCat(
        "candidate recovery could not safely drain native ingress: ",
        stopped.message()));
    co_return stopped;
  }
  if (!current())
    co_return absl::CancelledError(
        "candidate recovery was replaced before admission");
  if (!FailoverReplicaDomainMatches(action->desired_)) {
    // A fenced former Owner contains every locally published parent event.
    // Freeze only after accepted commands and expiration have drained; an
    // asynchronously sampled live tail is not a complete promotion cut.
    if (FailoverPopulationMatches(action->desired_) &&
        IsNativeSelfOriginDomain(action->desired_)) {
      auto frozen = co_await FreezeFormerOwnerPopulation(
          action->desired_.domain_.source_group_term_);
      if (!frozen.ok()) {
        LatchReplicationFailure(absl::StrCat(
            "self-origin recovery could not safely freeze its population: ",
            frozen.message()));
        co_return frozen;
      }
      if (current() && FailoverReplicaDomainMatches(action->desired_)) {
        auto cut = applied_frontier_->TrySnapshot();
        if (!cut.ok()) co_return cut.status();
        action->recovery_ = ClusterCandidateRecoveryResult{
            std::move(*cut), "coverage-unavailable"};
        action->state_ = ClusterFailoverActionState::kRecoveryComplete;
        co_return absl::OkStatus();
      }
    }
    PublishFailoverActionFailure(
        action, "population-domain",
        "candidate recovery has no complete same-domain population");
    co_return absl::FailedPreconditionError(action->failure_detail_);
  }
  const auto applied = applied_frontier_;
  auto initial = applied->TrySnapshot();
  if (!initial.ok()) co_return initial.status();
  detail::NativeReplay replay(applied, retained_histories_,
                              action->desired_.domain_.source_history_id_);
  std::vector<std::shared_ptr<RecoveryPeerSession>> peers;
  const auto receive_budget = std::make_shared<RecoveryReceiveBudget>();
  if (!scope->Expired()) {
    // Discovery and all transfers share the committed cutoff. A small,
    // bounded discovery window prevents one silent member from consuming
    // the entire budget before any usable donor can start replay.
    for (const auto& donor : scope->desired_.members_) {
      if (donor.member_.node_id_ == node_id_ || donor.endpoint_.port_ == 0)
        continue;
      if (peers.size() == 32)
        break;  // Bound optional transport/metadata owners.
      auto peer =
          std::make_shared<RecoveryPeerSession>(donor, &scope->sockets_);
      peers.push_back(peer);
      bycorf::ThisWorker().self_->Spawn(
          RunRecoveryPeer(scope, peer, receive_budget));
    }
  }
  const auto remaining = scope->lease_deadline_ - cluster::LeaseClockNow();
  const auto discovery_end =
      cluster::LeaseClockNow() +
      std::min(
          std::chrono::duration_cast<std::chrono::milliseconds>(remaining) / 4,
          std::chrono::milliseconds(100));
  while (current() && !scope->Expired() &&
         cluster::LeaseClockNow() < discovery_end &&
         std::ranges::any_of(
             peers, [](const auto& peer) { return peer->connecting_; })) {
    auto waited = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                            std::chrono::milliseconds(1));
    if (!waited.ok()) break;
  }
  std::vector<detail::NativeRecoveryAdvertisement> reports;
  for (const auto& peer : peers) {
    if (peer->report_.has_value())
      reports.push_back(*peer->report_);
    else
      peer->sockets_.Cancel();
  }
  auto envelope = detail::RecoveryTarget(*initial, reports);
  absl::Status result = envelope.ok() ? absl::OkStatus() : envelope.status();
  std::string reason = "coverage-unavailable";
  std::vector<std::unique_ptr<RecoveryReceivedEffect>> pending;
  while (result.ok() && current() && !scope->Expired()) {
    for (const auto& peer : peers)
      if (peer->received_ != nullptr)
        pending.push_back(std::move(peer->received_));
    bool advanced = false;
    for (auto item = pending.begin(); item != pending.end();) {
      auto effect = replay.PrepareEffect(std::move((*item)->records_));
      if (!effect.ok()) {
        // No effect has entered storage yet. Bad donor bytes retract only
        // this optional bundle, never the pre-existing complete population.
        item = pending.erase(item);
        continue;
      }
      if (effect->disposition_ ==
          detail::NativeReplayDisposition::kNeedsPredecessor) {
        (*item)->records_ = std::move(effect->records_);
        ++item;
        continue;
      }
      if (effect->disposition_ == detail::NativeReplayDisposition::kReady) {
        if (!current() || scope->Expired() || applied_frontier_ != applied ||
            !FailoverReplicaDomainMatches(action->desired_))
          break;
        // Once admitted, even cutoff/replacement joins this complete apply.
        // Never cancel it because its donor went away, and never report E
        // as applied. The common publication boundary advances actual A.
        // A verified clean-recovered population is already the complete
        // live root. Loading is only its serving fence, not a FULL apply
        // context. FDS/lease and expiration fencing remain closed here.
        if (recovered_population_fenced_) storage_->SetReplicaLoading(false);
        result = co_await ApplyReplicatedCommand(effect->command_);
        if (result.ok())
          result = replay.PublishAfterApply(0, effect->updates_,
                                            std::move(effect->records_));
        if (!result.ok()) {
          LatchReplicationFailure(
              absl::StrCat("candidate recovery apply outcome is uncertain: ",
                           result.message()));
          break;
        }
        advanced = true;
      }
      item = pending.erase(item);
    }
    if (!result.ok() || !current() || scope->Expired()) break;
    auto cut = applied->TrySnapshot();
    if (!cut.ok()) {
      result = cut.status();
      break;
    }
    if (*cut == *envelope) {
      reason = "target-reached";
      break;
    }
    const auto queued = [&](unsigned flow, std::uint64_t lsn) {
      for (const auto& item : pending) {
        if (std::ranges::any_of(item->records_, [&](const auto& record) {
              return record.flow_id_ == flow && record.lsn_ == lsn;
            }))
          return true;
      }
      for (const auto& peer : peers)
        if (peer->busy_ && peer->request_ == std::pair(flow, lsn)) return true;
      return false;
    };
    bool possible = advanced;
    for (const auto& peer : peers) {
      if (peer->busy_) {
        possible = true;
        continue;
      }
      if (peer->finished_ || !peer->report_.has_value() ||
          peer->sockets_.cancelled())
        continue;
      for (unsigned flow = 0; flow < cut->size(); ++flow) {
        if ((*cut)[flow] < (*envelope)[flow] && !queued(flow, (*cut)[flow]) &&
            detail::RecoveryCovers(*peer->report_, flow, (*cut)[flow])) {
          peer->request_ = std::pair(flow, (*cut)[flow]);
          peer->busy_ = true;
          possible = true;
          break;
        }
      }
    }
    if (!possible) break;
    auto waited = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                            std::chrono::milliseconds(1));
    if (!waited.ok()) {
      result = waited;
      break;
    }
  }
  if (scope->Expired()) reason = "deadline";
  for (const auto& peer : peers) peer->sockets_.Cancel();
  while (std::ranges::any_of(
      peers, [](const auto& peer) { return !peer->finished_; })) {
    auto waited = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                            std::chrono::milliseconds(1));
    if (!waited.ok()) {
      result = waited;
      break;
    }
  }
  if (!result.ok()) co_return result;
  if (!current())
    co_return absl::CancelledError("candidate recovery was replaced");
  if (applied_frontier_ != applied ||
      !FailoverReplicaDomainMatches(action->desired_)) {
    PublishFailoverActionFailure(
        action, "population-domain",
        "candidate population changed while draining recovery");
    co_return absl::FailedPreconditionError(action->failure_detail_);
  }
  auto final = applied->TrySnapshot();
  if (!final.ok()) co_return final.status();
  action->recovery_ =
      ClusterCandidateRecoveryResult{std::move(*final), std::move(reason)};
  action->state_ = ClusterFailoverActionState::kRecoveryComplete;
  spdlog::info(
      "candidate recovery completed group={} action={} reason={} applied={}",
      action->desired_.group_id_, HexBytes(action->desired_.action_id_),
      action->recovery_->completion_reason_,
      EncodeAppliedVector(action->recovery_->applied_next_lsns_));
  co_return absl::OkStatus();
}

auto ReplicationManager::ReplicationGroup::RunClusterFailoverAction(
    std::shared_ptr<ClusterFailoverActionContext> context)
    -> Task<absl::Status> {
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
    absl::Status installed =
        co_await InstallRecoveredPopulation(std::move(base));
    if (!installed.ok()) {
      PublishFailoverActionFailure(context, "operator-recovery",
                                   installed.ToString());
      finish();
      co_return installed;
    }
    operator_recovery_active_ = true;
  }
  if (cluster_rebuild_ != nullptr && !cluster_rebuild_->manifest_.has_value()) {
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
    ClusterPromotionPrepareDirective directive = BuildFailoverPrepareDirective(
        context->desired_, std::move(current_frontier));
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
      started =
          co_await StartClusterPromotionPrepareDirective(std::move(directive));
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

auto ReplicationManager::ReplicationGroup::ValidateClusterFailoverAction(
    const DesiredClusterFailoverAction& desired) const -> absl::Status {
  const ClusterFailoverCompatibilityDomain& domain = desired.domain_;
  if (desired.recovery_deadline_unix_ms_.has_value() &&
      (desired.mode_ != ClusterFailoverMode::kUncontrolled ||
       *desired.recovery_deadline_unix_ms_ == 0 ||
       *desired.recovery_deadline_unix_ms_ >
           static_cast<std::uint64_t>(
               std::numeric_limits<std::int64_t>::max()))) {
    return absl::InvalidArgumentError("invalid candidate recovery cutoff");
  }
  if (IsZeroBytes(desired.transition_id_) || IsZeroBytes(desired.action_id_) ||
      desired.transition_revision_ == 0 || desired.target_term_ == 0 ||
      desired.committed_group_term_ == 0 || desired.group_id_.empty() ||
      desired.candidate_node_id_ != node_id_ ||
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

auto ReplicationManager::ReplicationGroup::ReconcileClusterFailoverAction(
    std::optional<DesiredClusterFailoverAction> desired,
    std::optional<ClusterFailoverActionId> pending_activation_action_id)
    -> Task<absl::Status> {
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(
        0, [this, desired = std::move(desired),
            pending_activation_action_id]() mutable {
          return ReconcileClusterFailoverAction(std::move(desired),
                                                pending_activation_action_id);
        });
  }
  if (!meta_managed_ || cluster_group_ == nullptr) {
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
    DesiredClusterFailoverAction& current = cluster_failover_action_->desired_;
    const bool retained_controlled_degrade =
        IsRetainedControlledDegrade(current, *desired);
    if (!SameFailoverActionExceptAuthorization(current, *desired) &&
        !retained_controlled_degrade) {
      co_return absl::FailedPreconditionError(
          "committed failover action changed immutable execution anchors");
    }
    if (desired->transition_revision_ < current.transition_revision_ ||
        (current.recovery_deadline_unix_ms_.has_value() &&
         current.recovery_deadline_unix_ms_ !=
             desired->recovery_deadline_unix_ms_) ||
        (current.authorized_revision_.has_value() &&
         current.authorized_revision_ != desired->authorized_revision_)) {
      co_return absl::FailedPreconditionError(
          "committed failover action authorization regressed or changed");
    }
    current.transition_revision_ = desired->transition_revision_;
    current.authorized_revision_ = desired->authorized_revision_;
    current.recovery_deadline_unix_ms_ = desired->recovery_deadline_unix_ms_;
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
    MaybeStartCandidateRecovery();
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
    if (previous->recovery_running_ && cluster_recovery_ != nullptr)
      cluster_recovery_->sockets_.Cancel();
    while (!previous->runner_finished_ || previous->recovery_running_) {
      absl::Status waited = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) co_return waited;
    }
    const bool retain_for_activation =
        pending_activation_action_id == std::optional<ClusterFailoverActionId>(
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
  MaybeStartCandidateRecovery();
  co_return absl::OkStatus();
}

auto ReplicationManager::ReplicationGroup::cluster_failover_action_status()
    const -> Task<ClusterFailoverActionStatus> {
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
  result.recovery_ = cluster_failover_action_->recovery_;
  result.failure_class_ = cluster_failover_action_->failure_class_;
  result.failure_detail_ = cluster_failover_action_->failure_detail_;
  co_return result;
}

auto ReplicationManager::ReplicationGroup::FindClusterFailoverPreparedContext(
    const ClusterFailoverActionId& action_id) const
    -> Task<std::optional<ClusterFailoverPreparedContext>> {
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

auto ReplicationManager::ReplicationGroup::ValidateClusterFailoverActivation(
    const ClusterFailoverActivation& activation) const -> absl::Status {
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

auto ReplicationManager::ReplicationGroup::ClusterActivationPopulationMatches(
    const ClusterFailoverActivation& activation) const -> bool {
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

auto ReplicationManager::ReplicationGroup::ActivateClusterPreparedPromotion(
    ClusterFailoverActivation activation) -> Task<absl::Status> {
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(
        0, [this, activation = std::move(activation)]() mutable {
          return ActivateClusterPreparedPromotion(std::move(activation));
        });
  }
  if (!meta_managed_ || cluster_group_ == nullptr) {
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
      desired.candidate_assignment_id_ != activation.candidate_assignment_id_ ||
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

  if (history_bridge_ == nullptr ||
      history_bridge_->promotion_ != prepared.promotion_ ||
      history_bridge_->child_.source_group_term_ != activation.target_term_) {
    co_return absl::FailedPreconditionError(
        "activation has no exact parent/child history descriptor");
  }
  auto adopted = AdoptLocalOwnerHistory();
  if (!adopted.ok()) co_return adopted;
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

auto ReplicationManager::ReplicationGroup::
    EnableClusterExpirationAuthorityUntil(
        std::chrono::nanoseconds deadline_since_boot) -> Task<absl::Status> {
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(0, [this, deadline_since_boot] {
      return EnableClusterExpirationAuthorityUntil(deadline_since_boot);
    });
  }
  if (!meta_managed_ || cluster_group_ == nullptr) {
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

auto ReplicationManager::ReplicationGroup::RevokeClusterExpirationAuthority()
    -> Task<absl::Status> {
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(
        0, [this] { return RevokeClusterExpirationAuthority(); });
  }
  if (!meta_managed_ || cluster_group_ == nullptr) {
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

auto ReplicationManager::ReplicationGroup::NormalizeClusterFollowOwner(
    DesiredClusterUpstream desired) const
    -> absl::StatusOr<std::pair<DesiredClusterUpstream, PopulationManifest>> {
  if (!meta_managed_ || cluster_group_ == nullptr) {
    return absl::FailedPreconditionError(
        "follow-owner reconciliation requires Meta-managed population "
        "mode");
  }
  if (desired.group_id_.empty() || desired.group_term_ == 0 ||
      desired.local_node_id_ != node_id_ ||
      desired.local_assignment_id_.empty() ||
      desired.local_boot_id_ != boot_id_ || desired.owner_node_id_.empty() ||
      desired.owner_assignment_id_.empty() || desired.manifest_revision_ == 0 ||
      IsZeroBytes(desired.manifest_id_.bytes_) ||
      desired.partition_replication_epoch_ == 0) {
    return absl::InvalidArgumentError(
        "follow-owner desired identity is incomplete or belongs to another "
        "local boot");
  }
  const bool local_is_owner = desired.local_node_id_ == desired.owner_node_id_;
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

  std::sort(desired.manifest_entries_.begin(), desired.manifest_entries_.end(),
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

auto ReplicationManager::ReplicationGroup::ClusterFollowReadyPopulationMatches(
    const DesiredClusterUpstream& desired) const -> bool {
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

auto ReplicationManager::ReplicationGroup::StopClusterFollowIngress(
    const std::shared_ptr<ClusterFollowOwnerContext>& previous)
    -> Task<absl::Status> {
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

auto ReplicationManager::ReplicationGroup::BeginClusterFollowFullPopulation(
    const std::shared_ptr<ReplicaSession>& session, std::string source_boot_id,
    std::string source_history_id, std::uint32_t source_flow_count)
    -> absl::Status {
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

  // This is destructive FULL admission: the authenticated source and
  // current relationship have been validated. Withdraw the old Ready proof
  // before storage fences and replaces the root. Partial failure alone must
  // not reach this boundary while the source is still unavailable.
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
              .operation_id_ = absl::StrCat("steady-follow:", desired.group_id_,
                                            ":", desired.group_term_),
              .directive_id_ =
                  absl::StrCat("steady-follow-owner:", desired.owner_node_id_,
                               ":", desired.owner_assignment_id_),
              .attempt_id_ = absl::StrCat("steady-follow-attempt:", *revision),
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

auto ReplicationManager::ReplicationGroup::FreezeFormerOwnerPopulation(
    std::uint64_t source_term) -> Task<absl::Status> {
  AssertStateOwner();
  if (cluster_rebuild_ == nullptr ||
      !cluster_rebuild_->ready_token_.has_value() ||
      !native_dataset_valid_.load(std::memory_order_acquire) ||
      storage_->ReplicaRecoveryFenced()) {
    co_return absl::OkStatus();
  }
  if (!cluster_rebuild_->manifest_.has_value())
    co_return absl::FailedPreconditionError(
        "former Owner freeze requires the current population manifest");
  // Source exports have already been revoked with command admission open:
  // a cancelled FULL may itself own the gates. Keep publication available
  // until every previously admitted writer and expiration task has drained.
  while (!CloseAllCommandDbGates()) {
    auto waited = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                            std::chrono::milliseconds(1));
    if (!waited.ok()) co_return waited;
  }
  struct GateGuard {
    ~GateGuard() { OpenAllCommandDbGates(); }
  } gate;
  while (CommandDbOperationsActive()) {
    auto waited = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                            std::chrono::milliseconds(1));
    if (!waited.ok()) co_return waited;
  }
  auto catalog_guard = co_await AcquireFunctionCatalogOperation();
  (void)catalog_guard;
  auto quiesced = co_await storage_->QuiesceExpiration();
  if (!quiesced.ok()) co_return quiesced;
  struct ExpiryGuard {
    storage::StorageEngine* storage_;
    ~ExpiryGuard() { storage_->ResumeExpiration(); }
  } expiry{storage_};
  auto watermark = co_await CaptureNativeReplicationWatermark();
  if (!watermark.ok()) co_return watermark.status();
  if (!watermark->has_value()) {
    // An idle source has no retained events or reconnectable downstreams.
    // Establish its current publisher boundary only after the drain; this
    // proves the existing population, never fabricates historical records.
    for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
      const auto capacity = BacklogCapacityForFlow(
          worker, backlog_size_bytes_.load(std::memory_order_acquire));
      auto enabled = co_await bycorf::SubmitTaskTo(
          worker, [this, capacity]() -> Task<absl::Status> {
            co_return co_await storage_->EnableReplicationLog(
                role_epoch_.load(std::memory_order_acquire), capacity);
          });
      if (!enabled.ok()) co_return enabled;
    }
    watermark = co_await CaptureNativeReplicationWatermark();
    if (!watermark.ok()) co_return watermark.status();
    if (!watermark->has_value())
      co_return absl::FailedPreconditionError(
          "former Owner has no complete source boundary");
  }
  auto revision = cluster_group_->NextDirectiveRevision(source_term);
  if (!revision.ok()) co_return revision.status();
  RebuildDirective source = cluster_rebuild_->directive_;
  auto& identity = source.identity_;
  identity.term_ = source_term;
  identity.directive_revision_ = *revision;
  identity.source_node_id_ = node_id_;
  identity.source_assignment_id_ = identity.assignment_id_;
  identity.source_boot_id_ = boot_id_;
  identity.source_history_id_ = (*watermark)->history_id_;
  identity.target_history_id_.clear();
  identity.authority_id_ = identity.operation_id_ = identity.directive_id_ =
      identity.attempt_id_ = "source-freeze:" + NewReplicationId();
  source.flow_count_ = (*watermark)->next_lsns_.size();
  source.safe_source_active_ = true;
  auto adopted = std::make_shared<ClusterRebuildContext>(
      source, *cluster_rebuild_->manifest_, *cluster_rebuild_->ready_token_);
  auto applied = std::make_shared<detail::ReplicaAppliedFrontier>(
      source.flow_count_, storage_->worker_count());
  auto installed = applied->InstallNextLsns((*watermark)->next_lsns_);
  if (!installed.ok()) co_return installed;
  auto upstream_node = node_id_;
  auto upstream_history = (*watermark)->history_id_;
  auto ready = cluster_group_->BindLocalSourceHistory(
      *cluster_rebuild_->ready_token_, source, *cluster_rebuild_->manifest_,
      (*watermark)->next_lsns_);
  if (!ready.ok()) co_return ready.status();
  adopted->ready_token_ = std::move(*ready);
  cluster_rebuild_ = std::move(adopted);
  applied_frontier_ = std::move(applied);
  upstream_node_id_ = std::move(upstream_node);
  upstream_history_id_ = std::move(upstream_history);
  co_return absl::OkStatus();
}

auto ReplicationManager::ReplicationGroup::ReconcileClusterFollowOwner(
    std::optional<DesiredClusterUpstream> desired) -> Task<absl::Status> {
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(
        0, [this, desired = std::move(desired)]() mutable {
          return ReconcileClusterFollowOwner(std::move(desired));
        });
  }
  if (!meta_managed_ || cluster_group_ == nullptr) {
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
  const bool previous_was_follower = previous != nullptr && !previous_was_owner;
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
    absl::Status revoked = co_await RevokeClusterRebuildSourceAuthorizations();
    if (!revoked.ok()) co_return revoked;
  }
  if (previous_was_follower || legacy_ingress) {
    absl::Status stopped = co_await StopClusterFollowIngress(previous);
    if (!stopped.ok()) {
      const std::string failure =
          absl::StrCat("follow-owner ingress replacement could not be joined: ",
                       stopped.message());
      LatchReplicationFailure(failure);
      co_return absl::InternalError(failure);
    }
  }
  if (next == nullptr) co_return absl::OkStatus();

  if (next_is_owner) {
    SetDesiredUpstream(std::nullopt);
    for (auto entry = continuation_proofs_.begin();
         entry != continuation_proofs_.end();) {
      const bool retained =
          std::ranges::any_of(next->desired_.members_, [&](const auto& member) {
            return member.node_id_ == entry->second.target_node_id_ &&
                   member.assignment_id_ == entry->second.target_assignment_id_;
          });
      if (retained)
        ++entry;
      else
        continuation_proofs_.erase(entry++);
    }
    cluster_follow_owner_ = std::move(next);
    co_return absl::OkStatus();
  }

  if (local_was_source) {
    const auto source_term =
        previous_was_owner ? previous->desired_.group_term_
                           : (cluster_rebuild_ != nullptr
                                  ? cluster_rebuild_->directive_.identity_.term_
                                  : 0);
    auto frozen = co_await FreezeFormerOwnerPopulation(source_term);
    if (!frozen.ok()) {
      LatchReplicationFailure(absl::StrCat(
          "former Owner could not freeze its complete population: ",
          frozen.message()));
      co_return frozen;
    }
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

auto ReplicationManager::ReplicationGroup::RetireClusterPopulation(
    std::optional<DesiredClusterPopulation> desired, bool preserve_any_ready,
    bool preserve_current_follow_attempt, std::string_view reason)
    -> Task<absl::Status> {
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(
        0, [this, desired = std::move(desired), preserve_any_ready,
            preserve_current_follow_attempt,
            reason = std::string(reason)]() mutable {
          return RetireClusterPopulation(std::move(desired), preserve_any_ready,
                                         preserve_current_follow_attempt,
                                         reason);
        });
  }
  if (!meta_managed_ || cluster_group_ == nullptr) {
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
    const bool desired_ready_population = matches_population && completed_ready;
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
    if (cluster_recovery_ != nullptr) {
      auto revoked = co_await ReconcileClusterRecovery(std::nullopt);
      if (!revoked.ok()) co_return revoked;
      // Joining recovery may change readiness within the same context.
      // Retirement must not continue with the pre-suspension proof.
      if (cluster_rebuild_ != context ||
          context->state_.load(std::memory_order_acquire) != state ||
          context->ready_token_.has_value() != completed_ready)
        co_return absl::AbortedError(
            "population changed while recovery was joined");
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

auto ReplicationManager::ReplicationGroup::ReconcileClusterPopulation(
    std::optional<DesiredClusterPopulation> desired) -> Task<absl::Status> {
  return RetireClusterPopulation(std::move(desired),
                                 /*preserve_any_ready=*/false,
                                 /*preserve_current_follow_attempt=*/false,
                                 "cluster population was retired by FDS");
}

auto ReplicationManager::ReplicationGroup::CancelInProgressClusterPopulation(
    bool preserve_current_follow_attempt) -> Task<absl::Status> {
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

auto ReplicationManager::ReplicationGroup::InstallRecoveredPopulation(
    detail::RecoveredPopulation population) -> Task<absl::Status> {
  AssertStateOwner();
  const auto old_population = cluster_rebuild_;
  const auto old_action = cluster_failover_action_;
  const auto old_follow = cluster_follow_owner_;
  LAVIK_FAULT_INJECT({
    // Hold the same snapshot that the retained-history await must preserve.
    // The process test removes this marker to release startup recovery.
    const char* barrier = std::getenv("LAVIK_RECOVERY_INSTALL_BARRIER_PATH");
    if (barrier != nullptr && *barrier != '\0') {
      absl::Status signalled = SignalFaultBarrier(
          "LAVIK_RECOVERY_INSTALL_BARRIER_PATH", "population recovery");
      if (!signalled.ok()) co_return signalled;
      while (::access(barrier, F_OK) == 0) {
        absl::Status waited = co_await bycorf::SleepFor(
            *bycorf::ThisWorker().self_, std::chrono::milliseconds(10));
        if (!waited.ok()) co_return waited;
      }
    }
  });
  auto& identity = population.identity_;
  identity.target_boot_id_ = boot_id_;
  identity.directive_revision_ = 1;
  identity.authority_id_ = "recovered-population";
  identity.operation_id_ = "recovery";
  identity.directive_id_ = "recovery";
  identity.attempt_id_ = boot_id_;
  // The certificate recovers only a complete cut, never historical payload.
  // Bind an empty cache so newly applied donor/parent events retain their
  // original lineage and can become the next promotion's secondary suffix.
  auto retained = co_await ResetRetainedHistory(identity.source_history_id_,
                                                population.frontier_.size());
  if (!retained.ok()) co_return retained;
  if (cluster_rebuild_ != old_population ||
      cluster_failover_action_ != old_action ||
      cluster_follow_owner_ != old_follow || cluster_control_stopping_ ||
      (old_action != nullptr && old_action->cancelled_))
    co_return absl::CancelledError("recovered population install superseded");
  auto ready =
      cluster_group_->RecoverPopulation(identity, population.frontier_);
  if (!ready.ok()) co_return ready.status();
  auto frontier = std::make_shared<detail::ReplicaAppliedFrontier>(
      population.frontier_.size(), storage_->worker_count());
  absl::Status installed = frontier->InstallNextLsns(population.frontier_);
  if (!installed.ok()) co_return installed;
  cluster_rebuild_ = std::make_shared<ClusterRebuildContext>(std::move(*ready));
  applied_frontier_ = std::move(frontier);
  upstream_node_id_ = identity.source_node_id_;
  upstream_history_id_ = identity.source_history_id_;
  group_id_ = PopulationGroupToken(identity.group_id_);
  recovered_population_fenced_ = true;
  storage_->SetReplicaLoading(true);
  storage_->SetExpirationAuthority(false);
  co_return absl::OkStatus();
}

auto ReplicationManager::ReplicationGroup::RecoverClusterPopulation()
    -> Task<absl::Status> {
  AssertStateOwner();
  auto record = co_await storage_->ConsumePopulationRecovery();
  if (!record.ok()) co_return record.status();
  if (!meta_managed_ || !record->has_value()) co_return absl::OkStatus();
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
  absl::Status installed =
      co_await InstallRecoveredPopulation(std::move(*proof));
  if (installed.ok())
    spdlog::info(
        "consumed clean shutdown proof; recovered candidate remains fenced");
  co_return installed;
}

auto ReplicationManager::ReplicationGroup::CancelClusterRebuildForShutdown()
    -> Task<absl::Status> {
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(
        0, [this]() { return CancelClusterRebuildForShutdown(); });
  }
  if (!meta_managed_ || cluster_group_ == nullptr) {
    co_return absl::FailedPreconditionError(
        "population shutdown requires Meta-managed population mode");
  }
  AssertStateOwner();
  cluster_control_stopping_ = true;
  auto recovery_stopped = co_await ReconcileClusterRecovery(std::nullopt);
  if (!recovery_stopped.ok()) co_return recovery_stopped;
  auto partial_stopped = co_await CancelPartialExports();
  if (!partial_stopped.ok()) co_return partial_stopped;
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

auto ReplicationManager::ReplicationGroup::RequestShutdown() noexcept -> void {
  replication_shutdown_requested_.store(true, std::memory_order_release);
  // These transport-only sets cover connecting/TLS sockets too. Closing
  // them wakes the owner coordinator, which cancels barriers and joins its
  // flows. Main never reads the mutable session registry or partially
  // initialized session proof/barrier fields. Late socket registration
  // observes cancellation and cannot escape this one-way shutdown fence.
  outbound_sockets_.Cancel();
  source_sockets_.Cancel();
}

auto ReplicationManager::ReplicationGroup::QuiesceForShutdown()
    -> Task<absl::Status> {
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(
        0, [this]() { return QuiesceForShutdown(); });
  }
  RequestShutdown();
  absl::Status result = absl::OkStatus();
  if (meta_managed_) {
    // The Meta transition additionally retires its Ready proof. It is
    // idempotent when the control client already completed the same barrier.
    result = co_await CancelClusterRebuildForShutdown();
  } else {
    std::vector<std::shared_ptr<ReplicaSession>> sessions;
    std::uint64_t redis_full_sync_session = 0;
    {
      AssertStateOwner();
      role_epoch_.fetch_add(1, std::memory_order_acq_rel);
      initial_redis_connection_pending_ = false;
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

auto ReplicationManager::ReplicationGroup::FindCompletedClusterPopulation(
    const RebuildDirective& directive) const
    -> std::shared_ptr<detail::ClusterRebuildCompletionState> {
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

auto ReplicationManager::ReplicationGroup::cluster_population_status() const
    -> Task<ClusterPopulationStatus> {
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(
        0, [this] { return cluster_population_status(); });
  }
  ClusterPopulationStatus result;
  result.local_node_id_ = node_id_;
  result.local_boot_id_ = boot_id_;
  result.recovered_ = recovered_population_fenced_;
  if (operator_recovery_active_) result.failover_candidate_eligible_ = false;
  if (recovered_population_.has_value() && !storage_->ReplicaRecoveryFenced() &&
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

auto ReplicationManager::ReplicationGroup::AuthorizeClusterRebuildSource(
    RebuildDirective directive) -> Task<absl::Status> {
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(
        0, [this, directive = std::move(directive)]() mutable {
          return AuthorizeClusterRebuildSource(std::move(directive));
        });
  }
  if (!meta_managed_ || cluster_group_ == nullptr) {
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
    absl::Status revoked = co_await RevokeClusterRebuildSourceAuthorizations();
    if (!revoked.ok()) co_return revoked;
  }
}

auto ReplicationManager::ReplicationGroup::
    RevokeClusterRebuildSourceAuthorizations() -> Task<absl::Status> {
  return RetireClusterRebuildSourceAuthorizations(
      SourceAuthorizationRetirementMode::kStrongRevoke,
      /*preserve_current_population_exports=*/false);
}

auto ReplicationManager::ReplicationGroup::
    EnableClusterRebuildSourceAdmissionUntil(
        std::chrono::nanoseconds deadline_since_boot) -> Task<absl::Status> {
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(0, [this, deadline_since_boot] {
      return EnableClusterRebuildSourceAdmissionUntil(deadline_since_boot);
    });
  }
  if (!meta_managed_ || cluster_group_ == nullptr) {
    co_return absl::FailedPreconditionError(
        "cluster source admission requires Meta-managed population mode");
  }
  co_await master_mutex_.Lock(*bycorf::ThisWorker().self_);
  bycorf::CrossWorkerMutex::Guard lock(&master_mutex_);
  AssertStateOwner();
  if (cluster_control_stopping_ || cluster_source_revocations_in_flight_ != 0 ||
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

auto ReplicationManager::ReplicationGroup::
    ClearClusterRebuildSourceAuthorizationsForSessionReplacement(
        bool preserve_established_exports) -> Task<absl::Status> {
  return RetireClusterRebuildSourceAuthorizations(
      SourceAuthorizationRetirementMode::kSessionReplacement,
      preserve_established_exports);
}

auto ReplicationManager::ReplicationGroup::
    RefreshClusterRebuildSourceAuthorizationsForFdsReplacement(
        bool preserve_current_population_exports,
        std::size_t expected_authorization_replays) -> Task<absl::Status> {
  return RetireClusterRebuildSourceAuthorizations(
      SourceAuthorizationRetirementMode::kFdsReplacement,
      preserve_current_population_exports, expected_authorization_replays);
}

auto ReplicationManager::ReplicationGroup::
    RetireClusterRebuildSourceAuthorizations(
        SourceAuthorizationRetirementMode mode,
        bool preserve_current_population_exports,
        std::size_t expected_authorization_replays) -> Task<absl::Status> {
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(
        0, [this, mode, preserve_current_population_exports,
            expected_authorization_replays] {
          return RetireClusterRebuildSourceAuthorizations(
              mode, preserve_current_population_exports,
              expected_authorization_replays);
        });
  }
  if (!meta_managed_ || cluster_group_ == nullptr) {
    co_return absl::FailedPreconditionError(
        "cluster source revocation requires Meta-managed population mode");
  }
  if (mode != SourceAuthorizationRetirementMode::kFdsReplacement) {
    auto revoked = co_await ReconcileClusterRecovery(std::nullopt);
    if (!revoked.ok()) co_return revoked;
  }
  if (mode != SourceAuthorizationRetirementMode::kFdsReplacement) {
    auto stopped = co_await CancelPartialExports();
    if (!stopped.ok()) co_return stopped;
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
    sessions.reserve(master_sessions_.size() + retired_master_sessions_.size());
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
  absl::Status revocation_signalled =
      SignalFaultBarrier("LAVIK_REPLICATION_SOURCE_REVOCATION_BARRIER_ACK_PATH",
                         "source authorization fault barrier");
  if (!revocation_signalled.ok()) co_return revocation_signalled;
#endif
  for (const auto& session : sessions) session->Cancel();
  auto next_warning =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (active_unpublished_master_controls_.load(std::memory_order_acquire) !=
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
      next_warning = std::chrono::steady_clock::now() + std::chrono::seconds(5);
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

auto ReplicationManager::ReplicationGroup::PreparePromotion(
    storage::PromotionBase promotion_base)
    -> Task<absl::StatusOr<ClusterPromotionPrepared>> {
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
        worker, [this, child_log_epoch, flow_capacity]() -> Task<absl::Status> {
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
      .catalog_generation_ = promotion_base.catalog_token_.catalog_generation_,
      .catalog_dump_crc64_ = promotion_base.catalog_token_.dump_crc64_,
      .child_history_id_ = std::move(child_history),
  };
}

auto ReplicationManager::ReplicationGroup::ActivatePreparedPromotionRole()
    -> void {
  {
    AssertStateOwner();
    StoreRole(ReplicationRole::kMaster, std::memory_order_release);
  }
  if (!storage_->ReplicaRecoveryFenced()) {
    storage_->SetReplicaLoading(false);
  }
}

auto ReplicationManager::ReplicationGroup::ActivatePreparedPromotion() -> void {
  AssertStateOwner();
  pending_promotion_.reset();
  ActivatePreparedPromotionRole();
  if (!storage_->ReplicaRecoveryFenced()) {
    storage_->SetExpirationAuthority(true);
  }
  storage_->ResumeExpiration();
}

auto ReplicationManager::ReplicationGroup::CaptureNativeReplicationWatermark()
    -> Task<absl::StatusOr<std::optional<NativeReplicationWatermark>>> {
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

auto ReplicationManager::ReplicationGroup::CountAcknowledgedNativeReplicas(
    const NativeReplicationWatermark& watermark) const
    -> Task<std::optional<std::uint64_t>> {
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

auto ReplicationManager::ReplicationGroup::CountOnlineNativeReplicas() const
    -> Task<std::uint64_t> {
  co_await master_mutex_.Lock(*bycorf::ThisWorker().self_);
  bycorf::CrossWorkerMutex::Guard lock(&master_mutex_);
  co_return static_cast<std::uint64_t>(
      std::count_if(master_sessions_.begin(), master_sessions_.end(),
                    [](const auto& entry) { return entry.second->online(); }));
}

auto ReplicationManager::ReplicationGroup::identity() const
    -> Task<ReplicationIdentity> {
  co_await master_mutex_.Lock(*bycorf::ThisWorker().self_);
  bycorf::CrossWorkerMutex::Guard lock(&master_mutex_);
  co_return ReplicationIdentity{
      .local_node_id_ = node_id_,
      .boot_id_ = boot_id_,
      .local_history_id_ = history_id_,
  };
}

auto ReplicationManager::ReplicationGroup::upstream() const
    -> std::optional<ReplicaOfConfig> {
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

auto ReplicationManager::ReplicationGroup::status() const
    -> Task<ReplicationStatus> {
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
      result.connected_flows_ = active_replica_session_->connected_flows_.load(
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

auto ReplicationManager::ReplicationGroup::StoreRole(
    ReplicationRole next, std::memory_order order) noexcept -> void {
  constexpr std::uint64_t kServingOpen = 1;
  const auto serves_dataset = [](ReplicationRole role) {
    return role == ReplicationRole::kMaster || role == ReplicationRole::kOnline;
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
      link_state_changed_nanos_.store(SteadyNanos(), std::memory_order_release);
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

auto ReplicationManager::ReplicationGroup::is_replica() const noexcept -> bool {
  return role_.load(std::memory_order_acquire) != ReplicationRole::kMaster;
}

auto ReplicationManager::ReplicationGroup::role_epoch() const noexcept
    -> std::uint64_t {
  return role_epoch_.load(std::memory_order_acquire);
}

auto ReplicationManager::ReplicationGroup::is_redis_follower() const noexcept
    -> bool {
  return redis_psync_.load(std::memory_order_acquire);
}

auto ReplicationManager::ReplicationGroup::is_loading() const noexcept -> bool {
  const ReplicationRole role = role_.load(std::memory_order_acquire);
  return storage_->ReplicaRecoveryFenced() ||
         role == ReplicationRole::kConnecting ||
         role == ReplicationRole::kSyncing;
}

auto ReplicationManager::ReplicationGroup::SetSnapshotReadConcurrency(
    unsigned concurrency) noexcept -> absl::Status {
  if (concurrency == 0 ||
      concurrency > kMaxReplicationSnapshotReadConcurrency) {
    return absl::InvalidArgumentError(absl::StrCat(
        "replication snapshot read concurrency must be between 1 and ",
        kMaxReplicationSnapshotReadConcurrency));
  }
  snapshot_read_concurrency_.store(concurrency, std::memory_order_release);
  return absl::OkStatus();
}

auto ReplicationManager::ReplicationGroup::snapshot_read_concurrency()
    const noexcept -> unsigned {
  return snapshot_read_concurrency_.load(std::memory_order_acquire);
}

auto ReplicationManager::ReplicationGroup::SetSnapshotBatchSize(
    std::size_t count) noexcept -> absl::Status {
  if (count == 0 || count > kMaxReplicationSnapshotBatchSize) {
    return absl::InvalidArgumentError(
        absl::StrCat("replication snapshot batch size must be between 1 and ",
                     kMaxReplicationSnapshotBatchSize));
  }
  snapshot_batch_size_.store(count, std::memory_order_release);
  return absl::OkStatus();
}

auto ReplicationManager::ReplicationGroup::snapshot_batch_size() const noexcept
    -> std::size_t {
  return snapshot_batch_size_.load(std::memory_order_acquire);
}

auto ReplicationManager::ReplicationGroup::SetReplicaPriority(
    unsigned priority) noexcept -> absl::Status {
  replica_priority_.store(priority, std::memory_order_release);
  return absl::OkStatus();
}

auto ReplicationManager::ReplicationGroup::replica_priority() const noexcept
    -> unsigned {
  return replica_priority_.load(std::memory_order_acquire);
}

auto ReplicationManager::ReplicationGroup::BacklogCapacityForFlow(
    unsigned flow_id, std::size_t global_bytes) const noexcept -> std::size_t {
  const std::size_t total_blocks = global_bytes / storage::kStorageBlockBytes;
  const std::size_t workers = storage_->worker_count();
  const std::size_t blocks =
      total_blocks / workers + (flow_id < total_blocks % workers ? 1 : 0);
  return blocks * storage::kStorageBlockBytes;
}

auto ReplicationManager::ReplicationGroup::SetBacklogSizeBytes(
    std::size_t bytes) -> Task<absl::Status> {
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
    const std::size_t flow_capacity = BacklogCapacityForFlow(worker, effective);
    absl::Status configured;
    if (worker == 0) {
      configured = co_await storage_->SetReplicationLogCapacity(flow_capacity);
      if (configured.ok() && !retained_histories_.empty())
        retained_histories_[worker]->SetCapacity(flow_capacity);
    } else {
      configured = co_await bycorf::SubmitTaskTo(
          worker, [this, worker, flow_capacity]() -> Task<absl::Status> {
            auto status =
                co_await storage_->SetReplicationLogCapacity(flow_capacity);
            if (status.ok() && !retained_histories_.empty())
              retained_histories_[worker]->SetCapacity(flow_capacity);
            co_return status;
          });
    }
    if (!configured.ok()) co_return configured;
  }
  backlog_size_bytes_.store(effective, std::memory_order_release);
  co_return absl::OkStatus();
}

auto ReplicationManager::ReplicationGroup::backlog_size_bytes() const noexcept
    -> std::size_t {
  return backlog_size_bytes_.load(std::memory_order_acquire);
}

auto ReplicationManager::ReplicationGroup::SetBacklogBackpressure(bool enabled)
    -> Task<absl::Status> {
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

auto ReplicationManager::ReplicationGroup::backlog_backpressure() const noexcept
    -> bool {
  return backlog_backpressure_.load(std::memory_order_acquire);
}

auto ReplicationManager::ReplicationGroup::SetPublishQueueBytesPerWorker(
    std::size_t bytes) -> Task<absl::Status> {
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
      configured = co_await storage_->SetReplicationPublishQueueCapacity(bytes);
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

auto ReplicationManager::ReplicationGroup::publish_queue_bytes_per_worker()
    const noexcept -> std::size_t {
  return publish_queue_bytes_per_worker_.load(std::memory_order_acquire);
}

auto ReplicationManager::ReplicationGroup::ServeNativeConnection(
    TcpStream& stream, std::vector<std::string> args, std::uint64_t client_id,
    std::string client_address, bool tls) -> Task<absl::Status> {
  const bool recovery =
      !args.empty() && EqualCaseInsensitive(args.front(), "LVRECOVER");
  if (!recovery && is_loading()) {
    absl::Status sent = co_await WriteText(
        stream,
        "-LOADING node has no valid native replication source state\r\n");
    co_return sent.ok() ? absl::FailedPreconditionError(
                              "a fenced node cannot serve native replication")
                        : sent;
  }
  if (!recovery && is_replica()) {
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
    if (args.size() != 7 || !ParseUnsigned(args[2], &replication_session_id) ||
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
    absl::Status status =
        co_await ServeOwnedNativeConnection(stream, std::move(args), client_id);
    UnregisterClientConnection(client_id);
    if (!status.ok()) {
      spdlog::warn("replication native handshake failed: {}", status.message());
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
          return absl::InternalError("failed to adopt replication connection");
        }
        bycorf::ThisWorker().self_->Spawn(RunAdoptedConnection(
            registered, std::move(args), client_id, std::move(client_address),
            tls, replication_session_id));
        return absl::OkStatus();
      });
}

auto ReplicationManager::ReplicationGroup::AssertStateOwner() noexcept -> void {
  assert(bycorf::ThisWorker().self_ != nullptr &&
         bycorf::ThisWorker().id_ == 0);
}

auto ReplicationManager::ReplicationGroup::PublishUpstreamSnapshot() -> void {
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

auto ReplicationManager::ReplicationGroup::SetDesiredUpstream(
    std::optional<ReplicaOfConfig> upstream) -> void {
  AssertStateOwner();
  if (upstream_ == upstream) return;
  upstream_ = std::move(upstream);
  PublishUpstreamSnapshot();
}

auto ReplicationManager::ReplicationGroup::EmptyPopulationCurrent(
    const std::shared_ptr<ClusterRebuildContext>& context) -> bool {
  AssertStateOwner();
  return cluster_rebuild_ == context && !replica_reconfiguration_running_ &&
         !cluster_control_stopping_ &&
         !failed_stopped_.load(std::memory_order_relaxed) &&
         context->state_.load(std::memory_order_relaxed) ==
             ReplicationGroupState::kRebuilding;
}

auto ReplicationManager::ReplicationGroup::FinishEmptyPopulationFailure(
    const std::shared_ptr<ClusterRebuildContext>& context,
    std::uint64_t session_id, bool root_started, bool promoted,
    absl::Status failure) -> Task<absl::Status> {
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

auto ReplicationManager::ReplicationGroup::RunEmptyPopulationInitialization(
    std::shared_ptr<ClusterRebuildContext> context) -> Task<absl::Status> {
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
            session_id, partition.partition_id_, partition.replication_epoch_);
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
    catalog =
        absl::InternalError("injected empty-population catalog result failure");
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
        worker, [this, child_log_epoch, flow_capacity]() -> Task<absl::Status> {
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
  auto ready = group_ready.ok()
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
        absl::CancelledError("empty population completed after supersession"));
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

auto ReplicationManager::ReplicationGroup::StorageIsReady() const noexcept
    -> bool {
  return ready_workers_.load(std::memory_order_acquire) ==
         storage_->worker_count();
}

auto ReplicationManager::ReplicationGroup::WaitUntilStorageReady()
    -> Task<absl::Status> {
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

auto ReplicationManager::ReplicationGroup::StartCoordinator() -> void {
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
  if (initial_redis_connection_pending_) {
    if (coordinator_started_) return;
    coordinator_started_ = true;
    bycorf::ThisWorker().self_->SpawnRoot(ConnectInitialRedisUpstream());
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

auto ReplicationManager::ReplicationGroup::LatchReplicationFailure(
    std::string reason) -> void {
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
  if (meta_managed_) {
    bycorf::ThisWorker().self_->Spawn(
        RevokeClusterRebuildSourceAuthorizations());
  }
  spdlog::critical("replication failed-stopped until restart: {}",
                   latched_reason);
}

auto ReplicationManager::ReplicationGroup::Coordinator() -> Task<absl::Status> {
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
          !session->destructive_root_started_.load(std::memory_order_acquire) &&
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
        (!cluster_ready || cluster_proof_invalidated || fail_stop.has_value());
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
      if (cluster_attempt_must_retire && cluster_rebuild_ == cluster_context) {
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
    // The pre-mutation FULL path has a three-retry budget; preserve its
    // lease-renewal window while ordinary following reconnects promptly.
    absl::Status slept = co_await bycorf::SleepFor(
        *bycorf::ThisWorker().self_,
        lease_admission_retry ? kReconnectDelay : kNativeReconnectDelay);
    if (!slept.ok()) {
      coordinator_started_ = false;
      co_return slept;
    }
  }
  coordinator_started_ = false;
  co_return absl::OkStatus();
}

auto ReplicationManager::ReplicationGroup::ReplayAndSwitchParent(
    const std::shared_ptr<ReplicaSession>& session,
    const std::shared_ptr<TimedSocketContext>& transfer)
    -> Task<absl::StatusOr<bool>> {
  const auto relationship = session->cluster_follow_;
  const auto population = cluster_rebuild_;
  const auto applied = applied_frontier_;
  const auto& desired = relationship->desired_;
  if (!population->manifest_.has_value()) co_return false;
  const auto current = [&] {
    return active_replica_session_ == session &&
           cluster_follow_owner_ == relationship &&
           cluster_rebuild_ == population && applied_frontier_ == applied &&
           !session->cancelled() && !transfer->sockets_.cancelled() &&
           cluster::LeaseClockNow() < transfer->deadline_;
  };
  const auto& identity = population->ready_token_->identity();
  const ClusterFailoverCompatibilityDomain parent{
      identity.term_,
      identity.source_node_id_,
      identity.source_assignment_id_,
      identity.source_boot_id_,
      identity.source_history_id_,
      static_cast<unsigned>(applied->size())};
  auto cursor = applied->TrySnapshot();
  if (!cursor.ok()) co_return cursor.status();
  auto connected = co_await ConnectTcp(
      desired.owner_endpoint_->host_, desired.owner_endpoint_->port_,
      tls_context_, &transfer->sockets_, /*cancellable_dns=*/true);
  if (!connected.ok()) co_return connected.status();
  TcpStream stream = std::move(*connected);
  ScopedSocketSetMembership membership(&transfer->sockets_, stream.NativeFd());
  auto status = co_await AuthenticateUpstream(stream, masteruser_, masterauth_);
  if (!status.ok()) co_return status;
  const auto request =
      EncodeRespCommand(ParentHandshake(desired, parent, *cursor));
  status = co_await WriteText(stream, request);
  if (!status.ok()) co_return status;
  auto line = co_await ReadLine(stream);
  if (!line.ok()) co_return line.status();
  const auto words = SplitWords(*line);
  if (words.size() == 4 && words[0] == "-LVPARENTFULL" &&
      words[1] == desired.owner_node_id_ && IsReplicationId(words[2]) &&
      IsReplicationId(words[3]))
    co_return false;
  // No partial attempt has been admitted before the exact export-ready
  // Owner response. A lease/FDS race or unavailable Owner can retry without
  // prematurely choosing FULL or discarding the target's parent proof.
  if (words.size() != 8 || words[0] != "+LVPARENT" ||
      !IsReplicationId(words[1]) || words[2] != desired.owner_node_id_ ||
      !IsReplicationId(words[3]) || !IsReplicationId(words[4]) ||
      words[5] != parent.source_history_id_)
    co_return absl::UnavailableError(
        "current Owner has not admitted partial reparent");
  auto boundary = DecodeAppliedVector(words[6]);
  auto origin = DecodeAppliedVector(words[7]);
  if (!boundary.ok() || !origin.ok() ||
      boundary->size() != parent.flow_count_ || origin->empty() ||
      origin->size() > 1024 ||
      std::ranges::any_of(*origin, [](auto value) { return value != 1; }))
    co_return false;
  detail::NativeHistoryBridge bridge;
  bridge.id_ = words[1];
  bridge.group_id_ = desired.group_id_;
  bridge.parent_ = parent;
  bridge.child_ = {
      desired.group_term_,          desired.owner_node_id_,
      desired.owner_assignment_id_, std::string(words[3]),
      std::string(words[4]),        static_cast<unsigned>(origin->size())};
  bridge.promotion_.frozen_applied_next_lsns_ = *boundary;
  bridge.child_origin_ = *origin;
  auto encoded =
      co_await ReadRecoveryFrame(stream, detail::kRecoveryMetadataBytes);
  if (!encoded.ok()) co_return false;
  auto report = detail::DecodeRecoveryAdvertisement(*encoded);
  if (!report.ok() || report->boot_id_ != bridge.child_.source_boot_id_ ||
      report->applied_ != *boundary ||
      detail::PlanNativeReparent(bridge, parent, *cursor, report->coverage_,
                                 true) == detail::NativeReparentPlan::kFull)
    co_return false;
  detail::NativeReplay replay(applied, retained_histories_,
                              parent.source_history_id_);
  const auto budget = std::make_shared<RecoveryReceiveBudget>();
  std::vector<std::unique_ptr<RecoveryReceivedEffect>> pending;
  while (current()) {
    bool advanced = false;
    for (auto item = pending.begin(); item != pending.end();) {
      auto effect = replay.PrepareEffect(std::move((*item)->records_));
      if (!effect.ok()) co_return false;
      if (effect->disposition_ ==
          detail::NativeReplayDisposition::kNeedsPredecessor) {
        (*item)->records_ = std::move(effect->records_);
        ++item;
        continue;
      }
      if (effect->disposition_ == detail::NativeReplayDisposition::kReady) {
        if (!current()) co_return false;
        // Parent replay extends a complete Active, including a certified
        // recovered root; it never writes into a FULL staging context.
        if (recovered_population_fenced_) storage_->SetReplicaLoading(false);
        status = co_await ApplyReplicatedCommand(effect->command_);
        if (status.ok())
          status = replay.PublishAfterApply(0, effect->updates_,
                                            std::move(effect->records_));
        if (!status.ok()) {
          const auto failure = absl::StrCat(
              "partial replay apply outcome is uncertain: ", status.message());
          session->RequireFailStop(failure);
          LatchReplicationFailure(failure);
          co_return status;
        }
        advanced = true;
      }
      item = pending.erase(item);
    }
    if (!current()) co_return false;
    cursor = applied->TrySnapshot();
    if (!cursor.ok()) co_return false;
    if (*cursor == *boundary) break;
    std::optional<unsigned> missing;
    for (unsigned flow = 0; flow < cursor->size(); ++flow) {
      if ((*cursor)[flow] >= (*boundary)[flow]) continue;
      const bool queued = std::ranges::any_of(pending, [&](const auto& item) {
        return std::ranges::any_of(item->records_, [&](const auto& record) {
          return record.flow_id_ == flow && record.lsn_ == (*cursor)[flow];
        });
      });
      if (!queued && detail::RecoveryCovers(*report, flow, (*cursor)[flow])) {
        missing = flow;
        break;
      }
    }
    if (!missing.has_value()) {
      if (advanced) continue;
      co_return false;
    }
    auto effect = co_await FetchRetainedEffect(stream, *report, budget,
                                               *missing, (*cursor)[*missing]);
    if (!effect.ok()) co_return false;
    pending.push_back(std::move(*effect));
  }
  if (!current()) co_return false;
  const auto switch_request =
      absl::StrCat("SWITCH ", EncodeAppliedVector(*boundary), "\r\n");
  status = co_await WriteText(stream, switch_request);
  if (!status.ok()) co_return false;
  line = co_await ReadLine(stream);
  if (!line.ok()) co_return false;
  const auto switched = SplitWords(*line);
  if (switched.size() != 2 || switched[0] != "+LVSWITCH" ||
      !IsReplicationId(switched[1]) || !current())
    co_return false;
  auto revision = cluster_group_->NextDirectiveRevision(desired.group_term_);
  if (!revision.ok()) co_return false;
  RebuildDirective child = population->directive_;
  auto& next = child.identity_;
  next.term_ = desired.group_term_;
  next.directive_revision_ = *revision;
  next.authority_id_ = "history-switch:" + bridge.id_;
  next.source_node_id_ = bridge.child_.source_node_id_;
  next.source_assignment_id_ = bridge.child_.source_assignment_id_;
  next.source_boot_id_ = bridge.child_.source_boot_id_;
  next.source_history_id_ = bridge.child_.source_history_id_;
  next.target_history_id_.clear();
  next.operation_id_ = next.directive_id_ = next.attempt_id_ =
      "history-switch:" + bridge.id_;
  child.flow_count_ = bridge.child_.flow_count_;
  child.safe_source_active_ = true;
  auto child_applied = std::make_shared<detail::ReplicaAppliedFrontier>(
      child.flow_count_, storage_->worker_count());
  status = child_applied->InstallNextLsns(*origin);
  if (!status.ok()) co_return false;
  auto adopted = std::make_shared<ClusterRebuildContext>(
      child, *population->manifest_, *population->ready_token_);
  NativeContinuationProof proof{
      desired.local_node_id_,   desired.local_assignment_id_,
      desired.local_boot_id_,   bridge.child_.source_history_id_,
      std::string(switched[1]), *origin};
  auto upstream_node = bridge.child_.source_node_id_;
  auto upstream_history = bridge.child_.source_history_id_;
  auto group = PopulationGroupToken(desired.group_id_);
  // Cache turnover is optional and cannot change Active's proof. Allocate it
  // before the atomic no-suspension identity/cursor/publication below.
  status = co_await ResetRetainedHistory(upstream_history, child.flow_count_);
  if (!status.ok() || !current()) co_return false;
  auto ready = cluster_group_->SwitchHistory(*population->ready_token_, child,
                                             *population->manifest_, *cursor,
                                             *boundary, *origin);
  if (!ready.ok()) co_return false;
  adopted->ready_token_ = std::move(*ready);
  cluster_rebuild_ = std::move(adopted);
  session->cluster_rebuild_ = cluster_rebuild_;
  applied_frontier_ = std::move(child_applied);
  upstream_node_id_ = std::move(upstream_node);
  upstream_history_id_ = std::move(upstream_history);
  group_id_ = std::move(group);
  source_worker_count_ = child.flow_count_;
  upstream_continuation_proof_ = std::move(proof);
  storage_->SetReplicaLoading(false);
  recovered_population_fenced_ = false;
  native_dataset_valid_.store(true, std::memory_order_release);
  // Local commit precedes ACK. Losing this write cannot undo Ready or make
  // an initial child cursor ambiguous on the following ordinary CONTINUE.
  (void)co_await WriteText(stream, "ACK\r\n");
  co_return true;
}

auto ReplicationManager::ReplicationGroup::TryPartialReparent(
    const std::shared_ptr<ReplicaSession>& session) -> Task<absl::Status> {
  AssertStateOwner();
  if (session->cluster_follow_ == nullptr || cluster_rebuild_ == nullptr ||
      !cluster_rebuild_->ready_token_.has_value() ||
      applied_frontier_ == nullptr ||
      !ClusterFollowReadyPopulationMatches(session->cluster_follow_->desired_))
    co_return absl::OkStatus();
  const auto& desired = session->cluster_follow_->desired_;
  if (session->cluster_follow_->force_full_.load(std::memory_order_acquire)) {
    co_return absl::OkStatus();
  }
  const auto& parent = cluster_rebuild_->ready_token_->identity();
  if (parent.source_node_id_.empty() ||
      (parent.term_ == desired.group_term_ &&
       parent.source_node_id_ == desired.owner_node_id_ &&
       parent.source_assignment_id_ == desired.owner_assignment_id_))
    co_return absl::OkStatus();
  constexpr std::size_t kMetadataBytes = 3 * detail::kRecoveryMetadataBytes;
  auto reservation = TryReserveMemory(kMetadataBytes);
  if (!reservation.has_value())
    co_return absl::ResourceExhaustedError(
        "partial reparent cannot reserve metadata memory");
  RetainedMemoryCharge charge;
  charge.Adopt(&*reservation, kMetadataBytes);
  // Population retirement waits on this same count as ordinary flows, so
  // replacement cannot reset storage while an accepted partial apply runs.
  session->active_flows_.fetch_add(1, std::memory_order_acq_rel);
  ReplicaFlowActivityGuard activity(&session->active_flows_);
  const auto transfer =
      std::make_shared<TimedSocketContext>(&session->sockets_);
  const auto relationship = session->cluster_follow_;
  bycorf::ThisWorker().self_->Spawn(
      WatchTimedSockets(transfer, [this, session, relationship] {
        return active_replica_session_ == session &&
               cluster_follow_owner_ == relationship && !session->cancelled();
      }));
  auto result = co_await ReplayAndSwitchParent(session, transfer);
  transfer->finished_ = true;
  transfer->sockets_.Cancel();
  while (!transfer->watcher_finished_) {
    auto waited = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                            std::chrono::milliseconds(1));
    if (!waited.ok()) co_return waited;
  }
  if (!result.ok()) co_return result.status();
  if (!*result && active_replica_session_ == session &&
      cluster_follow_owner_ == relationship && !session->cancelled()) {
    relationship->force_full_.store(true, std::memory_order_release);
  }
  co_return absl::OkStatus();
}

auto ReplicationManager::ReplicationGroup::RunReplicaSession(
    const ReplicaOfConfig& upstream, std::uint64_t role_epoch,
    const std::shared_ptr<ReplicaSession>& session) -> Task<absl::Status> {
  auto partial = co_await TryPartialReparent(session);
  if (!partial.ok()) co_return partial;
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
        !native_dataset_valid_.load(std::memory_order_acquire) ||
        (session->cluster_follow_ != nullptr &&
         session->cluster_follow_->force_full_.load(std::memory_order_acquire));
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
    const DesiredClusterUpstream& desired = session->cluster_follow_->desired_;
    sync_args.insert(
        sync_args.end(),
        {"FOLLOW", desired.group_id_, desired.local_assignment_id_,
         desired.owner_assignment_id_, absl::StrCat(desired.group_term_),
         desired.owner_node_id_, absl::StrCat(desired.manifest_revision_),
         desired.manifest_id_.Hex(),
         absl::StrCat(desired.partition_replication_epoch_)});
    if (upstream_continuation_proof_.has_value() &&
        upstream_continuation_proof_->target_node_id_ ==
            desired.local_node_id_ &&
        upstream_continuation_proof_->target_assignment_id_ ==
            desired.local_assignment_id_ &&
        upstream_continuation_proof_->target_boot_id_ ==
            desired.local_boot_id_ &&
        upstream_continuation_proof_->child_history_id_ == requested_history) {
      sync_args.insert(sync_args.end(),
                       {"ORIGIN", upstream_continuation_proof_->capability_});
      session->allow_initial_cursor_ = true;
    }
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
         absl::StrCat(identity.manifest_revision_), identity.manifest_id_.Hex(),
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
    const DesiredClusterUpstream& desired = session->cluster_follow_->desired_;
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
       session->cluster_follow_->force_full_.load(std::memory_order_acquire))) {
    absl::Status admitted = BeginClusterFollowFullPopulation(
        session, std::string(words[4]), std::string(words[5]), source_workers);
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
    if (!retained_histories_.empty()) {
      if (!upstream_history_id_.has_value() ||
          *upstream_history_id_ != words[5] ||
          source_worker_count_ != source_workers ||
          !local_population_matches_response) {
        auto reset = co_await ResetRetainedHistory(std::string(words[5]),
                                                   source_workers);
        if (!reset.ok()) co_return reset;
        if (active_replica_session_ != session || session->cancelled() ||
            role_epoch_.load(std::memory_order_relaxed) != role_epoch ||
            replica_reconfiguration_running_)
          co_return absl::CancelledError("native history install superseded");
      }
      session->retained_histories_ = retained_histories_;
      session->source_history_id_ = std::string(words[5]);
    }
    applied_frontier_ = next_frontier;
    session->applied_frontier_ = std::move(next_frontier);
    session->replay_ = std::make_shared<detail::NativeReplay>(
        session->applied_frontier_, session->retained_histories_,
        std::string(words[5]));
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
    absl::Status failed = online.ok()
                              ? absl::InvalidArgumentError(
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
    still_current = active_replica_session_ == session &&
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
  if (meta_managed_ && session->cluster_rebuild_ != nullptr &&
      session->cluster_rebuild_->ready_token_.has_value()) {
    auto cut = session->applied_frontier_->TrySnapshot();
    if (!cut.ok()) co_return cut.status();
    upstream_continuation_proof_ = NativeContinuationProof{
        node_id_,
        session->cluster_rebuild_->ready_token_->identity().assignment_id_,
        boot_id_,
        std::string(words[5]),
        session->flow_capability_,
        std::move(*cut)};
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

auto ReplicationManager::ReplicationGroup::CancelAndWaitForReplicaFlows(
    const std::shared_ptr<ReplicaSession>& session) -> Task<absl::Status> {
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
          absl::StrCat("replica cancellation could not resolve worker ", owner,
                       ": ", cancelled.message()));
      co_return cancelled;
    }
  }
  auto next_warning =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (session->active_flows_.load(std::memory_order_acquire) != 0 ||
         session->active_transaction_applies_.load(std::memory_order_acquire) !=
             0) {
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
          session->active_transaction_applies_.load(std::memory_order_acquire));
      next_warning = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    }
  }
  co_return absl::OkStatus();
}

auto ReplicationManager::ReplicationGroup::ValidateClusterResetBoundary(
    const std::shared_ptr<ClusterRebuildContext>& context)
    -> Task<absl::Status> {
  if (context == nullptr) co_return absl::OkStatus();
  auto validate = [this, context] {
    if (!context->authorization_.has_value())
      return absl::FailedPreconditionError(
          "Ready history switch has no destructive reset capability");
    return cluster_group_->ValidateResetAuthorization(*context->authorization_);
  };
  co_return bycorf::ThisWorker().id_ == 0
      ? validate()
      : co_await bycorf::SubmitTo(0, std::move(validate));
}

auto ReplicationManager::ReplicationGroup::RecordClusterResetProof(
    const std::shared_ptr<ClusterRebuildContext>& context,
    std::vector<storage::ReplicaPartitionEpoch> resets) -> Task<absl::Status> {
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

auto ReplicationManager::ReplicationGroup::RecordClusterHandoffProof(
    const std::shared_ptr<ClusterRebuildContext>& context,
    std::uint16_t partition_id, std::uint64_t target_local_epoch)
    -> Task<absl::Status> {
  if (context == nullptr) co_return absl::OkStatus();
  auto record = [this, context, partition_id, target_local_epoch] {
    return cluster_group_->RecordPartitionHandoff(
        context->directive_.identity_, partition_id,
        context->manifest_->logical_epochs()[partition_id], target_local_epoch);
  };
  co_return bycorf::ThisWorker().id_ == 0
      ? record()
      : co_await bycorf::SubmitTo(0, std::move(record));
}

auto ReplicationManager::ReplicationGroup::PrepareReplicaFlowMode(
    const std::shared_ptr<ReplicaSession>& session, unsigned flow_id,
    bool fullsync) -> Task<absl::Status> {
  absl::Status agreed =
      co_await session->flow_modes_selected_->Wait(*bycorf::ThisWorker().self_);
  if (!agreed.ok()) co_return agreed;
  if (!fullsync) co_return absl::OkStatus();

  if (flow_id == 0 && session->cluster_follow_ != nullptr &&
      session->cluster_rebuild_ != nullptr &&
      session->cluster_rebuild_->state_.load(std::memory_order_acquire) ==
          ReplicationGroupState::kReady) {
    // The authenticated source matched our history but no longer retains
    // every requested cursor. Do not invalidate the usable population in a
    // flow worker. Fail this session before BeginReplicaFullSync and let the
    // fixed-delay coordinator admit a fresh destructive FULL on worker zero.
    // Keep Active intact until that new control handshake succeeds.
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

auto ReplicationManager::ReplicationGroup::RunReplicaFlow(
    ReplicaOfConfig upstream, std::shared_ptr<ReplicaSession> session,
    unsigned flow_id) -> Task<absl::Status> {
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
  spdlog::info("replication target session {} flow {} requesting cursor={}:{}",
               session->session_id_, flow_id, cursor.lsn_,
               cursor.fragment_index_);
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

auto ReplicationManager::ReplicationGroup::WaitForReplicaTransaction(
    const std::shared_ptr<ReplicaTransactionArrival>& arrival)
    -> Task<absl::Status> {
  const storage::ReplicationTransactionResolution resolution =
      co_await arrival->completion_.Wait(*bycorf::ThisWorker().self_);
  if (resolution == storage::ReplicationTransactionResolution::kDiscard &&
      arrival->status_.ok()) {
    co_return absl::CancelledError(
        "replicated transaction was discarded before completion");
  }
  co_return arrival->status_;
}

auto ReplicationManager::ReplicationGroup::ApplyReadyReplicaTransaction(
    std::shared_ptr<ReplicaSession> session, unsigned owner,
    std::shared_ptr<ReplicaTransactionArrival> arrival) -> Task<absl::Status> {
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
    frontier_updates.push_back({.flow_id_ = participant, .applied_lsn_ = lsn});
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
    status = session->replay_->PublishAfterApply(
        owner, frontier_updates, std::move(arrival->canonical_records_));
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

auto ReplicationManager::ReplicationGroup::PrepareReplicaTransactionArrival(
    const std::shared_ptr<ReplicaSession>& session, unsigned flow_id,
    std::uint64_t lsn, ReplicatedCommand envelope,
    std::shared_ptr<ReplicaTransactionArrival> predecessor)
    -> absl::StatusOr<PreparedReplicaTransactionArrival> {
  auto record = detail::DecodeNativeTransactionRecord(
      std::move(envelope), flow_id, session->source_worker_count_);
  if (!record.ok()) return record.status();
  const bool has_payload = !record->payload_.args_.empty();
  return PreparedReplicaTransactionArrival{
      .id_ = record->envelope_.id_,
      .db_id_ = record->payload_.db_id_,
      .participants_ = std::move(record->envelope_.participants_),
      .command_args_ = std::move(record->payload_.args_),
      .predecessor_ = std::move(predecessor),
      .payload_flow_ = record->envelope_.payload_flow_,
      .flow_id_ = flow_id,
      .lsn_ = lsn,
      .has_payload_ = has_payload,
      .canonical_ = {},
      .canonical_charge_ = {},
  };
}

auto ReplicationManager::ReplicationGroup::RegisterReplicaTransactionOnOwner(
    const std::shared_ptr<ReplicaSession>& session, unsigned owner,
    PreparedReplicaTransactionArrival prepared)
    -> absl::StatusOr<std::shared_ptr<ReplicaTransactionArrival>> {
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
  if (prepared.canonical_charge_ != nullptr)
    arrival->canonical_charges_.push_back(
        std::move(prepared.canonical_charge_));
  if (!session->retained_histories_.empty())
    arrival->canonical_records_.push_back(
        {prepared.flow_id_, prepared.lsn_, std::move(prepared.canonical_)});
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

auto ReplicationManager::ReplicationGroup::RegisterReplicaTransaction(
    const std::shared_ptr<ReplicaSession>& session, unsigned flow_id,
    std::uint64_t lsn, ReplicatedCommand envelope,
    std::shared_ptr<ReplicaTransactionArrival> predecessor,
    std::string canonical,
    std::shared_ptr<CanonicalReceiveCharge> canonical_charge)
    -> Task<absl::StatusOr<std::shared_ptr<ReplicaTransactionArrival>>> {
  auto prepared = PrepareReplicaTransactionArrival(
      session, flow_id, lsn, std::move(envelope), std::move(predecessor));
  if (!prepared.ok()) co_return prepared.status();
  prepared->canonical_ = std::move(canonical);
  prepared->canonical_charge_ = std::move(canonical_charge);
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

auto ReplicationManager::ReplicationGroup::ApplyReplicaControl(
    const std::shared_ptr<ReplicaSession>& session, unsigned flow_id,
    std::uint64_t lsn, ReplicatedCommand command, std::string canonical)
    -> Task<absl::Status> {
  auto parsed = detail::NativeControlBarrierId(command);
  if (!parsed.ok()) co_return parsed.status();
  if (flow_id >= session->source_worker_count_)
    co_return absl::InvalidArgumentError("invalid replicated control flow");
  const std::uint64_t barrier_id = *parsed;

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
    if (!session->retained_histories_.empty())
      arrival->canonical_records_.push_back(
          {flow_id, lsn, std::move(canonical)});
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
      status = session->replay_->PublishAfterApply(
          bycorf::ThisWorker().id_, frontier_updates,
          std::move(arrival->canonical_records_));
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

#if LAVIK_FAULTS_ENABLED
auto ReplicationManager::ReplicationGroup::
    InjectPeerFlowCancelAfterCommandApply(
        const std::shared_ptr<ReplicaSession>& session, unsigned flow_id,
        const ReplicatedCommand& command) -> void {
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

auto ReplicationManager::ReplicationGroup::StageReplicaOnlineCommands(
    const std::shared_ptr<ReplicaSession>& session, unsigned flow_id,
    const std::shared_ptr<ReplicaOnlineApplyState>& state)
    -> Task<absl::Status> {
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
          last_transaction, std::move(pending.canonical_),
          std::move(pending.canonical_charge_));
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
                                               std::move(pending.command_),
                                               std::move(pending.canonical_));
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
      applied = session->replay_->PublishAfterApply(
          bycorf::ThisWorker().id_,
          {flow_id, pending.lsn_, std::move(pending.canonical_)});
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

auto ReplicationManager::ReplicationGroup::AckReplicaOnlineCommands(
    TcpStream& stream, const std::shared_ptr<ReplicaSession>& session,
    unsigned flow_id, const std::shared_ptr<ReplicaOnlineApplyState>& state)
    -> Task<absl::Status> {
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
      co_return absl::UnavailableError("replication flow staging queue closed");
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

auto ReplicationManager::ReplicationGroup::TrackReplicaOnlineStage(
    TcpStream& stream, const std::shared_ptr<ReplicaSession>& session,
    unsigned flow_id, const std::shared_ptr<ReplicaOnlineApplyState>& state)
    -> Task<absl::Status> {
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

auto ReplicationManager::ReplicationGroup::TrackReplicaOnlineAcks(
    TcpStream& stream, const std::shared_ptr<ReplicaSession>& session,
    unsigned flow_id, const std::shared_ptr<ReplicaOnlineApplyState>& state)
    -> Task<absl::Status> {
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

auto ReplicationManager::ReplicationGroup::RunReplicaOnlineFlowData(
    TcpStream& stream, const std::shared_ptr<ReplicaSession>& session,
    unsigned flow_id, std::uint64_t first_expected_lsn,
    std::pair<DataFrameKind, std::string> first_frame) -> Task<absl::Status> {
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
    auto canonical_charge =
        !session->retained_histories_.empty()
            ? TryReserveCanonical(session->canonical_receive_bytes_,
                                  staged_command.capacity())
            : nullptr;
    state->commands_.push_back(ReplicaOnlineCommand{
        .lsn_ = lsn,
        .command_ = std::move(*command),
        .canonical_ = canonical_charge != nullptr ? std::move(staged_command)
                                                  : std::string{},
        .canonical_charge_ = std::move(canonical_charge),
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

auto ReplicationManager::ReplicationGroup::SendReplicaFullSyncAck(
    TcpStream& stream, const std::shared_ptr<ReplicaHandoffState>& state,
    std::uint16_t partition, std::uint64_t sequence) -> Task<absl::Status> {
  std::string payload;
  PutU16(payload, partition);
  PutU64(payload, sequence);
  std::string frame;
  absl::Status encoded =
      ReserveReplicationString(&frame, kDataFrameHeaderBytes + payload.size());
  if (!encoded.ok()) co_return encoded;
  encoded = AppendDataFrame(&frame, DataFrameKind::kAck, payload);
  if (!encoded.ok()) co_return encoded;

  // All writers run on this flow's worker, but WriteAll can suspend after a
  // short write. Keep another ACK from interleaving with the remaining bytes.
  co_await state->ack_mutex_.Lock();
  absl::Status sent = state->status_;
  if (sent.ok() && state->stopping_) {
    sent = absl::CancelledError("FULL ACK sender is stopping");
  }
  if (sent.ok()) {
    sent = co_await WriteText(stream, frame);
  }
  state->ack_mutex_.Unlock(*bycorf::ThisWorker().self_);
  co_return sent;
}

auto ReplicationManager::ReplicationGroup::ApplyReplicaHandoff(
    const std::shared_ptr<ReplicaSession>& session,
    const std::shared_ptr<ReplicaHandoffState>& state, std::uint16_t partition,
    std::uint64_t epoch) -> Task<absl::Status> {
  LAVIK_FAULT_INJECT(
      if (const char* hold = std::getenv(
              "LAVIK_REPLICATION_HOLD_FIRST_HANDOFF_UNTIL_NEXT_ACK");
          partition == 0 && hold != nullptr) {
        spdlog::info("holding first partition handoff until a later ACK");
        while ((state->completed_ == 0 || std::string_view(hold) == "cancel") &&
               !state->stopping_ && state->status_.ok() &&
               !session->cancelled()) {
          co_await state->changed_.Wait();
        }
      });
  if (state->stopping_ || session->cancelled()) {
    co_return absl::CancelledError("partition handoff cancelled");
  }
  const unsigned owner = partition % storage_->worker_count();
  absl::Status handed_off =
      co_await bycorf::SubmitTaskTo(owner, [this, session, partition, epoch]() {
        return storage_->HandoffReplicaPartition(session->session_id_,
                                                 partition, epoch);
      });
  if (!handed_off.ok()) co_return handed_off;
  if (state->stopping_ || session->cancelled()) {
    co_return absl::CancelledError("partition handoff superseded");
  }
  co_return co_await RecordClusterHandoffProof(session->cluster_rebuild_,
                                               partition, epoch);
}

auto ReplicationManager::ReplicationGroup::TrackReplicaHandoff(
    TcpStream& stream, const std::shared_ptr<ReplicaSession>& session,
    const std::shared_ptr<ReplicaHandoffState>& state, std::uint16_t partition,
    std::uint64_t epoch, std::uint64_t sequence) -> Task<absl::Status> {
  absl::Status result =
      co_await ApplyReplicaHandoff(session, state, partition, epoch);
  if (result.ok() && (state->stopping_ || session->cancelled())) {
    result = absl::CancelledError("partition handoff superseded before ACK");
  }
  if (result.ok()) {
    result =
        co_await SendReplicaFullSyncAck(stream, state, partition, sequence);
  }
  if (!result.ok()) {
    if (state->status_.ok()) state->status_ = result;
    session->Cancel();
  } else {
    ++state->completed_;
    LAVIK_FAULT_INJECT(
        if (std::getenv(
                "LAVIK_REPLICATION_HOLD_FIRST_HANDOFF_UNTIL_NEXT_ACK") !=
                nullptr &&
            (state->completed_ == 1 || partition == 0)) {
          spdlog::info("acknowledged async partition handoff {}", partition);
        });
  }
  state->pending_[partition] = false;
  --state->active_;
  state->changed_.NotifyAll(*bycorf::ThisWorker().self_);
  co_return result;
}

auto ReplicationManager::ReplicationGroup::RunReplicaFlowData(
    TcpStream& stream, const std::shared_ptr<ReplicaSession>& session,
    unsigned flow_id) -> Task<absl::Status> {
  auto state = std::make_shared<ReplicaHandoffState>();
  absl::Status result =
      co_await ReceiveReplicaFlowData(stream, session, flow_id, state);
  // No task may retain the stream or mutate this attempt after its owning
  // flow returns to the coordinator's abort/reparent cleanup.
  state->stopping_ = true;
  if (!result.ok()) {
    session->Cancel();
    (void)::shutdown(stream.NativeFd(), SHUT_RDWR);
  }
  state->changed_.NotifyAll(*bycorf::ThisWorker().self_);
  while (state->active_ != 0) co_await state->changed_.Wait();
  if (!state->status_.ok()) co_return state->status_;
  co_return result;
}

auto ReplicationManager::ReplicationGroup::WaitReplicaHandoffs(
    const std::shared_ptr<ReplicaSession>& session,
    const std::shared_ptr<ReplicaHandoffState>& state,
    std::optional<std::uint16_t> partition, std::size_t active_limit)
    -> Task<absl::Status> {
  while (state->status_.ok() &&
         (partition.has_value() ? state->pending_[*partition]
                                : state->active_ > active_limit)) {
    // Cancel closes session sockets, but a completion wait owns no pending
    // socket I/O. Bound cancellation latency before joining handoff tasks.
    if (session->cancelled()) {
      co_return absl::CancelledError("handoff wait cancelled");
    }
    absl::Status waited = co_await bycorf::SleepFor(
        *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
    if (!waited.ok()) co_return waited;
  }
  if (session->cancelled()) {
    co_return absl::CancelledError("handoff wait cancelled");
  }
  co_return state->status_;
}

auto ReplicationManager::ReplicationGroup::ReceiveReplicaFlowData(
    TcpStream& stream, const std::shared_ptr<ReplicaSession>& session,
    unsigned flow_id, const std::shared_ptr<ReplicaHandoffState>& state)
    -> Task<absl::Status> {
  absl::flat_hash_map<std::uint16_t, std::uint64_t> epochs;
  std::uint64_t expected_fullsync_sequence = 1;
  std::optional<std::uint64_t> online_next_lsn;
  std::uint64_t staged_command_lsn = 0;
  std::uint32_t next_command_fragment = 0;
  std::string staged_command;
  auto send_ack = [this, &stream, &state](std::uint16_t partition_id,
                                          std::uint64_t sequence) {
    return SendReplicaFullSyncAck(stream, state, partition_id, sequence);
  };
  while (stream.IsOpen()) {
    auto frame = co_await ReadDataFrame(stream);
    if (!frame.ok()) co_return frame.status();
    if (!state->status_.ok()) co_return state->status_;
    absl::Status phase = session->ValidateDataFramePhase(flow_id, frame->first);
    if (!phase.ok()) co_return phase;
    if (frame->first == DataFrameKind::kReset ||
        frame->first == DataFrameKind::kFullSyncCut ||
        frame->first == DataFrameKind::kCursor ||
        frame->first == DataFrameKind::kCommand) {
      absl::Status waited = co_await WaitReplicaHandoffs(session, state);
      if (!waited.ok()) co_return waited;
    }
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
            co_return absl::InvalidArgumentError("malformed replication reset");
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
      absl::Status acknowledged = co_await send_ack(kResetBatchAckPartition, 0);
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
      if (state->pending_[partition_id]) {
        co_return absl::InvalidArgumentError("overlapping partition handoff");
      }
      absl::Status waited = co_await WaitReplicaHandoffs(
          session, state, std::nullopt, kFullSyncHandoffWindow - 1);
      if (!waited.ok()) co_return waited;
      state->pending_[partition_id] = true;
      ++state->active_;
      // Receive order remains contiguous; completion and ACK order need not.
      ++expected_fullsync_sequence;
      bycorf::ThisWorker().self_->Spawn(
          TrackReplicaHandoff(stream, session, state, partition_id,
                              epochs.at(partition_id), sequence));
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
        co_return absl::InvalidArgumentError("invalid full-sync command flags");
      }
      const bool first = (flags & first_flag) != 0;
      const bool last = (flags & last_flag) != 0;
      absl::Status waited =
          co_await WaitReplicaHandoffs(session, state, partition_id);
      if (!waited.ok()) co_return waited;
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
        const bool publish = command.ok() && !command->args_.empty() &&
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
            publish || (command.ok() && !command->args_.empty() &&
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
          promoted = absl::InternalError("injected replica promotion failure");
        } else {
          promoted =
              co_await storage_->PromoteReplicaRoot(session->session_id_);
        }
        if (!promoted.ok()) {
          if (session->cluster_rebuild_ != nullptr) {
            (void)cluster_group_->FailStop(
                session->cluster_rebuild_->directive_.identity_);
          }
          session->RequireFailStop(absl::StrCat(
              "replica promotion outcome is uncertain: ", promoted.message()));
          spdlog::warn(
              "replica session entered fail-stop before coordinator latch");
          LAVIK_FAULT_INJECT(
              if (const char* configured =
                      std::getenv("LAVIK_REPLICATION_PAUSE_AFTER_FAIL_STOP_MS");
                  configured != nullptr) {
                std::uint64_t pause_ms = 0;
                const std::size_t length = std::strlen(configured);
                const auto parsed =
                    std::from_chars(configured, configured + length, pause_ms);
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
        if (!session->retained_histories_.empty()) {
          auto reset = co_await ResetRetainedHistory(
              session->source_history_id_, session->source_worker_count_);
          if (reset.ok() && session->cancelled())
            reset = absl::CancelledError("full-sync history install cancelled");
          if (!reset.ok()) {
            session->promotion_complete_->Abort(reset);
            co_return reset;
          }
        }
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
        spdlog::warn("injected disconnect after full-sync cut acknowledgement");
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
      absl::Status waited =
          co_await WaitReplicaHandoffs(session, state, partition_id);
      if (!waited.ok()) co_return waited;
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
      co_return absl::InvalidArgumentError("unexpected replication data frame");
    }
  }
  co_return absl::UnavailableError("replication flow closed");
}

auto ReplicationManager::ReplicationGroup::ShouldInjectFlowDrop(
    unsigned flow_id) -> bool {
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

auto ReplicationManager::ReplicationGroup::
    ShouldInjectControlDropAfterResponse() -> bool {
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

auto ReplicationManager::ReplicationGroup::ShouldInjectFullSyncCutDrop(
    unsigned flow_id) -> bool {
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

auto ReplicationManager::ReplicationGroup::ShouldInjectReplicaPromotionFailure()
    -> bool {
  LAVIK_FAULT_INJECT({
    const char* configured = std::getenv("LAVIK_REPLICATION_FAIL_PROMOTE_ONCE");
    if (configured == nullptr || std::string_view(configured) != "1") {
      return false;
    }
    return !replication_promotion_fault_used_.exchange(
        true, std::memory_order_acq_rel);
  });
  return false;
}

auto ReplicationManager::ReplicationGroup::
    ShouldInjectEmptyPopulationResetFailure(unsigned owner) -> bool {
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

auto ReplicationManager::ReplicationGroup::
    ShouldInjectEmptyPopulationCatalogFailure() -> bool {
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

auto ReplicationManager::ReplicationGroup::ShouldInjectPromotionPrepareFailure(
    std::string_view stage) -> bool {
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

auto ReplicationManager::ReplicationGroup::ShouldInjectEarlyOnline() const
    -> bool {
  return LAVIK_FAULT_MATCHES("LAVIK_REPLICATION_EARLY_ONLINE", "1");
}

auto ReplicationManager::ReplicationGroup::ShouldInjectPostCutReset(
    unsigned flow_id) -> bool {
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

auto ReplicationManager::ReplicationGroup::ShouldInjectDivergentTail(
    unsigned flow_id) -> bool {
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

auto ReplicationManager::ReplicationGroup::InvalidateReplicaContinuation(
    const std::shared_ptr<ReplicaSession>& session,
    bool require_installed_cursor) -> Task<absl::Status> {
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

auto ReplicationManager::ReplicationGroup::ShouldInjectFlowDropAfterTransaction(
    unsigned flow_id) -> bool {
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

auto ReplicationManager::ReplicationGroup::
    ShouldInjectFlowDropAfterCommandApply(unsigned flow_id) -> bool {
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
auto ReplicationManager::ReplicationGroup::MaybePauseBeforeReplicaCommandApply()
    -> Task<absl::Status> {
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
  co_return co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                      std::chrono::milliseconds(milliseconds));
}
#endif

#if LAVIK_FAULTS_ENABLED
auto ReplicationManager::ReplicationGroup::
    MaybePauseBeforeReplicaTransactionApply() -> Task<absl::Status> {
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
  co_return co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                      std::chrono::milliseconds(milliseconds));
}
#endif

#if LAVIK_FAULTS_ENABLED
auto ReplicationManager::ReplicationGroup::MaybePauseBeforeReplicaControlApply()
    -> Task<absl::Status> {
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
  co_return co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                      std::chrono::milliseconds(milliseconds));
}
#endif

auto ReplicationManager::ReplicationGroup::ReceiveFullSyncAcks(
    TcpStream& stream, const std::shared_ptr<MasterSession>& session,
    unsigned flow, const std::shared_ptr<FullSyncAckState>& state)
    -> Task<absl::Status> {
  absl::Status result;
  while (!state->stopping_) {
    while (!state->stopping_ && state->expected_.empty() &&
           state->handoffs_.inflight() == 0) {
      co_await state->changed_.Wait();
    }
    if (state->stopping_) break;
    auto frame = co_await ReadDataFrame(stream);
    if (!frame.ok()) {
      result = frame.status();
      break;
    }
    DataReader reader(frame->second);
    std::uint16_t partition = 0;
    std::uint64_t sequence = 0;
    if (frame->first != DataFrameKind::kAck || !reader.U16(&partition) ||
        !reader.U64(&sequence) || reader.remaining() != 0) {
      result = absl::InvalidArgumentError("malformed full-sync ACK");
      break;
    }
    auto handoff = state->handoffs_.Acknowledge(partition, sequence);
    if (!handoff.ok()) {
      result = handoff.status();
      break;
    }
    if (!*handoff) {
      const auto expected = state->expected_.find(sequence);
      if (expected == state->expected_.end() || expected->second != partition) {
        result = absl::InvalidArgumentError("unexpected full-sync ACK");
        break;
      }
      state->expected_.erase(expected);
    }
    session->TouchProgress(flow);
    state->changed_.NotifyAll(*bycorf::ThisWorker().self_);
  }
  state->status_ = result;
  state->receiver_done_ = true;
  state->changed_.NotifyAll(*bycorf::ThisWorker().self_);
  if (!result.ok()) {
    // The sender may be waiting at a cross-flow cut barrier rather than
    // awaiting this ACK. Abort the whole session to wake both kinds of wait.
    session->Cancel();
  }
  co_return result;
}

auto ReplicationManager::ReplicationGroup::WaitFullSyncRequest(
    const std::shared_ptr<FullSyncAckState>& state, std::uint64_t sequence)
    -> Task<absl::Status> {
  while (state->status_.ok() && state->expected_.contains(sequence)) {
    co_await state->changed_.Wait();
  }
  co_return state->status_;
}

auto ReplicationManager::ReplicationGroup::SendFullSyncRequest(
    TcpStream& stream, const std::shared_ptr<FullSyncAckState>& state,
    DataFrameKind kind, std::string_view body, std::uint16_t partition,
    std::uint64_t sequence) -> Task<absl::Status> {
  if (!state->status_.ok()) co_return state->status_;
  if (!state->expected_.emplace(sequence, partition).second) {
    co_return absl::InternalError("overlapping full-sync ACK sequence");
  }
  state->changed_.NotifyAll(*bycorf::ThisWorker().self_);
  std::string payload;
  // RESET predates the sequenced FULL frames and retains its wire layout.
  if (kind != DataFrameKind::kReset) PutU64(payload, sequence);
  PutString(payload, body);
  absl::Status sent = co_await WriteDataFrame(stream, kind, payload);
  if (!sent.ok()) co_return sent;
  co_return co_await WaitFullSyncRequest(state, sequence);
}

auto ReplicationManager::ReplicationGroup::RunMasterFlowData(
    TcpStream& stream, const std::shared_ptr<MasterSession>& session,
    unsigned flow_id) -> Task<absl::Status> {
  auto state =
      std::make_shared<FullSyncAckState>(flow_id, storage_->worker_count());
  bycorf::ThisWorker().self_->Spawn(
      ReceiveFullSyncAcks(stream, session, flow_id, state));
  auto cursor = co_await RunMasterFullSync(stream, session, flow_id, state);
  // The reader must relinquish the socket before ONLINE starts reading its
  // own ACKs. On failure, unblock any outstanding read before joining it.
  state->stopping_ = true;
  state->changed_.NotifyAll(*bycorf::ThisWorker().self_);
  if (!cursor.ok()) (void)::shutdown(stream.NativeFd(), SHUT_RDWR);
  while (!state->receiver_done_) co_await state->changed_.Wait();
  if (!state->status_.ok()) co_return state->status_;
  if (!cursor.ok()) co_return cursor.status();
  state.reset();
  co_return co_await EnterMasterFlowBacklog(stream, session, flow_id, *cursor,
                                            0);
}

auto ReplicationManager::ReplicationGroup::RunMasterFullSync(
    TcpStream& stream, const std::shared_ptr<MasterSession>& session,
    unsigned flow_id, const std::shared_ptr<FullSyncAckState>& ack_state)
    -> Task<absl::StatusOr<std::uint64_t>> {
  const std::uint8_t db_count = storage_->database_count();
  auto fullsync_start = storage_->BeginFullSyncSession(session->id_);
  if (!fullsync_start.ok()) co_return fullsync_start.status();
  const auto source_db_epochs = fullsync_start->db_epochs_;
  bool fullsync_session_active = true;
  const auto initial_log = storage_->LocalReplicationLogInfo();
  const std::uint64_t backlog_start_lsn =
      initial_log.tail_lsn_ == 0 ? 1 : initial_log.tail_lsn_ + 1;
  session->SetProgress(flow_id, ReplicationPhase::kReset, backlog_start_lsn, 0,
                       0, 0);
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
      storage_->EndPartitionReplication(session->id_, partition.partition_id_);
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
      absl::Status sent = co_await SendFullSyncRequest(
          stream, ack_state, DataFrameKind::kRecords, payload, partition_id,
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
        if (item.command_ != nullptr || item.record_->mutation_sequence_ == 0) {
          co_return absl::InvalidArgumentError(
              "invalid record in full-sync publish queue");
        }
        const std::uint16_t partition_id =
            storage::RedisSlot(item.record_->key_);
        auto materialized = co_await storage_->MaterializeFullSyncPublishRecord(
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
      constexpr std::size_t kWireOverhead =
          kDataFrameHeaderBytes + kFullSyncSequenceBytes + kCommandHeaderBytes;
      static_assert(kBacklogBatchBytes > kWireOverhead);
      constexpr std::size_t kFragmentBytes = kBacklogBatchBytes - kWireOverhead;

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
          const std::size_t count =
              std::min(kFragmentBytes, encoded.payload_bytes_ - command_offset);
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
            flags |=
                static_cast<std::uint8_t>(storage::ReplicationFrameFlag::kLast);
          }
          std::string payload;
          payload.reserve(kFullSyncSequenceBytes + kCommandHeaderBytes + count);
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
            ack_state->expected_.emplace(fullsync_sequence,
                                         encoded.item_.command_->partition_id_);
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
          wire_batch.push_back(iovec{
              .iov_base = frame_headers.data() + index * kDataFrameHeaderBytes,
              .iov_len = kDataFrameHeaderBytes});
          wire_batch.push_back(iovec{.iov_base = frame_payloads[index].data(),
                                     .iov_len = frame_payloads[index].size()});
        }
        ack_state->changed_.NotifyAll(*bycorf::ThisWorker().self_);
        absl::Status sent = co_await stream.WriteAllV(wire_batch);
        if (!sent.ok()) co_return sent;
        // Sending bytes is protocol progress even when the command's final
        // fragment (and therefore its ACK) is still minutes away.
        session->TouchProgress(flow_id);
        for (const PendingFullSyncAck& expected : pending_acks) {
          absl::Status acknowledged =
              co_await WaitFullSyncRequest(ack_state, expected.sequence_);
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
    std::vector<std::string> args{
        "FUNCTION", "RESTORE", GlobalFunctionCatalog().SnapshotDump(), "FLUSH"};
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
      if (last) {
        ack_state->expected_.emplace(sequence, 0);
        ack_state->changed_.NotifyAll(*bycorf::ThisWorker().self_);
      }
      absl::Status sent = co_await WriteDataFrame(
          stream, DataFrameKind::kFullSyncCommand, payload);
      if (!sent.ok()) co_return sent;
      session->TouchProgress(flow_id);
      if (last) {
        co_return co_await WaitFullSyncRequest(ack_state, sequence);
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
      absl::Status sent = co_await send_records(partition_id, batch->records_);
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
    // This is an in-flight ceiling, never a batch-fill threshold. Every
    // ready partition is sent immediately whenever a slot is available.
    while (ack_state->status_.ok() &&
           ack_state->handoffs_.inflight() >= kFullSyncHandoffWindow) {
      co_await ack_state->changed_.Wait();
    }
    if (!ack_state->status_.ok()) co_return ack_state->status_;
    absl::Status begun =
        ack_state->handoffs_.Begin(partition_id, fullsync_sequence);
    if (!begun.ok()) co_return begun;
    ack_state->changed_.NotifyAll(*bycorf::ThisWorker().self_);
    std::string payload;
    PutU64(payload, fullsync_sequence++);
    PutU16(payload, partition_id);
    // Handoff proves partition completion; the final cut supplies the
    // stable cursor for the entire flow.
    PutU64(payload, 1);
    absl::Status sent = co_await WriteDataFrame(
        stream, DataFrameKind::kPartitionHandoff, payload);
    if (!sent.ok()) co_return sent;
    session->TouchProgress(flow_id);
    LAVIK_FAULT_INJECT(if (const char* configured =
                               std::getenv("LAVIK_REPLICATION_PAUSE_FULLSYNC_"
                                           "AFTER_HANDOFF_MS");
                           configured != nullptr &&
                           !replication_fullsync_handoff_pause_used_.exchange(
                               true, std::memory_order_acq_rel)) {
      while (ack_state->status_.ok() &&
             !ack_state->handoffs_.acknowledged(partition_id)) {
        co_await ack_state->changed_.Wait();
      }
      if (!ack_state->status_.ok()) co_return ack_state->status_;
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

  // Empty scans may complete without suspending. Yield periodically for
  // foreground work and ACK processing without imposing a fixed delay.
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
    absl::Status sent =
        co_await SendFullSyncRequest(stream, ack_state, DataFrameKind::kReset,
                                     reset, kResetBatchAckPartition, 0);
    if (!sent.ok()) {
      cleanup();
      co_return sent;
    }
    LAVIK_FAULT_INJECT(
        if (const char* configured =
                std::getenv("LAVIK_REPLICATION_PAUSE_FULLSYNC_AFTER_RESET_MS");
            configured != nullptr && !replication_fullsync_pause_used_.exchange(
                                         true, std::memory_order_acq_rel)) {
          std::uint64_t pause_ms = 0;
          const std::size_t length = std::strlen(configured);
          const auto parsed =
              std::from_chars(configured, configured + length, pause_ms);
          if (parsed.ec == std::errc{} && parsed.ptr == configured + length &&
              pause_ms != 0) {
            absl::Status paused =
                co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
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
          co_await bycorf::Yield(*bycorf::ThisWorker().self_);
        }
        continue;
      }
      auto start = storage_->BeginPartitionReplication(session->id_,
                                                       partition_id, db_count);
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
      for (std::uint8_t db_id = 0; db_id < db_count; ++db_id) {
        auto skipped = storage_->TrySkipEmptyPartitionDbReplication(
            session->id_, partition_id, db_id);
        if (!skipped.ok()) {
          cleanup();
          co_return skipped.status();
        }
        if (*skipped) continue;
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
        {
          // An admitted write may have populated this DB while Begin waited.
          // Scan after admission drains instead of using an earlier empty
          // observation to decide whether baseline records are needed.
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
            absl::Status published = co_await drain_interleaved_publish_queue();
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
          absl::Status db_completed = storage_->CompletePartitionDbReplication(
              session->id_, partition_id, db_id);
          if (db_completed.ok()) break;
          if (db_completed.code() != absl::StatusCode::kUnavailable) {
            cleanup();
            co_return db_completed;
          }
        }
      }
      if ((++processed_partitions & 63U) == 0) {
        co_await bycorf::Yield(*bycorf::ThisWorker().self_);
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
        absl::Status drained = co_await drain_partition_overrides(partition_id);
        if (!drained.ok()) {
          cleanup();
          co_return drained;
        }
        absl::Status completed =
            storage_->CompletePartitionReplication(session->id_, partition_id);
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
  // Keep the live publisher moving while ACKs are outstanding. Merely
  // sending every partition must not release the all-flow cut barrier.
  while (ack_state->handoffs_.unfinished() != 0) {
    if (!ack_state->status_.ok()) {
      cleanup();
      co_return ack_state->status_;
    }
    absl::Status published =
        co_await drain_fullsync_publish_queue(kFullSyncReadyWaitCommands);
    if (!published.ok()) {
      cleanup();
      co_return published;
    }
    if (session->cancelled()) {
      cleanup();
      co_return absl::CancelledError("session ended while awaiting handoffs");
    }
    if (ack_state->handoffs_.unfinished() != 0) {
      absl::Status waited = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) {
        cleanup();
        co_return waited;
      }
    }
  }
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
  absl::Status capture_stopped = co_await session->WaitSnapshotCaptureStopped();
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
            absl::Status paused =
                co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
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
  absl::Status cut_sent = co_await SendFullSyncRequest(
      stream, ack_state, DataFrameKind::kFullSyncCut, cut_body,
      kResetBatchAckPartition, fullsync_sequence);
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
  co_return *backlog_cursor;
}

auto ReplicationManager::ReplicationGroup::EnterMasterFlowBacklog(
    TcpStream& stream, const std::shared_ptr<MasterSession>& session,
    unsigned flow_id, std::uint64_t next_lsn, std::uint32_t fragment_index)
    -> Task<absl::Status> {
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

auto ReplicationManager::ReplicationGroup::ReceiveMasterFlowBacklogAcks(
    TcpStream& stream, const std::shared_ptr<MasterSession>& session,
    unsigned flow_id, const std::shared_ptr<MasterBacklogDuplexState>& duplex)
    -> Task<absl::Status> {
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
    const storage::ReplicationLogCursor acknowledged{.lsn_ = expected_lsn + 1,
                                                     .fragment_index_ = 0};
    absl::Status retained =
        storage_->RetainReplicationLog(session->id_, acknowledged.lsn_);
    if (!retained.ok()) co_return retained;
    session->SetBacklogCursor(flow_id, ReplicationPhase::kReady, acknowledged);
    duplex->expected_acks_.pop_front();
  }
  co_return absl::UnavailableError("replication backlog flow closed");
}

auto ReplicationManager::ReplicationGroup::TrackMasterFlowBacklogAcks(
    TcpStream& stream, const std::shared_ptr<MasterSession>& session,
    unsigned flow_id, const std::shared_ptr<MasterBacklogDuplexState>& duplex)
    -> Task<absl::Status> {
  duplex->receiver_status_ =
      co_await ReceiveMasterFlowBacklogAcks(stream, session, flow_id, duplex);
  duplex->receiver_done_ = true;
  duplex->receiver_done_ready_.NotifyAll(*bycorf::ThisWorker().self_);
  if (!duplex->receiver_status_.ok()) {
    (void)::shutdown(stream.NativeFd(), SHUT_RDWR);
  }
  co_return duplex->receiver_status_;
}

auto ReplicationManager::ReplicationGroup::MonitorBacklogStall(
    std::shared_ptr<MasterSession> session, unsigned flow_id, int fd,
    std::uint64_t observed_generation) -> Task<absl::Status> {
  auto deadline = std::chrono::steady_clock::now() + kFullSyncStallTimeout;
  while (!session->cancelled()) {
    absl::Status slept = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
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
        "10 minutes; disconnecting it to release retained backlog pressure",
        session->id_, flow_id);
    (void)::shutdown(fd, SHUT_RDWR);
    co_return absl::DeadlineExceededError(
        "replica made no backlog ACK progress for 10 minutes");
  }
  co_return absl::OkStatus();
}

auto ReplicationManager::ReplicationGroup::RunMasterFlowBacklog(
    TcpStream& stream, const std::shared_ptr<MasterSession>& session,
    unsigned flow_id, storage::ReplicationLogCursor cursor)
    -> Task<absl::Status> {
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
        spdlog::warn("injected divergent replication tail on flow {}", flow_id);
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

auto ReplicationManager::ReplicationGroup::RunAdoptedConnection(
    Connection* connection, std::vector<std::string> args,
    std::uint64_t client_id, std::string client_address, bool tls,
    std::uint64_t replication_session_id) -> Task<absl::Status> {
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

auto ReplicationManager::ReplicationGroup::ServeOwnedNativeConnection(
    TcpStream& stream, std::vector<std::string> args, std::uint64_t client_id)
    -> Task<absl::Status> {
  if (EqualCaseInsensitive(args.front(), "LVPARENT")) {
    co_return co_await ServeParentExport(stream, std::move(args));
  }
  if (EqualCaseInsensitive(args.front(), "LVRECOVER")) {
    co_return co_await ServeRecoveryDonor(stream, std::move(args));
  }
  if (!source_sockets_.Add(stream.NativeFd())) {
    co_return absl::CancelledError(
        "native replication source stopped for process shutdown");
  }
  ScopedSocketSetMembership source_socket(&source_sockets_, stream.NativeFd());
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

auto ReplicationManager::ReplicationGroup::ServeMasterControl(
    TcpStream& stream, std::vector<std::string> args, std::uint64_t client_id)
    -> Task<absl::Status> {
  const bool population_handshake =
      args.size() == 26 && args[8] == "POPULATION";
  const bool origin_handshake =
      args.size() == 19 && args[17] == "ORIGIN" && IsReplicationId(args[18]);
  const bool follow_handshake =
      (args.size() == 17 || origin_handshake) && args[8] == "FOLLOW";
  const bool requested_group_valid =
      args.size() > 4 && (args[3] == "?" || IsReplicationId(args[3]) ||
                          ((population_handshake || follow_handshake) &&
                           IsPopulationGroupToken(args[3])));
  if ((!meta_managed_ && args.size() != 8) ||
      (meta_managed_ && !population_handshake && !follow_handshake) ||
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
        !ParseUnsigned(args[16], &relationship.partition_replication_epoch_) ||
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
  const bool protocol_probe = args[2] == "?";
  absl::Status history_ready = co_await EnsureReplicationHistoryReady();
  if (!history_ready.ok()) co_return history_ready;
  // A retiring idle monitor can still be running while we wait for its
  // reset. Check only after that wait so its exit cannot leave the newly
  // enabled history without a monitor. The active control pins history
  // until this handshake publishes its session or returns.
  StartIdleReplicationHistoryMonitor();
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
      co_return absl::InvalidArgumentError("invalid LVPSYNC replica identity");
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
      if (disposition != detail::SourceAuthorizationDisposition::kAuthorized) {
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
        IsReplicationId(args[6]) && applied->size() == storage_->worker_count();
    bool allow_initial_cursor = false;
    if (origin_handshake && requested_follow.has_value() && allow_continue) {
      const auto found =
          continuation_proofs_.find(requested_follow->target_node_id_);
      if (found != continuation_proofs_.end()) {
        const auto& proof = found->second;
        allow_initial_cursor = proof.target_assignment_id_ ==
                                   requested_follow->target_assignment_id_ &&
                               proof.target_boot_id_ == args[6] &&
                               proof.child_history_id_ == source_history_id &&
                               proof.capability_ == args[18] &&
                               proof.child_origin_.size() == applied->size();
        if (allow_initial_cursor) {
          for (unsigned flow = 0; flow < applied->size(); ++flow)
            allow_initial_cursor &=
                (*applied)[flow] >= proof.child_origin_[flow];
        }
      }
    }
    session = std::make_shared<MasterSession>(
        session_id, storage_->worker_count(), std::move(replica_node_id),
        std::move(replica_host), replica_port, source_history_id,
        allow_continue, std::move(*applied), std::move(authorized_population),
        std::move(authorized_follow));
    session->allow_initial_cursor_ = allow_initial_cursor;
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
  if (meta_managed_ && (session->steady_export_.has_value() ||
                        session->population_export_ != nullptr)) {
    co_await master_mutex_.Lock(*bycorf::ThisWorker().self_);
    bycorf::CrossWorkerMutex::Guard lock(&master_mutex_);
    if (!session->cancelled() && history_id_ == source_history_id &&
        master_sessions_.contains(session->id_)) {
      auto origin = session->ProvenOrigin();
      if (std::ranges::find(origin, 0) == origin.end()) {
        // population_export_ owns the source's Ready population. The
        // continuation belongs to the authenticated target membership,
        // including when an initial POPULATION session becomes FOLLOW.
        const auto& assignment =
            session->steady_export_.has_value()
                ? session->steady_export_->target_assignment_id_
                : requested_population->assignment_id_;
        continuation_proofs_[session->node_id_] =
            NativeContinuationProof{session->node_id_,
                                    assignment,
                                    args[6],
                                    source_history_id,
                                    session->flow_capability_,
                                    std::move(origin)};
      }
    }
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
  spdlog::info("accepted replication session {} with {} data flows", session_id,
               session->worker_count());
  absl::Status waited = co_await WaitForClose(stream);
  (void)co_await RemoveMasterSession(session);
  co_return waited;
}

auto ReplicationManager::ReplicationGroup::ServeMasterFlow(
    TcpStream& stream, std::vector<std::string> args) -> Task<absl::Status> {
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
      session->allow_continue_ &&
      (next_lsn > 1 || fragment_index != 0 || session->allow_initial_cursor_) &&
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
    waited = co_await EnterMasterFlowBacklog(stream, session, flow_id, next_lsn,
                                             fragment_index);
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

auto ReplicationManager::ReplicationGroup::RemoveMasterSession(
    const std::shared_ptr<MasterSession>& session) -> Task<absl::Status> {
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

auto ReplicationManager::ReplicationGroup::DrainSourceEgress()
    -> Task<absl::Status> {
  assert(bycorf::ThisWorker().id_ == 0);
  // Demotion has already made the role non-master. Process shutdown closes
  // every registered source socket before request drain so retained history
  // cannot deadlock an admitted publisher; this coroutine performs the
  // worker-affine registry join before source history is disabled.
  std::vector<std::shared_ptr<MasterSession>> sessions;
  {
    co_await master_mutex_.Lock(*bycorf::ThisWorker().self_);
    bycorf::CrossWorkerMutex::Guard lock(&master_mutex_);
    sessions.reserve(master_sessions_.size() + retired_master_sessions_.size());
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
    history_bridge_.reset();
    continuation_proofs_.clear();
  }
  for (const auto& session : sessions) session->Cancel();
  auto source_flows_active = [&] {
    return std::any_of(sessions.begin(), sessions.end(), [](const auto& item) {
      return item->connected_flows() != 0;
    });
  };
  while (active_master_controls_.load(std::memory_order_acquire) != 0 ||
         idle_history_monitor_running_ || history_reset_running_ ||
         source_flows_active()) {
    absl::Status waited = co_await bycorf::SleepFor(
        *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
    if (!waited.ok()) co_return waited;
  }
  // Flow teardown performs any history reset before ClearFlow drops the
  // final connected-flow count. Together with the monitor/reset flags, this
  // joins every old source task before its history can be disabled.
  co_return absl::OkStatus();
}

auto ReplicationManager::ReplicationGroup::DisableSourceHistory()
    -> Task<absl::Status> {
  for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
    absl::Status disabled = co_await bycorf::SubmitTaskTo(
        worker, [this]() { return storage_->DisableReplicationLog(); });
    if (!disabled.ok()) co_return disabled;
  }
  co_return absl::OkStatus();
}

auto ReplicationManager::ReplicationGroup::RetireSourceHistory()
    -> Task<absl::Status> {
  absl::Status drained = co_await DrainSourceEgress();
  if (!drained.ok()) co_return drained;
  co_return co_await DisableSourceHistory();
}

auto ReplicationManager::ReplicationGroup::FinalizeRetiredMasterSessionsLocked()
    -> void {
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

auto ReplicationManager::ReplicationGroup::MasterHistoryHasConsumersLocked()
    const -> bool {
  // A complete cluster Owner's native domain also anchors follower progress
  // and a future failover. Transient absence of sockets is not a reason to
  // rotate it. Its bounded log still evicts normally; demotion, invalidation
  // and explicit source retirement own the history lifetime.
  return (meta_managed_ && cluster_rebuild_ != nullptr &&
          cluster_rebuild_->ready_token_.has_value()) ||
         !master_sessions_.empty() || !retired_master_sessions_.empty() ||
         source_authorizations_.RetainsSourceHistory() ||
         active_master_controls_.load(std::memory_order_acquire) != 0;
}

auto ReplicationManager::ReplicationGroup::StartIdleReplicationHistoryMonitor()
    -> void {
  assert(bycorf::ThisWorker().id_ == 0);
  if (idle_history_monitor_running_) return;
  idle_history_monitor_running_ = true;
  bycorf::ThisWorker().self_->Spawn(MonitorIdleReplicationHistory());
}

auto ReplicationManager::ReplicationGroup::MonitorIdleReplicationHistory()
    -> Task<absl::Status> {
  assert(bycorf::ThisWorker().id_ == 0);
  struct MonitorGuard {
    bool* running_;
    ~MonitorGuard() { *running_ = false; }
  } monitor_guard{&idle_history_monitor_running_};

  for (;;) {
    absl::Status slept = co_await bycorf::SleepFor(
        *bycorf::ThisWorker().self_, std::chrono::milliseconds(10));
    if (!slept.ok()) co_return slept;
    if (is_replica() ||
        replication_shutdown_requested_.load(std::memory_order_acquire))
      co_return absl::OkStatus();

    std::string history_id;
    std::vector<storage::ReplicationLogInfo> logs;
    {
      co_await master_mutex_.Lock(*bycorf::ThisWorker().self_);
      bycorf::CrossWorkerMutex::Guard lock(&master_mutex_);
      FinalizeRetiredMasterSessionsLocked();
      if (MasterHistoryHasConsumersLocked()) continue;
      if (meta_managed_) {
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
        bool expired = candidate.history_id_ != history_id_ ||
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
          spdlog::info("replication reconnect lease expired node={} session={}",
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
    if (meta_managed_) continue;

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
      if (disable) {
        history_id_ = NewReplicationId();
        history_bridge_.reset();
        continuation_proofs_.clear();
      }
    }
    if (!disable) {
      continue;
    }

    // Exercise a handshake arriving after the final idle check while the
    // per-worker disable operations still yield under reset ownership.
    LAVIK_FAULT_INJECT(
        if (LAVIK_FAULT_MATCHES(
                "LAVIK_REPLICATION_PAUSE_IDLE_HISTORY_UNTIL_CONTROL_ONCE",
                "1") &&
            !std::exchange(replication_idle_history_pause_used_, true)) {
          spdlog::info(
              "paused idle history retirement until next native control");
          while (active_master_controls_.load(std::memory_order_acquire) == 0) {
            if (replication_shutdown_requested_.load(std::memory_order_acquire))
              co_return absl::OkStatus();
            absl::Status waited = co_await bycorf::SleepFor(
                *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
            if (!waited.ok()) co_return waited;
          }
        });

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

auto ReplicationManager::ReplicationGroup::ResetInvalidReplicationHistory()
    -> Task<absl::Status> {
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(
        0, [this]() { return EnsureReplicationHistoryReady(); });
  }
  co_return co_await EnsureReplicationHistoryReady();
}

auto ReplicationManager::ReplicationGroup::EnsureReplicationHistoryReady()
    -> Task<absl::Status> {
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
    history_bridge_.reset();
    continuation_proofs_.clear();
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

Task<absl::Status> ReplicationManager::ReconcileClusterRecovery(
    std::optional<DesiredClusterRecovery> desired) {
  return group_->ReconcileClusterRecovery(std::move(desired));
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
                           EqualCaseInsensitive(args.front(), "LVFLOW") ||
                           EqualCaseInsensitive(args.front(), "LVRECOVER") ||
                           EqualCaseInsensitive(args.front(), "LVPARENT"));
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
