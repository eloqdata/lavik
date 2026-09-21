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

#include "lavik/net/sync_stream.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <span>
#include <utility>

#include "lavik/numeric_endpoint.h"

namespace lavik::net {
namespace {

struct IoDeadline {
  SyncDeadline at_;
  int cancel_fd_ = -1;
};

absl::Status CheckDeadline(IoDeadline deadline) {
  if (deadline.cancel_fd_ >= 0) {
    pollfd descriptor{
        .fd = deadline.cancel_fd_, .events = POLLIN, .revents = 0};
    if (::poll(&descriptor, 1, 0) > 0)
      return absl::CancelledError("startup interrupted");
  }
  if (std::chrono::steady_clock::now() >= deadline.at_) {
    return absl::DeadlineExceededError("synchronous I/O deadline expired");
  }
  return absl::OkStatus();
}

class FileDescriptor {
 public:
  explicit FileDescriptor(int fd = -1) noexcept : fd_(fd) {}
  FileDescriptor(const FileDescriptor&) = delete;
  FileDescriptor& operator=(const FileDescriptor&) = delete;
  FileDescriptor(FileDescriptor&& other) noexcept
      : fd_(std::exchange(other.fd_, -1)) {}
  ~FileDescriptor() {
    if (fd_ >= 0) (void)::close(fd_);
  }
  [[nodiscard]] int get() const noexcept { return fd_; }

 private:
  int fd_;
};

class SslContext {
 public:
  explicit SslContext(SSL_CTX* context) noexcept : context_(context) {}
  SslContext(const SslContext&) = delete;
  SslContext& operator=(const SslContext&) = delete;
  SslContext(SslContext&& other) noexcept
      : context_(std::exchange(other.context_, nullptr)) {}
  ~SslContext() {
    if (context_ != nullptr) SSL_CTX_free(context_);
  }
  [[nodiscard]] SSL_CTX* get() const noexcept { return context_; }

 private:
  SSL_CTX* context_;
};

class SslSession {
 public:
  explicit SslSession(SSL* session) noexcept : session_(session) {}
  SslSession(const SslSession&) = delete;
  SslSession& operator=(const SslSession&) = delete;
  SslSession(SslSession&& other) noexcept
      : session_(std::exchange(other.session_, nullptr)) {}
  ~SslSession() {
    if (session_ != nullptr) SSL_free(session_);
  }
  [[nodiscard]] SSL* get() const noexcept { return session_; }

 private:
  SSL* session_;
};

struct SocketEndpoint {
  sockaddr_storage address_{};
  socklen_t length_ = 0;
  int family_ = AF_UNSPEC;
  std::string host_;
};

absl::Status ErrnoStatus(std::string_view operation, int error = errno) {
  return absl::UnavailableError(std::string(operation) + ": " +
                                std::strerror(error));
}

absl::Status OpenSslStatus(std::string_view operation) {
  const unsigned long error = ERR_get_error();
  if (error == 0) {
    return absl::PermissionDeniedError(std::string(operation) + " failed");
  }
  char text[256]{};
  ERR_error_string_n(error, text, sizeof(text));
  return absl::PermissionDeniedError(std::string(operation) + ": " + text);
}

absl::Status OpenSslTransportStatus(std::string_view operation) {
  const unsigned long error = ERR_get_error();
  if (error == 0) {
    return absl::UnavailableError(std::string(operation) + " failed");
  }
  char text[256]{};
  ERR_error_string_n(error, text, sizeof(text));
  return absl::UnavailableError(std::string(operation) + ": " + text);
}

int RemainingMillis(IoDeadline deadline) {
  const auto remaining = deadline.at_ - std::chrono::steady_clock::now();
  if (remaining <= std::chrono::steady_clock::duration::zero()) return 0;
  const auto millis =
      std::chrono::ceil<std::chrono::milliseconds>(remaining).count();
  return static_cast<int>(
      std::min<std::int64_t>(millis, std::numeric_limits<int>::max()));
}

absl::Status WaitFor(int fd, short events, IoDeadline deadline,
                     std::string_view operation) {
  while (true) {
    pollfd descriptors[2] = {
        {.fd = fd, .events = events, .revents = 0},
        {.fd = deadline.cancel_fd_, .events = POLLIN, .revents = 0}};
    const int timeout = RemainingMillis(deadline);
    if (timeout == 0) {
      return absl::DeadlineExceededError(std::string(operation) + " timed out");
    }
    const int result = ::poll(descriptors, 2, timeout);
    if (result > 0) {
      if (descriptors[1].revents != 0)
        return absl::CancelledError("startup interrupted");
      if ((descriptors[0].revents & events) != 0) return absl::OkStatus();
      if ((descriptors[0].revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
        return absl::UnavailableError(std::string(operation) + " failed");
      }
      continue;
    }
    if (result == 0) {
      return absl::DeadlineExceededError(std::string(operation) + " timed out");
    }
    if (errno != EINTR) return ErrnoStatus(operation);
  }
}

absl::StatusOr<FileDescriptor> Connect(int family, const sockaddr* address,
                                       socklen_t length, IoDeadline deadline) {
  FileDescriptor fd(
      ::socket(family, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0));
  if (fd.get() < 0) return ErrnoStatus("socket");
  if (::connect(fd.get(), address, length) == 0) return fd;
  if (errno != EINPROGRESS) return ErrnoStatus("connect");
  if (absl::Status ready = WaitFor(fd.get(), POLLOUT, deadline, "connect");
      !ready.ok()) {
    return ready;
  }
  int error = 0;
  socklen_t error_size = sizeof(error);
  if (::getsockopt(fd.get(), SOL_SOCKET, SO_ERROR, &error, &error_size) != 0) {
    return ErrnoStatus("getsockopt(SO_ERROR)");
  }
  if (error != 0) return ErrnoStatus("connect", error);
  return fd;
}

absl::StatusOr<FileDescriptor> ConnectUnix(const std::string& path,
                                           IoDeadline deadline) {
  sockaddr_un address{};
  address.sun_family = AF_UNIX;
  if (path.empty()) return absl::InvalidArgumentError("empty Unix socket path");
  if (path.size() >= sizeof(address.sun_path)) {
    return absl::InvalidArgumentError("Unix socket path is too long");
  }
  std::memcpy(address.sun_path, path.c_str(), path.size() + 1);
  return Connect(AF_UNIX, reinterpret_cast<const sockaddr*>(&address),
                 sizeof(address), deadline);
}

absl::StatusOr<SocketEndpoint> ParseEndpoint(std::string_view text) {
  auto parsed = lavik::ParseNumericEndpoint(text);
  if (!parsed.has_value()) {
    return absl::InvalidArgumentError(
        "address must be numeric IPv4:port or [IPv6]:port");
  }
  SocketEndpoint endpoint;
  endpoint.host_ = parsed->host_;
  sockaddr_in ipv4{};
  if (::inet_pton(AF_INET, endpoint.host_.c_str(), &ipv4.sin_addr) == 1) {
    ipv4.sin_family = AF_INET;
    ipv4.sin_port = htons(parsed->port_);
    std::memcpy(&endpoint.address_, &ipv4, sizeof(ipv4));
    endpoint.length_ = sizeof(ipv4);
    endpoint.family_ = AF_INET;
    return endpoint;
  }
  sockaddr_in6 ipv6{};
  if (::inet_pton(AF_INET6, endpoint.host_.c_str(), &ipv6.sin6_addr) == 1) {
    ipv6.sin6_family = AF_INET6;
    ipv6.sin6_port = htons(parsed->port_);
    std::memcpy(&endpoint.address_, &ipv6, sizeof(ipv6));
    endpoint.length_ = sizeof(ipv6);
    endpoint.family_ = AF_INET6;
    return endpoint;
  }
  return absl::InvalidArgumentError("unsupported address family");
}

absl::Status PlainWriteAll(int fd, std::string_view bytes,
                           IoDeadline deadline) {
  while (!bytes.empty()) {
    if (auto status = CheckDeadline(deadline); !status.ok()) return status;
    const ssize_t written =
        ::send(fd, bytes.data(), bytes.size(), MSG_NOSIGNAL);
    if (written > 0) {
      bytes.remove_prefix(static_cast<std::size_t>(written));
      continue;
    }
    if (written < 0 && errno == EINTR) continue;
    if (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      if (absl::Status ready = WaitFor(fd, POLLOUT, deadline, "write");
          !ready.ok()) {
        return ready;
      }
      continue;
    }
    return ErrnoStatus("write");
  }
  return absl::OkStatus();
}

absl::StatusOr<SslContext> MakeTlsContext(const SyncTlsOptions& options) {
  if (options.ca_file_.empty() || options.certificate_file_.empty() ||
      options.private_key_file_.empty()) {
    return absl::InvalidArgumentError(
        "TLS CA, certificate, and private key must be provided together");
  }
  SSL_CTX* raw = SSL_CTX_new(TLS_client_method());
  if (raw == nullptr) return OpenSslStatus("SSL_CTX_new");
  SslContext context(raw);
  if (SSL_CTX_set_min_proto_version(raw, TLS1_2_VERSION) != 1) {
    return OpenSslStatus("set TLS minimum version");
  }
  if (SSL_CTX_load_verify_locations(raw, options.ca_file_.c_str(), nullptr) !=
      1) {
    return OpenSslStatus("load TLS CA");
  }
  if (SSL_CTX_use_certificate_chain_file(
          raw, options.certificate_file_.c_str()) != 1) {
    return OpenSslStatus("load TLS certificate chain");
  }
  if (SSL_CTX_use_PrivateKey_file(raw, options.private_key_file_.c_str(),
                                  SSL_FILETYPE_PEM) != 1) {
    return OpenSslStatus("load TLS private key");
  }
  if (SSL_CTX_check_private_key(raw) != 1) {
    return OpenSslStatus("check TLS private key");
  }
  SSL_CTX_set_verify(raw, SSL_VERIFY_PEER, nullptr);
  return context;
}

absl::Status WaitForSsl(SSL* ssl, int result, IoDeadline deadline,
                        std::string_view operation) {
  const int error = SSL_get_error(ssl, result);
  if (error == SSL_ERROR_WANT_READ) {
    return WaitFor(SSL_get_fd(ssl), POLLIN, deadline, operation);
  }
  if (error == SSL_ERROR_WANT_WRITE) {
    return WaitFor(SSL_get_fd(ssl), POLLOUT, deadline, operation);
  }
  // TLS 1.3 may report the server's client-certificate rejection on the first
  // application read, after SSL_connect succeeded. Explicit alerts remain
  // fatal at every stage; only an unexpected EOF is retryable transport loss.
  if (error == SSL_ERROR_SSL &&
      ERR_GET_REASON(ERR_peek_error()) != SSL_R_UNEXPECTED_EOF_WHILE_READING) {
    return OpenSslStatus(operation);
  }
  if (error == SSL_ERROR_SYSCALL && errno != 0) {
    return ErrnoStatus(operation);
  }
  return OpenSslTransportStatus(operation);
}

absl::StatusOr<SslSession> StartTls(SSL_CTX* context, int fd,
                                    const SocketEndpoint& endpoint,
                                    const SyncTlsOptions& options,
                                    IoDeadline deadline) {
  SSL* raw = SSL_new(context);
  if (raw == nullptr) return OpenSslStatus("SSL_new");
  SslSession session(raw);
  if (SSL_set_fd(raw, fd) != 1) return OpenSslStatus("SSL_set_fd");
  X509_VERIFY_PARAM* verify = SSL_get0_param(raw);
  if (!options.server_name_.empty()) {
    if (SSL_set_tlsext_host_name(raw, options.server_name_.c_str()) != 1 ||
        X509_VERIFY_PARAM_set1_host(verify, options.server_name_.c_str(),
                                    options.server_name_.size()) != 1) {
      return OpenSslStatus("configure TLS server name");
    }
  } else if (X509_VERIFY_PARAM_set1_ip_asc(verify, endpoint.host_.c_str()) !=
             1) {
    return OpenSslStatus("configure TLS server IP");
  }
  while (true) {
    if (auto status = CheckDeadline(deadline); !status.ok()) return status;
    errno = 0;
    const int result = SSL_connect(raw);
    if (result == 1) break;
    if (absl::Status ready = WaitForSsl(raw, result, deadline, "TLS handshake");
        !ready.ok()) {
      return ready;
    }
  }
  if (SSL_get_verify_result(raw) != X509_V_OK) {
    return absl::PermissionDeniedError(
        "TLS peer certificate verification failed");
  }
  return session;
}

absl::Status TlsWriteAll(SSL* ssl, std::string_view bytes,
                         IoDeadline deadline) {
  while (!bytes.empty()) {
    if (auto status = CheckDeadline(deadline); !status.ok()) return status;
    const int amount = static_cast<int>(std::min<std::size_t>(
        bytes.size(),
        static_cast<std::size_t>(std::numeric_limits<int>::max())));
    errno = 0;
    const int written = SSL_write(ssl, bytes.data(), amount);
    if (written > 0) {
      bytes.remove_prefix(static_cast<std::size_t>(written));
      continue;
    }
    if (absl::Status ready = WaitForSsl(ssl, written, deadline, "TLS write");
        !ready.ok()) {
      return ready;
    }
  }
  return absl::OkStatus();
}

}  // namespace

struct SyncStream::Impl {
  FileDescriptor fd_;
  std::optional<SslContext> tls_context_;
  std::optional<SslSession> tls_;
  IoDeadline deadline_;
  // Bytes ReadLine pulled past the first newline; every later read must
  // consume them before touching the transport again.
  std::string leftover_;

  absl::StatusOr<std::size_t> ReadSome(std::span<char> buffer) {
    if (!leftover_.empty()) {
      const std::size_t count = std::min(buffer.size(), leftover_.size());
      std::memcpy(buffer.data(), leftover_.data(), count);
      leftover_.erase(0, count);
      return count;
    }
    while (true) {
      if (auto status = CheckDeadline(deadline_); !status.ok()) return status;
      errno = 0;
      const auto received =
          tls_ ? SSL_read(tls_->get(), buffer.data(),
                          static_cast<int>(buffer.size()))
               : ::recv(fd_.get(), buffer.data(), buffer.size(), 0);
      if (received > 0) return static_cast<std::size_t>(received);
      if (tls_) {
        if (SSL_get_error(tls_->get(), received) == SSL_ERROR_ZERO_RETURN) {
          return absl::UnavailableError(
              "server closed before terminating its reply");
        }
        if (auto status =
                WaitForSsl(tls_->get(), received, deadline_, "TLS read");
            !status.ok())
          return status;
      } else {
        if (received == 0)
          return absl::UnavailableError(
              "server closed before terminating its reply");
        if (errno == EINTR) continue;
        if (errno != EAGAIN && errno != EWOULDBLOCK) return ErrnoStatus("read");
        if (auto status = WaitFor(fd_.get(), POLLIN, deadline_, "read");
            !status.ok())
          return status;
      }
    }
  }
};

SyncStream::SyncStream(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
SyncStream::~SyncStream() = default;

absl::StatusOr<std::unique_ptr<SyncStream>> SyncStream::Connect(
    const SyncTarget& target, SyncDeadline deadline, int cancel_fd) {
  const IoDeadline io_deadline{deadline, cancel_fd};
  if (auto status = CheckDeadline(io_deadline); !status.ok()) return status;
  std::optional<SocketEndpoint> endpoint;
  auto connect = [&]() -> absl::StatusOr<FileDescriptor> {
    if (target.transport_ == SyncTarget::Transport::kUnix) {
      return ConnectUnix(target.endpoint_, io_deadline);
    }
    if (target.transport_ != SyncTarget::Transport::kTcpPlaintext &&
        target.transport_ != SyncTarget::Transport::kTcpMtls) {
      return absl::InvalidArgumentError("unsupported synchronous transport");
    }
    auto parsed = ParseEndpoint(target.endpoint_);
    if (!parsed.ok()) return parsed.status();
    endpoint = *parsed;
    return net::Connect(endpoint->family_,
                        reinterpret_cast<const sockaddr*>(&endpoint->address_),
                        endpoint->length_, io_deadline);
  };
  auto fd = connect();
  if (!fd.ok()) return fd.status();
  auto impl = std::make_unique<Impl>(Impl{std::move(*fd), {}, {}, io_deadline});
  if (target.transport_ == SyncTarget::Transport::kTcpMtls) {
    auto context = MakeTlsContext(target.tls_);
    if (!context.ok()) return context.status();
    auto session = StartTls(context->get(), impl->fd_.get(), *endpoint,
                            target.tls_, io_deadline);
    if (!session.ok()) return session.status();
    impl->tls_context_.emplace(std::move(*context));
    impl->tls_.emplace(std::move(*session));
  }
  return std::unique_ptr<SyncStream>(new SyncStream(std::move(impl)));
}

absl::Status SyncStream::WriteAll(std::string_view bytes) {
  return impl_->tls_ ? TlsWriteAll(impl_->tls_->get(), bytes, impl_->deadline_)
                     : PlainWriteAll(impl_->fd_.get(), bytes, impl_->deadline_);
}

absl::StatusOr<std::string> SyncStream::ReadExact(std::size_t length) {
  if (length > 256u * 1024u * 1024u)
    return absl::ResourceExhaustedError("read exceeds limit");
  std::string bytes(length, '\0');
  std::size_t offset = 0;
  while (offset < length) {
    auto read = impl_->ReadSome(std::span(bytes).subspan(offset));
    if (!read.ok()) {
      if (offset != 0 && absl::IsUnavailable(read.status()))
        return absl::DataLossError(read.status().message());
      return read.status();
    }
    offset += *read;
  }
  return bytes;
}

absl::StatusOr<std::string> SyncStream::ReadLine(std::size_t max_bytes) {
  std::string reply;
  char buffer[4096];
  while (true) {
    auto read = impl_->ReadSome(buffer);
    if (!read.ok()) {
      if (!reply.empty() && absl::IsUnavailable(read.status()))
        return absl::DataLossError(read.status().message());
      return read.status();
    }
    const std::string_view chunk(buffer, *read);
    const auto newline = chunk.find('\n');
    const auto prefix = chunk.substr(0, newline);
    if (prefix.size() > max_bytes - reply.size())
      return absl::ResourceExhaustedError("reply exceeds limit");
    reply.append(prefix);
    if (newline != std::string_view::npos) {
      // Anything already read past the newline belongs to the next message;
      // keep it for the next ReadLine/ReadExact instead of dropping it.
      impl_->leftover_.assign(chunk.substr(newline + 1));
      break;
    }
  }
  if (!reply.empty() && reply.back() == '\r') reply.pop_back();
  return reply;
}

absl::StatusOr<std::vector<std::string>> SyncStream::PeerUriSans() const {
  if (!impl_->tls_)
    return absl::FailedPreconditionError("connection has no TLS peer");
  X509* certificate = SSL_get1_peer_certificate(impl_->tls_->get());
  if (certificate == nullptr)
    return absl::UnauthenticatedError("TLS peer has no certificate");
  GENERAL_NAMES* names = static_cast<GENERAL_NAMES*>(
      X509_get_ext_d2i(certificate, NID_subject_alt_name, nullptr, nullptr));
  std::vector<std::string> result;
  if (names != nullptr) {
    for (int index = 0; index < sk_GENERAL_NAME_num(names); ++index) {
      const auto* name = sk_GENERAL_NAME_value(names, index);
      if (name->type != GEN_URI) continue;
      const auto* uri = name->d.uniformResourceIdentifier;
      result.emplace_back(
          reinterpret_cast<const char*>(ASN1_STRING_get0_data(uri)),
          ASN1_STRING_length(uri));
    }
    GENERAL_NAMES_free(names);
  }
  X509_free(certificate);
  return result;
}

}  // namespace lavik::net
