#include "keylane/meta/admin_client.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <utility>

#include "absl/status/status.h"
#include "keylane/numeric_endpoint.h"

namespace keylane::meta {
namespace {

constexpr std::size_t kMaxCommandBytes = 64 * 1024;
constexpr std::size_t kMaxReplyBytes = 256 * 1024 * 1024;

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

int RemainingMillis(MetaAdminDeadline deadline) {
  const auto remaining = deadline - std::chrono::steady_clock::now();
  if (remaining <= std::chrono::steady_clock::duration::zero()) return 0;
  const auto millis =
      std::chrono::ceil<std::chrono::milliseconds>(remaining).count();
  return static_cast<int>(
      std::min<std::int64_t>(millis, std::numeric_limits<int>::max()));
}

absl::Status WaitFor(int fd, short events, MetaAdminDeadline deadline,
                     std::string_view operation) {
  while (true) {
    pollfd descriptor{.fd = fd, .events = events, .revents = 0};
    const int timeout = RemainingMillis(deadline);
    if (timeout == 0) {
      return absl::DeadlineExceededError(std::string(operation) +
                                         " timed out");
    }
    const int result = ::poll(&descriptor, 1, timeout);
    if (result > 0) {
      if ((descriptor.revents & events) != 0) return absl::OkStatus();
      if ((descriptor.revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
        return absl::UnavailableError(std::string(operation) + " failed");
      }
      continue;
    }
    if (result == 0) {
      return absl::DeadlineExceededError(std::string(operation) +
                                         " timed out");
    }
    if (errno != EINTR) return ErrnoStatus(operation);
  }
}

absl::StatusOr<FileDescriptor> Connect(int family, const sockaddr* address,
                                       socklen_t length,
                                       MetaAdminDeadline deadline) {
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
                                           MetaAdminDeadline deadline) {
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
  auto parsed = keylane::ParseNumericEndpoint(text);
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
                           MetaAdminDeadline deadline) {
  while (!bytes.empty()) {
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

absl::StatusOr<std::string> PlainReadLine(int fd,
                                          MetaAdminDeadline deadline) {
  std::string reply;
  char buffer[4096];
  while (true) {
    const ssize_t received = ::recv(fd, buffer, sizeof(buffer), 0);
    if (received > 0) {
      const std::string_view chunk(buffer, static_cast<std::size_t>(received));
      const std::size_t newline = chunk.find('\n');
      reply.append(chunk.substr(0, newline));
      if (reply.size() > kMaxReplyBytes) {
        return absl::ResourceExhaustedError("reply exceeds 256 MiB limit");
      }
      if (newline != std::string_view::npos) break;
      continue;
    }
    if (received == 0) {
      // An empty close is ordinary connection churn and discovery may retry
      // it. Once any wire bytes arrive, EOF proves a truncated reply and must
      // remain fatal instead of being hidden by another discovery attempt.
      return reply.empty()
                 ? absl::UnavailableError(
                       "server closed before terminating its reply")
                 : absl::DataLossError(
                       "server closed before terminating its reply");
    }
    if (errno == EINTR) continue;
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      if (absl::Status ready = WaitFor(fd, POLLIN, deadline, "read");
          !ready.ok()) {
        return ready;
      }
      continue;
    }
    return ErrnoStatus("read");
  }
  if (!reply.empty() && reply.back() == '\r') reply.pop_back();
  return reply;
}

absl::StatusOr<SslContext> MakeTlsContext(
    const MetaAdminTlsOptions& options) {
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
  if (SSL_CTX_use_certificate_chain_file(raw,
                                         options.certificate_file_.c_str()) !=
      1) {
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

absl::Status WaitForSsl(SSL* ssl, int result, MetaAdminDeadline deadline,
                        std::string_view operation,
                        bool authentication_phase,
                        bool partial_reply = false) {
  const int error = SSL_get_error(ssl, result);
  if (error == SSL_ERROR_WANT_READ) {
    return WaitFor(SSL_get_fd(ssl), POLLIN, deadline, operation);
  }
  if (error == SSL_ERROR_WANT_WRITE) {
    return WaitFor(SSL_get_fd(ssl), POLLOUT, deadline, operation);
  }
  if (partial_reply) {
    return absl::DataLossError(
        "server closed before terminating its reply");
  }
  if (error == SSL_ERROR_SYSCALL && errno != 0) {
    return ErrnoStatus(operation);
  }
  return authentication_phase ? OpenSslStatus(operation)
                              : OpenSslTransportStatus(operation);
}

absl::StatusOr<SslSession> StartTls(SSL_CTX* context, int fd,
                                    const SocketEndpoint& endpoint,
                                    const MetaAdminTlsOptions& options,
                                    MetaAdminDeadline deadline) {
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
    errno = 0;
    const int result = SSL_connect(raw);
    if (result == 1) break;
    if (absl::Status ready =
            WaitForSsl(raw, result, deadline, "TLS handshake", true);
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
                         MetaAdminDeadline deadline) {
  while (!bytes.empty()) {
    const int amount = static_cast<int>(std::min<std::size_t>(
        bytes.size(),
        static_cast<std::size_t>(std::numeric_limits<int>::max())));
    errno = 0;
    const int written = SSL_write(ssl, bytes.data(), amount);
    if (written > 0) {
      bytes.remove_prefix(static_cast<std::size_t>(written));
      continue;
    }
    if (absl::Status ready =
            WaitForSsl(ssl, written, deadline, "TLS write", false);
        !ready.ok()) {
      return ready;
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<std::string> TlsReadLine(SSL* ssl,
                                        MetaAdminDeadline deadline) {
  std::string reply;
  char buffer[4096];
  while (true) {
    errno = 0;
    const int received = SSL_read(ssl, buffer, sizeof(buffer));
    if (received > 0) {
      const std::string_view chunk(buffer, static_cast<std::size_t>(received));
      const std::size_t newline = chunk.find('\n');
      reply.append(chunk.substr(0, newline));
      if (reply.size() > kMaxReplyBytes) {
        return absl::ResourceExhaustedError("reply exceeds 256 MiB limit");
      }
      if (newline != std::string_view::npos) break;
      continue;
    }
    if (SSL_get_error(ssl, received) == SSL_ERROR_ZERO_RETURN) {
      return reply.empty()
                 ? absl::UnavailableError(
                       "server closed before terminating its reply")
                 : absl::DataLossError(
                       "server closed before terminating its reply");
    }
    if (absl::Status ready =
            WaitForSsl(ssl, received, deadline, "TLS read", false,
                       !reply.empty());
        !ready.ok()) {
      return ready;
    }
  }
  if (!reply.empty() && reply.back() == '\r') reply.pop_back();
  return reply;
}

}  // namespace

absl::StatusOr<std::string> MetaAdminClient::RoundTrip(
    const MetaAdminTarget& target, std::string_view command,
    MetaAdminDeadline deadline) const {
  if (command.empty()) return absl::InvalidArgumentError("empty command");
  if (command.find_first_of("\r\n") != std::string_view::npos) {
    return absl::InvalidArgumentError("command must be exactly one line");
  }
  if (command.size() + 1 > kMaxCommandBytes) {
    return absl::ResourceExhaustedError("command exceeds 64 KiB limit");
  }
  std::string wire(command);
  wire.push_back('\n');

  if (target.transport_ == MetaAdminTarget::Transport::kUnix) {
    auto fd = ConnectUnix(target.endpoint_, deadline);
    if (!fd.ok()) return fd.status();
    if (absl::Status sent = PlainWriteAll(fd->get(), wire, deadline);
        !sent.ok()) {
      return sent;
    }
    return PlainReadLine(fd->get(), deadline);
  }

  auto endpoint = ParseEndpoint(target.endpoint_);
  if (!endpoint.ok()) return endpoint.status();
  auto fd = Connect(endpoint->family_,
                    reinterpret_cast<const sockaddr*>(&endpoint->address_),
                    endpoint->length_, deadline);
  if (!fd.ok()) return fd.status();
  if (target.transport_ == MetaAdminTarget::Transport::kTcpPlaintext) {
    if (absl::Status sent = PlainWriteAll(fd->get(), wire, deadline);
        !sent.ok()) {
      return sent;
    }
    return PlainReadLine(fd->get(), deadline);
  }
  if (target.transport_ != MetaAdminTarget::Transport::kTcpMtls) {
    return absl::InvalidArgumentError("unsupported Meta admin transport");
  }
  auto context = MakeTlsContext(target.tls_);
  if (!context.ok()) return context.status();
  auto ssl = StartTls(context->get(), fd->get(), *endpoint, target.tls_, deadline);
  if (!ssl.ok()) return ssl.status();
  if (absl::Status sent = TlsWriteAll(ssl->get(), wire, deadline); !sent.ok()) {
    return sent;
  }
  auto reply = TlsReadLine(ssl->get(), deadline);
  (void)SSL_shutdown(ssl->get());
  return reply;
}

}  // namespace keylane::meta
