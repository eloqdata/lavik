// One-shot client for keylane-meta's authenticated administrative surface.
//
// This binary deliberately speaks the Meta line protocol directly instead of
// importing Keylane's RESP service. Keeping it synchronous is appropriate for
// an operator CLI and preserves the boundary between the Redis data plane and
// the Raft-backed Meta control plane.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "keylane/version.h"

namespace {

constexpr std::size_t kMaxCommandBytes = 64 * 1024;
// Export replies can be substantially larger than ordinary status and
// mutation replies. Retain a finite ceiling so a compromised authenticated
// endpoint cannot grow an operator process without bound.
constexpr std::size_t kMaxReplyBytes = 256 * 1024 * 1024;

using Deadline = std::chrono::steady_clock::time_point;

class FileDescriptor {
 public:
  explicit FileDescriptor(int fd = -1) noexcept : fd_(fd) {}
  FileDescriptor(const FileDescriptor&) = delete;
  FileDescriptor& operator=(const FileDescriptor&) = delete;
  FileDescriptor(FileDescriptor&& other) noexcept
      : fd_(std::exchange(other.fd_, -1)) {}
  FileDescriptor& operator=(FileDescriptor&& other) noexcept {
    if (this != &other) {
      Reset();
      fd_ = std::exchange(other.fd_, -1);
    }
    return *this;
  }
  ~FileDescriptor() { Reset(); }

  [[nodiscard]] int get() const noexcept { return fd_; }

 private:
  void Reset() noexcept {
    if (fd_ >= 0) {
      (void)::close(fd_);
      fd_ = -1;
    }
  }

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

struct Endpoint {
  sockaddr_storage address_{};
  socklen_t length_ = 0;
  int family_ = AF_UNSPEC;
  std::string host_;
};

struct Options {
  std::string socket_path_;
  std::string address_;
  std::string tls_ca_;
  std::string tls_cert_;
  std::string tls_key_;
  std::string tls_server_name_;
  int timeout_ms_ = 5000;
  std::vector<std::string> command_;
};

[[noreturn]] void Fail(std::string message) {
  throw std::runtime_error(std::move(message));
}

std::string ErrnoMessage(std::string_view operation, int error = errno) {
  return std::string(operation) + ": " + std::strerror(error);
}

std::string OpenSslMessage(std::string_view operation) {
  const unsigned long error = ERR_get_error();
  if (error == 0) return std::string(operation) + " failed";
  char text[256]{};
  ERR_error_string_n(error, text, sizeof(text));
  return std::string(operation) + ": " + text;
}

void PrintUsage(const char* program) {
  std::fprintf(
      stderr,
      "Usage:\n"
      "  %s --socket PATH [--timeout-ms N] COMMAND [ARG...]\n"
      "  %s --addr IP:PORT --tls-ca FILE --tls-cert FILE --tls-key FILE\n"
      "     [--tls-server-name NAME] [--timeout-ms N] COMMAND [ARG...]\n"
      "\n"
      "The command is sent as one LF-terminated Meta control-protocol line.\n"
      "Exit status is 0 for an OK reply, 2 for an ERR reply, and 1 for a\n"
      "local, connection, TLS, or malformed-protocol failure.\n",
      program, program);
}

bool ParseInt(std::string_view text, int min, int max, int* result) {
  int value = 0;
  const auto [end, error] =
      std::from_chars(text.data(), text.data() + text.size(), value);
  if (error != std::errc() || end != text.data() + text.size() || value < min ||
      value > max) {
    return false;
  }
  *result = value;
  return true;
}

std::string_view OptionValue(int argc, char** argv, int* index,
                             std::string_view argument,
                             std::string_view option) {
  const std::string prefix = std::string(option) + "=";
  if (argument.starts_with(prefix)) return argument.substr(prefix.size());
  if (argument != option || *index + 1 >= argc) {
    Fail(std::string(option) + " requires a value");
  }
  ++*index;
  return argv[*index];
}

Options ParseOptions(int argc, char** argv, bool* early_exit) {
  Options options;
  int index = 1;
  for (; index < argc; ++index) {
    const std::string_view argument(argv[index]);
    if (argument == "--") {
      ++index;
      break;
    }
    if (!argument.starts_with("--")) break;
    if (argument == "--help") {
      PrintUsage(argv[0]);
      *early_exit = true;
      return options;
    }
    if (argument == "--version") {
      std::cout << "keylane-meta-ctl " << keylane::kVersion << '\n';
      *early_exit = true;
      return options;
    }

    auto assign = [&](std::string_view name, std::string* output) {
      const std::string prefix = std::string(name) + "=";
      if (argument == name || argument.starts_with(prefix)) {
        *output = OptionValue(argc, argv, &index, argument, name);
        if (output->empty()) Fail(std::string(name) + " must not be empty");
        return true;
      }
      return false;
    };
    if (assign("--socket", &options.socket_path_) ||
        assign("--addr", &options.address_) ||
        assign("--tls-ca", &options.tls_ca_) ||
        assign("--tls-cert", &options.tls_cert_) ||
        assign("--tls-key", &options.tls_key_) ||
        assign("--tls-server-name", &options.tls_server_name_)) {
      continue;
    }
    if (argument == "--timeout-ms" || argument.starts_with("--timeout-ms=")) {
      const std::string_view value =
          OptionValue(argc, argv, &index, argument, "--timeout-ms");
      if (!ParseInt(value, 1, 3'600'000, &options.timeout_ms_)) {
        Fail("--timeout-ms must be an integer from 1 through 3600000");
      }
      continue;
    }
    Fail("unknown option: " + std::string(argument));
  }

  for (; index < argc; ++index) options.command_.emplace_back(argv[index]);
  if (options.command_.empty()) Fail("a Meta command is required");
  if (options.socket_path_.empty() == options.address_.empty()) {
    Fail("exactly one of --socket and --addr is required");
  }
  const bool tls_any = !options.tls_ca_.empty() || !options.tls_cert_.empty() ||
                       !options.tls_key_.empty();
  const bool tls_all = !options.tls_ca_.empty() && !options.tls_cert_.empty() &&
                       !options.tls_key_.empty();
  if (!options.address_.empty() && !tls_all) {
    Fail("--addr requires --tls-ca, --tls-cert, and --tls-key");
  }
  if (!options.socket_path_.empty() &&
      (tls_any || !options.tls_server_name_.empty())) {
    Fail("TLS options apply only to --addr");
  }
  return options;
}

Endpoint ParseEndpoint(std::string_view text) {
  const std::size_t colon = text.rfind(':');
  if (colon == std::string_view::npos || colon == 0 ||
      colon + 1 == text.size()) {
    Fail("--addr must be a numeric IPv4/IPv6 address followed by :port");
  }
  std::string host(text.substr(0, colon));
  if (host.size() >= 2 && host.front() == '[' && host.back() == ']') {
    host = host.substr(1, host.size() - 2);
  }
  int port = 0;
  if (!ParseInt(text.substr(colon + 1), 1, 65535, &port)) {
    Fail("--addr has an invalid port");
  }

  Endpoint endpoint;
  endpoint.host_ = host;
  sockaddr_in ipv4{};
  if (::inet_pton(AF_INET, host.c_str(), &ipv4.sin_addr) == 1) {
    ipv4.sin_family = AF_INET;
    ipv4.sin_port = htons(static_cast<std::uint16_t>(port));
    std::memcpy(&endpoint.address_, &ipv4, sizeof(ipv4));
    endpoint.length_ = sizeof(ipv4);
    endpoint.family_ = AF_INET;
    return endpoint;
  }
  sockaddr_in6 ipv6{};
  if (::inet_pton(AF_INET6, host.c_str(), &ipv6.sin6_addr) == 1) {
    ipv6.sin6_family = AF_INET6;
    ipv6.sin6_port = htons(static_cast<std::uint16_t>(port));
    std::memcpy(&endpoint.address_, &ipv6, sizeof(ipv6));
    endpoint.length_ = sizeof(ipv6);
    endpoint.family_ = AF_INET6;
    return endpoint;
  }
  Fail("--addr host must be a numeric IPv4/IPv6 address");
}

int RemainingMillis(Deadline deadline) {
  const auto remaining = deadline - std::chrono::steady_clock::now();
  if (remaining <= std::chrono::steady_clock::duration::zero()) return 0;
  const auto millis =
      std::chrono::ceil<std::chrono::milliseconds>(remaining).count();
  return static_cast<int>(
      std::min<std::int64_t>(millis, std::numeric_limits<int>::max()));
}

void WaitFor(int fd, short events, Deadline deadline,
             std::string_view operation) {
  while (true) {
    pollfd descriptor{.fd = fd, .events = events, .revents = 0};
    const int timeout = RemainingMillis(deadline);
    if (timeout == 0) Fail(std::string(operation) + " timed out");
    const int result = ::poll(&descriptor, 1, timeout);
    if (result > 0) {
      if ((descriptor.revents & events) != 0) return;
      if ((descriptor.revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
        Fail(std::string(operation) + " failed");
      }
      continue;
    }
    if (result == 0) Fail(std::string(operation) + " timed out");
    if (errno != EINTR) Fail(ErrnoMessage(operation));
  }
}

FileDescriptor Connect(int family, const sockaddr* address, socklen_t length,
                       Deadline deadline) {
  FileDescriptor fd(
      ::socket(family, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0));
  if (fd.get() < 0) Fail(ErrnoMessage("socket"));
  if (::connect(fd.get(), address, length) == 0) return fd;
  if (errno != EINPROGRESS) Fail(ErrnoMessage("connect"));
  WaitFor(fd.get(), POLLOUT, deadline, "connect");
  int error = 0;
  socklen_t error_size = sizeof(error);
  if (::getsockopt(fd.get(), SOL_SOCKET, SO_ERROR, &error, &error_size) != 0) {
    Fail(ErrnoMessage("getsockopt(SO_ERROR)"));
  }
  if (error != 0) Fail(ErrnoMessage("connect", error));
  return fd;
}

FileDescriptor ConnectUnix(const std::string& path, Deadline deadline) {
  sockaddr_un address{};
  address.sun_family = AF_UNIX;
  if (path.size() >= sizeof(address.sun_path)) {
    Fail("Unix socket path is too long");
  }
  std::memcpy(address.sun_path, path.c_str(), path.size() + 1);
  return Connect(AF_UNIX, reinterpret_cast<const sockaddr*>(&address),
                 sizeof(address), deadline);
}

void PlainWriteAll(int fd, std::string_view bytes, Deadline deadline) {
  while (!bytes.empty()) {
    const ssize_t written =
        ::send(fd, bytes.data(), bytes.size(), MSG_NOSIGNAL);
    if (written > 0) {
      bytes.remove_prefix(static_cast<std::size_t>(written));
      continue;
    }
    if (written < 0 && errno == EINTR) continue;
    if (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      WaitFor(fd, POLLOUT, deadline, "write");
      continue;
    }
    Fail(ErrnoMessage("write"));
  }
}

std::string PlainReadLine(int fd, Deadline deadline) {
  std::string reply;
  char buffer[4096];
  while (true) {
    const ssize_t received = ::recv(fd, buffer, sizeof(buffer), 0);
    if (received > 0) {
      const std::string_view chunk(buffer, static_cast<std::size_t>(received));
      const std::size_t newline = chunk.find('\n');
      reply.append(chunk.substr(0, newline));
      if (reply.size() > kMaxReplyBytes) Fail("reply exceeds 256 MiB limit");
      if (newline != std::string_view::npos) break;
      continue;
    }
    if (received == 0) Fail("server closed before terminating its reply");
    if (errno == EINTR) continue;
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      WaitFor(fd, POLLIN, deadline, "read");
      continue;
    }
    Fail(ErrnoMessage("read"));
  }
  if (!reply.empty() && reply.back() == '\r') reply.pop_back();
  return reply;
}

SslContext MakeTlsContext(const Options& options) {
  SSL_CTX* raw = SSL_CTX_new(TLS_client_method());
  if (raw == nullptr) Fail(OpenSslMessage("SSL_CTX_new"));
  SslContext context(raw);
  if (SSL_CTX_set_min_proto_version(raw, TLS1_2_VERSION) != 1) {
    Fail(OpenSslMessage("set TLS minimum version"));
  }
  if (SSL_CTX_load_verify_locations(raw, options.tls_ca_.c_str(), nullptr) !=
      1) {
    Fail(OpenSslMessage("load TLS CA"));
  }
  if (SSL_CTX_use_certificate_chain_file(raw, options.tls_cert_.c_str()) != 1) {
    Fail(OpenSslMessage("load TLS certificate chain"));
  }
  if (SSL_CTX_use_PrivateKey_file(raw, options.tls_key_.c_str(),
                                  SSL_FILETYPE_PEM) != 1) {
    Fail(OpenSslMessage("load TLS private key"));
  }
  if (SSL_CTX_check_private_key(raw) != 1) {
    Fail(OpenSslMessage("check TLS private key"));
  }
  SSL_CTX_set_verify(raw, SSL_VERIFY_PEER, nullptr);
  return context;
}

void WaitForSsl(SSL* ssl, int result, Deadline deadline,
                std::string_view operation) {
  const int error = SSL_get_error(ssl, result);
  if (error == SSL_ERROR_WANT_READ) {
    WaitFor(SSL_get_fd(ssl), POLLIN, deadline, operation);
    return;
  }
  if (error == SSL_ERROR_WANT_WRITE) {
    WaitFor(SSL_get_fd(ssl), POLLOUT, deadline, operation);
    return;
  }
  if (error == SSL_ERROR_SYSCALL && errno != 0) {
    Fail(ErrnoMessage(operation));
  }
  Fail(OpenSslMessage(operation));
}

SslSession StartTls(SSL_CTX* context, int fd, const Endpoint& endpoint,
                    std::string_view server_name, Deadline deadline) {
  SSL* raw = SSL_new(context);
  if (raw == nullptr) Fail(OpenSslMessage("SSL_new"));
  SslSession session(raw);
  if (SSL_set_fd(raw, fd) != 1) Fail(OpenSslMessage("SSL_set_fd"));

  X509_VERIFY_PARAM* verify = SSL_get0_param(raw);
  if (!server_name.empty()) {
    const std::string name(server_name);
    if (SSL_set_tlsext_host_name(raw, name.c_str()) != 1 ||
        X509_VERIFY_PARAM_set1_host(verify, name.c_str(), name.size()) != 1) {
      Fail(OpenSslMessage("configure TLS server name"));
    }
  } else if (X509_VERIFY_PARAM_set1_ip_asc(verify, endpoint.host_.c_str()) !=
             1) {
    Fail(OpenSslMessage("configure TLS server IP"));
  }

  while (true) {
    errno = 0;
    const int result = SSL_connect(raw);
    if (result == 1) break;
    WaitForSsl(raw, result, deadline, "TLS handshake");
  }
  if (SSL_get_verify_result(raw) != X509_V_OK) {
    Fail("TLS peer certificate verification failed");
  }
  return session;
}

void TlsWriteAll(SSL* ssl, std::string_view bytes, Deadline deadline) {
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
    WaitForSsl(ssl, written, deadline, "TLS write");
  }
}

std::string TlsReadLine(SSL* ssl, Deadline deadline) {
  std::string reply;
  char buffer[4096];
  while (true) {
    errno = 0;
    const int received = SSL_read(ssl, buffer, sizeof(buffer));
    if (received > 0) {
      const std::string_view chunk(buffer, static_cast<std::size_t>(received));
      const std::size_t newline = chunk.find('\n');
      reply.append(chunk.substr(0, newline));
      if (reply.size() > kMaxReplyBytes) Fail("reply exceeds 256 MiB limit");
      if (newline != std::string_view::npos) break;
      continue;
    }
    const int error = SSL_get_error(ssl, received);
    if (error == SSL_ERROR_ZERO_RETURN) {
      Fail("server closed before terminating its reply");
    }
    WaitForSsl(ssl, received, deadline, "TLS read");
  }
  if (!reply.empty() && reply.back() == '\r') reply.pop_back();
  return reply;
}

std::string BuildCommand(const std::vector<std::string>& arguments) {
  std::string command;
  for (const std::string& argument : arguments) {
    if (argument.empty() ||
        std::any_of(argument.begin(), argument.end(),
                    [](unsigned char ch) { return std::isspace(ch) != 0; })) {
      Fail("command arguments must be non-empty and whitespace-free");
    }
    if (!command.empty()) command.push_back(' ');
    command.append(argument);
  }
  if (command.size() + 1 > kMaxCommandBytes) {
    Fail("command exceeds 64 KiB limit");
  }
  command.push_back('\n');
  return command;
}

bool IsReply(std::string_view reply, std::string_view prefix) {
  return reply == prefix ||
         (reply.starts_with(prefix) && reply.size() > prefix.size() &&
          reply[prefix.size()] == ' ');
}

int Run(const Options& options) {
  const Deadline deadline = std::chrono::steady_clock::now() +
                            std::chrono::milliseconds(options.timeout_ms_);
  const std::string command = BuildCommand(options.command_);
  std::string reply;
  if (!options.socket_path_.empty()) {
    FileDescriptor fd = ConnectUnix(options.socket_path_, deadline);
    PlainWriteAll(fd.get(), command, deadline);
    reply = PlainReadLine(fd.get(), deadline);
  } else {
    const Endpoint endpoint = ParseEndpoint(options.address_);
    FileDescriptor fd = Connect(
        endpoint.family_, reinterpret_cast<const sockaddr*>(&endpoint.address_),
        endpoint.length_, deadline);
    SslContext context = MakeTlsContext(options);
    SslSession ssl = StartTls(context.get(), fd.get(), endpoint,
                              options.tls_server_name_, deadline);
    TlsWriteAll(ssl.get(), command, deadline);
    reply = TlsReadLine(ssl.get(), deadline);
    (void)SSL_shutdown(ssl.get());
  }

  std::cout << reply << '\n';
  if (IsReply(reply, "OK")) return 0;
  if (IsReply(reply, "ERR")) return 2;
  Fail("server returned a malformed reply");
}

}  // namespace

int main(int argc, char** argv) {
  // OpenSSL ultimately writes through the socket fd. A disconnected remote
  // must become a normal CLI error instead of terminating the process with
  // SIGPIPE before its diagnostic can be printed.
  (void)::signal(SIGPIPE, SIG_IGN);
  try {
    bool early_exit = false;
    const Options options = ParseOptions(argc, argv, &early_exit);
    if (early_exit) return 0;
    return Run(options);
  } catch (const std::exception& error) {
    std::fprintf(stderr, "keylane-meta-ctl: %s\n", error.what());
    return 1;
  }
}
