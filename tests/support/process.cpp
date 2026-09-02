#include "tests/support/process.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <spawn.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <charconv>
#include <cstring>
#include <fstream>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <thread>

extern char** environ;

namespace keylane::test {
namespace {

constexpr std::size_t kMaxRespLineBytes = 64 * 1024;
constexpr std::size_t kMaxRespReplyBytes = 16 * 1024 * 1024;
constexpr std::size_t kMaxRespNesting = 32;

std::vector<std::string> BuildEnvironment(
    const std::vector<std::pair<std::string, std::string>>& overrides) {
  std::vector<std::string> result;
  for (char** current = environ; current != nullptr && *current != nullptr;
       ++current) {
    const std::string_view entry(*current);
    const std::size_t separator = entry.find('=');
    const std::string_view name = entry.substr(0, separator);
    bool replaced = false;
    for (const auto& [override_name, ignored] : overrides) {
      (void)ignored;
      if (name == override_name) {
        replaced = true;
        break;
      }
    }
    if (!replaced) result.emplace_back(entry);
  }
  for (const auto& [name, value] : overrides) {
    result.push_back(name + "=" + value);
  }
  return result;
}

std::vector<char*> MutablePointers(std::vector<std::string>* values) {
  std::vector<char*> result;
  result.reserve(values->size() + 1);
  for (std::string& value : *values) result.push_back(value.data());
  result.push_back(nullptr);
  return result;
}

void CloseFd(int* fd) {
  if (*fd >= 0) {
    (void)::close(*fd);
    *fd = -1;
  }
}

}  // namespace

[[noreturn]] void Fail(std::string message) {
  throw std::runtime_error(std::move(message));
}

void WriteFile(const std::filesystem::path& path, std::string_view contents) {
  const int fd =
      ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  if (fd < 0) Fail("failed to create " + path.string());
  while (!contents.empty()) {
    const ssize_t written = ::write(fd, contents.data(), contents.size());
    if (written < 0 && errno == EINTR) continue;
    if (written <= 0) {
      const int saved_errno = errno;
      (void)::close(fd);
      Fail("failed to write " + path.string() + ": " +
           std::strerror(saved_errno));
    }
    contents.remove_prefix(static_cast<std::size_t>(written));
  }
  if (::close(fd) != 0) Fail("failed to close " + path.string());
}

void CreateDataFile(const std::filesystem::path& path, std::uint64_t bytes) {
  const int fd =
      ::open(path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  if (fd < 0) Fail("failed to create data file " + path.string());
  const int allocated = ::posix_fallocate(fd, 0, static_cast<off_t>(bytes));
  const int closed = ::close(fd);
  if (allocated != 0 || closed != 0) {
    Fail("failed to allocate data file " + path.string());
  }
}

std::string ReadFile(const std::filesystem::path& path) {
  std::ifstream input(path);
  if (!input) Fail("failed to read " + path.string());
  return std::string(std::istreambuf_iterator<char>(input),
                     std::istreambuf_iterator<char>());
}

TempDirectory::TempDirectory(std::string_view label) {
  std::string pattern = "/tmp/keylane-" + std::string(label) + "-XXXXXX";
  std::vector<char> mutable_pattern(pattern.begin(), pattern.end());
  mutable_pattern.push_back('\0');
  char* created = ::mkdtemp(mutable_pattern.data());
  if (created == nullptr) Fail("mkdtemp failed");
  path_ = created;
}

TempDirectory::~TempDirectory() {
  if (preserve_ || std::uncaught_exceptions() != 0) {
    std::cerr << "test artifacts retained at " << path_ << '\n';
    return;
  }
  std::error_code ignored;
  std::filesystem::remove_all(path_, ignored);
}

PortReservation::PortReservation() {
  fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (fd_ < 0) Fail("socket failed while reserving a test port");
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = 0;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (::bind(fd_, reinterpret_cast<const sockaddr*>(&address),
             sizeof(address)) != 0 ||
      ::listen(fd_, 1) != 0) {
    CloseFd(&fd_);
    Fail("failed to reserve a loopback test port");
  }
  socklen_t size = sizeof(address);
  if (::getsockname(fd_, reinterpret_cast<sockaddr*>(&address), &size) != 0) {
    CloseFd(&fd_);
    Fail("getsockname failed for reserved test port");
  }
  port_ = ntohs(address.sin_port);
}

PortReservation::PortReservation(PortReservation&& other) noexcept
    : fd_(other.fd_), port_(other.port_) {
  other.fd_ = -1;
  other.port_ = 0;
}

PortReservation& PortReservation::operator=(PortReservation&& other) noexcept {
  if (this == &other) return *this;
  CloseFd(&fd_);
  fd_ = other.fd_;
  port_ = other.port_;
  other.fd_ = -1;
  other.port_ = 0;
  return *this;
}

PortReservation::~PortReservation() { CloseFd(&fd_); }

std::uint16_t PortReservation::ReleaseForSpawn() {
  if (fd_ < 0) Fail("test port reservation was already released");
  CloseFd(&fd_);
  return port_;
}

ChildProcess::ChildProcess(
    std::vector<std::string> arguments, const std::filesystem::path& log_path,
    const std::vector<std::pair<std::string, std::string>>& environment) {
  if (arguments.empty()) Fail("cannot spawn a process without an executable");
  const int log_fd =
      ::open(log_path.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0600);
  if (log_fd < 0) Fail("failed to open child process log");

  posix_spawn_file_actions_t actions;
  posix_spawnattr_t attributes;
  if (::posix_spawn_file_actions_init(&actions) != 0) {
    (void)::close(log_fd);
    Fail("failed to initialize posix_spawn file actions");
  }
  if (::posix_spawnattr_init(&attributes) != 0) {
    (void)::posix_spawn_file_actions_destroy(&actions);
    (void)::close(log_fd);
    Fail("failed to initialize posix_spawn attributes");
  }
  auto cleanup = [&] {
    (void)::posix_spawn_file_actions_destroy(&actions);
    (void)::posix_spawnattr_destroy(&attributes);
    (void)::close(log_fd);
  };
  if (::posix_spawn_file_actions_adddup2(&actions, log_fd, STDOUT_FILENO) !=
          0 ||
      ::posix_spawn_file_actions_adddup2(&actions, log_fd, STDERR_FILENO) !=
          0 ||
      ::posix_spawn_file_actions_addclose(&actions, log_fd) != 0) {
    cleanup();
    Fail("failed to configure posix_spawn log actions");
  }
  short flags = POSIX_SPAWN_SETPGROUP;
  if (::posix_spawnattr_setflags(&attributes, flags) != 0 ||
      ::posix_spawnattr_setpgroup(&attributes, 0) != 0) {
    cleanup();
    Fail("failed to configure child process group");
  }

  std::vector<std::string> child_environment = BuildEnvironment(environment);
  std::vector<char*> argv = MutablePointers(&arguments);
  std::vector<char*> envp = MutablePointers(&child_environment);
  const int spawned = ::posix_spawn(&pid_, arguments.front().c_str(), &actions,
                                    &attributes, argv.data(), envp.data());
  cleanup();
  if (spawned != 0) {
    pid_ = -1;
    Fail("posix_spawn failed: " + std::string(std::strerror(spawned)));
  }
}

ChildProcess::ChildProcess(ChildProcess&& other) noexcept
    : pid_(other.pid_), stopped_(other.stopped_) {
  other.pid_ = -1;
  other.stopped_ = false;
}

ChildProcess& ChildProcess::operator=(ChildProcess&& other) noexcept {
  if (this == &other) return *this;
  StopNoThrow();
  pid_ = other.pid_;
  stopped_ = other.stopped_;
  other.pid_ = -1;
  other.stopped_ = false;
  return *this;
}

ChildProcess::~ChildProcess() { StopNoThrow(); }

void ChildProcess::StopNoThrow() noexcept {
  if (pid_ <= 0) return;
  if (stopped_) (void)::kill(-pid_, SIGCONT);
  (void)::kill(-pid_, SIGKILL);
  for (;;) {
    int status = 0;
    const pid_t waited = ::waitpid(pid_, &status, 0);
    if (waited == pid_ || (waited < 0 && errno == ECHILD)) break;
    if (waited < 0 && errno == EINTR) continue;
    break;
  }
  pid_ = -1;
  stopped_ = false;
}

void ChildProcess::KillProcessGroup(int signal) const {
  if (pid_ <= 0) return;
  if (::kill(-pid_, signal) != 0 && errno != ESRCH) {
    Fail("failed to signal child process group");
  }
}

void ChildProcess::Pause() {
  if (pid_ <= 0 || stopped_) Fail("cannot pause inactive child process");
  KillProcessGroup(SIGSTOP);
  int status = 0;
  for (;;) {
    const pid_t waited = ::waitpid(pid_, &status, WUNTRACED);
    if (waited < 0 && errno == EINTR) continue;
    if (waited != pid_ || !WIFSTOPPED(status)) {
      Fail("child process did not enter stopped state");
    }
    break;
  }
  stopped_ = true;
}

void ChildProcess::Resume() {
  if (pid_ <= 0 || !stopped_) Fail("cannot resume active child process");
  KillProcessGroup(SIGCONT);
  stopped_ = false;
}

int ChildProcess::Wait(std::chrono::milliseconds timeout) {
  if (pid_ <= 0) return 0;
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    int status = 0;
    const pid_t waited = ::waitpid(pid_, &status, WNOHANG);
    if (waited == pid_) {
      pid_ = -1;
      stopped_ = false;
      return status;
    }
    if (waited < 0 && errno != EINTR) Fail("waitpid failed");
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  Fail("child process did not exit before deadline");
}

void ChildProcess::Stop(int signal) {
  if (pid_ <= 0) return;
  if (stopped_) {
    KillProcessGroup(SIGCONT);
    stopped_ = false;
  }
  KillProcessGroup(signal);
  int status = 0;
  for (;;) {
    const pid_t waited = ::waitpid(pid_, &status, 0);
    if (waited < 0 && errno == EINTR) continue;
    if (waited < 0 && errno == ECHILD) break;
    if (waited != pid_) Fail("waitpid failed while stopping child process");
    break;
  }
  pid_ = -1;
}

RespClient::RespClient(RespClient&& other) noexcept : fd_(other.fd_) {
  other.fd_ = -1;
}

RespClient& RespClient::operator=(RespClient&& other) noexcept {
  if (this == &other) return *this;
  CloseFd(&fd_);
  fd_ = other.fd_;
  other.fd_ = -1;
  return *this;
}

RespClient::~RespClient() { CloseFd(&fd_); }

std::size_t RespClient::Length(std::string_view line) {
  std::size_t value = 0;
  const auto parsed =
      std::from_chars(line.data() + 1, line.data() + line.size(), value);
  if (parsed.ec != std::errc{} || parsed.ptr != line.data() + line.size()) {
    Fail("invalid RESP length");
  }
  return value;
}

void RespClient::SendAll(std::string_view bytes) {
  while (!bytes.empty()) {
    const ssize_t sent = ::send(fd_, bytes.data(), bytes.size(), MSG_NOSIGNAL);
    if (sent < 0 && errno == EINTR) continue;
    if (sent <= 0) Fail("send failed");
    bytes.remove_prefix(static_cast<std::size_t>(sent));
  }
}

void RespClient::ReadExact(char* output, std::size_t bytes) {
  while (bytes != 0) {
    const ssize_t count = ::recv(fd_, output, bytes, 0);
    if (count < 0 && errno == EINTR) continue;
    if (count <= 0) Fail("connection closed while reading RESP");
    output += count;
    bytes -= static_cast<std::size_t>(count);
  }
}

std::string RespClient::ReadLine() {
  std::string line;
  while (!line.ends_with("\r\n")) {
    char byte = 0;
    ReadExact(&byte, 1);
    line.push_back(byte);
    if (line.size() > kMaxRespLineBytes) Fail("RESP line too long");
  }
  line.resize(line.size() - 2);
  return line;
}

std::string RespClient::ReadReply() {
  std::size_t wire_bytes = 0;
  return ReadReply(0, &wire_bytes);
}

std::string RespClient::ReadReply(std::size_t depth, std::size_t* wire_bytes) {
  if (depth > kMaxRespNesting) Fail("RESP nesting too deep");
  const std::string line = ReadLine();
  if (line.size() + 2 > kMaxRespReplyBytes - *wire_bytes) {
    Fail("RESP reply too large");
  }
  *wire_bytes += line.size() + 2;
  if (line.empty()) Fail("empty RESP reply");
  if (line[0] == '+' || line[0] == '-' || line[0] == ':') return line;
  if (line[0] == '$') {
    if (line == "$-1") return line;
    const std::size_t body_size = Length(line);
    if (body_size > kMaxRespReplyBytes - *wire_bytes ||
        body_size + 2 > kMaxRespReplyBytes - *wire_bytes) {
      Fail("RESP reply too large");
    }
    std::string body(body_size + 2, '\0');
    ReadExact(body.data(), body.size());
    *wire_bytes += body.size();
    if (!body.ends_with("\r\n")) Fail("invalid RESP bulk terminator");
    body.resize(body.size() - 2);
    return line + "\r\n" + body;
  }
  if (line[0] == '*') {
    if (line == "*-1") return line;
    const std::size_t element_count = Length(line);
    // Even the smallest valid child consumes three wire bytes, so this also
    // bounds loop work before reading nested replies.
    if (element_count > (kMaxRespReplyBytes - *wire_bytes) / 3) {
      Fail("RESP array too large");
    }
    std::string result = line;
    for (std::size_t i = 0; i < element_count; ++i) {
      result += "\r\n" + ReadReply(depth + 1, wire_bytes);
    }
    return result;
  }
  Fail("unsupported RESP reply");
}

std::string RespClient::Command(
    const std::vector<std::string_view>& arguments) {
  std::string wire = "*" + std::to_string(arguments.size()) + "\r\n";
  for (const std::string_view argument : arguments) {
    wire += "$" + std::to_string(argument.size()) + "\r\n";
    wire.append(argument);
    wire += "\r\n";
  }
  SendAll(wire);
  return ReadReply();
}

RespClient Connect(std::uint16_t port, std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) Fail("client socket failed");
    timeval socket_timeout{.tv_sec = 10, .tv_usec = 0};
    (void)::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &socket_timeout,
                       sizeof(socket_timeout));
    (void)::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &socket_timeout,
                       sizeof(socket_timeout));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::connect(fd, reinterpret_cast<const sockaddr*>(&address),
                  sizeof(address)) == 0) {
      return RespClient(fd);
    }
    (void)::close(fd);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  Fail("timed out connecting to port " + std::to_string(port));
}

void WaitUntil(std::string_view label, std::chrono::milliseconds timeout,
               const std::function<bool()>& ready,
               std::chrono::milliseconds poll_interval) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    try {
      if (ready()) return;
    } catch (const std::exception&) {
    }
    std::this_thread::sleep_for(poll_interval);
  }
  Fail("timed out waiting for " + std::string(label));
}

}  // namespace keylane::test
