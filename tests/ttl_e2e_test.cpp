#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <charconv>
#include <chrono>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <iterator>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

using namespace std::chrono_literals;

[[noreturn]] void Fail(std::string message) {
  throw std::runtime_error(std::move(message));
}

class RespClient {
 public:
  explicit RespClient(int fd) : fd_(fd) {}
  RespClient(const RespClient&) = delete;
  RespClient& operator=(const RespClient&) = delete;
  RespClient(RespClient&& other) noexcept : fd_(other.fd_) {
    other.fd_ = -1;
  }
  RespClient& operator=(RespClient&&) = delete;
  ~RespClient() {
    if (fd_ >= 0) ::close(fd_);
  }

  std::string Command(const std::vector<std::string_view>& args) {
    std::string request = "*" + std::to_string(args.size()) + "\r\n";
    for (std::string_view arg : args) {
      request += "$" + std::to_string(arg.size()) + "\r\n";
      request.append(arg);
      request += "\r\n";
    }
    SendAll(request);
    const std::string line = ReadLine();
    if (!line.starts_with('$') || line == "$-1") {
      return line;
    }
    std::size_t size = 0;
    const char* begin = line.data() + 1;
    const char* end = line.data() + line.size();
    const auto [parsed_end, error] = std::from_chars(begin, end, size);
    if (error != std::errc{} || parsed_end != end) {
      Fail("malformed bulk reply length");
    }
    std::string payload(size + 2, '\0');
    ReadExact(payload.data(), payload.size());
    if (!payload.ends_with("\r\n")) {
      Fail("malformed bulk reply terminator");
    }
    payload.resize(size);
    return line + "\r\n" + payload;
  }

 private:
  void SendAll(std::string_view bytes) {
    while (!bytes.empty()) {
      const ssize_t sent = ::send(fd_, bytes.data(), bytes.size(), MSG_NOSIGNAL);
      if (sent < 0) {
        if (errno == EINTR) continue;
        Fail("send failed: " + std::string(std::strerror(errno)));
      }
      if (sent == 0) Fail("send returned zero bytes");
      bytes.remove_prefix(static_cast<std::size_t>(sent));
    }
  }

  void ReadExact(char* output, std::size_t size) {
    while (size != 0) {
      const ssize_t received = ::recv(fd_, output, size, 0);
      if (received < 0) {
        if (errno == EINTR) continue;
        Fail("recv failed: " + std::string(std::strerror(errno)));
      }
      if (received == 0) Fail("server closed the connection");
      output += received;
      size -= static_cast<std::size_t>(received);
    }
  }

  std::string ReadLine() {
    std::string response;
    while (!response.ends_with("\r\n")) {
      char byte = 0;
      ReadExact(&byte, 1);
      response.push_back(byte);
      if (response.size() > 4096) Fail("unexpectedly long RESP line");
    }
    response.resize(response.size() - 2);
    return response;
  }

  int fd_ = -1;
};

std::uint16_t FindFreePort() {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (fd < 0) Fail("socket failed while selecting a port");
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  if (::bind(fd, reinterpret_cast<const sockaddr*>(&address),
             sizeof(address)) != 0) {
    ::close(fd);
    Fail("bind failed while selecting a port");
  }
  socklen_t bytes = sizeof(address);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &bytes) != 0) {
    ::close(fd);
    Fail("getsockname failed while selecting a port");
  }
  ::close(fd);
  return ntohs(address.sin_port);
}

void CreateDataFile(const std::string& path, std::uint64_t bytes) {
  const int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC,
                        0600);
  if (fd < 0) Fail("failed to create test data file");
  const int allocated = ::posix_fallocate(fd, 0, static_cast<off_t>(bytes));
  const int close_error = ::close(fd);
  if (allocated != 0 || close_error != 0) Fail("failed to size data file");
}

RespClient Connect(std::uint16_t port) {
  const auto deadline = std::chrono::steady_clock::now() + 20s;
  while (std::chrono::steady_clock::now() < deadline) {
    const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) Fail("client socket failed");
    timeval timeout{.tv_sec = 30, .tv_usec = 0};
    (void)::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    (void)::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(port);
    if (::connect(fd, reinterpret_cast<const sockaddr*>(&address),
                  sizeof(address)) == 0) {
      return RespClient(fd);
    }
    ::close(fd);
    std::this_thread::sleep_for(10ms);
  }
  Fail("timed out connecting to Keylane");
}

class ServerProcess {
 public:
  ServerProcess(const std::string& binary, std::uint16_t port,
                const std::string& data_path, const std::string& log_path) {
    pid_ = ::fork();
    if (pid_ < 0) Fail("fork failed");
    if (pid_ == 0) {
      const int log_fd = ::open(log_path.c_str(),
                                O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC,
                                0600);
      if (log_fd >= 0) {
        (void)::dup2(log_fd, STDOUT_FILENO);
        (void)::dup2(log_fd, STDERR_FILENO);
        ::close(log_fd);
      }
      std::vector<std::string> arguments{
          binary,          "--port",         std::to_string(port),
          "--threads",     "1",              "--recv-buffers",
          "0",             "--flush-max-ms", "20",
          "--data-file",   data_path,
      };
      std::vector<char*> child_argv;
      for (std::string& argument : arguments) {
        child_argv.push_back(argument.data());
      }
      child_argv.push_back(nullptr);
      ::execv(binary.c_str(), child_argv.data());
      _exit(127);
    }
  }

  ServerProcess(const ServerProcess&) = delete;
  ServerProcess& operator=(const ServerProcess&) = delete;
  ~ServerProcess() {
    if (pid_ > 0) {
      (void)::kill(pid_, SIGKILL);
      (void)::waitpid(pid_, nullptr, 0);
    }
  }

  void Stop() {
    if (pid_ <= 0) return;
    if (::kill(pid_, SIGINT) != 0 && errno != ESRCH) Fail("signal failed");
    const auto deadline = std::chrono::steady_clock::now() + 30s;
    while (std::chrono::steady_clock::now() < deadline) {
      int status = 0;
      const pid_t result = ::waitpid(pid_, &status, WNOHANG);
      if (result == pid_) {
        pid_ = -1;
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
          Fail("Keylane exited unsuccessfully");
        }
        return;
      }
      if (result < 0) Fail("waitpid failed");
      std::this_thread::sleep_for(10ms);
    }
    Fail("Keylane did not stop");
  }

 private:
  pid_t pid_ = -1;
};

void Expect(std::string_view actual, std::string_view expected,
            std::string_view operation) {
  if (actual != expected) {
    Fail(std::string(operation) + " returned '" + std::string(actual) +
         "', expected '" + std::string(expected) + "'");
  }
}

long long IntegerReply(std::string_view reply, std::string_view operation) {
  if (!reply.starts_with(':')) Fail(std::string(operation) + " was not integer");
  long long value = 0;
  const char* begin = reply.data() + 1;
  const char* end = reply.data() + reply.size();
  const auto [parsed_end, error] = std::from_chars(begin, end, value);
  if (error != std::errc{} || parsed_end != end) {
    Fail(std::string(operation) + " was malformed");
  }
  return value;
}

void ExpectRange(long long actual, long long minimum, long long maximum,
                 std::string_view operation) {
  if (actual < minimum || actual > maximum) {
    Fail(std::string(operation) + " returned " + std::to_string(actual));
  }
}

std::string ReadFile(const std::string& path) {
  std::ifstream input(path);
  return std::string(std::istreambuf_iterator<char>(input),
                     std::istreambuf_iterator<char>());
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "usage: ttl_e2e_test /path/to/keylane\n";
    return 2;
  }
  const std::string prefix =
      "/tmp/keylane-ttl-" + std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  (void)::unlink(data_path.c_str());
  (void)::unlink(log_path.c_str());

  try {
    const std::uint16_t port = FindFreePort();
    CreateDataFile(data_path, 96ULL * 1024 * 1024);
    {
      ServerProcess server(argv[1], port, data_path, log_path);
      RespClient client = Connect(port);
      Expect(client.Command({"PING"}), "+PONG", "PING");

      Expect(client.Command({"SET", "conditional", "old"}), "+OK",
             "initial SET");
      Expect(client.Command({"SET", "conditional", "new", "NX", "GET"}),
             "$3\r\nold", "SET NX GET failure");
      Expect(client.Command({"GET", "conditional"}), "$3\r\nold",
             "GET after failed NX");
      Expect(client.Command({"SET", "conditional", "new", "XX", "GET"}),
             "$3\r\nold", "SET XX GET success");
      Expect(client.Command({"GET", "conditional"}), "$3\r\nnew",
             "GET after successful XX");
      Expect(client.Command({"SET", "missing", "new", "XX"}), "$-1",
             "SET XX missing");
      Expect(client.Command({"SET", "missing", "new", "NX", "GET"}),
             "$-1", "SET NX GET create");
      Expect(client.Command({"GET", "missing"}), "$3\r\nnew",
             "GET created value");

      Expect(client.Command({"SET", "ttl", "one", "PX", "2000"}), "+OK",
             "SET PX");
      ExpectRange(IntegerReply(client.Command({"PTTL", "ttl"}), "PTTL"),
                  1, 2000, "PTTL after SET PX");
      Expect(client.Command({"SET", "ttl", "two", "KEEPTTL"}), "+OK",
             "SET KEEPTTL");
      ExpectRange(IntegerReply(client.Command({"PTTL", "ttl"}), "PTTL"),
                  1, 2000, "PTTL after KEEPTTL");
      Expect(client.Command({"SET", "ttl", "three"}), "+OK",
             "SET clears TTL");
      Expect(client.Command({"TTL", "ttl"}), ":-1", "TTL persistent");
      Expect(client.Command({"TTL", "does-not-exist"}), ":-2",
             "TTL missing");

      const auto unix_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                               std::chrono::system_clock::now()
                                   .time_since_epoch())
                               .count();
      const std::string future_ms = std::to_string(unix_ms + 2000);
      Expect(client.Command({"SET", "pxat", "v", "PXAT", future_ms}),
             "+OK", "SET PXAT");
      ExpectRange(IntegerReply(client.Command({"PTTL", "pxat"}), "PXAT PTTL"),
                  1, 2000, "PXAT deadline");
      const std::string future_seconds =
          std::to_string(unix_ms / 1000 + 3);
      Expect(client.Command({"SET", "exat", "v", "EXAT", future_seconds}),
             "+OK", "SET EXAT");
      ExpectRange(IntegerReply(client.Command({"PTTL", "exat"}), "EXAT PTTL"),
                  1, 3000, "EXAT deadline");
      Expect(client.Command({"SET", "past", "v", "PXAT", "1"}), "+OK",
             "SET past PXAT");
      Expect(client.Command({"GET", "past"}), "$-1", "past PXAT hidden");

      Expect(client.Command({"SET", "persistent-conditions", "v"}), "+OK",
             "SET persistent-conditions");
      Expect(client.Command(
                 {"EXPIRE", "persistent-conditions", "10", "GT"}),
             ":0", "EXPIRE GT treats persistence as infinity");
      Expect(client.Command(
                 {"EXPIRE", "persistent-conditions", "10", "LT"}),
             ":1", "EXPIRE LT on persistent key");
      Expect(client.Command(
                 {"EXPIRE", "persistent-conditions", "20", "XX"}),
             ":1", "EXPIRE XX on expiring key");

      Expect(client.Command({"SET", "expire-options", "v"}), "+OK",
             "SET expire-options");
      Expect(client.Command({"EXPIRE", "expire-options", "10", "NX"}),
             ":1", "EXPIRE NX");
      Expect(client.Command({"EXPIRE", "expire-options", "20", "NX"}),
             ":0", "EXPIRE NX failure");
      Expect(client.Command({"EXPIRE", "expire-options", "20", "GT"}),
             ":1", "EXPIRE GT");
      Expect(client.Command({"EXPIRE", "expire-options", "30", "LT"}),
             ":0", "EXPIRE LT failure");
      Expect(client.Command({"PERSIST", "expire-options"}), ":1",
             "PERSIST");
      Expect(client.Command({"PERSIST", "expire-options"}), ":0",
             "PERSIST without TTL");
      Expect(client.Command({"PEXPIRE", "expire-options", "0"}), ":1",
             "PEXPIRE immediate delete");
      Expect(client.Command({"GET", "expire-options"}), "$-1",
             "GET immediate deletion");

      Expect(client.Command({"SET", "counter", "1", "PX", "2000"}),
             "+OK", "counter SET");
      Expect(client.Command({"INCR", "counter"}), ":2", "counter INCR");
      ExpectRange(
          IntegerReply(client.Command({"PTTL", "counter"}), "counter PTTL"),
          1, 2000, "INCR preserves TTL");

      Expect(client.Command({"SET", "race", "old", "PX", "1"}), "+OK",
             "race old SET");
      std::this_thread::sleep_for(3ms);
      Expect(client.Command({"GET", "race"}), "$-1", "lazy expiration");
      Expect(client.Command({"SET", "race", "new"}), "+OK",
             "race replacement SET");
      std::this_thread::sleep_for(50ms);
      Expect(client.Command({"GET", "race"}), "$3\r\nnew",
             "stale expiration candidate");

      Expect(client.Command({"SET", "bad", "v", "NX", "XX"}),
             "-ERR syntax error", "conflicting SET conditions");
      Expect(client.Command({"SET", "bad", "v", "EX", "0"}),
             "-ERR invalid expire time in 'set' command",
             "invalid SET expiration");

      Expect(client.Command({"SET", "restart-live", "v", "PX", "5000"}),
             "+OK", "restart-live SET");
      Expect(client.Command({"SET", "restart-dead", "v", "PX", "50"}),
             "+OK", "restart-dead SET");
      server.Stop();
    }

    std::this_thread::sleep_for(100ms);
    {
      ServerProcess server(argv[1], port, data_path, log_path);
      RespClient client = Connect(port);
      Expect(client.Command({"GET", "restart-dead"}), "$-1",
             "expired while stopped");
      Expect(client.Command({"TTL", "restart-dead"}), ":-2",
             "restart expired TTL");
      Expect(client.Command({"GET", "restart-live"}), "$1\r\nv",
             "restart live value");
      ExpectRange(IntegerReply(client.Command({"PTTL", "restart-live"}),
                               "restart-live PTTL"),
                  1, 5000, "restart live TTL");
      server.Stop();
    }

    (void)::unlink(data_path.c_str());
    (void)::unlink(log_path.c_str());
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    const std::string log = ReadFile(log_path);
    if (!log.empty()) std::cerr << "--- Keylane log ---\n" << log;
    (void)::unlink(data_path.c_str());
    (void)::unlink(log_path.c_str());
    return 1;
  }
}
