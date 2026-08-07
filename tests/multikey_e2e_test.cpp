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

// RESP client returning the raw wire text of one complete reply, including
// nested array elements, so expectations compare exact protocol output.
class RespClient {
 public:
  explicit RespClient(int fd) : fd_(fd) {}
  RespClient(const RespClient&) = delete;
  RespClient& operator=(const RespClient&) = delete;
  RespClient(RespClient&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }
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
    return ReadReply();
  }

 private:
  std::string ReadReply() {
    const std::string line = ReadLine();
    switch (line.empty() ? '\0' : line.front()) {
      case '+':
      case '-':
      case ':':
        return line;
      case '$': {
        if (line == "$-1") {
          return line;
        }
        const std::size_t size = ParseLength(line);
        std::string payload(size + 2, '\0');
        ReadExact(payload.data(), payload.size());
        if (!payload.ends_with("\r\n")) {
          Fail("malformed bulk terminator");
        }
        payload.resize(size);
        return line + "\r\n" + payload;
      }
      case '*': {
        if (line == "*-1") {
          return line;
        }
        const std::size_t count = ParseLength(line);
        std::string reply = line;
        for (std::size_t i = 0; i < count; ++i) {
          reply += "\r\n" + ReadReply();
        }
        return reply;
      }
      default:
        Fail("unexpected RESP type: " + line);
    }
  }

  static std::size_t ParseLength(const std::string& line) {
    std::size_t size = 0;
    const char* begin = line.data() + 1;
    const char* end = line.data() + line.size();
    const auto [parsed, error] = std::from_chars(begin, end, size);
    if (error != std::errc{} || parsed != end) {
      Fail("malformed RESP length: " + line);
    }
    return size;
  }

  void SendAll(std::string_view bytes) {
    while (!bytes.empty()) {
      const ssize_t sent =
          ::send(fd_, bytes.data(), bytes.size(), MSG_NOSIGNAL);
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
  const int fd =
      ::open(path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
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
      const int log_fd = ::open(
          log_path.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0600);
      if (log_fd >= 0) {
        (void)::dup2(log_fd, STDOUT_FILENO);
        (void)::dup2(log_fd, STDERR_FILENO);
        ::close(log_fd);
      }
      std::vector<std::string> arguments{
          binary,        "--port",         std::to_string(port),
          "--threads",   "4",              "--recv-buffers",
          "0",           "--flush-max-ms", "20",
          "--data-file", data_path,
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

std::string Bulk(std::string_view value) {
  return "$" + std::to_string(value.size()) + "\r\n" + std::string(value);
}

std::string ReadFile(const std::string& path) {
  std::ifstream input(path);
  return std::string(std::istreambuf_iterator<char>(input),
                     std::istreambuf_iterator<char>());
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "usage: multikey_e2e_test /path/to/keylane\n";
    return 1;
  }
  const std::string suffix = std::to_string(::getpid());
  const std::string data_path = "/tmp/keylane-multikey-" + suffix + ".data";
  const std::string log_path = "/tmp/keylane-multikey-" + suffix + ".log";
  (void)::unlink(data_path.c_str());
  (void)::unlink(log_path.c_str());

  int exit_code = 0;
  try {
    const std::uint16_t port = FindFreePort();
    CreateDataFile(data_path, 512ULL * 1024 * 1024);
    ServerProcess server(argv[1], port, data_path, log_path);
    RespClient client = Connect(port);
    Expect(client.Command({"PING"}), "+PONG", "PING");

    // Cross-shard MSET/MGET: values come back in request order regardless of
    // which worker owns each key.
    Expect(client.Command({"MSET", "mk0", "v0", "mk1", "v1", "mk2", "v2",
                           "mk3", "v3", "mk4", "v4", "mk5", "v5", "mk6", "v6",
                           "mk7", "v7"}),
           "+OK", "cross-shard MSET");
    Expect(client.Command({"MGET", "mk5", "mk0", "missing", "mk7", "mk2"}),
           "*5\r\n" + Bulk("v5") + "\r\n" + Bulk("v0") + "\r\n$-1\r\n" +
               Bulk("v7") + "\r\n" + Bulk("v2"),
           "shuffled MGET");
    Expect(client.Command({"GET", "mk3"}), Bulk("v3"),
           "single GET after MSET");

    // Arity and pairing errors.
    Expect(client.Command({"MSET", "solo"}),
           "-ERR wrong number of arguments for 'mset' command",
           "MSET missing value");
    Expect(client.Command({"MSET", "a", "1", "b"}),
           "-ERR wrong number of arguments for 'mset' command",
           "MSET odd pair");
    Expect(client.Command({"MGET"}),
           "-ERR wrong number of arguments for 'mget' command", "empty MGET");

    // Duplicate keys: MSET applies in argument order (last wins), EXISTS
    // counts every occurrence, DEL deletes once.
    Expect(client.Command({"MSET", "dup", "first", "dup", "second"}), "+OK",
           "duplicate MSET");
    Expect(client.Command({"GET", "dup"}), Bulk("second"),
           "duplicate MSET last wins");
    Expect(client.Command({"EXISTS", "dup", "dup", "missing", "mk0"}), ":3",
           "EXISTS with duplicates");
    Expect(client.Command({"DEL", "dup", "dup"}), ":1", "duplicate DEL");
    Expect(client.Command({"EXISTS", "dup"}), ":0", "deleted dup");

    // Cross-shard DEL counts exactly the live keys it removed.
    Expect(client.Command({"DEL", "mk0", "missing", "mk5", "mk7", "mk7"}),
           ":3", "cross-shard DEL");
    Expect(client.Command({"MGET", "mk0", "mk5", "mk7", "mk1"}),
           "*4\r\n$-1\r\n$-1\r\n$-1\r\n" + Bulk("v1"), "MGET after DEL");

    // Hashtag keys share one slot: the whole command stays on a single shard
    // (fast path) and must behave identically.
    Expect(client.Command({"MSET", "{tag}a", "1", "{tag}b", "2", "{tag}c",
                           "3"}),
           "+OK", "hashtag MSET");
    Expect(client.Command({"MGET", "{tag}c", "{tag}a", "{tag}b"}),
           "*3\r\n" + Bulk("3") + "\r\n" + Bulk("1") + "\r\n" + Bulk("2"),
           "hashtag MGET");
    Expect(client.Command({"DEL", "{tag}a", "{tag}b", "{tag}c", "{tag}d"}),
           ":3", "hashtag DEL");

    // Binary safety: RESP is length-prefixed, so keys and values may carry
    // CRLF, NUL, and arbitrary bytes with no escaping anywhere in the chain.
    {
      const std::string bin_key("k\r\n\x00\xff\x01", 6);
      const std::string bin_value("v\x00\r\n\xfe\\x41", 8);
      Expect(client.Command({"SET", bin_key, bin_value}), "+OK",
             "binary SET");
      Expect(client.Command({"GET", bin_key}), Bulk(bin_value),
             "binary GET");
      Expect(client.Command({"MGET", bin_key, "missing"}),
             "*2\r\n" + Bulk(bin_value) + "\r\n$-1", "binary MGET");
      Expect(client.Command({"EXISTS", bin_key}), ":1", "binary EXISTS");
      Expect(client.Command({"DEL", bin_key}), ":1", "binary DEL");
    }

    // Mixed sizes across shards, including a value above the inline limit.
    const std::string large(9ULL * 1024 * 1024, 'L');
    Expect(client.Command({"MSET", "small", "s", "large", large}), "+OK",
           "MSET with large value");
    Expect(client.Command({"MGET", "large", "small"}),
           "*2\r\n" + Bulk(large) + "\r\n" + Bulk("s"), "MGET large");
    Expect(client.Command({"DEL", "large", "small"}), ":2", "DEL large");

    // ---- KEYS / SCAN TYPE ----
    Expect(client.Command({"MSET", "kx:1", "a", "kx:2", "b", "kx:3", "c",
                           "other", "1"}),
           "+OK", "KEYS seed");
    auto expect_members = [&](const std::string& reply, std::size_t count,
                              const std::vector<std::string>& members,
                              const char* what) {
      const std::string header = "*" + std::to_string(count) + "\r\n";
      if (reply.compare(0, header.size(), header) != 0) {
        Fail(std::string(what) + " count mismatch: " + reply.substr(0, 120));
      }
      for (const std::string& member : members) {
        const std::string element =
            "$" + std::to_string(member.size()) + "\r\n" + member;
        if (reply.find(element) == std::string::npos) {
          Fail(std::string(what) + " missing member '" + member + "'");
        }
      }
    };
    expect_members(client.Command({"KEYS", "kx:*"}), 3,
                   {"kx:1", "kx:2", "kx:3"}, "KEYS glob");
    expect_members(client.Command({"KEYS", "kx:?"}), 3,
                   {"kx:1", "kx:2", "kx:3"}, "KEYS question mark");
    Expect(client.Command({"KEYS", "nomatch:*"}), "*0", "KEYS no match");
    expect_members(client.Command({"KEYS", "other"}), 1, {"other"},
                   "KEYS exact");

    // Streaming stays bounded: several hundred keys still arrive with an
    // exact element count.
    std::vector<std::string> volume_storage;
    for (unsigned batch = 0; batch < 6; ++batch) {
      std::vector<std::string_view> mset_args;
      volume_storage.clear();
      mset_args.push_back("MSET");
      for (unsigned i = 0; i < 50; ++i) {
        volume_storage.push_back("vol:" +
                                 std::to_string(batch * 50 + i));
        volume_storage.push_back("v");
      }
      for (const std::string& arg : volume_storage) {
        mset_args.push_back(arg);
      }
      Expect(client.Command(mset_args), "+OK", "volume MSET");
    }
    {
      const std::string reply = client.Command({"KEYS", "vol:*"});
      if (reply.compare(0, 6, "*300\r\n") != 0) {
        Fail("KEYS volume count mismatch: " + reply.substr(0, 60));
      }
      if (reply.find("$5\r\nvol:0\r\n") == std::string::npos ||
          reply.find("$7\r\nvol:299") == std::string::npos) {
        Fail("KEYS volume members missing");
      }
    }

    // SCAN TYPE: strings match, other types match nothing. Follow the
    // cursor to completion as any SCAN client must.
    auto scan_all = [&](std::string_view type) {
      std::string collected;
      std::string cursor = "0";
      do {
        const std::string reply = client.Command(
            {"SCAN", cursor, "MATCH", "kx:*", "COUNT", "1000", "TYPE",
             type});
        const std::size_t cursor_start = reply.find("\r\n") + 2;
        const std::size_t digits = reply.find("\r\n", cursor_start) + 2;
        const std::size_t digits_end = reply.find("\r\n", digits);
        cursor = reply.substr(digits, digits_end - digits);
        collected += reply.substr(digits_end);
      } while (cursor != "0");
      return collected;
    };
    if (scan_all("string").find("kx:1") == std::string::npos) {
      Fail("SCAN TYPE string missing keys");
    }
    if (scan_all("hash").find("kx:") != std::string::npos) {
      Fail("SCAN TYPE hash returned string keys");
    }

    server.Stop();
  } catch (const std::exception& error) {
    std::cerr << error.what() << "\n--- Keylane log ---\n"
              << ReadFile(log_path) << std::flush;
    exit_code = 1;
  }

  (void)::unlink(data_path.c_str());
  (void)::unlink(log_path.c_str());
  std::cout << (exit_code == 0 ? "multikey e2e passed\n" : "") << std::flush;
  return exit_code;
}
