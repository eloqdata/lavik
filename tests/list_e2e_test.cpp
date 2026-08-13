#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <future>
#include <iterator>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "keylane/storage/format.h"

namespace {

using namespace std::chrono_literals;

std::string g_keylane_binary;

class FileCleanup {
 public:
  explicit FileCleanup(std::string path) : path_(std::move(path)) {}
  ~FileCleanup() { (void)::unlink(path_.c_str()); }

 private:
  std::string path_;
};

void SendAll(int fd, std::string_view bytes) {
  while (!bytes.empty()) {
    const ssize_t sent = ::send(fd, bytes.data(), bytes.size(), MSG_NOSIGNAL);
    if (sent < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw std::runtime_error("send failed: " +
                               std::string(std::strerror(errno)));
    }
    if (sent == 0) {
      throw std::runtime_error("send returned zero bytes");
    }
    bytes.remove_prefix(static_cast<std::size_t>(sent));
  }
}

std::uint16_t FindFreePort() {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (fd < 0) {
    throw std::runtime_error("socket failed while selecting a port");
  }
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  if (::bind(fd, reinterpret_cast<const sockaddr*>(&address),
             sizeof(address)) != 0) {
    ::close(fd);
    throw std::runtime_error("bind failed while selecting a port");
  }
  socklen_t size = sizeof(address);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &size) != 0) {
    ::close(fd);
    throw std::runtime_error("getsockname failed while selecting a port");
  }
  ::close(fd);
  return ntohs(address.sin_port);
}

int ConnectSocket(std::uint16_t port) {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (fd < 0) {
    return -1;
  }
  timeval timeout{.tv_sec = 30, .tv_usec = 0};
  (void)::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
  (void)::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = htons(port);
  if (::connect(fd, reinterpret_cast<const sockaddr*>(&address),
                sizeof(address)) != 0) {
    ::close(fd);
    return -1;
  }
  return fd;
}

class RespClient {
 public:
  explicit RespClient(std::uint16_t port) {
    const auto deadline = std::chrono::steady_clock::now() + 30s;
    while (std::chrono::steady_clock::now() < deadline) {
      fd_ = ConnectSocket(port);
      if (fd_ >= 0) {
        try {
          if (Command({"PING"}) == "+PONG") return;
        } catch (const std::exception&) {
        }
        ::close(fd_);
        fd_ = -1;
      }
      std::this_thread::sleep_for(10ms);
    }
    throw std::runtime_error("timed out connecting to Redis port");
  }

  ~RespClient() {
    if (fd_ >= 0) {
      ::close(fd_);
    }
  }

  std::string Command(const std::vector<std::string_view>& args) {
    std::string request = "*" + std::to_string(args.size()) + "\r\n";
    for (std::string_view arg : args) {
      request += "$" + std::to_string(arg.size()) + "\r\n";
      request.append(arg);
      request += "\r\n";
    }
    SendAll(fd_, request);
    try {
      return ReadReply();
    } catch (const std::exception& error) {
      throw std::runtime_error(std::string(args.front()) + ": " + error.what());
    }
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
        if (line == "$-1") return line;
        const std::size_t size = ParseLength(line);
        std::string payload(size + 2, '\0');
        ReadExact(payload.data(), payload.size());
        if (!payload.ends_with("\r\n")) {
          throw std::runtime_error("malformed bulk terminator");
        }
        payload.resize(size);
        return line + "\r\n" + payload;
      }
      case '*': {
        if (line == "*-1") return line;
        const std::size_t count = ParseLength(line);
        std::string reply = line;
        for (std::size_t i = 0; i < count; ++i) {
          reply += "\r\n" + ReadReply();
        }
        return reply;
      }
      default:
        throw std::runtime_error("unexpected RESP type: " + line);
    }
  }

  static std::size_t ParseLength(const std::string& line) {
    std::size_t size = 0;
    const char* begin = line.data() + 1;
    const char* end = line.data() + line.size();
    const auto [parsed, error] = std::from_chars(begin, end, size);
    if (error != std::errc{} || parsed != end) {
      throw std::runtime_error("malformed RESP length: " + line);
    }
    return size;
  }

  void ReadExact(char* output, std::size_t size) {
    while (size != 0) {
      const ssize_t received = ::recv(fd_, output, size, 0);
      if (received < 0 && errno == EINTR) continue;
      if (received <= 0) {
        throw std::runtime_error("failed to read RESP response");
      }
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
    }
    response.resize(response.size() - 2);
    return response;
  }

  int fd_ = -1;
};

bool WaitForDurability(RespClient& client) {
  const auto deadline = std::chrono::steady_clock::now() + 30s;
  while (std::chrono::steady_clock::now() < deadline) {
    const std::string info = client.Command({"INFO", "STATS"});
    if (info.find("storage_durability_pending:0\r\n") !=
        std::string::npos) {
      return true;
    }
    std::this_thread::sleep_for(10ms);
  }
  return false;
}

class ServerProcess {
 public:
  ServerProcess(std::string_view binary, std::uint16_t port,
                std::string_view data_path, std::string_view log_path,
                unsigned threads, std::string_view crash_point = {},
                std::vector<std::string> extra_arguments = {}) {
    pid_ = ::fork();
    if (pid_ < 0) {
      throw std::runtime_error("fork failed");
    }
    if (pid_ == 0) {
      if (!crash_point.empty()) {
        (void)::setenv("KEYLANE_CRASH_POINT", std::string(crash_point).c_str(),
                       1);
      }
      const int log_fd =
          ::open(std::string(log_path).c_str(),
                 O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0600);
      if (log_fd >= 0) {
        (void)::dup2(log_fd, STDOUT_FILENO);
        (void)::dup2(log_fd, STDERR_FILENO);
        ::close(log_fd);
      }
      std::vector<std::string> arguments{
          std::string(binary),
          "--port",
          std::to_string(port),
          "--threads",
          std::to_string(threads),
          "--recv-buffers",
          "0",
          "--max-memory",
          "8589934592",
          "--flush-max-ms",
          "20",
          "--data-file",
          std::string(data_path),
      };
      arguments.insert(arguments.end(),
                       std::make_move_iterator(extra_arguments.begin()),
                       std::make_move_iterator(extra_arguments.end()));
      std::vector<char*> argv;
      for (std::string& argument : arguments) {
        argv.push_back(argument.data());
      }
      argv.push_back(nullptr);
      ::execv(argv[0], argv.data());
      _exit(127);
    }
  }

  ~ServerProcess() {
    if (pid_ > 0) {
      (void)::kill(pid_, SIGKILL);
      (void)::waitpid(pid_, nullptr, 0);
    }
  }

  void Stop() {
    ASSERT_GT(pid_, 0);
    ASSERT_EQ(::kill(pid_, SIGINT), 0);
    const auto deadline = std::chrono::steady_clock::now() + 30s;
    while (std::chrono::steady_clock::now() < deadline) {
      int status = 0;
      const pid_t waited = ::waitpid(pid_, &status, WNOHANG);
      if (waited == pid_) {
        pid_ = -1;
        ASSERT_TRUE(WIFEXITED(status));
        ASSERT_EQ(WEXITSTATUS(status), 0);
        return;
      }
      ASSERT_GE(waited, 0);
      std::this_thread::sleep_for(10ms);
    }
    FAIL() << "Keylane did not stop";
  }

  void Kill() {
    ASSERT_GT(pid_, 0);
    ASSERT_EQ(::kill(pid_, SIGKILL), 0);
    int status = 0;
    ASSERT_EQ(::waitpid(pid_, &status, 0), pid_);
    ASSERT_TRUE(WIFSIGNALED(status));
    ASSERT_EQ(WTERMSIG(status), SIGKILL);
    pid_ = -1;
  }

  void WaitForCrash() {
    ASSERT_GT(pid_, 0);
    const auto deadline = std::chrono::steady_clock::now() + 30s;
    while (std::chrono::steady_clock::now() < deadline) {
      int status = 0;
      const pid_t waited = ::waitpid(pid_, &status, WNOHANG);
      if (waited == pid_) {
        pid_ = -1;
        ASSERT_TRUE(WIFEXITED(status));
        ASSERT_EQ(WEXITSTATUS(status), 86);
        return;
      }
      ASSERT_GE(waited, 0);
      std::this_thread::sleep_for(10ms);
    }
    FAIL() << "Keylane did not reach the armed crash point";
  }

 private:
  pid_t pid_ = -1;
};

std::string Bulk(std::string_view value) {
  return "$" + std::to_string(value.size()) + "\r\n" + std::string(value);
}

std::string BulkArray(const std::vector<std::string_view>& values) {
  std::string reply = "*" + std::to_string(values.size());
  for (std::string_view value : values) reply += "\r\n" + Bulk(value);
  return reply;
}

TEST(ListE2eTest, PersistsLogicalLengthSeparatelyFromSerializedBytes) {
  ASSERT_FALSE(g_keylane_binary.empty());
  const std::string prefix =
      "/tmp/keylane-list-e2e-" + std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::posix_fallocate(fd, 0, 256ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(fd), 0);

  const std::uint16_t port = FindFreePort();
  // One list element contributes a four-byte length after the eight-byte list
  // header. This makes the first encoding exactly three full extents; pushing
  // an empty element adds four bytes and forces a fourth extent.
  constexpr std::size_t kThreeExtentElementBytes =
      3 * keylane::storage::kExtentPayloadBytes - 12;
  const std::string large_element(kThreeExtentElementBytes, 'x');
  std::string binary_element;
  binary_element.push_back('\0');
  binary_element.push_back(static_cast<char>(0xff));
  binary_element.append("\r\n");
  const std::string external_key(5000, 'k');
  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 2);
    RespClient client(port);
    EXPECT_EQ(client.Command({"LPUSH", "list", "a", "bb", "ccc"}), ":3");
    EXPECT_EQ(client.Command({"LPUSH", "list", "z"}), ":4");
    EXPECT_EQ(client.Command({"LPUSH", "list", "", binary_element}), ":6");
    EXPECT_EQ(client.Command({"LPOS", "list", "", "MAXLEN", "0"}), ":1");
    EXPECT_EQ(client.Command({"LPOS", "list", "", "RANK",
                              "-9223372036854775808"}),
              "$-1");
    EXPECT_EQ(client.Command({"PEXPIRE", "list", "600000"}), ":1");
    EXPECT_EQ(client.Command({"LPUSH", "list", "keeps-ttl"}), ":7");
    const std::string ttl = client.Command({"PTTL", "list"});
    ASSERT_TRUE(ttl.starts_with(':'));
    EXPECT_GT(std::stoll(ttl.substr(1)), 0);
    EXPECT_EQ(client.Command({"GET", "list"}),
              "-WRONGTYPE Operation against a key holding the wrong kind of "
              "value");
    EXPECT_EQ(client.Command({"SET", "string", "value"}), "+OK");
    EXPECT_EQ(client.Command({"LPUSH", "string", "x"}),
              "-WRONGTYPE Operation against a key holding the wrong kind of "
              "value");
    EXPECT_EQ(client.Command({"LPUSH", "", ""}), ":1");
    EXPECT_EQ(client.Command({"LPUSH", external_key, binary_element}), ":1");
    EXPECT_EQ(client.Command({"LPUSH", "multi-extent", large_element}), ":1");
    EXPECT_EQ(client.Command({"LPUSH", "multi-extent", ""}), ":2");
    const std::string binary_blocking_key = "binary\r\n*1\r\nkey";
    EXPECT_EQ(client.Command({"RPUSH", binary_blocking_key, binary_element}),
              ":1");
    EXPECT_EQ(client.Command({"BLPOP", binary_blocking_key, "0.1"}),
              "*2\r\n" + Bulk(binary_blocking_key) + "\r\n" +
                  Bulk(binary_element));
    const std::string oversized_key(
        keylane::storage::kStorageBlockBytes + 4096, 'q');
    EXPECT_EQ(client.Command({"SET", oversized_key, "string-value"}), "+OK");
    EXPECT_EQ(client.Command({"DEL", oversized_key}), ":1");
    EXPECT_EQ(client.Command({"RPUSH", oversized_key, "list-value"}), ":1");
    EXPECT_EQ(client.Command({"LLEN", oversized_key}), ":1");
    server.Stop();
  }

  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 3);
    RespClient client(port);
    EXPECT_EQ(client.Command({"LPUSH", "list", "after-restart"}), ":8");
    EXPECT_EQ(client.Command({"LPUSH", "", "after-restart"}), ":2");
    EXPECT_EQ(client.Command({"LPUSH", external_key, "after-restart"}), ":2");
    EXPECT_EQ(client.Command(
                  {"LPUSH", "multi-extent", binary_element, "after-restart"}),
              ":4");
    EXPECT_EQ(client.Command({"STRLEN", "multi-extent"}),
              "-WRONGTYPE Operation against a key holding the wrong kind of "
              "value");
    server.Stop();
  }
}

TEST(ListE2eTest, CommandsLargeKeyTransactionsAndCrashRecovery) {
  ASSERT_FALSE(g_keylane_binary.empty());
  const std::string prefix =
      "/tmp/keylane-list-complete-e2e-" + std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::posix_fallocate(fd, 0, 512ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(fd), 0);

  const std::uint16_t port = FindFreePort();
  std::vector<std::string> large_values;
  large_values.reserve(1200);
  for (int i = 0; i < 1200; ++i) {
    large_values.push_back("element-" + std::to_string(i) + "-" +
                           std::string(1024, static_cast<char>('a' + i % 26)));
  }

  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 4);
    RespClient client(port);

    EXPECT_EQ(client.Command({"LPUSHX", "missing", "x"}), ":0");
    EXPECT_EQ(client.Command({"RPUSHX", "missing", "x"}), ":0");
    EXPECT_EQ(client.Command({"RPUSH", "list", "a", "b", "c", "b", "d"}), ":5");
    EXPECT_EQ(client.Command({"LPUSH", "list", "z", "y"}), ":7");
    EXPECT_EQ(client.Command({"LRANGE", "list", "0", "-1"}),
              BulkArray({"y", "z", "a", "b", "c", "b", "d"}));
    EXPECT_EQ(client.Command({"LINDEX", "list", "-2"}), Bulk("b"));
    EXPECT_EQ(client.Command({"LSET", "list", "2", "A"}), "+OK");
    EXPECT_EQ(client.Command({"LINSERT", "list", "BEFORE", "c", "before"}),
              ":8");
    EXPECT_EQ(client.Command({"LINSERT", "list", "AFTER", "absent", "x"}),
              ":-1");
    EXPECT_EQ(client.Command({"LPOS", "list", "b"}), ":3");
    EXPECT_EQ(client.Command({"LPOS", "list", "b", "RANK", "-1"}), ":6");
    EXPECT_EQ(client.Command({"LPOS", "list", "b", "COUNT", "0"}),
              "*2\r\n:3\r\n:6");
    EXPECT_EQ(client.Command({"LREM", "list", "-1", "b"}), ":1");
    EXPECT_EQ(client.Command(
                  {"RPUSH", "min-remove-compact", "target", "keep",
                   "target", "target"}),
              ":4");
    EXPECT_EQ(client.Command(
                  {"LREM", "min-remove-compact",
                   "-9223372036854775808", "target"}),
              ":3");
    EXPECT_EQ(client.Command({"LRANGE", "min-remove-compact", "0", "-1"}),
              BulkArray({"keep"}));
    EXPECT_EQ(client.Command({"LTRIM", "list", "1", "-2"}), "+OK");
    EXPECT_EQ(client.Command({"LLEN", "list"}), ":5");
    EXPECT_EQ(client.Command({"LPOP", "list", "2"}), BulkArray({"z", "A"}));
    EXPECT_EQ(client.Command({"RPOP", "list"}), Bulk("c"));
    EXPECT_EQ(client.Command({"LRANGE", "list", "0", "-1"}),
              BulkArray({"b", "before"}));

    EXPECT_EQ(client.Command({"RPUSH", "source", "s1", "s2"}), ":2");
    EXPECT_EQ(client.Command({"RPUSH", "destination", "d1"}), ":1");
    EXPECT_EQ(
        client.Command({"LMOVE", "source", "destination", "RIGHT", "LEFT"}),
        Bulk("s2"));
    EXPECT_EQ(client.Command({"RPOPLPUSH", "source", "destination"}),
              Bulk("s1"));
    EXPECT_EQ(client.Command({"LRANGE", "destination", "0", "-1"}),
              BulkArray({"s1", "s2", "d1"}));
    EXPECT_EQ(client.Command({"RPUSH", "mpop-two", "m1", "m2", "m3"}), ":3");
    EXPECT_EQ(client.Command({"LMPOP", "2", "mpop-one", "mpop-two", "RIGHT",
                              "COUNT", "2"}),
              "*2\r\n" + Bulk("mpop-two") + "\r\n" + BulkArray({"m3", "m2"}));

    EXPECT_EQ(client.Command({"RPUSH", "blocking", "v1", "v2"}), ":2");
    EXPECT_EQ(client.Command({"BLPOP", "blocking", "0.1"}),
              "*2\r\n" + Bulk("blocking") + "\r\n" + Bulk("v1"));
    EXPECT_EQ(client.Command({"BRPOP", "blocking", "0.1"}),
              "*2\r\n" + Bulk("blocking") + "\r\n" + Bulk("v2"));
    const auto timeout_started = std::chrono::steady_clock::now();
    EXPECT_EQ(client.Command({"BLPOP", "blocking-missing", "0.01"}), "*-1");
    EXPECT_GE(std::chrono::steady_clock::now() - timeout_started, 8ms);
    auto blocked = std::async(std::launch::async, [port] {
      RespClient waiting(port);
      return waiting.Command({"BLPOP", "blocking-wakeup", "1"});
    });
    std::this_thread::sleep_for(20ms);
    EXPECT_EQ(client.Command({"RPUSH", "blocking-wakeup", "awake"}), ":1");
    EXPECT_EQ(blocked.get(),
              "*2\r\n" + Bulk("blocking-wakeup") + "\r\n" + Bulk("awake"));

    auto first_waiter = std::async(std::launch::async, [port] {
      RespClient waiting(port);
      return waiting.Command({"BLPOP", "blocking-fifo", "2"});
    });
    std::this_thread::sleep_for(20ms);
    auto second_waiter = std::async(std::launch::async, [port] {
      RespClient waiting(port);
      return waiting.Command({"BLPOP", "blocking-fifo", "2"});
    });
    std::this_thread::sleep_for(20ms);
    EXPECT_EQ(client.Command({"RPUSH", "blocking-fifo", "first"}), ":1");
    EXPECT_EQ(first_waiter.get(),
              "*2\r\n" + Bulk("blocking-fifo") + "\r\n" + Bulk("first"));
    EXPECT_EQ(second_waiter.wait_for(20ms), std::future_status::timeout);
    EXPECT_EQ(client.Command({"RPUSH", "blocking-fifo", "second"}), ":1");
    EXPECT_EQ(second_waiter.get(),
              "*2\r\n" + Bulk("blocking-fifo") + "\r\n" + Bulk("second"));

    auto multi_waiter = std::async(std::launch::async, [port] {
      RespClient waiting(port);
      return waiting.Command({"BLMPOP", "2", "2", "blocking-multi-a",
                              "blocking-multi-b", "LEFT", "COUNT", "2"});
    });
    std::this_thread::sleep_for(20ms);
    EXPECT_EQ(
        client.Command({"RPUSH", "blocking-multi-b", "multi-1", "multi-2"}),
        ":2");
    EXPECT_EQ(multi_waiter.get(), "*2\r\n" + Bulk("blocking-multi-b") + "\r\n" +
                                      BulkArray({"multi-1", "multi-2"}));

    auto move_waiter = std::async(std::launch::async, [port] {
      RespClient waiting(port);
      return waiting.Command({"BLMOVE", "blocking-move-source",
                              "blocking-move-destination", "RIGHT", "LEFT",
                              "2"});
    });
    std::this_thread::sleep_for(20ms);
    EXPECT_EQ(client.Command({"RPUSH", "blocking-move-source", "move-wakeup"}),
              ":1");
    EXPECT_EQ(move_waiter.get(), Bulk("move-wakeup"));
    EXPECT_EQ(
        client.Command({"LRANGE", "blocking-move-destination", "0", "-1"}),
        BulkArray({"move-wakeup"}));

    EXPECT_EQ(client.Command({"RPUSH", "blmove-source", "a", "b"}), ":2");
    EXPECT_EQ(client.Command({"BLMOVE", "blmove-source", "blmove-dest", "RIGHT",
                              "LEFT", "0.1"}),
              Bulk("b"));
    EXPECT_EQ(client.Command({"LRANGE", "blmove-dest", "0", "-1"}),
              BulkArray({"b"}));
    EXPECT_EQ(client.Command({"RPUSH", "brpoplpush-source", "moved"}), ":1");
    EXPECT_EQ(client.Command({"BRPOPLPUSH", "brpoplpush-source",
                              "brpoplpush-dest", "0.1"}),
              Bulk("moved"));
    EXPECT_EQ(client.Command({"RPUSH", "blmpop-two", "p1", "p2"}), ":2");
    EXPECT_EQ(client.Command({"BLMPOP", "0.1", "2", "blmpop-one", "blmpop-two",
                              "LEFT", "COUNT", "2"}),
              "*2\r\n" + Bulk("blmpop-two") + "\r\n" + BulkArray({"p1", "p2"}));
    EXPECT_EQ(client.Command({"BLMOVE", "missing-source", "missing-dest",
                              "LEFT", "RIGHT", "0.01"}),
              "$-1");

    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"RPUSH", "tx-list", "1", "2"}), "+QUEUED");
    EXPECT_EQ(client.Command({"LPUSH", "tx-list", "0"}), "+QUEUED");
    EXPECT_EQ(client.Command({"LPOP", "tx-list"}), "+QUEUED");
    EXPECT_EQ(client.Command({"LRANGE", "tx-list", "0", "-1"}), "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*4\r\n:2\r\n:3\r\n" + Bulk("0") +
                                            "\r\n" + BulkArray({"1", "2"}));

    EXPECT_EQ(client.Command({"RPUSH", "tx-mpop", "a", "b", "c"}), ":3");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command(
                  {"LMPOP", "2", "tx-missing", "tx-mpop", "LEFT", "COUNT",
                   "2"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}),
              "*1\r\n*2\r\n" + Bulk("tx-mpop") + "\r\n" +
                  BulkArray({"a", "b"}));
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command(
                  {"BLMPOP", "1", "1", "tx-mpop", "RIGHT", "COUNT", "1"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}),
              "*1\r\n*2\r\n" + Bulk("tx-mpop") + "\r\n" +
                  BulkArray({"c"}));

    const std::string removable(100 * 1024, 'r');
    for (int i = 0; i < 18; ++i) {
      EXPECT_TRUE(client.Command({"RPUSH", "merge-list", removable,
                                  "separator-" + std::to_string(i)})
                      .starts_with(':'));
    }
    EXPECT_EQ(client.Command(
                  {"LSET", "merge-list", "1", "extreme-remove"}),
              "+OK");
    EXPECT_EQ(client.Command(
                  {"LSET", "merge-list", "35", "extreme-remove"}),
              "+OK");
    EXPECT_EQ(client.Command(
                  {"LREM", "merge-list", "-9223372036854775808",
                   "extreme-remove"}),
              ":2");
    EXPECT_EQ(client.Command({"LREM", "merge-list", "0", removable}), ":18");
    EXPECT_EQ(client.Command({"LLEN", "merge-list"}), ":16");
    EXPECT_EQ(client.Command({"LPOS", "merge-list", removable}), "$-1");

    for (std::size_t begin = 0; begin < large_values.size(); begin += 100) {
      std::vector<std::string_view> large_push{"RPUSH", "large-list"};
      const std::size_t end = std::min(large_values.size(), begin + 100);
      for (std::size_t i = begin; i < end; ++i) {
        large_push.push_back(large_values[i]);
      }
      EXPECT_EQ(client.Command(large_push), ":" + std::to_string(end));
    }

    // Exercise the same segmented List with committed transactional roots and
    // ordinary roots racing on disjoint elements. EXEC holds the List key for
    // both of its mutations, while each ordinary LSET must serialize either
    // before or after the complete transaction. The final values are
    // deterministic even though the physical root records are interleaved.
    auto transaction_writer = std::async(std::launch::async, [port] {
      try {
        RespClient tx_client(port);
        for (int round = 0; round < 16; ++round) {
          const std::string first = "tx-a-" + std::to_string(round);
          const std::string second = "tx-b-" + std::to_string(round);
          if (tx_client.Command({"MULTI"}) != "+OK" ||
              tx_client.Command({"LSET", "large-list", "300", first}) !=
                  "+QUEUED" ||
              tx_client.Command({"LSET", "large-list", "301", second}) !=
                  "+QUEUED" ||
              tx_client.Command({"EXEC"}) != "*2\r\n+OK\r\n+OK") {
            return std::string("unexpected transactional List reply");
          }
          std::this_thread::yield();
        }
        return std::string{};
      } catch (const std::exception& error) {
        return std::string(error.what());
      }
    });
    auto ordinary_writer = std::async(std::launch::async, [port] {
      try {
        RespClient plain_client(port);
        for (int round = 0; round < 16; ++round) {
          const std::string first = "plain-a-" + std::to_string(round);
          const std::string second = "plain-b-" + std::to_string(round);
          if (plain_client.Command(
                  {"LSET", "large-list", "900", first}) != "+OK" ||
              plain_client.Command(
                  {"LSET", "large-list", "901", second}) != "+OK") {
            return std::string("unexpected ordinary List reply");
          }
          std::this_thread::yield();
        }
        return std::string{};
      } catch (const std::exception& error) {
        return std::string(error.what());
      }
    });
    EXPECT_EQ(transaction_writer.get(), "");
    EXPECT_EQ(ordinary_writer.get(), "");
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "300"}),
              Bulk("tx-a-15"));
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "301"}),
              Bulk("tx-b-15"));
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "900"}),
              Bulk("plain-a-15"));
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "901"}),
              Bulk("plain-b-15"));

    // Pin both possible winner orders independently of scheduler timing: a
    // committed transaction supersedes an ordinary root at index 302, while
    // an ordinary root supersedes a committed transaction at index 902.
    EXPECT_EQ(client.Command({"LSET", "large-list", "302", "plain-old"}),
              "+OK");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"LSET", "large-list", "302", "tx-new"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*1\r\n+OK");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"LSET", "large-list", "902", "tx-old"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*1\r\n+OK");
    EXPECT_EQ(client.Command({"LSET", "large-list", "902", "plain-new"}),
              "+OK");

    EXPECT_EQ(client.Command({"LLEN", "large-list"}), ":1200");
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "0"}),
              Bulk(large_values.front()));
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "-1"}),
              Bulk(large_values.back()));
    EXPECT_EQ(client.Command({"PEXPIRE", "large-list", "600000"}), ":1");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"LSET", "large-list", "80", "tx-segment"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"LSET", "large-list", "700", "tx-segment-2"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "80"}), "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}),
              "*3\r\n+OK\r\n+OK\r\n" + Bulk("tx-segment"));
    server.Stop();
  }

  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 3);
    RespClient client(port);
    EXPECT_EQ(client.Command({"LLEN", "merge-list"}), ":16");
    EXPECT_EQ(client.Command({"LPOS", "merge-list", std::string(100 * 1024, 'r')}),
              "$-1");
    EXPECT_EQ(client.Command({"LLEN", "large-list"}), ":1200");
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "79"}),
              Bulk(large_values[79]));
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "80"}),
              Bulk("tx-segment"));
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "700"}),
              Bulk("tx-segment-2"));
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "300"}),
              Bulk("tx-a-15"));
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "301"}),
              Bulk("tx-b-15"));
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "900"}),
              Bulk("plain-a-15"));
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "901"}),
              Bulk("plain-b-15"));
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "302"}),
              Bulk("tx-new"));
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "902"}),
              Bulk("plain-new"));
    const std::string recovered_ttl = client.Command({"PTTL", "large-list"});
    ASSERT_TRUE(recovered_ttl.starts_with(':'));
    EXPECT_GT(std::stoll(recovered_ttl.substr(1)), 0);
    EXPECT_EQ(client.Command({"LRANGE", "tx-list", "0", "-1"}),
              BulkArray({"1", "2"}));
    EXPECT_EQ(client.Command({"LSET", "large-list", "80", "recovered"}), "+OK");
    EXPECT_EQ(client.Command({"LREM", "large-list", "1", large_values[10]}),
              ":1");
    EXPECT_EQ(client.Command({"LLEN", "large-list"}), ":1199");
    ASSERT_TRUE(WaitForDurability(client));
    server.Kill();
  }

  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 2);
    RespClient client(port);
    EXPECT_EQ(client.Command({"LLEN", "large-list"}), ":1199");
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "79"}),
              Bulk("recovered"));
    EXPECT_EQ(client.Command({"LPOS", "large-list", large_values[10]}), "$-1");
    server.Stop();
  }

  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 2);
    RespClient client(port);
    EXPECT_EQ(client.Command({"LTRIM", "large-list", "0", "9"}), "+OK");
    EXPECT_EQ(client.Command({"LLEN", "large-list"}), ":10");
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "0"}),
              Bulk(large_values.front()));
    server.Stop();
  }

  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 3);
    RespClient client(port);
    EXPECT_EQ(client.Command({"LLEN", "large-list"}), ":10");
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "-1"}),
              Bulk(large_values[9]));
    EXPECT_EQ(client.Command({"DEL", "large-list"}), ":1");
    EXPECT_EQ(client.Command({"LLEN", "large-list"}), ":0");
    server.Stop();
  }

  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 2);
    RespClient client(port);
    EXPECT_EQ(client.Command({"LLEN", "large-list"}), ":0");
    server.Stop();
  }
}

TEST(ListE2eTest, ReplicatesSegmentedListsWithoutPhysicalReferences) {
  ASSERT_FALSE(g_keylane_binary.empty());
  const std::string prefix =
      "/tmp/keylane-list-replication-e2e-" + std::to_string(::getpid());
  const std::string source_data = prefix + "-source.data";
  const std::string replica_data = prefix + "-replica.data";
  const std::string source_log = prefix + "-source.log";
  const std::string replica_log = prefix + "-replica.log";
  FileCleanup source_cleanup(source_data);
  FileCleanup replica_cleanup(replica_data);
  FileCleanup source_log_cleanup(source_log);
  FileCleanup replica_log_cleanup(replica_log);
  for (const std::string* path : {&source_data, &replica_data}) {
    const int fd =
        ::open(path->c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(::posix_fallocate(fd, 0, 256ULL * 1024 * 1024), 0);
    ASSERT_EQ(::close(fd), 0);
  }

  const std::uint16_t source_port = FindFreePort();
  std::uint16_t replica_port = FindFreePort();
  while (replica_port == source_port) replica_port = FindFreePort();
  std::uint16_t replication_port = FindFreePort();
  while (replication_port == source_port ||
         replication_port == replica_port) {
    replication_port = FindFreePort();
  }
  // 104 elements put the portable KLL1 image above the 12 MiB replication
  // RPC limit, exercising begin/chunk/commit framing as well as segmentation.
  const std::string element(128 * 1024, 'p');

  // Persist the source tree before attaching the replica so this exercises
  // both baseline snapshot materialization and later delta materialization.
  {
    ServerProcess source(g_keylane_binary, source_port, source_data, source_log,
                         3);
    RespClient client(source_port);
    for (int i = 0; i < 104; ++i) {
      ASSERT_EQ(client.Command(
                    {"RPUSH", "replicated-list",
                     element + "-" + std::to_string(i)}),
                ":" + std::to_string(i + 1));
    }
    source.Stop();
  }

  {
    ServerProcess replica(
        g_keylane_binary, replica_port, replica_data, replica_log, 2, {},
        {"--replication-port", std::to_string(replication_port),
         "--replica-read-only"});
    RespClient replica_client(replica_port);
    ServerProcess source(
        g_keylane_binary, source_port, source_data, source_log, 4, {},
        {"--replicate-to",
         "127.0.0.1:" + std::to_string(replication_port)});
    RespClient source_client(source_port);

    auto wait_for = [&](auto&& predicate) {
      const auto deadline = std::chrono::steady_clock::now() + 30s;
      while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) return true;
        std::this_thread::sleep_for(20ms);
      }
      return false;
    };
    ASSERT_TRUE(wait_for([&] {
      return replica_client.Command({"LLEN", "replicated-list"}) == ":104";
    }));
    EXPECT_EQ(replica_client.Command({"LINDEX", "replicated-list", "10"}),
              Bulk(element + "-10"));

    EXPECT_EQ(source_client.Command(
                  {"LSET", "replicated-list", "10", "delta-value"}),
              "+OK");
    ASSERT_TRUE(wait_for([&] {
      return replica_client.Command({"LINDEX", "replicated-list", "10"}) ==
             Bulk("delta-value");
    }));
    EXPECT_EQ(source_client.Command(
                  {"LPUSH", "replicated-list", "delta-head"}),
              ":105");
    ASSERT_TRUE(wait_for([&] {
      return replica_client.Command({"LINDEX", "replicated-list", "0"}) ==
             Bulk("delta-head");
    }));
    EXPECT_EQ(source_client.Command(
                  {"PEXPIRE", "replicated-list", "600000"}),
              ":1");
    ASSERT_TRUE(wait_for([&] {
      const std::string ttl =
          replica_client.Command({"PTTL", "replicated-list"});
      return ttl.starts_with(':') && std::stoll(ttl.substr(1)) > 0;
    }));
    EXPECT_EQ(source_client.Command(
                  {"LTRIM", "replicated-list", "0", "39"}),
              "+OK");
    ASSERT_TRUE(wait_for([&] {
      return replica_client.Command({"LLEN", "replicated-list"}) == ":40";
    }));
    source.Stop();
    replica.Stop();
  }

  // The receiver persisted a local logical List, not the source's block
  // references; standalone recovery must therefore rebuild it normally.
  {
    ServerProcess replica(g_keylane_binary, replica_port, replica_data,
                          replica_log, 3);
    RespClient client(replica_port);
    EXPECT_EQ(client.Command({"LLEN", "replicated-list"}), ":40");
    EXPECT_EQ(client.Command({"LINDEX", "replicated-list", "0"}),
              Bulk("delta-head"));
    replica.Stop();
  }
}

TEST(ListE2eTest, DefragRelocatesLiveCollectionObjectsCrashSafely) {
#ifdef NDEBUG
  GTEST_SKIP() << "crash points are compiled out of release builds";
#else
  ASSERT_FALSE(g_keylane_binary.empty());
  const std::string prefix =
      "/tmp/keylane-list-defrag-e2e-" + std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::posix_fallocate(fd, 0, 256ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(fd), 0);

  const std::uint16_t port = FindFreePort();
  constexpr std::size_t kElements = 14;
  const std::string original(480 * 1024, 'o');
  const std::string replacement(480 * 1024, 'n');
  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 2,
                         "list-defrag-root-published");
    RespClient client(port);
    ASSERT_EQ(client.Command({"DEFRAG", "PAUSE"}), "+OK");

    std::vector<std::string_view> push{"RPUSH", "defrag-list"};
    push.insert(push.end(), kElements, original);
    ASSERT_EQ(client.Command(push), ":14");
    // All initial segments are appended together. Replacing all but one
    // leaves a live collection object pinning a very sparse records block.
    for (std::size_t index = 0; index + 2 < kElements; ++index) {
      ASSERT_EQ(client.Command({"LSET", "defrag-list",
                                std::to_string(index), replacement}),
                "+OK");
    }
    ASSERT_TRUE(WaitForDurability(client));
    try {
      EXPECT_EQ(client.Command({"DEFRAG", "RESUME"}), "+OK");
    } catch (const std::runtime_error&) {
      // The background pass may reach the armed point before the reply is
      // drained; both orders have the same durable relocation boundary.
    }
    server.WaitForCrash();
  }

  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 3);
    RespClient client(port);
    EXPECT_EQ(client.Command({"LLEN", "defrag-list"}), ":14");
    for (std::size_t index = 0; index < kElements; ++index) {
      EXPECT_EQ(client.Command(
                    {"LINDEX", "defrag-list", std::to_string(index)}),
                Bulk(index + 2 >= kElements ? original : replacement));
    }
    EXPECT_EQ(client.Command({"LSET", "defrag-list", "13", "after"}),
              "+OK");
    server.Stop();
  }
#endif
}

TEST(ListE2eTest, SegmentedPublicationAndTransactionCrashesRecoverAtomically) {
#ifdef NDEBUG
  GTEST_SKIP() << "crash points are compiled out of release builds";
#else
  ASSERT_FALSE(g_keylane_binary.empty());
  const std::string prefix =
      "/tmp/keylane-list-crash-e2e-" + std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::posix_fallocate(fd, 0, 256ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(fd), 0);
  const std::uint16_t port = FindFreePort();
  const std::string value(2048, 'c');

  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 2,
                         "list-segments-durable");
    RespClient client(port);
    for (int i = 0; i < 31; ++i) {
      ASSERT_TRUE(
          client.Command({"RPUSH", "crash-list", value}).starts_with(':'));
    }
    ASSERT_TRUE(WaitForDurability(client));
    EXPECT_THROW((void)client.Command({"RPUSH", "crash-list", value}),
                 std::runtime_error);
    server.WaitForCrash();
  }
  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 3);
    RespClient client(port);
    EXPECT_EQ(client.Command({"LLEN", "crash-list"}), ":31");
    EXPECT_EQ(client.Command({"LINDEX", "crash-list", "-1"}), Bulk(value));
    server.Stop();
  }

  // Once the new root is visible in memory, a power cut may recover either
  // the old durable root or the complete new root. It must never recover a
  // descriptor whose segments are missing or a partially updated List.
  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 2,
                         "list-root-published");
    RespClient client(port);
    EXPECT_THROW((void)client.Command({"RPUSH", "crash-list", value}),
                 std::runtime_error);
    server.WaitForCrash();
  }
  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 3);
    RespClient client(port);
    const std::string length = client.Command({"LLEN", "crash-list"});
    EXPECT_TRUE(length == ":31" || length == ":32") << length;
    EXPECT_EQ(client.Command({"LINDEX", "crash-list", "0"}), Bulk(value));
    EXPECT_EQ(client.Command({"LINDEX", "crash-list", "-1"}), Bulk(value));

    // Establish a definitely durable segmented baseline for the transaction
    // crash below, independent of which legal root won the previous restart.
    while (client.Command({"LLEN", "crash-list"}) != ":40") {
      ASSERT_TRUE(
          client.Command({"RPUSH", "crash-list", value}).starts_with(':'));
    }
    server.Stop();
  }

  // EXEC publishes tagged data records before its detached commit record.
  // Crashing at that exact boundary must discard every participant, including
  // the segmented List root, rather than recover half of the transaction.
  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 4,
                         "tx-commit-append");
    RespClient client(port);
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"LSET", "crash-list", "5", "tx-value"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"SET", "tx-marker", "committed"}), "+QUEUED");
    try {
      const std::string reply = client.Command({"EXEC"});
      EXPECT_EQ(reply, "*2\r\n+OK\r\n+OK");
    } catch (const std::runtime_error&) {
      // The detached commit can fail-stop the server before the reply reaches
      // the socket; both timings exercise the same durable boundary.
    }
    server.WaitForCrash();
  }
  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 2);
    RespClient client(port);
    EXPECT_EQ(client.Command({"LLEN", "crash-list"}), ":40");
    EXPECT_EQ(client.Command({"LINDEX", "crash-list", "5"}), Bulk(value));
    EXPECT_EQ(client.Command({"GET", "tx-marker"}), "$-1");
    // Leave the aborted transaction's tagged COW path on disk, then publish a
    // newer ordinary root. The following boot must ignore the orphaned tx
    // objects while retaining this non-transactional mutation.
    EXPECT_EQ(client.Command(
                  {"LSET", "crash-list", "5", "plain-after-aborted-tx"}),
              "+OK");
    server.Stop();
  }
  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 3);
    RespClient client(port);
    EXPECT_EQ(client.Command({"LINDEX", "crash-list", "5"}),
              Bulk("plain-after-aborted-tx"));
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command(
                  {"LSET", "crash-list", "6", "tx-after-ordinary"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*1\r\n+OK");
    EXPECT_EQ(client.Command(
                  {"LSET", "crash-list", "7", "ordinary-after-tx"}),
              "+OK");
    ASSERT_TRUE(WaitForDurability(client));
    server.Kill();
  }
  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 2);
    RespClient client(port);
    EXPECT_EQ(client.Command({"LLEN", "crash-list"}), ":40");
    EXPECT_EQ(client.Command({"LINDEX", "crash-list", "5"}),
              Bulk("plain-after-aborted-tx"));
    EXPECT_EQ(client.Command({"LINDEX", "crash-list", "6"}),
              Bulk("tx-after-ordinary"));
    EXPECT_EQ(client.Command({"LINDEX", "crash-list", "7"}),
              Bulk("ordinary-after-tx"));
    EXPECT_EQ(client.Command({"GET", "tx-marker"}), "$-1");
    server.Stop();
  }
#endif
}

}  // namespace

int main(int argc, char** argv) {
  if (argc >= 2) {
    g_keylane_binary = argv[1];
    for (int index = 1; index + 1 < argc; ++index) {
      argv[index] = argv[index + 1];
    }
    --argc;
  }
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
