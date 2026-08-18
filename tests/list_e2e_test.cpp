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
#include <unordered_set>
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

std::string EncodeCommand(const std::vector<std::string_view>& args) {
  std::string request = "*" + std::to_string(args.size()) + "\r\n";
  for (std::string_view arg : args) {
    request += "$" + std::to_string(arg.size()) + "\r\n";
    request.append(arg);
    request += "\r\n";
  }
  return request;
}

std::string ReadRespLine(int fd) {
  std::string line;
  while (!line.ends_with("\r\n")) {
    char byte = 0;
    const ssize_t received = ::recv(fd, &byte, 1, 0);
    if (received < 0 && errno == EINTR) continue;
    if (received <= 0) {
      throw std::runtime_error("failed to read RESP line");
    }
    line.push_back(byte);
  }
  line.resize(line.size() - 2);
  return line;
}

std::string ReadRespBulk(int fd) {
  const std::string header = ReadRespLine(fd);
  if (header.empty() || header.front() != '$')
    throw std::runtime_error("expected RESP bulk string, got: " + header);
  std::size_t size = 0;
  const auto parsed =
      std::from_chars(header.data() + 1, header.data() + header.size(), size);
  if (parsed.ec != std::errc{} || parsed.ptr != header.data() + header.size())
    throw std::runtime_error("invalid RESP bulk length: " + header);
  std::string payload(size + 2, '\0');
  std::size_t received_total = 0;
  while (received_total < payload.size()) {
    const ssize_t received = ::recv(fd, payload.data() + received_total,
                                    payload.size() - received_total, 0);
    if (received < 0 && errno == EINTR) continue;
    if (received <= 0) throw std::runtime_error("failed to read RESP bulk");
    received_total += static_cast<std::size_t>(received);
  }
  if (!payload.ends_with("\r\n"))
    throw std::runtime_error("RESP bulk string lacks terminator");
  payload.resize(size);
  return payload;
}

int ConnectSocket(std::uint16_t port);

void CloseStreamingReplyAfterHeader(std::uint16_t port,
                                    const std::vector<std::string_view>& args,
                                    std::string_view expected_header) {
  const int fd = ConnectSocket(port);
  if (fd < 0) throw std::runtime_error("failed to connect streaming client");
  SendAll(fd, EncodeCommand(args));
  const std::string header = ReadRespLine(fd);
  if (header != expected_header) {
    ::close(fd);
    throw std::runtime_error("unexpected streaming header: " + header);
  }
  linger reset{.l_onoff = 1, .l_linger = 0};
  (void)::setsockopt(fd, SOL_SOCKET, SO_LINGER, &reset, sizeof(reset));
  ::close(fd);
}

void CloseExecStreamingReplyAfterHeader(
    std::uint16_t port, const std::vector<std::string_view>& args,
    std::string_view expected_header) {
  const int fd = ConnectSocket(port);
  if (fd < 0) throw std::runtime_error("failed to connect streaming client");
  SendAll(fd, EncodeCommand({"MULTI"}));
  if (ReadRespLine(fd) != "+OK") {
    ::close(fd);
    throw std::runtime_error("MULTI failed for streaming EXEC");
  }
  SendAll(fd, EncodeCommand(args));
  if (ReadRespLine(fd) != "+QUEUED") {
    ::close(fd);
    throw std::runtime_error("random command was not queued");
  }
  SendAll(fd, EncodeCommand({"EXEC"}));
  if (ReadRespLine(fd) != "*1" || ReadRespLine(fd) != expected_header) {
    ::close(fd);
    throw std::runtime_error("unexpected streaming EXEC header");
  }
  linger reset{.l_onoff = 1, .l_linger = 0};
  (void)::setsockopt(fd, SOL_SOCKET, SO_LINGER, &reset, sizeof(reset));
  ::close(fd);
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
    SendAll(fd_, EncodeCommand(args));
    try {
      return ReadReply();
    } catch (const std::exception& error) {
      std::string command;
      for (std::string_view argument : args) {
        if (!command.empty()) command.push_back(' ');
        command.append(argument);
      }
      throw std::runtime_error(command + ": " + error.what());
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

bool WaitForReply(RespClient& client,
                  const std::vector<std::string_view>& command,
                  std::string_view expected) {
  const auto deadline = std::chrono::steady_clock::now() + 30s;
  while (std::chrono::steady_clock::now() < deadline) {
    const std::string reply = client.Command(command);
    if (reply == expected) return true;
    if (!reply.starts_with("-TRYAGAIN") && !reply.starts_with("-BUSY")) {
      return false;
    }
    std::this_thread::sleep_for(10ms);
  }
  return false;
}

std::string KeyForWorker(std::string_view prefix, unsigned worker,
                         unsigned worker_count) {
  for (std::uint64_t candidate = 0;; ++candidate) {
    std::string key = std::string(prefix) + "-" + std::to_string(candidate);
    if (keylane::storage::StorageShardForKey(key) % worker_count == worker) {
      return key;
    }
  }
}

struct ParsedRespValue {
  std::string scalar_;
  std::vector<ParsedRespValue> elements_;
};

ParsedRespValue ParseEncodedResp(std::string_view input, std::size_t* offset) {
  std::size_t line_end = input.find("\r\n", *offset);
  if (line_end == std::string_view::npos) line_end = input.size();
  if (line_end == *offset) {
    throw std::runtime_error("malformed encoded RESP value at offset " +
                             std::to_string(*offset) + ": " +
                             std::string(input));
  }
  const char type = input[*offset];
  const std::string_view body =
      input.substr(*offset + 1, line_end - *offset - 1);
  *offset = line_end == input.size() ? line_end : line_end + 2;
  ParsedRespValue result;
  if (type == '$') {
    std::size_t length = 0;
    const auto parsed =
        std::from_chars(body.data(), body.data() + body.size(), length);
    if (parsed.ec != std::errc{} || parsed.ptr != body.data() + body.size() ||
        length > input.size() - *offset) {
      throw std::runtime_error("malformed encoded RESP bulk string");
    }
    result.scalar_.assign(input.substr(*offset, length));
    *offset += length;
    return result;
  }
  if (type != '*') {
    result.scalar_.assign(body);
    return result;
  }
  std::size_t count = 0;
  const auto parsed =
      std::from_chars(body.data(), body.data() + body.size(), count);
  if (parsed.ec != std::errc{} || parsed.ptr != body.data() + body.size()) {
    throw std::runtime_error("malformed encoded RESP array");
  }
  result.elements_.reserve(count);
  for (std::size_t i = 0; i < count; ++i) {
    result.elements_.push_back(ParseEncodedResp(input, offset));
    if (i + 1 < count) {
      if (input.substr(*offset, 2) != "\r\n") {
        throw std::runtime_error("malformed encoded RESP array separator");
      }
      *offset += 2;
    }
  }
  return result;
}

std::pair<std::string, std::vector<std::string>> ParseScanReply(
    std::string_view encoded) {
  std::size_t offset = 0;
  ParsedRespValue parsed = ParseEncodedResp(encoded, &offset);
  if (offset != encoded.size() || parsed.elements_.size() != 2) {
    throw std::runtime_error("malformed SCAN reply");
  }
  std::vector<std::string> values;
  for (ParsedRespValue& value : parsed.elements_[1].elements_) {
    values.push_back(std::move(value.scalar_));
  }
  return {std::move(parsed.elements_[0].scalar_), std::move(values)};
}

bool WaitForDurability(RespClient& client) {
  const auto deadline = std::chrono::steady_clock::now() + 30s;
  while (std::chrono::steady_clock::now() < deadline) {
    const std::string info = client.Command({"INFO", "STATS"});
    if (info.find("storage_durability_pending:0\r\n") != std::string::npos) {
      return true;
    }
    std::this_thread::sleep_for(10ms);
  }
  return false;
}

class ServerProcess {
 public:
  ServerProcess(
      std::string_view binary, std::uint16_t port, std::string_view data_path,
      std::string_view log_path, unsigned threads,
      std::string_view crash_point = {},
      std::vector<std::string> extra_arguments = {},
      std::vector<std::pair<std::string, std::string>> environment = {},
      std::string_view config_path = {}) {
    pid_ = ::fork();
    if (pid_ < 0) {
      throw std::runtime_error("fork failed");
    }
    if (pid_ == 0) {
      if (!crash_point.empty()) {
        (void)::setenv("KEYLANE_CRASH_POINT", std::string(crash_point).c_str(),
                       1);
      }
      for (const auto& [name, value] : environment) {
        (void)::setenv(name.c_str(), value.c_str(), 1);
      }
      const int log_fd =
          ::open(std::string(log_path).c_str(),
                 O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0600);
      if (log_fd >= 0) {
        (void)::dup2(log_fd, STDOUT_FILENO);
        (void)::dup2(log_fd, STDERR_FILENO);
        ::close(log_fd);
      }
      std::string max_memory = "8589934592";
      for (std::size_t i = 0; i + 1 < extra_arguments.size(); ++i) {
        if (extra_arguments[i] != "--max-memory") continue;
        max_memory = std::move(extra_arguments[i + 1]);
        extra_arguments.erase(extra_arguments.begin() + i,
                              extra_arguments.begin() + i + 2);
        break;
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
          std::move(max_memory),
          "--flush-max-ms",
          "20",
          "--data-file",
          std::string(data_path),
      };
      if (!config_path.empty()) {
        arguments.insert(arguments.begin() + 1, std::string(config_path));
      }
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
    EXPECT_EQ(
        client.Command({"LPOS", "list", "", "RANK", "-9223372036854775808"}),
        "-ERR value is out of range");
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
    EXPECT_EQ(
        client.Command({"BLPOP", binary_blocking_key, "0.1"}),
        "*2\r\n" + Bulk(binary_blocking_key) + "\r\n" + Bulk(binary_element));
    const std::string oversized_key(keylane::storage::kStorageBlockBytes + 4096,
                                    'q');
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

TEST(ListE2eTest, BlockingAndStreamedCommandsDoNotHoldFlushDbGate) {
  ASSERT_FALSE(g_keylane_binary.empty());
  const std::string prefix =
      "/tmp/keylane-db-gate-e2e-" + std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int data_fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(data_fd, 0);
  ASSERT_EQ(::posix_fallocate(data_fd, 0, 128ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(data_fd), 0);

  const std::uint16_t port = FindFreePort();
  ServerProcess server(g_keylane_binary, port, data_path, log_path, 2);
  RespClient client(port);

  auto blocked = std::async(std::launch::async, [port] {
    RespClient waiting(port);
    return waiting.Command({"BLPOP", "gate-blocked-list", "0.3"});
  });
  std::this_thread::sleep_for(30ms);
  auto flushed = std::async(std::launch::async, [port] {
    RespClient flushing(port);
    return flushing.Command({"FLUSHDB", "SYNC"});
  });
  ASSERT_EQ(flushed.wait_for(2s), std::future_status::ready);
  EXPECT_EQ(flushed.get(), "+OK");
  EXPECT_EQ(client.Command({"SET", "after-blocking-flush", "alive"}), "+OK");
  EXPECT_EQ(blocked.get(), "*-1");

  auto blocked_stream = std::async(std::launch::async, [port] {
    RespClient waiting(port);
    return waiting.Command(
        {"XREAD", "BLOCK", "0", "STREAMS", "gate-blocked-stream", "0-0"});
  });
  std::this_thread::sleep_for(30ms);
  auto stream_read_flush = std::async(std::launch::async, [port] {
    RespClient flushing(port);
    return flushing.Command({"FLUSHDB", "SYNC"});
  });
  ASSERT_EQ(stream_read_flush.wait_for(2s), std::future_status::ready);
  EXPECT_EQ(stream_read_flush.get(), "+OK");
  EXPECT_EQ(
      client.Command({"XADD", "gate-blocked-stream", "1-0", "field", "value"}),
      Bulk("1-0"));
  ASSERT_EQ(blocked_stream.wait_for(2s), std::future_status::ready);
  EXPECT_EQ(blocked_stream.get(), "*1\r\n*2\r\n" + Bulk("gate-blocked-stream") +
                                      "\r\n*1\r\n*2\r\n" + Bulk("1-0") +
                                      "\r\n*2\r\n" + Bulk("field") + "\r\n" +
                                      Bulk("value"));

  ASSERT_EQ(client.Command({"XGROUP", "CREATE", "gate-blocked-group", "g", "$",
                            "MKSTREAM"}),
            "+OK");
  auto blocked_group = std::async(std::launch::async, [port] {
    RespClient waiting(port);
    return waiting.Command({"XREADGROUP", "GROUP", "g", "consumer", "BLOCK",
                            "0", "STREAMS", "gate-blocked-group", ">"});
  });
  std::this_thread::sleep_for(30ms);
  auto group_read_flush = std::async(std::launch::async, [port] {
    RespClient flushing(port);
    return flushing.Command({"FLUSHDB", "SYNC"});
  });
  ASSERT_EQ(group_read_flush.wait_for(2s), std::future_status::ready);
  EXPECT_EQ(group_read_flush.get(), "+OK");
  ASSERT_EQ(blocked_group.wait_for(2s), std::future_status::ready);
  EXPECT_TRUE(blocked_group.get().starts_with("-NOGROUP"));

  const std::string member(4096, 's');
  ASSERT_EQ(client.Command({"SADD", "gate-stream-set", member}), ":1");
  const int locked_stream_fd = ConnectSocket(port);
  ASSERT_GE(locked_stream_fd, 0);
  SendAll(locked_stream_fd,
          EncodeCommand({"SRANDMEMBER", "gate-stream-set", "-2000"}));
  ASSERT_EQ(ReadRespLine(locked_stream_fd), "*2000");
  // The stalled stream holds neither the process-wide DB gate nor its key lock
  // while its generated chunk waits for socket backpressure.
  EXPECT_NE(client.Command({"INFO", "STATS"})
                .find("storage_expiration_pause_count:0\r\n"),
            std::string::npos);
  EXPECT_EQ(client.Command({"SET", "stream-unrelated-write", "alive"}), "+OK");
  auto removal = std::async(std::launch::async, [port, member] {
    RespClient writer(port);
    return writer.Command({"SREM", "gate-stream-set", member});
  });
  ASSERT_EQ(removal.wait_for(2s), std::future_status::ready);
  EXPECT_EQ(removal.get(), ":1");
  for (std::size_t i = 0; i < 2000; ++i) {
    EXPECT_EQ(ReadRespBulk(locked_stream_fd), member);
  }
  SendAll(locked_stream_fd, EncodeCommand({"PING"}));
  EXPECT_EQ(ReadRespLine(locked_stream_fd), "+PONG");
  linger reset{.l_onoff = 1, .l_linger = 0};
  (void)::setsockopt(locked_stream_fd, SOL_SOCKET, SO_LINGER, &reset,
                     sizeof(reset));
  ::close(locked_stream_fd);

  ASSERT_EQ(client.Command({"SADD", "gate-stream-set", member}), ":1");
  const int flush_stream_fd = ConnectSocket(port);
  ASSERT_GE(flush_stream_fd, 0);
  SendAll(flush_stream_fd,
          EncodeCommand({"SRANDMEMBER", "gate-stream-set", "-1000000000"}));
  ASSERT_EQ(ReadRespLine(flush_stream_fd), "*1000000000");
  std::this_thread::sleep_for(30ms);
  auto stream_flush = std::async(std::launch::async, [port] {
    RespClient flushing(port);
    return flushing.Command({"FLUSHDB", "SYNC"});
  });
  ASSERT_EQ(stream_flush.wait_for(2s), std::future_status::ready);
  EXPECT_EQ(stream_flush.get(), "+OK");
  (void)::setsockopt(flush_stream_fd, SOL_SOCKET, SO_LINGER, &reset,
                     sizeof(reset));
  ::close(flush_stream_fd);
  EXPECT_EQ(client.Command({"SET", "after-stream-flush", "alive"}), "+OK");
  server.Stop();
}

TEST(ListE2eTest, StreamBlockingRegistryBroadcastsAndKeepsGroupFifo) {
  ASSERT_FALSE(g_keylane_binary.empty());
  const std::string prefix =
      "/tmp/keylane-stream-wait-e2e-" + std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int data_fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(data_fd, 0);
  ASSERT_EQ(::posix_fallocate(data_fd, 0, 128ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(data_fd), 0);

  const std::uint16_t port = FindFreePort();
  ServerProcess server(g_keylane_binary, port, data_path, log_path, 3);
  RespClient client(port);

  auto xread = [port] {
    RespClient waiting(port);
    return waiting.Command(
        {"XREAD", "BLOCK", "1000", "STREAMS", "broadcast-stream", "0-0"});
  };
  auto first_reader = std::async(std::launch::async, xread);
  auto second_reader = std::async(std::launch::async, xread);
  std::this_thread::sleep_for(50ms);
  EXPECT_EQ(
      client.Command({"XADD", "broadcast-stream", "1-0", "field", "value"}),
      Bulk("1-0"));
  ASSERT_EQ(first_reader.wait_for(1s), std::future_status::ready);
  ASSERT_EQ(second_reader.wait_for(1s), std::future_status::ready);
  const std::string broadcast_reply =
      "*1\r\n*2\r\n" + Bulk("broadcast-stream") + "\r\n*1\r\n*2\r\n" +
      Bulk("1-0") + "\r\n*2\r\n" + Bulk("field") + "\r\n" + Bulk("value");
  EXPECT_EQ(first_reader.get(), broadcast_reply);
  EXPECT_EQ(second_reader.get(), broadcast_reply);

  EXPECT_EQ(
      client.Command({"XGROUP", "CREATE", "fifo-stream", "g", "$", "MKSTREAM"}),
      "+OK");
  auto first_group = std::async(std::launch::async, [port] {
    RespClient waiting(port);
    return waiting.Command({"XREADGROUP", "GROUP", "g", "first", "COUNT", "1",
                            "BLOCK", "1000", "STREAMS", "fifo-stream", ">"});
  });
  std::this_thread::sleep_for(30ms);
  auto second_group = std::async(std::launch::async, [port] {
    RespClient waiting(port);
    return waiting.Command({"XREADGROUP", "GROUP", "g", "second", "COUNT", "1",
                            "BLOCK", "1000", "STREAMS", "fifo-stream", ">"});
  });
  std::this_thread::sleep_for(50ms);
  EXPECT_EQ(client.Command({"XADD", "fifo-stream", "1-0", "f", "one"}),
            Bulk("1-0"));
  ASSERT_EQ(first_group.wait_for(1s), std::future_status::ready);
  EXPECT_EQ(second_group.wait_for(100ms), std::future_status::timeout);
  EXPECT_TRUE(first_group.get().starts_with("*1\r\n"));
  EXPECT_EQ(client.Command({"XADD", "fifo-stream", "2-0", "f", "two"}),
            Bulk("2-0"));
  ASSERT_EQ(second_group.wait_for(1s), std::future_status::ready);
  EXPECT_TRUE(second_group.get().starts_with("*1\r\n"));

  EXPECT_EQ(client.Command({"XADD", "rewind-wake", "1-0", "f", "v"}),
            Bulk("1-0"));
  EXPECT_EQ(
      client.Command({"XGROUP", "CREATE", "rewind-wake", "rewind-group", "$"}),
      "+OK");
  auto rewound_group = std::async(std::launch::async, [port] {
    RespClient waiting(port);
    return waiting.Command({"XREADGROUP", "GROUP", "rewind-group", "reader",
                            "BLOCK", "1000", "STREAMS", "rewind-wake", ">"});
  });
  std::this_thread::sleep_for(50ms);
  EXPECT_EQ(
      client.Command({"XGROUP", "SETID", "rewind-wake", "rewind-group", "0"}),
      "+OK");
  ASSERT_EQ(rewound_group.wait_for(1s), std::future_status::ready);
  EXPECT_TRUE(rewound_group.get().starts_with("*1\r\n"));

  server.Stop();
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

    const auto expect_arity_error = [&](std::vector<std::string_view> args,
                                        std::string_view command) {
      EXPECT_EQ(client.Command(args), "-ERR wrong number of arguments for '" +
                                          std::string(command) + "' command");
      EXPECT_EQ(client.Command({"PING"}), "+PONG");
    };
    expect_arity_error({"SISMEMBER", "s"}, "sismember");
    expect_arity_error({"SSCAN", "s"}, "sscan");
    expect_arity_error({"HGET", "h"}, "hget");
    expect_arity_error({"HSCAN", "h"}, "hscan");
    expect_arity_error({"LINDEX", "list"}, "lindex");
    expect_arity_error({"HSETNX", "h"}, "hsetnx");
    expect_arity_error({"SMISMEMBER", "s"}, "smismember");
    expect_arity_error({"HDEL", "h"}, "hdel");
    expect_arity_error({"SADD", "s"}, "sadd");
    expect_arity_error({"SPOP", "s", "1", "2"}, "spop");
    expect_arity_error({"SRANDMEMBER", "s", "1", "2"}, "srandmember");

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
    EXPECT_EQ(client.Command({"RPUSH", "min-remove-compact", "target", "keep",
                              "target", "target"}),
              ":4");
    EXPECT_EQ(client.Command({"LREM", "min-remove-compact",
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
    EXPECT_EQ(client.Command({"BLPOP", "blocking-missing", "-1"}),
              "-ERR timeout is negative");
    // This value rounds to 2^63 nanoseconds in double precision. It must be
    // rejected before integer conversion/time_point addition rather than
    // overflowing into an immediate timeout.
    EXPECT_EQ(
        client.Command({"BLPOP", "blocking-missing", "9223372036.854776"}),
        "-ERR timeout is out of range");
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

    auto unrelated_move_waiter = std::async(std::launch::async, [port] {
      RespClient waiting(port);
      return waiting.Command({"BLMOVE", "unrelated-move-source",
                              "shared-blocking-destination", "RIGHT", "LEFT",
                              "0.2"});
    });
    std::this_thread::sleep_for(20ms);
    auto destination_waiter = std::async(std::launch::async, [port] {
      RespClient waiting(port);
      return waiting.Command({"BLPOP", "shared-blocking-destination", "1"});
    });
    std::this_thread::sleep_for(20ms);
    EXPECT_EQ(client.Command({"RPUSH", "shared-blocking-destination",
                              "destination-value"}),
              ":1");
    EXPECT_EQ(destination_waiter.get(),
              "*2\r\n" + Bulk("shared-blocking-destination") + "\r\n" +
                  Bulk("destination-value"));
    EXPECT_EQ(unrelated_move_waiter.get(), "$-1");

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
    EXPECT_EQ(client.Command({"BLMPOP", "0", "3", "only-one", "LEFT"}),
              "-ERR syntax error");
    EXPECT_EQ(client.Command({"BLMPOP", "0", "9223372036854775807", "only-one",
                              "LEFT"}),
              "-ERR syntax error");
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
    EXPECT_EQ(client.Command({"LMPOP", "2", "tx-missing", "tx-mpop", "LEFT",
                              "COUNT", "2"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*1\r\n*2\r\n" + Bulk("tx-mpop") +
                                            "\r\n" + BulkArray({"a", "b"}));
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(
        client.Command({"BLMPOP", "1", "1", "tx-mpop", "RIGHT", "COUNT", "1"}),
        "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}),
              "*1\r\n*2\r\n" + Bulk("tx-mpop") + "\r\n" + BulkArray({"c"}));
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(
        client.Command({"LMPOP", "1", "tx-mpop", "LEFT", "COUNT", "1", "junk"}),
        "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*1\r\n-ERR syntax error");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"BLPOP", "tx-mpop", "abc"}), "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}),
              "*1\r\n-ERR timeout is not a float or out of range");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"BLMPOP", "abc", "1", "tx-mpop", "LEFT"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}),
              "*1\r\n-ERR timeout is not a float or out of range");

    EXPECT_EQ(client.Command({"RPUSH", "tx-lmove-source", "a", "b"}), ":2");
    EXPECT_EQ(client.Command({"RPUSH", "tx-lmove-dest", "tail"}), ":1");
    EXPECT_EQ(client.Command({"RPUSH", "tx-rpop-source", "x", "y"}), ":2");
    EXPECT_EQ(client.Command({"RPUSH", "tx-blmove-source", "m", "n"}), ":2");
    EXPECT_EQ(client.Command({"RPUSH", "tx-brpop-source", "p", "q"}), ":2");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"LMOVE", "tx-lmove-source", "tx-lmove-dest",
                              "RIGHT", "LEFT"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"RPOPLPUSH", "tx-rpop-source", "tx-rpop-dest"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"BLMOVE", "tx-blmove-source", "tx-blmove-dest",
                              "LEFT", "RIGHT", "10"}),
              "+QUEUED");
    EXPECT_EQ(client.Command(
                  {"BRPOPLPUSH", "tx-brpop-source", "tx-brpop-dest", "10"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*4\r\n" + Bulk("b") + "\r\n" +
                                            Bulk("y") + "\r\n" + Bulk("m") +
                                            "\r\n" + Bulk("q"));
    EXPECT_EQ(client.Command({"LRANGE", "tx-lmove-dest", "0", "-1"}),
              BulkArray({"b", "tail"}));
    EXPECT_EQ(client.Command({"LRANGE", "tx-rpop-dest", "0", "-1"}),
              BulkArray({"y"}));
    EXPECT_EQ(client.Command({"LRANGE", "tx-blmove-dest", "0", "-1"}),
              BulkArray({"m"}));
    EXPECT_EQ(client.Command({"LRANGE", "tx-brpop-dest", "0", "-1"}),
              BulkArray({"q"}));

    const std::string cross_move_source =
        KeyForWorker("tx-cross-move-source", 0, 4);
    const std::string cross_move_destination =
        KeyForWorker("tx-cross-move-destination", 1, 4);
    EXPECT_EQ(client.Command({"RPUSH", cross_move_source, "cross"}), ":1");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"LMOVE", cross_move_source,
                              cross_move_destination, "RIGHT", "LEFT"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*1\r\n" + Bulk("cross"));
    EXPECT_EQ(client.Command({"LRANGE", cross_move_destination, "0", "-1"}),
              BulkArray({"cross"}));

    const std::string removable(100 * 1024, 'r');
    for (int i = 0; i < 18; ++i) {
      EXPECT_TRUE(client
                      .Command({"RPUSH", "merge-list", removable,
                                "separator-" + std::to_string(i)})
                      .starts_with(':'));
    }
    EXPECT_EQ(client.Command({"LSET", "merge-list", "1", "extreme-remove"}),
              "+OK");
    EXPECT_EQ(client.Command({"LSET", "merge-list", "35", "extreme-remove"}),
              "+OK");
    EXPECT_EQ(client.Command({"LREM", "merge-list", "-9223372036854775808",
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
    // Search and mutation operate on the complete decoded List value.
    EXPECT_EQ(client.Command({"LINSERT", "large-list", "BEFORE",
                              large_values[777], "streamed-insert"}),
              ":1201");
    EXPECT_EQ(client.Command({"LPOS", "large-list", "streamed-insert"}),
              ":777");
    EXPECT_EQ(client.Command({"LREM", "large-list", "1", "streamed-insert"}),
              ":1");

    // Exercise the same List with committed transactional writes and ordinary
    // writes racing on disjoint elements. EXEC holds the List key for
    // both of its mutations, while each ordinary LSET must serialize either
    // before or after the complete transaction. The final values are
    // deterministic even though the physical records are interleaved.
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
          if (plain_client.Command({"LSET", "large-list", "900", first}) !=
                  "+OK" ||
              plain_client.Command({"LSET", "large-list", "901", second}) !=
                  "+OK") {
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
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "300"}), Bulk("tx-a-15"));
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "301"}), Bulk("tx-b-15"));
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
    EXPECT_EQ(
        client.Command({"LPOS", "merge-list", std::string(100 * 1024, 'r')}),
        "$-1");
    EXPECT_EQ(client.Command({"LLEN", "large-list"}), ":1200");
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "79"}),
              Bulk(large_values[79]));
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "80"}),
              Bulk("tx-segment"));
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "700"}),
              Bulk("tx-segment-2"));
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "300"}), Bulk("tx-a-15"));
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "301"}), Bulk("tx-b-15"));
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "900"}),
              Bulk("plain-a-15"));
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "901"}),
              Bulk("plain-b-15"));
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "302"}), Bulk("tx-new"));
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

TEST(ListE2eTest, EstablishesNativeReplicationFlowsAndChangesRole) {
  ASSERT_FALSE(g_keylane_binary.empty());
  const std::string prefix =
      "/tmp/keylane-replication-session-e2e-" + std::to_string(::getpid());
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
  ServerProcess source(g_keylane_binary, source_port, source_data, source_log,
                       3);
  ServerProcess replica(g_keylane_binary, replica_port, replica_data,
                        replica_log, 2);
  RespClient source_client(source_port);
  RespClient replica_client(replica_port);

  ASSERT_EQ(source_client.Command({"SET", "replicated-before{mvp}", "snapshot"}),
            "+OK");

  ASSERT_EQ(replica_client.Command(
                {"REPLICAOF", "127.0.0.1", std::to_string(source_port)}),
            "+OK");
  const auto online_deadline = std::chrono::steady_clock::now() + 30s;
  std::string replication_info;
  do {
    replication_info = replica_client.Command({"INFO", "replication"});
    if (replication_info.find("master_link_status:up") != std::string::npos &&
        replication_info.find("keylane_source_workers:3") !=
            std::string::npos &&
        replication_info.find("keylane_connected_flows:3") !=
            std::string::npos) {
      break;
    }
    std::this_thread::sleep_for(10ms);
  } while (std::chrono::steady_clock::now() < online_deadline);
  EXPECT_NE(replication_info.find("keylane_replication_state:online"),
            std::string::npos);
  EXPECT_NE(replication_info.find("keylane_source_workers:3"),
            std::string::npos);
  EXPECT_NE(replication_info.find("keylane_connected_flows:3"),
            std::string::npos);
  std::string snapshot_value;
  const auto snapshot_deadline = std::chrono::steady_clock::now() + 10s;
  do {
    snapshot_value = replica_client.Command({"GET", "replicated-before{mvp}"});
    if (snapshot_value == Bulk("snapshot")) break;
    std::this_thread::sleep_for(10ms);
  } while (std::chrono::steady_clock::now() < snapshot_deadline);
  EXPECT_EQ(snapshot_value, Bulk("snapshot"));
  ASSERT_EQ(source_client.Command({"SET", "replicated-after{mvp}", "delta"}),
            "+OK");
  std::string delta_value;
  const auto delta_deadline = std::chrono::steady_clock::now() + 10s;
  do {
    delta_value = replica_client.Command({"GET", "replicated-after{mvp}"});
    if (delta_value == Bulk("delta")) break;
    std::this_thread::sleep_for(10ms);
  } while (std::chrono::steady_clock::now() < delta_deadline);
  EXPECT_EQ(delta_value, Bulk("delta"));
  EXPECT_NE(
      source_client.Command({"INFO", "clients"}).find("connected_clients:1"),
      std::string::npos);
  EXPECT_NE(
      replica_client.Command({"INFO", "clients"}).find("connected_clients:1"),
      std::string::npos);
  EXPECT_TRUE(replica_client.Command({"SET", "blocked", "value"})
                  .starts_with("-READONLY"));

  ASSERT_EQ(replica_client.Command({"REPLICAOF", "NO", "ONE"}), "+OK");
  replication_info = replica_client.Command({"INFO", "replication"});
  EXPECT_NE(replication_info.find("role:master"), std::string::npos);
  EXPECT_NE(replication_info.find("keylane_replication_state:master"),
            std::string::npos);
  EXPECT_EQ(replica_client.Command({"SET", "writable", "again"}), "+OK");
  EXPECT_EQ(source_client.Command({"PING"}), "+PONG");
  replica.Stop();

  const std::string config_path = prefix + "-replica.conf";
  FileCleanup config_cleanup(config_path);
  const int config_fd = ::open(config_path.c_str(),
                               O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(config_fd, 0);
  const std::string config = "replicaof 127.0.0.1 " +
                             std::to_string(source_port) +
                             "\nreplica-read-only yes\n";
  ASSERT_EQ(::write(config_fd, config.data(), config.size()),
            static_cast<ssize_t>(config.size()));
  ASSERT_EQ(::close(config_fd), 0);
  {
    ServerProcess startup_replica(g_keylane_binary, replica_port, replica_data,
                                  replica_log, 2, {}, {}, {}, config_path);
    RespClient startup_client(replica_port);
    const auto startup_deadline = std::chrono::steady_clock::now() + 30s;
    do {
      replication_info = startup_client.Command({"INFO", "replication"});
      if (replication_info.find("master_link_status:up") != std::string::npos) {
        break;
      }
      std::this_thread::sleep_for(10ms);
    } while (std::chrono::steady_clock::now() < startup_deadline);
    EXPECT_NE(replication_info.find("keylane_replication_state:online"),
              std::string::npos);
    EXPECT_NE(replication_info.find("keylane_connected_flows:3"),
              std::string::npos);
    startup_replica.Stop();
  }
  source.Stop();
}

// Covers asymmetric source/replica worker counts, dynamic replica admission,
// a one-shot flow disconnect, backlog continuation, and FLUSHDB propagation.
TEST(ListE2eTest, MultiReplicaWriteFlushAndReconnectFlow) {
  ASSERT_FALSE(g_keylane_binary.empty());
  const std::string prefix =
      "/tmp/keylane-multi-replica-e2e-" + std::to_string(::getpid());
  const std::string source_data = prefix + "-source.data";
  const std::string first_data = prefix + "-first.data";
  const std::string second_data = prefix + "-second.data";
  const std::string source_log = prefix + "-source.log";
  const std::string first_log = prefix + "-first.log";
  const std::string second_log = prefix + "-second.log";
  const std::string first_conf = prefix + "-first.conf";
  FileCleanup source_cleanup(source_data), first_cleanup(first_data),
      second_cleanup(second_data), source_log_cleanup(source_log),
      first_log_cleanup(first_log), second_log_cleanup(second_log),
      first_conf_cleanup(first_conf);
  for (const std::string* path : {&source_data, &first_data, &second_data}) {
    const int fd =
        ::open(path->c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(::posix_fallocate(fd, 0, 256ULL * 1024 * 1024), 0);
    ASSERT_EQ(::close(fd), 0);
  }
  const std::uint16_t source_port = FindFreePort();
  const std::uint16_t first_port = FindFreePort();
  const std::uint16_t second_port = FindFreePort();
  {
    const int fd = ::open(first_conf.c_str(), O_WRONLY | O_CREAT | O_EXCL,
                          0600);
    ASSERT_GE(fd, 0);
    const std::string config = "replicaof 127.0.0.1 " +
                               std::to_string(source_port) +
                               "\nreplica-read-only yes\n";
    ASSERT_EQ(::write(fd, config.data(), config.size()),
              static_cast<ssize_t>(config.size()));
    ASSERT_EQ(::close(fd), 0);
  }

  ServerProcess source(g_keylane_binary, source_port, source_data, source_log,
                       2);
  RespClient source_before(source_port);
  ASSERT_EQ(source_before.Command({"SET", "startup", "ready"}), "+OK");
  ServerProcess first(g_keylane_binary, first_port, first_data, first_log, 2,
                      {}, {}, {}, first_conf);
  RespClient source_client(source_port);
  RespClient first_client(first_port);
  const auto online_deadline = std::chrono::steady_clock::now() + 20s;
  std::string first_info;
  do {
    first_info = first_client.Command({"INFO", "replication"});
    if (first_info.find("keylane_replication_state:online") !=
        std::string::npos) {
      break;
    }
    std::this_thread::sleep_for(10ms);
  } while (std::chrono::steady_clock::now() < online_deadline);
  ASSERT_NE(first_info.find("keylane_replication_state:online"),
            std::string::npos);
  const auto wait_value = [](RespClient& client, std::string_view key,
                             std::string_view expected) {
    const auto deadline = std::chrono::steady_clock::now() + 20s;
    std::string actual;
    do {
      actual = client.Command({"GET", std::string(key)});
      if (actual == Bulk(expected)) return true;
      std::this_thread::sleep_for(10ms);
    } while (std::chrono::steady_clock::now() < deadline);
    return false;
  };
  ASSERT_TRUE(wait_value(first_client, "startup", "ready"));

  ServerProcess second(g_keylane_binary, second_port, second_data, second_log,
                        2);
  RespClient second_client(second_port);
  ASSERT_EQ(second_client.Command(
                {"REPLICAOF", "127.0.0.1", std::to_string(source_port)}),
            "+OK");
  const auto second_online_deadline = std::chrono::steady_clock::now() + 20s;
  std::string second_info;
  do {
    second_info = second_client.Command({"INFO", "replication"});
    if (second_info.find("keylane_replication_state:online") !=
        std::string::npos) {
      break;
    }
    std::this_thread::sleep_for(10ms);
  } while (std::chrono::steady_clock::now() < second_online_deadline);
  ASSERT_NE(second_info.find("keylane_replication_state:online"),
            std::string::npos);
  for (int i = 0; i < 200; ++i) {
    ASSERT_EQ(source_client.Command({"SET", "bulk:" + std::to_string(i),
                                     "value:" + std::to_string(i)}),
              "+OK");
  }
  ASSERT_TRUE(wait_value(first_client, "bulk:199", "value:199"));
  ASSERT_TRUE(wait_value(second_client, "bulk:199", "value:199"));

  ASSERT_EQ(source_client.Command({"FLUSHDB"}), "+OK");
  const auto empty_deadline = std::chrono::steady_clock::now() + 20s;
  while (std::chrono::steady_clock::now() < empty_deadline &&
         (first_client.Command({"GET", "bulk:199"}) != "$-1" ||
          second_client.Command({"GET", "bulk:199"}) != "$-1")) {
    std::this_thread::sleep_for(10ms);
  }
  EXPECT_EQ(first_client.Command({"GET", "bulk:199"}), "$-1");
  EXPECT_EQ(second_client.Command({"GET", "bulk:199"}), "$-1");
  ASSERT_EQ(source_client.Command({"SET", "after-flush", "present"}),
            "+OK");
  EXPECT_TRUE(wait_value(first_client, "after-flush", "present"));
  EXPECT_TRUE(wait_value(second_client, "after-flush", "present"));
  first.Stop();
  second.Stop();
  source.Stop();
}

// The legacy source-push RPC tests remain as executable documentation until
// snapshot/apply is moved onto KLPSYNC/KLFLOW in the next replication slice.
TEST(ListE2eTest, DISABLED_ReplicatesMonolithicCollectionsInChunks) {
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
  while (replication_port == source_port || replication_port == replica_port) {
    replication_port = FindFreePort();
  }
  // 104 elements put each encoded value above the 12 MiB replication RPC
  // limit, exercising begin/chunk/commit framing for every unbounded type.
  const std::string element(128 * 1024, 'p');

  // Persist source values before attaching the replica so this exercises both
  // the baseline snapshot and later deltas.
  {
    ServerProcess source(g_keylane_binary, source_port, source_data, source_log,
                         3);
    RespClient client(source_port);
    std::vector<std::string> hash_fields;
    std::vector<std::string> set_members;
    std::vector<std::string> zset_scores;
    std::vector<std::string> zset_members;
    std::vector<std::string> list_values;
    hash_fields.reserve(104);
    set_members.reserve(104);
    zset_scores.reserve(104);
    zset_members.reserve(104);
    list_values.reserve(104);
    std::vector<std::string_view> rpush{"RPUSH", "replicated-list"};
    std::vector<std::string_view> hset{"HSET", "replicated-hash"};
    std::vector<std::string_view> sadd{"SADD", "replicated-set"};
    std::vector<std::string_view> zadd{"ZADD", "replicated-zset"};
    for (int i = 0; i < 104; ++i) {
      list_values.push_back(element + "-" + std::to_string(i));
      rpush.push_back(list_values.back());
      hash_fields.push_back("field-" + std::to_string(i));
      set_members.push_back(element + "-member-" + std::to_string(i));
      hset.push_back(hash_fields.back());
      hset.push_back(set_members.back());
      sadd.push_back(set_members.back());
      zset_scores.push_back(std::to_string(i));
      zset_members.push_back(element + "-zmember-" + std::to_string(i));
      zadd.push_back(zset_scores.back());
      zadd.push_back(zset_members.back());
    }
    ASSERT_EQ(client.Command(rpush), ":104");
    ASSERT_EQ(client.Command(hset), ":104");
    ASSERT_EQ(client.Command(sadd), ":104");
    ASSERT_EQ(client.Command(zadd), ":104");
    const std::string stream_payload(13 * 1024 * 1024, 's');
    ASSERT_EQ(client.Command({"XADD", "replicated-stream", "1-0", "field",
                              stream_payload}),
              Bulk("1-0"));
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
        {"--replicate-to", "127.0.0.1:" + std::to_string(replication_port)});
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
    ASSERT_TRUE(wait_for([&] {
      return replica_client.Command({"HLEN", "replicated-hash"}) == ":104" &&
             replica_client.Command({"SCARD", "replicated-set"}) == ":104" &&
             replica_client.Command({"ZCARD", "replicated-zset"}) == ":104" &&
             replica_client.Command({"XLEN", "replicated-stream"}) == ":1";
    }));
    EXPECT_EQ(replica_client.Command({"HGET", "replicated-hash", "field-10"}),
              Bulk(element + "-member-10"));
    EXPECT_EQ(replica_client.Command(
                  {"SISMEMBER", "replicated-set", element + "-member-10"}),
              ":1");
    EXPECT_EQ(replica_client.Command(
                  {"ZSCORE", "replicated-zset", element + "-zmember-10"}),
              Bulk("10"));

    // Delta capture is already active; the complete updated value is sent.
    EXPECT_EQ(source_client.Command(
                  {"RPUSH", "replica-created-list", element, "tail"}),
              ":2");
    ASSERT_TRUE(wait_for([&] {
      return replica_client.Command({"LLEN", "replica-created-list"}) == ":2" &&
             replica_client.Command({"LINDEX", "replica-created-list", "-1"}) ==
                 Bulk("tail");
    }));
    EXPECT_EQ(
        source_client.Command({"LSET", "replicated-list", "10", "delta-value"}),
        "+OK");
    ASSERT_TRUE(wait_for([&] {
      return replica_client.Command({"LINDEX", "replicated-list", "10"}) ==
             Bulk("delta-value");
    }));
    EXPECT_EQ(source_client.Command({"LPUSH", "replicated-list", "delta-head"}),
              ":105");
    ASSERT_TRUE(wait_for([&] {
      return replica_client.Command({"LINDEX", "replicated-list", "0"}) ==
             Bulk("delta-head");
    }));
    EXPECT_EQ(source_client.Command(
                  {"HSET", "replicated-hash", "field-10", "hash-delta"}),
              ":0");
    ASSERT_TRUE(wait_for([&] {
      return replica_client.Command({"HGET", "replicated-hash", "field-10"}) ==
             Bulk("hash-delta");
    }));
    ASSERT_EQ(source_client.Command(
                  {"SREM", "replicated-set", element + "-member-10"}),
              ":1");
    ASSERT_TRUE(wait_for([&] {
      return replica_client.Command({"SISMEMBER", "replicated-set",
                                     element + "-member-10"}) == ":0";
    }));
    EXPECT_EQ(source_client.Command({"PEXPIRE", "replicated-list", "600000"}),
              ":1");
    ASSERT_TRUE(wait_for([&] {
      const std::string ttl =
          replica_client.Command({"PTTL", "replicated-list"});
      return ttl.starts_with(':') && std::stoll(ttl.substr(1)) > 0;
    }));
    EXPECT_EQ(source_client.Command({"LTRIM", "replicated-list", "0", "39"}),
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
    EXPECT_EQ(client.Command({"HGET", "replicated-hash", "field-10"}),
              Bulk("hash-delta"));
    EXPECT_EQ(
        client.Command({"SISMEMBER", "replicated-set", element + "-member-10"}),
        ":0");
    EXPECT_EQ(client.Command({"ZCARD", "replicated-zset"}), ":104");
    EXPECT_EQ(client.Command({"XLEN", "replicated-stream"}), ":1");
    replica.Stop();
  }
}

TEST(ListE2eTest, DISABLED_ReplicaFlushDbDoesNotInvalidateInFlightListRead) {
  ASSERT_FALSE(g_keylane_binary.empty());
  const std::string prefix =
      "/tmp/keylane-list-replica-flush-race-" + std::to_string(::getpid());
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
    ASSERT_EQ(::posix_fallocate(fd, 0, 128ULL * 1024 * 1024), 0);
    ASSERT_EQ(::close(fd), 0);
  }

  const std::uint16_t source_port = FindFreePort();
  std::uint16_t replica_port = FindFreePort();
  while (replica_port == source_port) replica_port = FindFreePort();
  std::uint16_t replication_port = FindFreePort();
  while (replication_port == source_port || replication_port == replica_port) {
    replication_port = FindFreePort();
  }
  std::vector<std::pair<std::string, std::string>> replica_environment;
#ifndef NDEBUG
  replica_environment.emplace_back("KEYLANE_LIST_READ_PAUSE_MS", "500");
  replica_environment.emplace_back("KEYLANE_HASH_READ_PAUSE_MS", "500");
#endif
  ServerProcess replica(g_keylane_binary, replica_port, replica_data,
                        replica_log, 2, {},
                        {"--replication-port", std::to_string(replication_port),
                         "--replica-read-only"},
                        std::move(replica_environment));
  ServerProcess source(
      g_keylane_binary, source_port, source_data, source_log, 2, {},
      {"--replicate-to", "127.0.0.1:" + std::to_string(replication_port)});
  RespClient source_client(source_port);
  RespClient replica_client(replica_port);
  ASSERT_EQ(source_client.Command({"SELECT", "1"}), "+OK");
  ASSERT_EQ(replica_client.Command({"SELECT", "1"}), "+OK");

  const std::string element(128 * 1024, 'r');
  ASSERT_EQ(source_client.Command({"RPUSH", "race-list", element, "tail"}),
            ":2");
  ASSERT_EQ(source_client.Command({"HSET", "race-hash", "field", "value"}),
            ":1");
  ASSERT_EQ(source_client.Command({"SADD", "race-set", "member"}), ":1");
  const auto replication_deadline = std::chrono::steady_clock::now() + 120s;
  while (std::chrono::steady_clock::now() < replication_deadline &&
         replica_client.Command({"LLEN", "race-list"}) != ":2") {
    std::this_thread::sleep_for(20ms);
  }
  ASSERT_EQ(replica_client.Command({"LLEN", "race-list"}), ":2");
  while (std::chrono::steady_clock::now() < replication_deadline &&
         replica_client.Command({"HGET", "race-hash", "field"}) !=
             Bulk("value")) {
    std::this_thread::sleep_for(20ms);
  }
  ASSERT_EQ(replica_client.Command({"HGET", "race-hash", "field"}),
            Bulk("value"));
  while (std::chrono::steady_clock::now() < replication_deadline &&
         replica_client.Command({"SISMEMBER", "race-set", "member"}) != ":1") {
    std::this_thread::sleep_for(20ms);
  }
  ASSERT_EQ(replica_client.Command({"SISMEMBER", "race-set", "member"}), ":1");

  // A replica epoch delta bypasses the client DB gate. Pause after the read
  // snapshots RecordLocation/extents and drops store_state_mutex_, then
  // detach that complete index. The read can linearize on either side of
  // FLUSHDB, but it must neither touch the freed Entry nor surface an internal
  // storage error.
  auto in_flight_read = std::async(std::launch::async, [replica_port] {
    RespClient reader(replica_port);
    if (reader.Command({"SELECT", "1"}) != "+OK") return std::string{};
    return reader.Command({"LINDEX", "race-list", "0"});
  });
  auto in_flight_hash_read = std::async(std::launch::async, [replica_port] {
    RespClient reader(replica_port);
    if (reader.Command({"SELECT", "1"}) != "+OK") return std::string{};
    return reader.Command({"HGET", "race-hash", "field"});
  });
  auto in_flight_set_read = std::async(std::launch::async, [replica_port] {
    RespClient reader(replica_port);
    if (reader.Command({"SELECT", "1"}) != "+OK") return std::string{};
    return reader.Command({"SISMEMBER", "race-set", "member"});
  });
#ifndef NDEBUG
  std::this_thread::sleep_for(100ms);
#endif
  ASSERT_EQ(source_client.Command({"FLUSHDB"}), "+OK");
  const auto flush_deadline = std::chrono::steady_clock::now() + 120s;
  while (std::chrono::steady_clock::now() < flush_deadline &&
         replica_client.Command({"DBSIZE"}) != ":0") {
    std::this_thread::sleep_for(20ms);
  }
  ASSERT_EQ(replica_client.Command({"DBSIZE"}), ":0");
  const std::string raced_reply = in_flight_read.get();
  EXPECT_TRUE(raced_reply == Bulk(element) || raced_reply == "$-1")
      << raced_reply.substr(0, 256);
  const std::string raced_hash_reply = in_flight_hash_read.get();
  EXPECT_TRUE(raced_hash_reply == Bulk("value") || raced_hash_reply == "$-1")
      << raced_hash_reply.substr(0, 256);
  const std::string raced_set_reply = in_flight_set_read.get();
  EXPECT_TRUE(raced_set_reply == ":1" || raced_set_reply == ":0")
      << raced_set_reply.substr(0, 256);
  EXPECT_EQ(replica_client.Command({"PING"}), "+PONG");
  source.Stop();
  replica.Stop();
}

TEST(HashE2eTest, UpdatesTransactionsAndRecoversMonolithicValues) {
  ASSERT_FALSE(g_keylane_binary.empty());
  const std::string prefix =
      "/tmp/keylane-hash-e2e-" + std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::posix_fallocate(fd, 0, 256ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(fd), 0);

  constexpr std::size_t kFields = 180;
  std::vector<std::string> fields;
  std::vector<std::string> values;
  fields.reserve(kFields);
  values.reserve(kFields);
  for (std::size_t i = 0; i < kFields; ++i) {
    fields.push_back("field-" + std::to_string(i));
    values.push_back(std::string(4096, static_cast<char>('a' + i % 23)) + "-" +
                     std::to_string(i));
  }

  const std::uint16_t port = FindFreePort();
  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 2);
    RespClient client(port);
    std::vector<std::string_view> hset{"HSET", "large-hash"};
    for (std::size_t i = 0; i < kFields; ++i) {
      hset.push_back(fields[i]);
      hset.push_back(values[i]);
    }
    EXPECT_EQ(client.Command(hset), ":180");
    EXPECT_EQ(client.Command({"HLEN", "large-hash"}), ":180");
    EXPECT_EQ(client.Command({"HGET", "large-hash", fields[0]}),
              Bulk(values[0]));
    EXPECT_EQ(client.Command({"HGET", "large-hash", fields[179]}),
              Bulk(values[179]));
    EXPECT_EQ(client.Command({"HINCRBY", "numeric-hash", "counter", "-2"}),
              ":-2");
    EXPECT_EQ(client.Command({"HINCRBY", "numeric-hash", "counter", "5"}),
              ":3");
    EXPECT_EQ(
        client.Command({"HINCRBYFLOAT", "numeric-hash", "counter", "1.5"}),
        Bulk("4.5"));
    EXPECT_EQ(client.Command({"HSET", "empty-field-hash", "", "value"}), ":1");
    auto [empty_hash_cursor, empty_hash_scan] = ParseScanReply(
        client.Command({"HSCAN", "empty-field-hash", "0", "COUNT", "100"}));
    EXPECT_EQ(empty_hash_cursor, "0");
    EXPECT_EQ(empty_hash_scan, (std::vector<std::string>{"", "value"}));
    EXPECT_EQ(client.Command({"HSET", "numeric-hash", "plus-counter", "+1.5"}),
              ":1");
    EXPECT_EQ(client.Command(
                  {"HINCRBYFLOAT", "numeric-hash", "plus-counter", "+0.5"}),
              Bulk("2"));
    const std::string scan =
        client.Command({"HSCAN", "large-hash", "0", "COUNT", "1000"});
    EXPECT_TRUE(scan.starts_with("*2\r\n$1\r\n0\r\n*360\r\n"));
    const std::string random =
        client.Command({"HRANDFIELD", "large-hash", "10", "WITHVALUES"});
    EXPECT_TRUE(random.starts_with("*20\r\n"));
    const std::string repeated_random =
        client.Command({"HRANDFIELD", "large-hash", "-1000"});
    EXPECT_TRUE(repeated_random.starts_with("*1000\r\n"));
    EXPECT_EQ(
        client.Command({"HRANDFIELD", "large-hash", "-9223372036854775808"}),
        "-ERR value is out of range");
    EXPECT_EQ(client.Command({"HRANDFIELD", "large-hash",
                              "-4611686018427387904", "WITHVALUES"}),
              "-ERR value is out of range");
    EXPECT_EQ(
        client.Command({"HRANDFIELD", "missing-hash", "-9223372036854775807"}),
        "*0");
    EXPECT_TRUE(
        client.Command({"HRANDFIELD", "numeric-hash", "-2001", "WITHVALUES"})
            .starts_with("*4002\r\n"));
    CloseStreamingReplyAfterHeader(
        port, {"HRANDFIELD", "numeric-hash", "-9223372036854775807"},
        "*9223372036854775807");
    EXPECT_TRUE(WaitForReply(client, {"HLEN", "large-hash"}, ":180"));
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(
        client.Command({"HRANDFIELD", "numeric-hash", "-2001", "WITHVALUES"}),
        "+QUEUED");
    EXPECT_EQ(client.Command({"SET", "after-random-oom", "alive"}), "+QUEUED");
    const std::string hash_exec_random = client.Command({"EXEC"});
    EXPECT_TRUE(hash_exec_random.starts_with("*2\r\n*4002\r\n"));
    EXPECT_TRUE(hash_exec_random.ends_with("+OK"));
    EXPECT_EQ(client.Command({"GET", "after-random-oom"}), Bulk("alive"));
    EXPECT_EQ(
        client.Command({"HSET", "large-hash", fields[79], "ordinary-update"}),
        ":0");
    EXPECT_EQ(client.Command({"HDEL", "large-hash", fields[20], fields[21]}),
              ":2");
    EXPECT_EQ(client.Command({"HSETNX", "large-hash", fields[79], "no"}), ":0");
    EXPECT_EQ(client.Command({"HSETNX", "large-hash", "new-field", "new"}),
              ":1");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"HSET", "large-hash", fields[80], "tx-update"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"HDEL", "large-hash", fields[22]}), "+QUEUED");
    EXPECT_EQ(client.Command({"HGET", "large-hash", fields[80]}), "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}),
              "*3\r\n:0\r\n:1\r\n" + Bulk("tx-update"));
    EXPECT_EQ(
        client.Command({"HSET", "large-hash", fields[81], "ordinary-after-tx"}),
        ":0");
    EXPECT_EQ(client.Command({"PEXPIRE", "large-hash", "600000"}), ":1");
    ASSERT_TRUE(WaitForDurability(client));
    server.Kill();
  }

  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 3);
    RespClient client(port);
    EXPECT_EQ(client.Command({"HLEN", "large-hash"}), ":178");
    EXPECT_EQ(client.Command({"HGET", "large-hash", fields[20]}), "$-1");
    EXPECT_EQ(client.Command({"HGET", "large-hash", fields[79]}),
              Bulk("ordinary-update"));
    EXPECT_EQ(client.Command({"HGET", "large-hash", fields[80]}),
              Bulk("tx-update"));
    EXPECT_EQ(client.Command({"HGET", "large-hash", fields[81]}),
              Bulk("ordinary-after-tx"));
    EXPECT_EQ(client.Command({"HGET", "large-hash", "new-field"}), Bulk("new"));
    EXPECT_EQ(client.Command({"HGET", "numeric-hash", "counter"}), Bulk("4.5"));
    const std::string ttl = client.Command({"PTTL", "large-hash"});
    ASSERT_TRUE(ttl.starts_with(':'));
    EXPECT_GT(std::stoll(ttl.substr(1)), 0);
    EXPECT_EQ(client.Command({"SET", "large-hash", "string-now"}), "+OK");
    EXPECT_EQ(client.Command({"GET", "large-hash"}), Bulk("string-now"));
    ASSERT_TRUE(WaitForDurability(client));
    server.Kill();
  }

  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 2);
    RespClient client(port);
    EXPECT_EQ(client.Command({"GET", "large-hash"}), Bulk("string-now"));
    EXPECT_TRUE(client.Command({"HGET", "large-hash", fields[0]})
                    .starts_with("-WRONGTYPE"));
    server.Stop();
  }
}

TEST(SetE2eTest, ScanCursorDoesNotSkipAfterEarlierMembersAreDeleted) {
  ASSERT_FALSE(g_keylane_binary.empty());
  const std::string prefix =
      "/tmp/keylane-set-scan-stable-" + std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::posix_fallocate(fd, 0, 128ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(fd), 0);

  std::vector<std::string> members;
  members.reserve(1000);
  std::vector<std::string_view> sadd{"SADD", "scan-set"};
  sadd.reserve(1002);
  for (int i = 0; i < 1000; ++i) {
    members.push_back("scan-member-" + std::to_string(i));
    sadd.push_back(members.back());
  }
  const std::uint16_t port = FindFreePort();
  ServerProcess server(g_keylane_binary, port, data_path, log_path, 2);
  RespClient client(port);
  ASSERT_EQ(client.Command(sadd), ":1000");

  std::unordered_set<std::string> seen;
  std::string cursor = "0";
  bool deleted_first_batch = false;
  std::size_t calls = 0;
  do {
    auto [next, batch] = ParseScanReply(
        client.Command({"SSCAN", "scan-set", cursor, "COUNT", "10"}));
    for (const std::string& member : batch) seen.insert(member);
    if (!deleted_first_batch && next != "0") {
      std::vector<std::string_view> srem{"SREM", "scan-set"};
      for (const std::string& member : batch) srem.push_back(member);
      ASSERT_EQ(client.Command(srem), ":" + std::to_string(batch.size()));
      deleted_first_batch = true;
    }
    cursor = std::move(next);
    ASSERT_LT(++calls, 2000u);
  } while (cursor != "0");
  EXPECT_TRUE(deleted_first_batch);
  EXPECT_EQ(seen.size(), members.size());
  for (const std::string& member : members) EXPECT_TRUE(seen.contains(member));
  server.Stop();
}

TEST(SetE2eTest, Redis72CommandsTransactionsAndRecovery) {
  ASSERT_FALSE(g_keylane_binary.empty());
  const std::string prefix =
      "/tmp/keylane-set-e2e-" + std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::posix_fallocate(fd, 0, 512ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(fd), 0);

  constexpr std::size_t kMembers = 180;
  std::vector<std::string> members;
  members.reserve(kMembers);
  for (std::size_t i = 0; i < kMembers; ++i) {
    members.push_back("member-" + std::to_string(i) + "-" +
                      std::string(4096, static_cast<char>('a' + i % 23)));
  }

  const std::uint16_t port = FindFreePort();
  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 4);
    RespClient client(port);
    EXPECT_EQ(client.Command({"SADD", "basic", "a", "b", "a", "c"}), ":3");
    EXPECT_EQ(client.Command({"SCARD", "basic"}), ":3");
    EXPECT_EQ(client.Command({"SISMEMBER", "basic", "b"}), ":1");
    EXPECT_EQ(client.Command({"SMISMEMBER", "basic", "a", "z", "c"}),
              "*3\r\n:1\r\n:0\r\n:1");
    EXPECT_EQ(client.Command({"SREM", "basic", "b", "b", "missing"}), ":1");
    EXPECT_EQ(client.Command({"SPOP", "basic", "0"}), "*0");
    EXPECT_EQ(client.Command({"SADD", "empty-member-set", ""}), ":1");
    auto [empty_set_cursor, empty_set_scan] = ParseScanReply(
        client.Command({"SSCAN", "empty-member-set", "0", "COUNT", "100"}));
    EXPECT_EQ(empty_set_cursor, "0");
    EXPECT_EQ(empty_set_scan, (std::vector<std::string>{""}));
    EXPECT_EQ(client.Command({"SRANDMEMBER", "missing", "3"}), "*0");
    EXPECT_EQ(client.Command({"SRANDMEMBER", "basic", "-9223372036854775808"}),
              "-ERR value is out of range");
    EXPECT_EQ(
        client.Command({"SRANDMEMBER", "missing", "-9223372036854775807"}),
        "*0");
    EXPECT_TRUE(client.Command({"SRANDMEMBER", "basic", "-2001"})
                    .starts_with("*2001\r\n"));
    CloseStreamingReplyAfterHeader(
        port, {"SRANDMEMBER", "basic", "-9223372036854775807"},
        "*9223372036854775807");
    EXPECT_TRUE(WaitForReply(client, {"SCARD", "basic"}, ":2"));
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"SRANDMEMBER", "basic", "-1001"}), "+QUEUED");
    EXPECT_TRUE(client.Command({"EXEC"}).starts_with("*1\r\n*1001\r\n"));
    CloseExecStreamingReplyAfterHeader(
        port, {"SRANDMEMBER", "basic", "-9223372036854775807"},
        "*9223372036854775807");
    EXPECT_TRUE(WaitForReply(client, {"SCARD", "basic"}, ":2"));
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"SADD", "tx-random-view", "only"}), "+QUEUED");
    EXPECT_EQ(client.Command({"SRANDMEMBER", "tx-random-view", "-1001"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"DEL", "tx-random-view"}), "+QUEUED");
    const std::string serial_random = client.Command({"EXEC"});
    EXPECT_TRUE(serial_random.starts_with("*3\r\n:1\r\n*1001\r\n$4\r\nonly"));
    EXPECT_TRUE(serial_random.ends_with(":1"));
    EXPECT_EQ(client.Command({"SET", "not-set", "value"}), "+OK");
    EXPECT_TRUE(
        client.Command({"SMEMBERS", "not-set"}).starts_with("-WRONGTYPE"));
    EXPECT_TRUE(client.Command({"SMOVE", "basic", "not-set", "missing"})
                    .starts_with("-WRONGTYPE"));
    EXPECT_EQ(client.Command({"SMOVE", "missing-source", "not-set", "member"}),
              ":0");
    EXPECT_TRUE(client.Command({"SMOVE", "basic", "not-set", "c"})
                    .starts_with("-WRONGTYPE"));
    EXPECT_EQ(client.Command({"SMOVE", "basic", "basic", "missing"}), ":0");
    EXPECT_EQ(client.Command({"SMOVE", "basic", "basic", "a"}), ":1");
    EXPECT_TRUE(client.Command({"SINTERCARD", "0", "basic"})
                    .starts_with("-ERR numkeys"));
    EXPECT_TRUE(client.Command({"SINTERCARD", "2", "basic"})
                    .starts_with("-ERR Number of keys"));
    EXPECT_TRUE(client.Command({"SINTERCARD", "1", "basic", "LIMIT", "-1"})
                    .starts_with("-ERR LIMIT"));

    EXPECT_EQ(client.Command({"SADD", "left", "a", "b", "c", "d"}), ":4");
    EXPECT_EQ(client.Command({"SADD", "right", "c", "d", "e"}), ":3");
    EXPECT_EQ(
        client.Command({"SINTERCARD", "2", "left", "right", "LiMiT", "1"}),
        ":1");
    EXPECT_EQ(client.Command({"SINTERSTORE", "intersection", "left", "right"}),
              ":2");
    EXPECT_EQ(client.Command({"SCARD", "intersection"}), ":2");
    EXPECT_EQ(client.Command({"SMOVE", "left", "right", "a"}), ":1");
    EXPECT_EQ(client.Command({"SISMEMBER", "left", "a"}), ":0");
    EXPECT_EQ(client.Command({"SISMEMBER", "right", "a"}), ":1");

    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"SMOVE", "missing-source", "not-set", "member"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*1\r\n:0");

    std::vector<std::string_view> sadd{"SADD", "large-set"};
    for (const std::string& member : members) sadd.push_back(member);
    EXPECT_EQ(client.Command(sadd), ":180");
    EXPECT_EQ(client.Command({"SCARD", "large-set"}), ":180");
    EXPECT_EQ(client.Command({"SISMEMBER", "large-set", members[0]}), ":1");
    EXPECT_EQ(client.Command({"SISMEMBER", "large-set", members[179]}), ":1");
    const std::string scan =
        client.Command({"SSCAN", "large-set", "0", "mAtCh", "member-[0-2]-*",
                        "cOuNt", "1000"});
    EXPECT_TRUE(scan.starts_with("*2\r\n$1\r\n0\r\n*3\r\n"));
    EXPECT_TRUE(scan.find(members[0]) != std::string::npos);
    EXPECT_TRUE(scan.find(members[1]) != std::string::npos);
    EXPECT_TRUE(scan.find(members[2]) != std::string::npos);
    EXPECT_TRUE(client.Command({"SRANDMEMBER", "large-set", "10"})
                    .starts_with("*10\r\n"));

    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"SADD", "large-set", "tx-member"}), "+QUEUED");
    EXPECT_EQ(client.Command({"SREM", "large-set", members[20]}), "+QUEUED");
    EXPECT_EQ(
        client.Command({"SINTERSTORE", "tx-intersection", "left", "right"}),
        "+QUEUED");
    EXPECT_EQ(client.Command({"SMOVE", "right", "left", "e"}), "+QUEUED");
    EXPECT_EQ(client.Command({"SCARD", "large-set"}), "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*5\r\n:1\r\n:1\r\n:2\r\n:1\r\n:180");
    EXPECT_EQ(client.Command({"SADD", "large-set", "ordinary-after-tx"}), ":1");
    EXPECT_EQ(client.Command({"SADD", "watched-set", "member"}), ":1");
    EXPECT_EQ(client.Command({"WATCH", "watched-set"}), "+OK");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"SCARD", "watched-set"}), "+QUEUED");
    {
      RespClient concurrent(port);
      EXPECT_EQ(concurrent.Command({"SADD", "watched-set", "member"}), ":0");
    }
    EXPECT_EQ(client.Command({"EXEC"}), "*1\r\n:1");
    EXPECT_EQ(client.Command({"PEXPIRE", "large-set", "600000"}), ":1");
    ASSERT_TRUE(WaitForDurability(client));
    server.Kill();
  }

  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 3);
    RespClient client(port);
    EXPECT_EQ(client.Command({"SCARD", "large-set"}), ":181");
    EXPECT_EQ(client.Command({"SISMEMBER", "large-set", members[20]}), ":0");
    EXPECT_EQ(client.Command({"SISMEMBER", "large-set", "tx-member"}), ":1");
    EXPECT_EQ(client.Command({"SISMEMBER", "large-set", "ordinary-after-tx"}),
              ":1");
    EXPECT_EQ(client.Command({"SCARD", "tx-intersection"}), ":2");
    const std::string ttl = client.Command({"PTTL", "large-set"});
    ASSERT_TRUE(ttl.starts_with(':'));
    EXPECT_GT(std::stoll(ttl.substr(1)), 0);
    EXPECT_EQ(client.Command({"SET", "large-set", "string-now"}), "+OK");
    EXPECT_EQ(client.Command({"GET", "large-set"}), Bulk("string-now"));
    ASSERT_TRUE(WaitForDurability(client));
    server.Kill();
  }

  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 2);
    RespClient client(port);
    EXPECT_EQ(client.Command({"GET", "large-set"}), Bulk("string-now"));
    EXPECT_TRUE(
        client.Command({"SCARD", "large-set"}).starts_with("-WRONGTYPE"));
    server.Stop();
  }
}

TEST(HashE2eTest, ExpiredShieldedWinnerDoesNotResurrectOlderString) {
  ASSERT_FALSE(g_keylane_binary.empty());
  const std::string prefix =
      "/tmp/keylane-expired-shield-recovery-" + std::to_string(::getpid());
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
  const std::string old_value(3900 * 1024, 'o');
  const std::string new_value(3900 * 1024, 'n');
  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 2);
    RespClient client(port);
    ASSERT_EQ(client.Command({"SET", "shielded", old_value, "PX", "600000"}),
              "+OK");
    // This live neighbor keeps the old version's record block allocated. The
    // equally large overwrite rotates into the next records block.
    ASSERT_EQ(client.Command({"SET", "old-block-anchor", old_value}), "+OK");
    ASSERT_EQ(client.Command({"SET", "shielded", new_value, "PX", "5000"}),
              "+OK");
    ASSERT_EQ(client.Command({"SET", "unshielded", "single", "PX", "5000"}),
              "+OK");
    ASSERT_TRUE(WaitForDurability(client));
    server.Stop();
  }
  std::this_thread::sleep_for(5200ms);
  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 3);
    RespClient client(port);
    EXPECT_EQ(client.Command({"GET", "shielded"}), "$-1");
    EXPECT_EQ(client.Command({"GET", "unshielded"}), "$-1");
    const auto expiry_deadline = std::chrono::steady_clock::now() + 10s;
    while (std::chrono::steady_clock::now() < expiry_deadline &&
           client.Command({"DBSIZE"}) != ":1") {
      std::this_thread::sleep_for(10ms);
    }
    ASSERT_EQ(client.Command({"DBSIZE"}), ":1");
    ASSERT_TRUE(WaitForDurability(client));

    const std::string filler(480 * 1024, 'f');
    for (int batch = 0; batch < 4; ++batch) {
      std::vector<std::string> keys;
      std::vector<std::string_view> mset{"MSET"};
      for (int i = 0; i < 16; ++i) {
        keys.push_back("shield-reuse-" + std::to_string(batch) + "-" +
                       std::to_string(i));
        mset.push_back(keys.back());
        mset.push_back(filler);
      }
      ASSERT_EQ(client.Command(mset), "+OK");
    }
    ASSERT_TRUE(WaitForDurability(client));
    server.Kill();
  }
  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 2, {}, {},
                         {{"KEYLANE_RECOVERY_NOW_MS", "1"}});
    RespClient client(port);
    EXPECT_EQ(client.Command({"GET", "shielded"}), "$-1");
    EXPECT_EQ(client.Command({"GET", "unshielded"}), "$-1");
    EXPECT_EQ(client.Command({"GET", "old-block-anchor"}), Bulk(old_value));
    server.Stop();
  }
}

TEST(CollectionE2eTest, MemoryLimitStillAllowsShrinkingCommands) {
  ASSERT_FALSE(g_keylane_binary.empty());
  const std::string prefix =
      "/tmp/keylane-memory-recovery-" + std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::posix_fallocate(fd, 0, 128ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(fd), 0);

  const std::uint16_t port = FindFreePort();
  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 2);
    RespClient client(port);
    EXPECT_EQ(client.Command({"RPUSH", "l", "a", "b"}), ":2");
    EXPECT_EQ(client.Command({"HSET", "h", "f", "v"}), ":1");
    EXPECT_EQ(client.Command({"SADD", "s", "m"}), ":1");
    EXPECT_EQ(client.Command({"ZADD", "z", "1", "m"}), ":1");
    EXPECT_EQ(client.Command({"XADD", "x", "1-0", "f", "v"}), Bulk("1-0"));
    EXPECT_EQ(client.Command({"XADD", "x", "2-0", "f", "v"}), Bulk("2-0"));
    EXPECT_EQ(client.Command({"XGROUP", "CREATE", "x", "g", "0"}), "+OK");
    EXPECT_TRUE(client
                    .Command({"XREADGROUP", "GROUP", "g", "c", "COUNT", "1",
                              "STREAMS", "x", ">"})
                    .starts_with("*1\r\n"));
    ASSERT_TRUE(WaitForDurability(client));
    server.Stop();
  }
  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 2, {},
                         {"--max-memory", "1"});
    RespClient client(port);
    std::this_thread::sleep_for(200ms);
    EXPECT_TRUE(client.Command({"SET", "must-be-rejected", "value"})
                    .starts_with("-OOM command not allowed"));
    EXPECT_EQ(client.Command({"LPOP", "l"}), Bulk("a"));
    EXPECT_EQ(client.Command({"HDEL", "h", "f"}), ":1");
    EXPECT_EQ(client.Command({"SREM", "s", "m"}), ":1");
    EXPECT_EQ(client.Command({"ZREMRANGEBYRANK", "z", "0", "-1"}), ":1");
    EXPECT_EQ(client.Command({"XACK", "x", "g", "1-0"}), ":1");
    EXPECT_EQ(client.Command({"XTRIM", "x", "MAXLEN", "0"}), ":2");
    EXPECT_EQ(client.Command({"XGROUP", "DESTROY", "x", "g"}), ":1");
    server.Stop();
  }
}

TEST(CollectionE2eTest, ExecPartialWritesRollbackDurably) {
#ifdef NDEBUG
  GTEST_SKIP() << "transaction write fault injection is debug-only";
#else
  ASSERT_FALSE(g_keylane_binary.empty());
  const std::string prefix =
      "/tmp/keylane-exec-command-rollback-" + std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::posix_fallocate(fd, 0, 128ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(fd), 0);

  const std::uint16_t port = FindFreePort();
  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 3, {}, {},
                         {{"KEYLANE_FAIL_TX_WRITE", "exec-fail-dst"}});
    RespClient client(port);
    EXPECT_EQ(client.Command({"RPUSH", "exec-list-src", "source"}), ":1");
    EXPECT_EQ(client.Command({"RPUSH", "exec-fail-dst", "destination"}), ":1");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"SET", "exec-before-list", "kept"}), "+QUEUED");
    EXPECT_EQ(client.Command(
                  {"LMOVE", "exec-list-src", "exec-fail-dst", "LEFT", "RIGHT"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"SET", "exec-after-list", "kept"}), "+QUEUED");
    const std::string list_exec = client.Command({"EXEC"});
    EXPECT_TRUE(list_exec.starts_with("*3\r\n+OK\r\n-ERR injected"))
        << list_exec;
    EXPECT_EQ(client.Command({"LRANGE", "exec-list-src", "0", "-1"}),
              "*1\r\n" + Bulk("source"));
    EXPECT_EQ(client.Command({"LRANGE", "exec-fail-dst", "0", "-1"}),
              "*1\r\n" + Bulk("destination"));

    EXPECT_EQ(client.Command({"DEL", "exec-fail-dst"}), ":1");
    EXPECT_EQ(client.Command({"SADD", "exec-set-src", "member"}), ":1");
    EXPECT_EQ(client.Command({"SADD", "exec-fail-dst", "existing"}), ":1");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"SET", "exec-before-set", "kept"}), "+QUEUED");
    EXPECT_EQ(
        client.Command({"SMOVE", "exec-set-src", "exec-fail-dst", "member"}),
        "+QUEUED");
    const std::string set_exec = client.Command({"EXEC"});
    EXPECT_TRUE(set_exec.starts_with("*2\r\n+OK\r\n-ERR injected")) << set_exec;
    EXPECT_EQ(client.Command({"SISMEMBER", "exec-set-src", "member"}), ":1");
    EXPECT_EQ(client.Command({"SMEMBERS", "exec-fail-dst"}),
              "*1\r\n" + Bulk("existing"));
    ASSERT_TRUE(WaitForDurability(client));
    server.Kill();
  }
  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 3);
    RespClient client(port);
    EXPECT_EQ(client.Command({"GET", "exec-before-list"}), Bulk("kept"));
    EXPECT_EQ(client.Command({"GET", "exec-after-list"}), Bulk("kept"));
    EXPECT_EQ(client.Command({"LRANGE", "exec-list-src", "0", "-1"}),
              "*1\r\n" + Bulk("source"));
    EXPECT_EQ(client.Command({"GET", "exec-before-set"}), Bulk("kept"));
    EXPECT_EQ(client.Command({"SISMEMBER", "exec-set-src", "member"}), ":1");
    EXPECT_EQ(client.Command({"SMEMBERS", "exec-fail-dst"}),
              "*1\r\n" + Bulk("existing"));
    server.Stop();
  }
#endif
}

TEST(CollectionE2eTest, ExecStoreReplacementRollbackDurably) {
#ifdef NDEBUG
  GTEST_SKIP() << "transaction write fault injection is debug-only";
#else
  ASSERT_FALSE(g_keylane_binary.empty());
  const std::string prefix =
      "/tmp/keylane-exec-store-rollback-" + std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::posix_fallocate(fd, 0, 128ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(fd), 0);

  const std::uint16_t port = FindFreePort();
  const auto fault_environment =
      std::vector<std::pair<std::string, std::string>>{
          {"KEYLANE_FAIL_TX_WRITE", "exec-store-dst"},
          {"KEYLANE_FAIL_TX_WRITE_AFTER", "1"}};
  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 3, {}, {},
                         fault_environment);
    RespClient client(port);
    EXPECT_EQ(client.Command({"SADD", "exec-store-source", "member"}), ":1");
    EXPECT_EQ(client.Command({"SET", "exec-store-dst", "old-set"}), "+OK");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"SET", "exec-before-set-store", "kept"}),
              "+QUEUED");
    EXPECT_EQ(
        client.Command({"SINTERSTORE", "exec-store-dst", "exec-store-source"}),
        "+QUEUED");
    const std::string executed = client.Command({"EXEC"});
    EXPECT_TRUE(executed.starts_with("*2\r\n+OK\r\n-ERR injected")) << executed;
    EXPECT_EQ(client.Command({"GET", "exec-store-dst"}), Bulk("old-set"));
    ASSERT_TRUE(WaitForDurability(client));
    server.Kill();
  }
  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 3, {}, {},
                         fault_environment);
    RespClient client(port);
    EXPECT_EQ(client.Command({"GET", "exec-before-set-store"}), Bulk("kept"));
    EXPECT_EQ(client.Command({"GET", "exec-store-dst"}), Bulk("old-set"));
    EXPECT_EQ(client.Command({"ZADD", "exec-zstore-source", "1", "member"}),
              ":1");
    EXPECT_EQ(client.Command({"SET", "exec-store-dst", "old-zset"}), "+OK");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"SET", "exec-before-zset-store", "kept"}),
              "+QUEUED");
    EXPECT_EQ(client.Command(
                  {"ZUNIONSTORE", "exec-store-dst", "1", "exec-zstore-source"}),
              "+QUEUED");
    const std::string executed = client.Command({"EXEC"});
    EXPECT_TRUE(executed.starts_with("*2\r\n+OK\r\n-ERR injected")) << executed;
    EXPECT_EQ(client.Command({"GET", "exec-store-dst"}), Bulk("old-zset"));
    ASSERT_TRUE(WaitForDurability(client));
    server.Kill();
  }
  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 3);
    RespClient client(port);
    EXPECT_EQ(client.Command({"GET", "exec-before-set-store"}), Bulk("kept"));
    EXPECT_EQ(client.Command({"GET", "exec-before-zset-store"}), Bulk("kept"));
    EXPECT_EQ(client.Command({"GET", "exec-store-dst"}), Bulk("old-zset"));
    server.Stop();
  }
#endif
}

TEST(CollectionE2eTest, SortedSetGeoAndStreamCommandsRecover) {
  ASSERT_FALSE(g_keylane_binary.empty());
  const std::string prefix =
      "/tmp/keylane-compact-collections-" + std::to_string(::getpid());
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
  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 2);
    RespClient client(port);
    EXPECT_EQ(
        client.Command({"ZADD", "z", "1", "one", "2", "two", "1.5", "mid"}),
        ":3");
    EXPECT_EQ(client.Command({"SET", "type-string", "value"}), "+OK");
    EXPECT_EQ(client.Command({"RPUSH", "type-list", "value"}), ":1");
    EXPECT_EQ(client.Command({"HSET", "type-hash", "field", "value"}), ":1");
    EXPECT_EQ(client.Command({"SADD", "type-set", "member"}), ":1");
    EXPECT_EQ(client.Command({"XADD", "type-stream", "1-0", "field", "value"}),
              Bulk("1-0"));
    EXPECT_EQ(client.Command({"TYPE", "type-string"}), "+string");
    EXPECT_EQ(client.Command({"TYPE", "type-list"}), "+list");
    EXPECT_EQ(client.Command({"TYPE", "type-hash"}), "+hash");
    EXPECT_EQ(client.Command({"TYPE", "type-set"}), "+set");
    EXPECT_EQ(client.Command({"TYPE", "z"}), "+zset");
    EXPECT_EQ(client.Command({"TYPE", "type-stream"}), "+stream");
    EXPECT_EQ(client.Command({"TYPE", "missing-type"}), "+none");

    const std::string rename_source = KeyForWorker("rename-source", 0, 2);
    const std::string rename_destination =
        KeyForWorker("rename-destination", 1, 2);
    EXPECT_EQ(
        client.Command({"SET", rename_source, "renamed-value", "PX", "60000"}),
        "+OK");
    EXPECT_EQ(client.Command({"SET", rename_destination, "overwritten"}),
              "+OK");
    EXPECT_EQ(client.Command({"RENAME", rename_source, rename_destination}),
              "+OK");
    EXPECT_EQ(client.Command({"GET", rename_source}), "$-1");
    EXPECT_EQ(client.Command({"GET", rename_destination}),
              Bulk("renamed-value"));
    const std::string renamed_ttl =
        client.Command({"PTTL", rename_destination});
    ASSERT_TRUE(renamed_ttl.starts_with(":"));
    EXPECT_GT(std::stoll(renamed_ttl.substr(1)), 0);

    EXPECT_EQ(client.Command({"HSET", "rename-hash", "field", "value"}), ":1");
    EXPECT_EQ(client.Command({"RPUSH", "rename-list-target", "old"}), ":1");
    EXPECT_EQ(client.Command({"RENAME", "rename-hash", "rename-list-target"}),
              "+OK");
    EXPECT_EQ(client.Command({"TYPE", "rename-list-target"}), "+hash");
    EXPECT_EQ(client.Command({"HGET", "rename-list-target", "field"}),
              Bulk("value"));
    EXPECT_EQ(client.Command({"SET", "rename-nx-source", "source"}), "+OK");
    EXPECT_EQ(client.Command({"SET", "rename-nx-target", "target"}), "+OK");
    EXPECT_EQ(
        client.Command({"RENAMENX", "rename-nx-source", "rename-nx-target"}),
        ":0");
    EXPECT_EQ(client.Command({"GET", "rename-nx-source"}), Bulk("source"));
    EXPECT_EQ(
        client.Command({"RENAME", "rename-nx-source", "rename-nx-source"}),
        "+OK");
    EXPECT_EQ(
        client.Command({"RENAMENX", "rename-nx-source", "rename-nx-source"}),
        ":0");
    EXPECT_EQ(client.Command({"RENAME", "missing-rename", "destination"}),
              "-ERR no such key");
    EXPECT_EQ(client.Command({"SET", "rename-exec-source", "exec-value"}),
              "+OK");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(
        client.Command({"RENAME", "rename-exec-source", "rename-persisted"}),
        "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*1\r\n+OK");
    EXPECT_EQ(client.Command({"GET", "rename-persisted"}), Bulk("exec-value"));
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"TYPE", "type-hash"}), "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*1\r\n+hash");
    auto scan_type = [&](std::string_view type) {
      std::unordered_set<std::string> found;
      std::string cursor = "0";
      std::size_t calls = 0;
      do {
        auto [next, keys] = ParseScanReply(
            client.Command({"SCAN", cursor, "TYPE", type, "COUNT", "1000"}));
        found.insert(keys.begin(), keys.end());
        cursor = std::move(next);
        EXPECT_LT(++calls, 1000u);
      } while (cursor != "0");
      return found;
    };
    EXPECT_TRUE(scan_type("string").contains("type-string"));
    EXPECT_TRUE(scan_type("list").contains("type-list"));
    EXPECT_TRUE(scan_type("hash").contains("type-hash"));
    EXPECT_TRUE(scan_type("set").contains("type-set"));
    EXPECT_TRUE(scan_type("zset").contains("z"));
    EXPECT_TRUE(scan_type("stream").contains("type-stream"));
    EXPECT_TRUE(scan_type("unknown").empty());
    EXPECT_EQ(client.Command({"ZRANGE", "z", "0", "-1", "WITHSCORES"}),
              "*6\r\n" + Bulk("one") + "\r\n" + Bulk("1") + "\r\n" +
                  Bulk("mid") + "\r\n" + Bulk("1.5") + "\r\n" + Bulk("two") +
                  "\r\n" + Bulk("2"));
    EXPECT_EQ(client.Command({"ZADD", "zmpop-second", "1", "one", "2", "two",
                              "3", "three"}),
              ":3");
    EXPECT_EQ(client.Command({"ZMPOP", "2", "zmpop-empty", "zmpop-second",
                              "MIN", "COUNT", "2"}),
              "*2\r\n" + Bulk("zmpop-second") + "\r\n*2\r\n*2\r\n" +
                  Bulk("one") + "\r\n" + Bulk("1") + "\r\n*2\r\n" +
                  Bulk("two") + "\r\n" + Bulk("2"));
    EXPECT_EQ(client.Command({"BZPOPMAX", "zmpop-second", "1"}),
              "*3\r\n" + Bulk("zmpop-second") + "\r\n" + Bulk("three") +
                  "\r\n" + Bulk("3"));
    const std::string cross_zm_empty = KeyForWorker("cross-zm-empty", 0, 2);
    const std::string cross_zm_ready = KeyForWorker("cross-zm-ready", 1, 2);
    EXPECT_EQ(client.Command({"ZADD", cross_zm_ready, "8", "eight"}), ":1");
    EXPECT_EQ(
        client.Command({"ZMPOP", "2", cross_zm_empty, cross_zm_ready, "MIN"}),
        "*2\r\n" + Bulk(cross_zm_ready) + "\r\n*1\r\n*2\r\n" + Bulk("eight") +
            "\r\n" + Bulk("8"));
    EXPECT_EQ(client.Command({"BZPOPMIN", "zmpop-timeout", "0.01"}), "*-1");
    EXPECT_EQ(client.Command({"ZMPOP", "1", "zmpop-empty", "MIN", "garbage"}),
              "-ERR syntax error");
    EXPECT_EQ(client.Command({"ZMPOP", "01", "zmpop-empty", "MIN"}),
              "-ERR numkeys should be greater than 0");
    EXPECT_EQ(
        client.Command({"ZMPOP", "1", "zmpop-empty", "MIN", "COUNT", "bad"}),
        "-ERR count should be greater than 0");
    EXPECT_EQ(
        client.Command({"ZMPOP", "1", "zmpop-empty", "MIN", "COUNT", "02"}),
        "-ERR count should be greater than 0");

    auto zset_waiting_on_list = std::async(std::launch::async, [port] {
      RespClient blocked(port);
      return blocked.Command({"BZPOPMIN", "cross-type-zset-wait", "0.15"});
    });
    std::this_thread::sleep_for(30ms);
    EXPECT_EQ(client.Command({"RPUSH", "cross-type-zset-wait", "list"}), ":1");
    EXPECT_EQ(zset_waiting_on_list.get(), "*-1");
    EXPECT_EQ(client.Command({"DEL", "cross-type-zset-wait"}), ":1");

    auto list_waiting_on_zset = std::async(std::launch::async, [port] {
      RespClient blocked(port);
      return blocked.Command({"BLPOP", "cross-type-list-wait", "0.15"});
    });
    std::this_thread::sleep_for(30ms);
    EXPECT_EQ(client.Command({"ZADD", "cross-type-list-wait", "1", "zset"}),
              ":1");
    EXPECT_EQ(list_waiting_on_zset.get(), "*-1");
    EXPECT_EQ(client.Command({"DEL", "cross-type-list-wait"}), ":1");

    auto blocked_zpop = std::async(std::launch::async, [port] {
      RespClient blocked(port);
      return blocked.Command({"BZPOPMIN", "blocking-zset", "2"});
    });
    std::this_thread::sleep_for(50ms);
    EXPECT_EQ(client.Command({"ZADD", "blocking-zset", "4", "ready"}), ":1");
    EXPECT_EQ(blocked_zpop.get(), "*3\r\n" + Bulk("blocking-zset") + "\r\n" +
                                      Bulk("ready") + "\r\n" + Bulk("4"));

    auto first_chained_zpop = std::async(std::launch::async, [port] {
      RespClient blocked(port);
      return blocked.Command({"BZPOPMIN", "blocking-zset-chain", "2"});
    });
    std::this_thread::sleep_for(30ms);
    auto second_chained_zpop = std::async(std::launch::async, [port] {
      RespClient blocked(port);
      return blocked.Command({"BZPOPMIN", "blocking-zset-chain", "2"});
    });
    std::this_thread::sleep_for(50ms);
    EXPECT_EQ(client.Command(
                  {"ZADD", "blocking-zset-chain", "1", "first", "2", "second"}),
              ":2");
    EXPECT_EQ(first_chained_zpop.get(), "*3\r\n" + Bulk("blocking-zset-chain") +
                                            "\r\n" + Bulk("first") + "\r\n" +
                                            Bulk("1"));
    EXPECT_EQ(second_chained_zpop.get(),
              "*3\r\n" + Bulk("blocking-zset-chain") + "\r\n" + Bulk("second") +
                  "\r\n" + Bulk("2"));

    auto blocked_zmpop = std::async(std::launch::async, [port] {
      RespClient blocked(port);
      return blocked.Command({"BZMPOP", "2", "2", "blocking-zm-a",
                              "blocking-zm-b", "MAX", "COUNT", "2"});
    });
    std::this_thread::sleep_for(50ms);
    EXPECT_EQ(
        client.Command({"ZADD", "blocking-zm-b", "5", "five", "6", "six"}),
        ":2");
    EXPECT_EQ(blocked_zmpop.get(), "*2\r\n" + Bulk("blocking-zm-b") +
                                       "\r\n*2\r\n*2\r\n" + Bulk("six") +
                                       "\r\n" + Bulk("6") + "\r\n*2\r\n" +
                                       Bulk("five") + "\r\n" + Bulk("5"));

    EXPECT_EQ(client.Command({"ZADD", "exec-zmpop", "7", "seven"}), ":1");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"ZMPOP", "1", "exec-zmpop", "MIN"}), "+QUEUED");
    EXPECT_EQ(client.Command({"BZPOPMIN", "missing-exec-zpop", "100"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*2\r\n*2\r\n" + Bulk("exec-zmpop") +
                                            "\r\n*1\r\n*2\r\n" + Bulk("seven") +
                                            "\r\n" + Bulk("7") + "\r\n*-1");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"ZMPOP", "0", "ignored", "MIN"}), "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}),
              "*1\r\n-ERR numkeys should be greater than 0");
    EXPECT_EQ(client.Command({"ZADD", "bad-timeout-zpop", "1", "member"}),
              ":1");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"BZPOPMIN", "bad-timeout-zpop", "bad"}),
              "+QUEUED");
    EXPECT_TRUE(client.Command({"EXEC"}).starts_with(
        "*1\r\n-ERR timeout is not a float or out of range"));
    EXPECT_EQ(client.Command({"ZCARD", "bad-timeout-zpop"}), ":1");
    EXPECT_EQ(client.Command({"BZPOPMIN", "bad-timeout-zpop", "1e100"}),
              "-ERR timeout is out of range");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"BZPOPMIN", "bad-timeout-zpop", "1e100"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*1\r\n-ERR timeout is out of range");
    EXPECT_EQ(client.Command({"ZCARD", "bad-timeout-zpop"}), ":1");
    EXPECT_EQ(client.Command({"ZADD", "z2", "4", "one", "5", "four"}), ":2");
    EXPECT_EQ(client.Command({"ZINTER", "2", "z", "z2", "WITHSCORES"}),
              "*2\r\n" + Bulk("one") + "\r\n" + Bulk("5"));
    EXPECT_EQ(client.Command({"ZUNIONSTORE", "zout", "2", "z", "z2"}), ":4");
    EXPECT_TRUE(
        client.Command({"ZINTERCARD", "2", "z", "z2", "AGGREGATE", "MAX"})
            .starts_with("-ERR syntax error"));
    EXPECT_EQ(client.Command({"ZINTERCARD", "2", "z", "z2", "LIMIT", "-1"}),
              "-ERR LIMIT can't be negative");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"ZDIFF", "2", "z", "z2"}), "+QUEUED");
    EXPECT_EQ(client.Command({"ZINTER", "2", "z", "z2"}), "+QUEUED");
    EXPECT_EQ(client.Command({"ZUNION", "2", "z", "z2"}), "+QUEUED");
    EXPECT_EQ(client.Command({"ZINTERCARD", "2", "z", "z2"}), "+QUEUED");
    EXPECT_EQ(client.Command({"ZUNIONSTORE", "exec-z", "2", "z", "z2"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}),
              "*5\r\n*2\r\n" + Bulk("mid") + "\r\n" + Bulk("two") +
                  "\r\n*1\r\n" + Bulk("one") + "\r\n*4\r\n" + Bulk("mid") +
                  "\r\n" + Bulk("two") + "\r\n" + Bulk("four") + "\r\n" +
                  Bulk("one") + "\r\n:1\r\n:4");
    EXPECT_EQ(client.Command({"ZCARD", "exec-z"}), ":4");
    EXPECT_EQ(client.Command({"SET", "exec-replace-zset", "string"}), "+OK");
    EXPECT_EQ(client.Command({"PEXPIRE", "exec-replace-zset", "60000"}), ":1");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"ZUNIONSTORE", "exec-replace-zset", "1", "z"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*1\r\n:3");
    EXPECT_EQ(client.Command({"PTTL", "exec-replace-zset"}), ":-1");
    EXPECT_TRUE(client.Command({"ZADD", "malformed-score", "+-5", "member"})
                    .starts_with("-ERR value is not a valid float"));
    EXPECT_EQ(client.Command({"ZADD", "z", "NX", "XX", "1", "member"}),
              "-ERR XX and NX options at the same time are not compatible");
    EXPECT_EQ(client.Command({"ZADD", "z", "GT", "LT", "1", "member"}),
              "-ERR GT, LT, and/or NX options at the same time are not "
              "compatible");
    EXPECT_EQ(client.Command({"ZADD", "z", "INCR", "1", "one", "2", "two"}),
              "-ERR INCR option supports a single increment-element pair");
    EXPECT_TRUE(client.Command({"ZINCRBY", "malformed-score", "+-5", "member"})
                    .starts_with("-ERR value is not a valid float"));
    EXPECT_TRUE(client.Command({"ZRANGEBYSCORE", "z", "+-5", "10"})
                    .starts_with("-ERR min or max is not a float"));
    EXPECT_EQ(client.Command({"RPUSH", "zset-syntax-wrongtype", "value"}),
              ":1");
    EXPECT_TRUE(
        client.Command({"ZRANGEBYSCORE", "zset-syntax-wrongtype", "+-5", "10"})
            .starts_with("-ERR min or max is not a float"));
    EXPECT_TRUE(
        client.Command({"ZSCAN", "zset-syntax-wrongtype", "not-a-cursor"})
            .starts_with("-ERR invalid cursor"));
    EXPECT_EQ(client.Command({"ZADD", "decimal-z", "1.1", "one", "2.3", "two"}),
              ":2");
    EXPECT_EQ(client.Command({"ZSCORE", "decimal-z", "one"}), Bulk("1.1"));
    EXPECT_EQ(client.Command({"ZSCORE", "decimal-z", "two"}), Bulk("2.3"));
    EXPECT_EQ(client.Command({"ZRANGEBYSCORE", "decimal-z", "0", "10",
                              "WITHSCORES", "LIMIT", "0", "1"}),
              "*2\r\n" + Bulk("one") + "\r\n" + Bulk("1.1"));
    EXPECT_EQ(client.Command({"ZREVRANGEBYSCORE", "decimal-z", "10", "0",
                              "WITHSCORES", "LIMIT", "0", "1"}),
              "*2\r\n" + Bulk("two") + "\r\n" + Bulk("2.3"));
    EXPECT_EQ(client.Command({"ZADD", "decimal-z", "1e10", "integer", "0.1",
                              "tenth", "4611686018427387904", "boundary",
                              "5e18", "outside", "-0", "negative-zero"}),
              ":5");
    EXPECT_EQ(client.Command({"ZSCORE", "decimal-z", "integer"}),
              Bulk("10000000000"));
    EXPECT_EQ(client.Command({"ZSCORE", "decimal-z", "tenth"}), Bulk("0.1"));
    EXPECT_EQ(client.Command({"ZSCORE", "decimal-z", "boundary"}),
              Bulk("4611686018427387904"));
    EXPECT_EQ(client.Command({"ZSCORE", "decimal-z", "outside"}),
              Bulk("5e+18"));
    EXPECT_EQ(client.Command({"ZSCORE", "decimal-z", "negative-zero"}),
              Bulk("-0"));
    EXPECT_EQ(client.Command({"ZADD", "decimal-z", "0.0001", "fixed-four",
                              "0.00003", "fixed-five", "1e-7", "small-exp"}),
              ":3");
    EXPECT_EQ(client.Command({"ZSCORE", "decimal-z", "fixed-four"}),
              Bulk("0.0001"));
    EXPECT_EQ(client.Command({"ZSCORE", "decimal-z", "fixed-five"}),
              Bulk("0.00003"));
    EXPECT_EQ(client.Command({"ZSCORE", "decimal-z", "small-exp"}),
              Bulk("1e-7"));
    EXPECT_EQ(client.Command({"ZRANK", "decimal-z", "missing", "WITHSCORE"}),
              "*-1");
    EXPECT_EQ(
        client.Command({"ZRANK", "decimal-z", "one", "WITHSCORE", "extra"}),
        "-ERR syntax error");
    EXPECT_EQ(
        client.Command({"ZREVRANK", "decimal-z", "one", "WITHSCORE", "extra"}),
        "-ERR syntax error");
    EXPECT_EQ(client.Command(
                  {"ZREVRANGE", "decimal-z", "0", "-1", "WITHSCORES", "extra"}),
              "-ERR syntax error");
    EXPECT_TRUE(client.Command({"ZRANK", "missing-z", "member", "bad"})
                    .starts_with("-ERR syntax error"));
    EXPECT_EQ(client.Command({"ZADD", "z-infinity", "inf", "last", "-inf",
                              "first", "+1", "plus"}),
              ":3");
    EXPECT_EQ(client.Command({"ZRANGE", "z-infinity", "0", "-1", "WITHSCORES"}),
              "*6\r\n" + Bulk("first") + "\r\n" + Bulk("-inf") + "\r\n" +
                  Bulk("plus") + "\r\n" + Bulk("1") + "\r\n" + Bulk("last") +
                  "\r\n" + Bulk("inf"));
    EXPECT_EQ(client.Command({"ZINCRBY", "z-infinity", "+1", "plus"}),
              Bulk("2"));
    EXPECT_EQ(client.Command({"ZINCRBY", "z-infinity", "+inf", "plus"}),
              Bulk("inf"));
    EXPECT_EQ(client.Command({"ZADD", "positive-infinity", "inf", "same"}),
              ":1");
    EXPECT_EQ(client.Command({"ZADD", "negative-infinity", "-inf", "same"}),
              ":1");
    EXPECT_EQ(client.Command({"ZUNIONSTORE", "nan-union", "2",
                              "positive-infinity", "negative-infinity"}),
              ":1");
    EXPECT_EQ(client.Command({"ZSCORE", "nan-union", "same"}), Bulk("0"));
    EXPECT_EQ(
        client.Command({"ZUNION", "2", "positive-infinity", "positive-infinity",
                        "WEIGHTS", "0.5", "0", "WITHSCORES"}),
        "*2\r\n" + Bulk("same") + "\r\n" + Bulk("inf"));
    EXPECT_EQ(client.Command({"ZUNION", "2", "positive-infinity",
                              "positive-infinity", "WEIGHTS", "0.5", "0",
                              "AGGREGATE", "MIN", "WITHSCORES"}),
              "*2\r\n" + Bulk("same") + "\r\n" + Bulk("0"));
    EXPECT_EQ(
        client.Command({"ZINTER", "2", "positive-infinity", "positive-infinity",
                        "WEIGHTS", "0.5", "0", "WITHSCORES"}),
        "*2\r\n" + Bulk("same") + "\r\n" + Bulk("0"));
    EXPECT_EQ(client.Command({"ZINTER", "2", "positive-infinity",
                              "positive-infinity", "WEIGHTS", "0.5", "0",
                              "AGGREGATE", "MIN", "WITHSCORES"}),
              "*2\r\n" + Bulk("same") + "\r\n" + Bulk("inf"));
    EXPECT_EQ(client.Command({"SADD", "aggregate-set", "set-only", "same"}),
              ":2");
    EXPECT_EQ(
        client.Command({"ZUNION", "2", "positive-infinity", "aggregate-set"}),
        "*2\r\n" + Bulk("set-only") + "\r\n" + Bulk("same"));
    EXPECT_EQ(client.Command({"ZADD", "pop-overflow", "1", "member"}), ":1");
    EXPECT_TRUE(
        client.Command({"ZPOPMIN", "pop-overflow", "9223372036854775808"})
            .starts_with("-ERR value is out of range"));
    EXPECT_EQ(client.Command({"ZCARD", "pop-overflow"}), ":1");
    EXPECT_EQ(client.Command({"ZRANGEBYLEX", "z", "-", "+", "WITHSCORES"}),
              "-ERR syntax error, WITHSCORES not supported in combination "
              "with BYLEX");
    EXPECT_EQ(client.Command({"ZRANGE", "z", "0", "-1", "LIMIT", "0", "1"}),
              "-ERR syntax error, LIMIT is only supported in combination "
              "with either BYSCORE or BYLEX");
    EXPECT_EQ(client.Command(
                  {"ZRANGE", "z", "0", "10", "BYSCORE", "LIMIT", "bad", "1"}),
              "-ERR value is not an integer or out of range");
    EXPECT_EQ(
        client.Command({"ZRANGEBYSCORE", "z", "0", "10", "LIMIT", "0", "bad"}),
        "-ERR value is not an integer or out of range");
    EXPECT_EQ(client.Command({"ZADD", "empty-member-zset", "1", ""}), ":1");
    auto [empty_zset_cursor, empty_zset_scan] = ParseScanReply(
        client.Command({"ZSCAN", "empty-member-zset", "0", "COUNT", "100"}));
    EXPECT_EQ(empty_zset_cursor, "0");
    EXPECT_EQ(empty_zset_scan, (std::vector<std::string>{"", "1"}));
    EXPECT_EQ(client.Command({"SET", "replace-zset", "string"}), "+OK");
    EXPECT_EQ(client.Command({"PEXPIRE", "replace-zset", "60000"}), ":1");
    EXPECT_EQ(client.Command({"ZUNIONSTORE", "replace-zset", "1", "z"}), ":3");
    EXPECT_EQ(client.Command({"PTTL", "replace-zset"}), ":-1");
    EXPECT_EQ(client.Command({"ZRANDMEMBER", "z", "-9223372036854775808"}),
              "-ERR value is not an integer or out of range");
    EXPECT_EQ(client.Command(
                  {"ZRANDMEMBER", "z", "9223372036854775807", "WITHSCORES"}),
              "-ERR value is not an integer or out of range");
    EXPECT_EQ(client.Command({"ZRANDMEMBER", "missing-zset", "invalid"}),
              "-ERR value is not an integer or out of range");
    EXPECT_EQ(
        client.Command({"ZRANDMEMBER", "missing-zset", "-9223372036854775807"}),
        "*0");
    CloseStreamingReplyAfterHeader(port,
                                   {"ZRANDMEMBER", "z", "-9223372036854775807"},
                                   "*9223372036854775807");
    CloseStreamingReplyAfterHeader(
        port, {"ZRANDMEMBER", "z", "-3000000", "WITHSCORES"}, "*6000000");
    EXPECT_TRUE(WaitForReply(client, {"ZCARD", "z"}, ":3"));
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"ZRANDMEMBER", "z", "-2001", "WITHSCORES"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"SET", "after-zrand-oom", "alive"}), "+QUEUED");
    const std::string zset_exec_random = client.Command({"EXEC"});
    EXPECT_TRUE(zset_exec_random.starts_with("*2\r\n*4002\r\n"));
    EXPECT_TRUE(zset_exec_random.ends_with("+OK"));
    EXPECT_EQ(client.Command({"GET", "after-zrand-oom"}), Bulk("alive"));
    EXPECT_EQ(client.Command({"RPUSH", "sintercard-wrongtype", "value"}), ":1");
    EXPECT_TRUE(
        client
            .Command({"SINTERCARD", "1", "sintercard-wrongtype", "LIMIT", "-1"})
            .starts_with("-ERR LIMIT can't be negative"));
    EXPECT_EQ(client.Command({"SADD", "sintercard-repeat", "a", "b", "c"}),
              ":3");
    EXPECT_EQ(client.Command({"SINTERCARD", "1", "sintercard-repeat", "LIMIT",
                              "1", "LIMIT", "2"}),
              ":2");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"SINTERCARD", "1", "sintercard-repeat", "LIMIT",
                              "1", "LIMIT", "2"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*1\r\n:2");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"BLPOP", "never-created", ""}), "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}),
              "*1\r\n-ERR timeout is not a float or out of range");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"BLPOP", "never-created", " 0"}), "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}),
              "*1\r\n-ERR timeout is not a float or out of range");

    EXPECT_EQ(client.Command({"GEOADD", "geo", "13.361389", "38.115556",
                              "Palermo", "15.087269", "37.502669", "Catania"}),
              ":2");
    EXPECT_EQ(client.Command({"GEODIST", "geo", "Palermo", "Catania", "km"}),
              Bulk("166.2742"));
    EXPECT_EQ(client.Command({"GEOPOS", "geo", "Palermo"}),
              "*1\r\n*2\r\n" + Bulk("13.36138933897018433") + "\r\n" +
                  Bulk("38.11555639549629859"));
    EXPECT_EQ(client.Command(
                  {"GEORADIUS", "geo", "15", "37", "200", "km", "COUNT", "1"}),
              "*1\r\n" + Bulk("Catania"));
    EXPECT_TRUE(client.Command({"GEORADIUS", "geo", "200", "100", "10", "km"})
                    .starts_with("-ERR invalid longitude,latitude pair"));
    EXPECT_TRUE(client
                    .Command({"GEORADIUS", "zset-syntax-wrongtype", "200",
                              "100", "10", "km"})
                    .starts_with("-ERR invalid longitude,latitude pair"));
    EXPECT_TRUE(client
                    .Command({"GEOSEARCH", "geo", "FROMLONLAT", "200", "100",
                              "BYRADIUS", "10", "km"})
                    .starts_with("-ERR invalid longitude,latitude pair"));
    EXPECT_EQ(
        client.Command({"GEORADIUSBYMEMBER", "geo", "missing", "10", "km"}),
        "-ERR could not decode requested zset member");
    EXPECT_EQ(client.Command(
                  {"GEORADIUSBYMEMBER", "missing-geo", "member", "10", "km"}),
              "*0");
    EXPECT_EQ(client.Command({"GEOPOS", "geo", "missing"}), "*1\r\n*-1");
    EXPECT_EQ(client.Command({"GEOHASH", "geo", "Palermo", "Catania"}),
              "*2\r\n" + Bulk("sqc8b49rny0") + "\r\n" + Bulk("sqdtr74hyu0"));
    EXPECT_EQ(client.Command({"ZADD", "invalid-geo-score", "-1", "member"}),
              ":1");
    EXPECT_TRUE(client.Command({"GEOPOS", "invalid-geo-score", "member"})
                    .starts_with("*1\r\n*2\r\n"));
    EXPECT_TRUE(client.Command({"GEOHASH", "invalid-geo-score", "member"})
                    .starts_with("*1\r\n$"));
    EXPECT_EQ(client.Command({"GEOPOS", "geo"}), "*0");
    EXPECT_EQ(client.Command({"GEOHASH", "geo"}), "*0");
    EXPECT_TRUE(client.Command({"GEOADD", "bad-geo", "+-100", "+-50", "m"})
                    .starts_with("-ERR invalid longitude"));
    EXPECT_EQ(client.Command({"GEOADD", "geo-empty", "13", "38", ""}), ":1");
    EXPECT_EQ(client.Command({"GEOSEARCH", "geo-empty", "FROMMEMBER", "",
                              "BYRADIUS", "1", "km"}),
              "*1\r\n" + Bulk(""));
    EXPECT_EQ(
        client
            .Command({"GEOSEARCH", "geo", "WITHCOORD", "COUNT", "1", "ANY",
                      "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km"})
            .substr(0, 8),
        "*1\r\n*2\r\n");
    EXPECT_EQ(client.Command({"GEOADD", "geo-dateline", "179.9", "0", "east",
                              "-179.9", "0", "west"}),
              ":2");
    const std::string dateline =
        client.Command({"GEOSEARCH", "geo-dateline", "FROMLONLAT", "179.95",
                        "0", "BYBOX", "40", "10", "km"});
    EXPECT_NE(dateline.find(Bulk("east")), std::string::npos) << dateline;
    EXPECT_NE(dateline.find(Bulk("west")), std::string::npos) << dateline;

    EXPECT_EQ(client.Command({"ZRANGESTORE", "range-store", "z", "1", "2"}),
              ":2");
    EXPECT_EQ(
        client.Command({"ZRANGE", "range-store", "0", "-1", "WITHSCORES"}),
        "*4\r\n" + Bulk("mid") + "\r\n" + Bulk("1.5") + "\r\n" + Bulk("two") +
            "\r\n" + Bulk("2"));
    EXPECT_TRUE(client
                    .Command({"ZRANGESTORE", "wrong-range-store",
                              "aggregate-set", "0", "-1"})
                    .starts_with("-WRONGTYPE"));
    EXPECT_EQ(
        client.Command({"GEOSEARCHSTORE", "geo-store", "geo", "WITHCOORD",
                        "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km"}),
        "-ERR syntax error");
    EXPECT_EQ(
        client.Command({"GEOSEARCHSTORE", "geo-store", "geo", "STOREDIST",
                        "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km"}),
        ":2");
    EXPECT_EQ(client.Command({"ZCARD", "geo-store"}), ":2");
    EXPECT_EQ(client.Command({"GEORADIUS", "geo", "15", "37", "200", "km",
                              "STORE", "radius-store"}),
              ":2");
    EXPECT_EQ(client.Command({"ZCARD", "radius-store"}), ":2");
    EXPECT_TRUE(client
                    .Command({"GEORADIUS_RO", "geo", "15", "37", "200", "km",
                              "STORE", "forbidden"})
                    .starts_with("-ERR syntax error"));
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(
        client.Command({"ZRANGESTORE", "exec-range-store", "z", "0", "0"}),
        "+QUEUED");
    EXPECT_EQ(
        client.Command({"GEOSEARCHSTORE", "exec-geo-store", "geo", "FROMMEMBER",
                        "Palermo", "BYRADIUS", "200", "km"}),
        "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*2\r\n:1\r\n:2");

    EXPECT_EQ(client.Command({"SET", "", "empty-key-value"}), "+OK");
    bool saw_empty_key = false;
    std::string scan_cursor = "0";
    std::size_t scan_calls = 0;
    do {
      auto [next, keys] = ParseScanReply(
          client.Command({"SCAN", scan_cursor, "MATCH", "*", "COUNT", "100"}));
      saw_empty_key = saw_empty_key ||
                      std::find(keys.begin(), keys.end(), "") != keys.end();
      scan_cursor = std::move(next);
      ASSERT_LT(++scan_calls, 1000u);
    } while (scan_cursor != "0");
    EXPECT_TRUE(saw_empty_key);

    EXPECT_EQ(client.Command({"XADD", "bare-ms", "1", "f", "v"}), Bulk("1-0"));
    EXPECT_TRUE(client.Command({"XADD", "bare-ms", "1", "f", "v2"})
                    .starts_with("-ERR The ID specified in XADD"));
    EXPECT_EQ(client.Command({"XADD", "bare-ms", "2", "f", "v2"}), Bulk("2-0"));
    EXPECT_TRUE(client.Command({"XADD", "zero-ms", "0", "f", "v"})
                    .starts_with("-ERR The ID specified in XADD"));
    EXPECT_TRUE(
        client
            .Command({"XADD", "missing-nomk", "NOMKSTREAM", "bad-id", "f", "v"})
            .starts_with("-ERR Invalid stream ID"));
    EXPECT_TRUE(client.Command({"XTRIM", "bare-ms", "MAXLEN", "1", "junk"})
                    .starts_with("-ERR syntax error"));
    EXPECT_EQ(
        client.Command({"XTRIM", "bare-ms", "MAXLEN", "~", "1", "LIMIT", "1"}),
        ":1");
    EXPECT_EQ(client.Command({"XADD", "xadd-limit", "1-0", "f", "1"}),
              Bulk("1-0"));
    EXPECT_EQ(client.Command({"XADD", "xadd-limit", "2-0", "f", "2"}),
              Bulk("2-0"));
    EXPECT_EQ(client.Command({"XADD", "xadd-limit", "3-0", "f", "3"}),
              Bulk("3-0"));
    EXPECT_EQ(client.Command({"XADD", "xadd-limit", "MAXLEN", "~", "1", "LIMIT",
                              "1", "4-0", "f", "4"}),
              Bulk("4-0"));
    EXPECT_EQ(client.Command({"XLEN", "xadd-limit"}), ":3");
    EXPECT_TRUE(client
                    .Command({"XADD", "xadd-limit", "MAXLEN", "=", "1", "LIMIT",
                              "1", "5-0", "f", "5"})
                    .starts_with("-ERR syntax error"));
    EXPECT_TRUE(
        client
            .Command({"XADD", "xadd-limit", "MAXLEN", "1", "MINID", "2-0",
                      "6-0", "f", "6"})
            .starts_with("-ERR syntax error, MAXLEN and MINID options at the "
                         "same time are not compatible"));
    EXPECT_EQ(client.Command({"XGROUP", "CREATE", "xgroup-options", "g", "0",
                              "MKSTREAM", "ENTRIESREAD", "0"}),
              "+OK");
    EXPECT_TRUE(client.Command({"XGROUP", "HELP"})
                    .starts_with("*17\r\n+XGROUP <subcommand>"));
    EXPECT_TRUE(client.Command({"XINFO", "HELP"})
                    .starts_with("*9\r\n+XINFO <subcommand>"));
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"XGROUP", "HELP"}), "+QUEUED");
    EXPECT_EQ(client.Command({"XINFO", "HELP"}), "+QUEUED");
    const std::string exec_stream_help = client.Command({"EXEC"});
    EXPECT_TRUE(
        exec_stream_help.starts_with("*2\r\n*17\r\n+XGROUP <subcommand>"))
        << exec_stream_help;
    EXPECT_NE(exec_stream_help.find("\r\n*9\r\n+XINFO <subcommand>"),
              std::string::npos)
        << exec_stream_help;
    EXPECT_EQ(client.Command({"XINFO", "FOO"}),
              "-ERR unknown subcommand or wrong number of arguments for "
              "'FOO'. Try XINFO HELP.");
    EXPECT_EQ(client.Command({"PING"}), "+PONG");
    EXPECT_TRUE(client.Command({"XGROUP", "DESTROY", "xgroup-options"})
                    .starts_with("-ERR unknown subcommand or wrong number"));
    EXPECT_TRUE(client.Command({"XGROUP", "SETID", "xgroup-options"})
                    .starts_with("-ERR unknown subcommand or wrong number"));
    EXPECT_EQ(client.Command({"XLEN", "xgroup-options"}), ":0");
    EXPECT_EQ(
        client.Command({"XGROUP", "DESTROY", "missing-stream", "g"}),
        "-ERR The XGROUP subcommand requires the key to exist. Note that for "
        "CREATE you may want to use the MKSTREAM option to create an empty "
        "stream automatically.");
    EXPECT_EQ(client.Command({"XPENDING", "xgroup-options", "g"}),
              "*4\r\n:0\r\n$-1\r\n$-1\r\n*-1");
    const std::string empty_group_info =
        client.Command({"XINFO", "GROUPS", "xgroup-options"});
    EXPECT_NE(empty_group_info.find(Bulk("lag") + "\r\n:0"), std::string::npos)
        << empty_group_info;
    EXPECT_EQ(client.Command({"XACK", "xgroup-options", "missing", "1-0"}),
              ":0");

    EXPECT_EQ(client.Command({"XADD", "stream", "1-0", "f", "v"}), Bulk("1-0"));
    EXPECT_EQ(client.Command({"XADD", "stream", "2-0", "f2", "v2"}),
              Bulk("2-0"));
    EXPECT_EQ(client.Command({"XREAD", "STREAMS", "stream", "$"}), "*-1");
    EXPECT_EQ(
        client.Command({"XREAD", "BLOCK", "10", "STREAMS", "stream", "$"}),
        "*-1");
    EXPECT_TRUE(
        client.Command({"XREAD", "COUNT", "-1", "STREAMS", "stream", "0-0"})
            .starts_with("*1\r\n"));
    EXPECT_EQ(
        client.Command({"XREAD", "BLOCK", "-1", "STREAMS", "stream", "$"}),
        "-ERR timeout is negative");
    EXPECT_EQ(client.Command({"XREAD", "BLOCK", "18446744073709551615",
                              "STREAMS", "stream", "$"}),
              "-ERR timeout is not an integer or out of range");
    EXPECT_TRUE(client.Command({"XSETID", "stream", "1-1"})
                    .starts_with("-ERR The ID specified in XSETID"));
    EXPECT_TRUE(client.Command({"XADD", "stream", "1-2", "bad", "order"})
                    .starts_with("-ERR The ID specified in XADD"));
    EXPECT_EQ(client.Command({"XRANGE", "stream", "-", "+"}),
              "*2\r\n*2\r\n" + Bulk("1-0") + "\r\n*2\r\n" + Bulk("f") + "\r\n" +
                  Bulk("v") + "\r\n*2\r\n" + Bulk("2-0") + "\r\n*2\r\n" +
                  Bulk("f2") + "\r\n" + Bulk("v2"));
    EXPECT_EQ(client.Command({"XRANGE", "stream", "-", "+", "COUNT", "0"}),
              "*-1");
    EXPECT_EQ(client.Command({"XRANGE", "stream", "-", "+", "COUNT", "-1"}),
              "*-1");
    EXPECT_TRUE(client
                    .Command({"XSETID", "stream", "2-0", "ENTRIESADDED",
                              "9223372036854775808"})
                    .starts_with("-ERR value is not an integer"));
    EXPECT_TRUE(
        client.Command({"XSETID", "stream", "2-0", "ENTRIESADDED", "1"})
            .starts_with("-ERR The entries_added specified in XSETID is "
                         "smaller than the target stream length"));
    EXPECT_TRUE(
        client.Command({"XSETID", "stream", "2-0", "MAXDELETEDID", "3-0"})
            .starts_with("-ERR The ID specified in XSETID is smaller than the "
                         "provided max_deleted_entry_id"));
    EXPECT_EQ(client.Command({"XADD", "xsetid-deleted", "5-0", "f", "v"}),
              Bulk("5-0"));
    EXPECT_EQ(client.Command({"XDEL", "xsetid-deleted", "5-0"}), ":1");
    EXPECT_TRUE(
        client.Command({"XSETID", "xsetid-deleted", "4-0"})
            .starts_with("-ERR The ID specified in XSETID is smaller than "
                         "current max_deleted_entry_id"));
    EXPECT_EQ(
        client.Command({"XADD", "overflow-stream",
                        "18446744073709551615-18446744073709551615", "f", "v"}),
        Bulk("18446744073709551615-18446744073709551615"));
    EXPECT_TRUE(client.Command({"XADD", "overflow-stream", "*", "f", "v"})
                    .starts_with("-ERR The stream has exhausted"));
    EXPECT_EQ(client.Command({"XGROUP", "CREATE", "stream", "g", "0-0"}),
              "+OK");
    EXPECT_TRUE(client
                    .Command({"XREADGROUP", "GROUP", "g", "consumer", "COUNT",
                              "1", "STREAMS", "stream", ">"})
                    .starts_with("*1\r\n"));
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(
        client.Command({"XREAD", "COUNT", "1", "STREAMS", "stream", "0-0"}),
        "+QUEUED");
    EXPECT_EQ(client.Command({"XREADGROUP", "GROUP", "g", "exec-consumer",
                              "COUNT", "1", "STREAMS", "stream", ">"}),
              "+QUEUED");
    const std::string stream_exec = client.Command({"EXEC"});
    EXPECT_TRUE(stream_exec.starts_with("*2\r\n*1\r\n"));
    EXPECT_EQ(stream_exec.find("command is not allowed in transactions"),
              std::string::npos);
    EXPECT_EQ(client.Command({"XADD", "stream", "3-0", "f3", "v3"}),
              Bulk("3-0"));
    EXPECT_TRUE(
        client.Command({"XREAD", "COUNT", "0", "STREAMS", "stream", "0-0"})
            .starts_with("*1\r\n"));
    EXPECT_EQ(client.Command(
                  {"XCLAIM", "stream", "g", "other", "0", "1-0", "JUSTID"}),
              "*1\r\n" + Bulk("1-0"));
    EXPECT_TRUE(
        client.Command({"XCLAIM", "stream", "g", "other", "0", "JUSTID"})
            .starts_with("-ERR syntax error"));
    EXPECT_EQ(client.Command(
                  {"XAUTOCLAIM", "stream", "g", "third", "0", "0-0", "JUSTID"}),
              "*3\r\n" + Bulk("0-0") + "\r\n*2\r\n" + Bulk("1-0") + "\r\n" +
                  Bulk("2-0") + "\r\n*0");
    EXPECT_FALSE(client
                     .Command({"XAUTOCLAIM", "stream", "g", "special-minus",
                               "0", "-", "JUSTID"})
                     .starts_with("-ERR"));
    EXPECT_FALSE(client
                     .Command({"XAUTOCLAIM", "stream", "g", "special-plus", "0",
                               "+", "JUSTID"})
                     .starts_with("-ERR"));
    EXPECT_FALSE(client
                     .Command({"XAUTOCLAIM", "stream", "g", "special-exclusive",
                               "0", "(0-0", "JUSTID"})
                     .starts_with("-ERR"));
    EXPECT_EQ(client.Command({"XREADGROUP", "GROUP", "g", "nobody", "STREAMS",
                              "stream", "0-0"}),
              "*1\r\n*2\r\n" + Bulk("stream") + "\r\n*0");
    EXPECT_EQ(client.Command({"XPENDING", "stream", "g", "-", "+", "0"}), "*0");
    EXPECT_EQ(client.Command({"XPENDING", "stream", "g", "-", "+", "-1"}),
              "*0");
    EXPECT_EQ(client.Command({"XAUTOCLAIM", "stream", "g", "nobody", "0", "0-0",
                              "COUNT", "0"}),
              "-ERR COUNT must be > 0");
    EXPECT_EQ(client.Command({"XCLAIM", "stream", "g", "forced", "0", "3-0",
                              "FORCE", "JUSTID"}),
              "*1\r\n" + Bulk("3-0"));
    EXPECT_EQ(client.Command({"XDEL", "stream", "1-0"}), ":1");
    EXPECT_EQ(client.Command({"XAUTOCLAIM", "stream", "g", "cleanup", "0",
                              "0-0", "COUNT", "10", "JUSTID"}),
              "*3\r\n" + Bulk("0-0") + "\r\n*2\r\n" + Bulk("2-0") + "\r\n" +
                  Bulk("3-0") + "\r\n*1\r\n" + Bulk("1-0"));
    EXPECT_EQ(client.Command({"XPENDING", "stream", "g"}).substr(0, 4),
              "*4\r\n");

    EXPECT_EQ(client.Command({"XADD", "force-count", "1-0", "f", "v"}),
              Bulk("1-0"));
    EXPECT_EQ(
        client.Command({"XGROUP", "CREATE", "force-count", "force-group", "$"}),
        "+OK");
    EXPECT_TRUE(client
                    .Command({"XCLAIM", "force-count", "force-group", "forced",
                              "0", "1-0", "FORCE"})
                    .starts_with("*1\r\n"));
    EXPECT_TRUE(client
                    .Command({"XPENDING", "force-count", "force-group", "1-0",
                              "1-0", "1"})
                    .ends_with("\r\n:2"));

    EXPECT_EQ(client.Command({"XADD", "autoclean", "1-0", "f", "v"}),
              Bulk("1-0"));
    EXPECT_EQ(
        client.Command({"XGROUP", "CREATE", "autoclean", "auto-group", "0"}),
        "+OK");
    EXPECT_TRUE(client
                    .Command({"XREADGROUP", "GROUP", "auto-group", "old",
                              "STREAMS", "autoclean", ">"})
                    .starts_with("*1\r\n"));
    EXPECT_EQ(client.Command({"XDEL", "autoclean", "1-0"}), ":1");
    EXPECT_EQ(client.Command({"XAUTOCLAIM", "autoclean", "auto-group", "new",
                              "999999999", "0-0", "COUNT", "10", "JUSTID"}),
              "*3\r\n" + Bulk("0-0") + "\r\n*0\r\n*1\r\n" + Bulk("1-0"));
    EXPECT_TRUE(client.Command({"XPENDING", "autoclean", "auto-group"})
                    .starts_with("*4\r\n:0"));

    EXPECT_EQ(client.Command({"XADD", "auto-cursor", "1-0", "f", "1"}),
              Bulk("1-0"));
    EXPECT_EQ(client.Command({"XADD", "auto-cursor", "2-0", "f", "2"}),
              Bulk("2-0"));
    EXPECT_EQ(client.Command(
                  {"XGROUP", "CREATE", "auto-cursor", "cursor-group", "0"}),
              "+OK");
    EXPECT_TRUE(client
                    .Command({"XREADGROUP", "GROUP", "cursor-group", "old",
                              "COUNT", "2", "STREAMS", "auto-cursor", ">"})
                    .starts_with("*1\r\n"));
    EXPECT_EQ(client.Command({"XAUTOCLAIM", "auto-cursor", "cursor-group",
                              "new", "0", "0-0", "COUNT", "1", "JUSTID"}),
              "*3\r\n" + Bulk("2-0") + "\r\n*1\r\n" + Bulk("1-0") + "\r\n*0");
    EXPECT_EQ(client.Command({"XAUTOCLAIM", "auto-cursor", "cursor-group",
                              "new", "0", "2-0", "COUNT", "1", "JUSTID"}),
              "*3\r\n" + Bulk("0-0") + "\r\n*1\r\n" + Bulk("2-0") + "\r\n*0");

    EXPECT_EQ(client.Command({"XADD", "lag-tombstone", "1-0", "f", "1"}),
              Bulk("1-0"));
    EXPECT_EQ(client.Command({"XADD", "lag-tombstone", "2-0", "f", "2"}),
              Bulk("2-0"));
    EXPECT_EQ(client.Command({"XADD", "lag-tombstone", "3-0", "f", "3"}),
              Bulk("3-0"));
    EXPECT_EQ(
        client.Command({"XGROUP", "CREATE", "lag-tombstone", "lag-group", "0"}),
        "+OK");
    EXPECT_EQ(client.Command({"XDEL", "lag-tombstone", "2-0"}), ":1");
    EXPECT_TRUE(client
                    .Command({"XREADGROUP", "GROUP", "lag-group", "reader",
                              "COUNT", "1", "STREAMS", "lag-tombstone", ">"})
                    .starts_with("*1\r\n"));
    const std::string fragmented_lag =
        client.Command({"XINFO", "GROUPS", "lag-tombstone"});
    EXPECT_NE(fragmented_lag.find(Bulk("lag") + "\r\n$-1"), std::string::npos)
        << fragmented_lag;
    EXPECT_TRUE(client
                    .Command({"XREADGROUP", "GROUP", "lag-group", "reader",
                              "COUNT", "1", "STREAMS", "lag-tombstone", ">"})
                    .starts_with("*1\r\n"));
    const std::string complete_lag =
        client.Command({"XINFO", "GROUPS", "lag-tombstone"});
    EXPECT_NE(complete_lag.find(Bulk("lag") + "\r\n:0"), std::string::npos)
        << complete_lag;

    EXPECT_EQ(client.Command({"XADD", "history-deleted", "1-0", "f", "v"}),
              Bulk("1-0"));
    EXPECT_EQ(
        client.Command({"XGROUP", "CREATE", "history-deleted", "history", "0"}),
        "+OK");
    EXPECT_TRUE(client
                    .Command({"XREADGROUP", "GROUP", "history", "reader",
                              "STREAMS", "history-deleted", ">"})
                    .starts_with("*1\r\n"));
    EXPECT_EQ(client.Command({"XDEL", "history-deleted", "1-0"}), ":1");
    const std::string deleted_history =
        "*1\r\n*2\r\n" + Bulk("history-deleted") + "\r\n*1\r\n*2\r\n" +
        Bulk("1-0") + "\r\n*-1";
    EXPECT_EQ(client.Command({"XREADGROUP", "GROUP", "history", "reader",
                              "STREAMS", "history-deleted", "0"}),
              deleted_history);
    const std::string deleted_pending = client.Command(
        {"XPENDING", "history-deleted", "history", "-", "+", "1"});
    EXPECT_TRUE(deleted_pending.ends_with("\r\n:1")) << deleted_pending;

    EXPECT_EQ(client.Command({"XADD", "history-empty", "1-0", "f", "empty"}),
              Bulk("1-0"));
    EXPECT_EQ(client.Command({"XADD", "history-data", "1-0", "f", "data"}),
              Bulk("1-0"));
    EXPECT_EQ(
        client.Command({"XGROUP", "CREATE", "history-empty", "mixed", "$"}),
        "+OK");
    EXPECT_EQ(
        client.Command({"XGROUP", "CREATE", "history-data", "mixed", "0"}),
        "+OK");
    EXPECT_TRUE(client
                    .Command({"XREADGROUP", "GROUP", "mixed", "reader",
                              "STREAMS", "history-data", ">"})
                    .starts_with("*1\r\n"));
    const std::string mixed_history =
        "*2\r\n*2\r\n" + Bulk("history-empty") + "\r\n*0\r\n*2\r\n" +
        Bulk("history-data") + "\r\n*1\r\n*2\r\n" + Bulk("1-0") + "\r\n*2\r\n" +
        Bulk("f") + "\r\n" + Bulk("data");
    EXPECT_EQ(
        client.Command({"XREADGROUP", "GROUP", "mixed", "reader", "STREAMS",
                        "history-empty", "history-data", "0", "0"}),
        mixed_history);
    const auto mixed_started = std::chrono::steady_clock::now();
    EXPECT_EQ(client.Command({"XREADGROUP", "GROUP", "mixed", "reader", "BLOCK",
                              "1000", "STREAMS", "history-empty",
                              "history-data", "0", ">"}),
              "*1\r\n*2\r\n" + Bulk("history-empty") + "\r\n*0");
    EXPECT_LT(std::chrono::steady_clock::now() - mixed_started, 500ms);

    EXPECT_EQ(client.Command({"XADD", "claim-order", "1-0", "f", "1"}),
              Bulk("1-0"));
    EXPECT_EQ(client.Command({"XADD", "claim-order", "2-0", "f", "2"}),
              Bulk("2-0"));
    EXPECT_EQ(client.Command({"XADD", "claim-order", "3-0", "f", "3"}),
              Bulk("3-0"));
    EXPECT_EQ(
        client.Command({"XGROUP", "CREATE", "claim-order", "ordered", "0"}),
        "+OK");
    EXPECT_TRUE(client
                    .Command({"XREADGROUP", "GROUP", "ordered", "first",
                              "COUNT", "3", "STREAMS", "claim-order", ">"})
                    .starts_with("*1\r\n"));
    EXPECT_EQ(client.Command({"XACK", "claim-order", "ordered", "2-0"}), ":1");
    EXPECT_EQ(client.Command({"XCLAIM", "claim-order", "ordered", "second", "0",
                              "2-0", "IDLE", "50000", "RETRYCOUNT", "7",
                              "LASTID", "9-0", "FORCE", "JUSTID"}),
              "*1\r\n" + Bulk("2-0"));
    const std::string ordered_summary =
        client.Command({"XPENDING", "claim-order", "ordered"});
    EXPECT_TRUE(ordered_summary.starts_with("*4\r\n:3\r\n" + Bulk("1-0") +
                                            "\r\n" + Bulk("3-0")))
        << ordered_summary;
    EXPECT_TRUE(
        client
            .Command({"XPENDING", "claim-order", "ordered", "(1-0", "+", "10"})
            .starts_with("*2\r\n"));
    const std::string forced_pending = client.Command(
        {"XPENDING", "claim-order", "ordered", "2-0", "2-0", "1"});
    ASSERT_TRUE(forced_pending.ends_with("\r\n:7")) << forced_pending;
    const std::size_t count_separator = forced_pending.rfind("\r\n:7");
    ASSERT_NE(count_separator, std::string::npos);
    const std::size_t idle_separator =
        forced_pending.rfind("\r\n:", count_separator - 1);
    ASSERT_NE(idle_separator, std::string::npos);
    const std::string idle = forced_pending.substr(
        idle_separator + 3, count_separator - idle_separator - 3);
    EXPECT_GE(std::stoull(idle), 49000u);
    EXPECT_TRUE(client
                    .Command({"XPENDING", "claim-order", "ordered", "IDLE",
                              "49000", "-", "+", "10"})
                    .starts_with("*1\r\n"));
    EXPECT_EQ(client.Command({"XPENDING", "claim-order", "ordered", "IDLE",
                              "100000", "-", "+", "10"}),
              "*0");
    EXPECT_EQ(
        client.Command({"XCLAIM", "claim-order", "ordered", "huge-idle", "0",
                        "1-0", "IDLE", "18446744073709551615", "JUSTID"}),
        "*1\r\n" + Bulk("1-0"));
    const std::string huge_idle_pending = client.Command(
        {"XPENDING", "claim-order", "ordered", "1-0", "1-0", "1"});
    const std::size_t huge_idle_count = huge_idle_pending.rfind("\r\n:");
    ASSERT_NE(huge_idle_count, std::string::npos);
    const std::size_t huge_idle_field =
        huge_idle_pending.rfind("\r\n:", huge_idle_count - 1);
    ASSERT_NE(huge_idle_field, std::string::npos);
    EXPECT_LT(std::stoull(huge_idle_pending.substr(
                  huge_idle_field + 3, huge_idle_count - huge_idle_field - 3)),
              1000u);
    EXPECT_TRUE(client
                    .Command({"XAUTOCLAIM", "claim-order", "ordered",
                              "exclusive", "0", "(1-0", "COUNT", "1", "JUSTID"})
                    .starts_with("*3\r\n"));
    EXPECT_TRUE(client.Command({"XINFO", "GROUPS", "missing-stream"})
                    .starts_with("-ERR no such key"));
    const std::string group_info =
        client.Command({"XINFO", "GROUPS", "claim-order"});
    EXPECT_NE(group_info.find(Bulk("last-delivered-id") + "\r\n" + Bulk("9-0")),
              std::string::npos);
    const std::string consumer_info =
        client.Command({"XINFO", "CONSUMERS", "claim-order", "ordered"});
    EXPECT_NE(consumer_info.find("*8\r\n"), std::string::npos) << consumer_info;
    const std::string full_info = client.Command(
        {"XINFO", "STREAM", "claim-order", "FULL", "COUNT", "1"});
    EXPECT_TRUE(full_info.starts_with("*18\r\n")) << full_info;
    EXPECT_NE(full_info.find(Bulk("entries")), std::string::npos);
    EXPECT_NE(full_info.find(Bulk("pending")), std::string::npos);
    for (int i = 1; i <= 12; ++i) {
      EXPECT_EQ(client.Command({"XADD", "full-count-stream",
                                std::to_string(i) + "-0", "f", "v"}),
                Bulk(std::to_string(i) + "-0"));
    }
    const std::string negative_full_count = client.Command(
        {"XINFO", "STREAM", "full-count-stream", "FULL", "COUNT", "-1"});
    EXPECT_NE(negative_full_count.find(Bulk("entries") + "\r\n*10\r\n"),
              std::string::npos)
        << negative_full_count;
    const std::string unlimited_full_count = client.Command(
        {"XINFO", "STREAM", "full-count-stream", "FULL", "COUNT", "0"});
    EXPECT_NE(unlimited_full_count.find(Bulk("entries") + "\r\n*12\r\n"),
              std::string::npos)
        << unlimited_full_count;
    EXPECT_TRUE(
        client.Command({"XINFO", "STREAM", "claim-order", "FULL", "garbage"})
            .starts_with("-ERR syntax error"));
    EXPECT_TRUE(
        client
            .Command({"XINFO", "CONSUMERS", "missing-stream", "missing-group"})
            .starts_with("-ERR no such key"));

    EXPECT_EQ(
        client.Command({"XADD", "empty-consumer-stream", "1-0", "f", "v"}),
        Bulk("1-0"));
    EXPECT_EQ(client.Command({"XGROUP", "CREATE", "empty-consumer-stream",
                              "empty-group", "$"}),
              "+OK");
    EXPECT_EQ(
        client.Command({"XREADGROUP", "GROUP", "empty-group", "read-empty",
                        "STREAMS", "empty-consumer-stream", ">"}),
        "*-1");
    EXPECT_NE(client
                  .Command({"XINFO", "CONSUMERS", "empty-consumer-stream",
                            "empty-group"})
                  .find(Bulk("read-empty")),
              std::string::npos);
    EXPECT_TRUE(client
                    .Command({"XAUTOCLAIM", "empty-consumer-stream",
                              "empty-group", "claim-empty", "0", "0-0"})
                    .starts_with("*3\r\n"));
    EXPECT_NE(client
                  .Command({"XINFO", "CONSUMERS", "empty-consumer-stream",
                            "empty-group"})
                  .find(Bulk("claim-empty")),
              std::string::npos);

    EXPECT_EQ(client.Command({"XADD", "rewind-pel", "1-0", "f", "1"}),
              Bulk("1-0"));
    EXPECT_EQ(client.Command({"XADD", "rewind-pel", "2-0", "f", "2"}),
              Bulk("2-0"));
    EXPECT_EQ(client.Command({"XGROUP", "CREATE", "rewind-pel", "g", "0"}),
              "+OK");
    EXPECT_TRUE(client
                    .Command({"XREADGROUP", "GROUP", "g", "first", "STREAMS",
                              "rewind-pel", ">"})
                    .starts_with("*1\r\n"));
    EXPECT_EQ(client.Command({"XGROUP", "SETID", "rewind-pel", "g", "0"}),
              "+OK");
    EXPECT_TRUE(client
                    .Command({"XREADGROUP", "GROUP", "g", "second", "STREAMS",
                              "rewind-pel", ">"})
                    .starts_with("*1\r\n"));
    const std::string rewind_pending =
        client.Command({"XPENDING", "rewind-pel", "g", "1-0", "1-0", "1"});
    EXPECT_TRUE(rewind_pending.ends_with("\r\n:1")) << rewind_pending;
    EXPECT_TRUE(client.Command({"XPENDING", "rewind-pel", "g"})
                    .starts_with("*4\r\n:2\r\n"));
    EXPECT_EQ(client.Command({"XACK", "rewind-pel", "g", "1-0"}), ":1");
    EXPECT_TRUE(client.Command({"XPENDING", "rewind-pel", "g"})
                    .starts_with("*4\r\n:1\r\n"));

    EXPECT_EQ(client.Command({"HSET", "watched-same-hash", "field", "value"}),
              ":1");
    EXPECT_EQ(client.Command({"WATCH", "watched-same-hash"}), "+OK");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"HGET", "watched-same-hash", "field"}),
              "+QUEUED");
    {
      RespClient concurrent(port);
      EXPECT_EQ(
          concurrent.Command({"HSET", "watched-same-hash", "field", "value"}),
          ":0");
    }
    EXPECT_EQ(client.Command({"EXEC"}), "*-1");

    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"SINTERCARD", "5", "z", "z2"}), "+QUEUED");
    const std::string invalid_exec = client.Command({"EXEC"});
    EXPECT_TRUE(invalid_exec.starts_with(
        "*1\r\n-ERR Number of keys can't be greater than number of args"))
        << invalid_exec;
    ASSERT_TRUE(WaitForDurability(client));
    server.Stop();
  }
  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 3);
    RespClient client(port);
    EXPECT_EQ(client.Command({"ZCARD", "zout"}), ":4");
    EXPECT_EQ(client.Command({"GET", "rename-persisted"}), Bulk("exec-value"));
    EXPECT_EQ(client.Command({"XLEN", "stream"}), ":2");
    EXPECT_TRUE(
        client.Command({"XINFO", "GROUPS", "stream"}).starts_with("*1\r\n"));
    server.Stop();
  }
}

TEST(CollectionE2eTest, StringCommandsRecover) {
  ASSERT_FALSE(g_keylane_binary.empty());
  const std::string prefix =
      "/tmp/keylane-string-commands-" + std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::posix_fallocate(fd, 0, 128ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(fd), 0);

  const std::uint16_t port = FindFreePort();
  const std::string cross_a = KeyForWorker("string-cross-a", 0, 2);
  const std::string cross_b = KeyForWorker("string-cross-b", 1, 2);
  const std::string bitmap_a = KeyForWorker("bitmap-cross-a", 0, 2);
  const std::string bitmap_b = KeyForWorker("bitmap-cross-b", 1, 2);
  const std::string bitmap_destination =
      KeyForWorker("bitmap-cross-destination", 1, 2);
  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 2);
    RespClient client(port);

    EXPECT_EQ(client.Command({"APPEND", "append", "abc"}), ":3");
    EXPECT_EQ(client.Command({"PEXPIRE", "append", "60000"}), ":1");
    EXPECT_EQ(client.Command({"APPEND", "append", "def"}), ":6");
    EXPECT_EQ(client.Command({"GET", "append"}), Bulk("abcdef"));
    EXPECT_GT(std::stoll(client.Command({"PTTL", "append"}).substr(1)), 0);
    EXPECT_EQ(client.Command({"GETRANGE", "append", "1", "-2"}), Bulk("bcde"));
    EXPECT_EQ(client.Command({"SUBSTR", "append", "-3", "-1"}), Bulk("def"));
    EXPECT_EQ(client.Command({"GETRANGE", "missing", "0", "-1"}), Bulk(""));

    EXPECT_EQ(client.Command({"SETRANGE", "range", "3", "x"}), ":4");
    const std::string zero_padded("\0\0\0x", 4);
    EXPECT_EQ(client.Command({"GET", "range"}), Bulk(zero_padded));
    EXPECT_EQ(client.Command({"SETRANGE", "range", "1", "yz"}), ":4");
    const std::string overwritten("\0yzx", 4);
    EXPECT_EQ(client.Command({"GET", "range"}), Bulk(overwritten));
    EXPECT_EQ(client.Command({"SETRANGE", "range", "2", ""}), ":4");
    EXPECT_EQ(client.Command({"SETRANGE", "range", "-1", "x"}),
              "-ERR offset is out of range");

    EXPECT_EQ(client.Command({"SETNX", "nx", "first"}), ":1");
    EXPECT_EQ(client.Command({"SETNX", "nx", "second"}), ":0");
    EXPECT_EQ(client.Command({"GET", "nx"}), Bulk("first"));
    EXPECT_EQ(client.Command({"SETEX", "seconds", "30", "value"}), "+OK");
    EXPECT_EQ(client.Command({"PSETEX", "millis", "30000", "value"}), "+OK");
    EXPECT_GT(std::stoll(client.Command({"PTTL", "seconds"}).substr(1)), 0);
    EXPECT_GT(std::stoll(client.Command({"PTTL", "millis"}).substr(1)), 0);
    EXPECT_EQ(client.Command({"SETEX", "bad-expire", "0", "value"}),
              "-ERR invalid expire time in 'setex' command");

    EXPECT_EQ(client.Command({"SET", "swap", "old", "PX", "60000"}), "+OK");
    EXPECT_EQ(client.Command({"GETSET", "swap", "new"}), Bulk("old"));
    EXPECT_EQ(client.Command({"GET", "swap"}), Bulk("new"));
    EXPECT_EQ(client.Command({"PTTL", "swap"}), ":-1");
    EXPECT_EQ(client.Command({"GETEX", "swap", "EX", "30"}), Bulk("new"));
    EXPECT_GT(std::stoll(client.Command({"PTTL", "swap"}).substr(1)), 0);
    EXPECT_EQ(client.Command({"GETEX", "swap", "PERSIST"}), Bulk("new"));
    EXPECT_EQ(client.Command({"PTTL", "swap"}), ":-1");
    EXPECT_EQ(client.Command({"GETDEL", "swap"}), Bulk("new"));
    EXPECT_EQ(client.Command({"GETDEL", "swap"}), "$-1");
    EXPECT_EQ(client.Command({"GETEX", "missing", "PX", "not-an-integer"}),
              "$-1");
    EXPECT_EQ(client.Command({"GETEX", "missing", "UNKNOWN"}),
              "-ERR syntax error");
    EXPECT_EQ(client.Command({"GETEX", "missing", "EX", "1", "PERSIST"}),
              "-ERR syntax error");
    EXPECT_EQ(client.Command({"SET", "past-expiry", "old"}), "+OK");
    EXPECT_EQ(client.Command({"GETEX", "past-expiry", "PXAT", "1"}),
              Bulk("old"));
    EXPECT_EQ(client.Command({"GET", "past-expiry"}), "$-1");
    EXPECT_EQ(client.Command({"RPUSH", "getset-list", "x"}), ":1");
    EXPECT_EQ(
        client.Command({"GETSET", "getset-list", "replacement"}),
        "-WRONGTYPE Operation against a key holding the wrong kind of value");
    EXPECT_EQ(client.Command({"SETNX", "getset-list", "replacement"}), ":0");

    EXPECT_EQ(client.Command({"DECR", "integer"}), ":-1");
    EXPECT_EQ(client.Command({"INCRBY", "integer", "11"}), ":10");
    EXPECT_EQ(client.Command({"DECRBY", "integer", "3"}), ":7");
    EXPECT_EQ(client.Command({"INCRBY", "integer", "+5"}),
              "-ERR value is not an integer or out of range");
    EXPECT_EQ(client.Command({"INCRBY", "integer", "01"}),
              "-ERR value is not an integer or out of range");
    EXPECT_EQ(client.Command({"INCRBY", "integer", "-0"}),
              "-ERR value is not an integer or out of range");
    EXPECT_EQ(client.Command({"SET", "noncanonical-integer", "01"}), "+OK");
    EXPECT_EQ(client.Command({"DECR", "noncanonical-integer"}),
              "-ERR value is not an integer or out of range");
    EXPECT_EQ(client.Command({"INCR", "noncanonical-integer"}),
              "-ERR value is not an integer or out of range");
    EXPECT_EQ(client.Command({"INCRBYFLOAT", "floating", "0.1"}), Bulk("0.1"));
    EXPECT_EQ(client.Command({"INCRBYFLOAT", "floating", "1.25"}),
              Bulk("1.35"));
    EXPECT_EQ(client.Command({"SET", "overflow", "9223372036854775807"}),
              "+OK");
    EXPECT_EQ(client.Command({"INCRBY", "overflow", "1"}),
              "-ERR increment or decrement would overflow");
    EXPECT_EQ(client.Command({"INCRBYFLOAT", "floating", "inf"}),
              "-ERR increment would produce NaN or Infinity");

    EXPECT_EQ(client.Command({"MSETNX", cross_a, "one", cross_b, "two"}), ":1");
    EXPECT_EQ(
        client.Command({"MSETNX", cross_a, "changed", "new-msetnx", "new"}),
        ":0");
    EXPECT_EQ(client.Command({"GET", cross_a}), Bulk("one"));
    EXPECT_EQ(client.Command({"GET", "new-msetnx"}), "$-1");
    EXPECT_EQ(client.Command({"MSETNX", "duplicate-msetnx", "first",
                              "duplicate-msetnx", "last"}),
              ":1");
    EXPECT_EQ(client.Command({"GET", "duplicate-msetnx"}), Bulk("last"));

    EXPECT_EQ(client.Command({"SET", cross_a, "abcXYZ"}), "+OK");
    EXPECT_EQ(client.Command({"SET", cross_b, "123XYZ"}), "+OK");
    EXPECT_EQ(client.Command({"LCS", cross_a, cross_b}), Bulk("XYZ"));
    EXPECT_EQ(client.Command({"LCS", cross_a, cross_b, "LEN"}), ":3");
    EXPECT_EQ(client.Command({"LCS", cross_a, cross_b, "IDX", "MINMATCHLEN",
                              "2", "WITHMATCHLEN"}),
              "*4\r\n" + Bulk("matches") +
                  "\r\n*1\r\n*3\r\n*2\r\n:3\r\n:5\r\n*2\r\n:3\r\n:5\r\n:3\r\n" +
                  Bulk("len") + "\r\n:3");
    EXPECT_EQ(client.Command({"LCS", cross_a, cross_a}), Bulk("abcXYZ"));
    EXPECT_EQ(client.Command({"LCS", "missing-a", "missing-b"}), Bulk(""));
    EXPECT_EQ(client.Command({"RPUSH", "not-string", "x"}), ":1");
    EXPECT_EQ(client.Command({"LCS", "not-string", cross_b}),
              "-ERR The specified keys must contain string values");

    EXPECT_EQ(client.Command({"GETBIT", "missing-bitmap", "123"}), ":0");
    EXPECT_EQ(client.Command({"SETBIT", "bitmap", "0", "1"}), ":0");
    EXPECT_EQ(client.Command({"SETBIT", "bitmap", "9", "1"}), ":0");
    EXPECT_EQ(client.Command({"GETBIT", "bitmap", "0"}), ":1");
    EXPECT_EQ(client.Command({"GETBIT", "bitmap", "1"}), ":0");
    EXPECT_EQ(client.Command({"GET", "bitmap"}),
              Bulk(std::string("\x80\x40", 2)));
    EXPECT_EQ(client.Command({"PEXPIRE", "bitmap", "60000"}), ":1");
    EXPECT_EQ(client.Command({"SETBIT", "bitmap", "1", "1"}), ":0");
    EXPECT_GT(std::stoll(client.Command({"PTTL", "bitmap"}).substr(1)), 0);
    EXPECT_EQ(client.Command({"BITCOUNT", "bitmap"}), ":3");
    EXPECT_EQ(client.Command({"BITCOUNT", "bitmap", "-1", "-1"}), ":1");
    EXPECT_EQ(client.Command({"BITCOUNT", "bitmap", "0", "8", "BIT"}), ":2");
    EXPECT_EQ(client.Command({"BITCOUNT", "bitmap", "0", "1", "BYTE", "extra"}),
              "-ERR syntax error");
    EXPECT_EQ(client.Command({"BITPOS", "bitmap", "1"}), ":0");
    EXPECT_EQ(client.Command({"BITPOS", "bitmap", "0"}), ":2");
    EXPECT_EQ(client.Command({"BITPOS", "bitmap", "1", "1", "1"}), ":9");
    EXPECT_EQ(client.Command({"BITPOS", "bitmap", "1", "2", "7", "BIT"}),
              ":-1");
    EXPECT_EQ(
        client.Command({"BITPOS", "bitmap", "1", "0", "1", "BIT", "extra"}),
        "-ERR syntax error");
    EXPECT_EQ(client.Command({"BITPOS", "missing-bitmap", "0", "bad"}), ":0");
    EXPECT_EQ(client.Command({"SET", "empty-bitmap", ""}), "+OK");
    EXPECT_EQ(client.Command({"BITPOS", "empty-bitmap", "0"}), ":-1");
    EXPECT_EQ(client.Command({"GETBIT", "bitmap", "-1"}),
              "-ERR bit offset is not an integer or out of range");
    EXPECT_EQ(client.Command({"SETBIT", "bitmap", "2", "2"}),
              "-ERR bit is not an integer or out of range");

    EXPECT_EQ(
        client.Command({"BITFIELD", "field", "SET", "u4", "0", "15", "OVERFLOW",
                        "FAIL", "INCRBY", "u4", "0", "1", "GET", "u4", "0"}),
        "*3\r\n:0\r\n$-1\r\n:15");
    EXPECT_EQ(client.Command({"BITFIELD", "field", "OVERFLOW", "SAT", "INCRBY",
                              "u4", "0", "1"}),
              "*1\r\n:15");
    EXPECT_EQ(client.Command({"BITFIELD", "field", "SET", "i12", "4", "-2",
                              "GET", "i12", "4"}),
              "*2\r\n:0\r\n:-2");
    EXPECT_EQ(client.Command({"BITFIELD", "field", "SET", "u8", "#2", "42",
                              "GET", "u8", "16"}),
              "*2\r\n:0\r\n:42");
    EXPECT_EQ(client.Command({"BITFIELD_RO", "field", "GET", "u8", "#2"}),
              "*1\r\n:42");
    EXPECT_EQ(client.Command({"BITFIELD_RO", "field", "SET", "u8", "0", "1"}),
              "-ERR BITFIELD_RO only supports the GET subcommand");
    EXPECT_EQ(client.Command({"BITFIELD", "field", "GET", "u64", "0"}),
              "-ERR Invalid bitfield type. Use something like i16 u8. Note "
              "that u64 is not supported but i64 is.");
    EXPECT_EQ(
        client.Command({"GETBIT", "not-string", "0"}),
        "-WRONGTYPE Operation against a key holding the wrong kind of value");

    EXPECT_EQ(client.Command({"SET", bitmap_a, std::string("\x0f\xf0", 2)}),
              "+OK");
    EXPECT_EQ(client.Command({"SET", bitmap_b, std::string("\x33\x55", 2)}),
              "+OK");
    EXPECT_EQ(client.Command(
                  {"BITOP", "XOR", bitmap_destination, bitmap_a, bitmap_b}),
              ":2");
    EXPECT_EQ(client.Command({"GET", bitmap_destination}),
              Bulk(std::string("\x3c\xa5", 2)));
    EXPECT_EQ(client.Command({"BITOP", "AND", "bitmap-and-missing", bitmap_a,
                              "bitmap-missing-source"}),
              ":2");
    EXPECT_EQ(client.Command({"GET", "bitmap-and-missing"}),
              Bulk(std::string("\0\0", 2)));
    EXPECT_EQ(client.Command({"PEXPIRE", bitmap_destination, "60000"}), ":1");
    EXPECT_EQ(client.Command({"BITOP", "OR", bitmap_destination, bitmap_a}),
              ":2");
    EXPECT_EQ(client.Command({"PTTL", bitmap_destination}), ":-1");
    EXPECT_EQ(
        client.Command({"SET", "bitmap-in-place", std::string("\x0f", 1)}),
        "+OK");
    EXPECT_EQ(
        client.Command({"BITOP", "NOT", "bitmap-in-place", "bitmap-in-place"}),
        ":1");
    EXPECT_EQ(client.Command({"GET", "bitmap-in-place"}),
              Bulk(std::string("\xf0", 1)));
    EXPECT_EQ(client.Command({"SET", "bitmap-empty-destination", "old"}),
              "+OK");
    EXPECT_EQ(client.Command({"BITOP", "OR", "bitmap-empty-destination",
                              "bitmap-missing-source"}),
              ":0");
    EXPECT_EQ(client.Command({"GET", "bitmap-empty-destination"}), "$-1");
    EXPECT_EQ(client.Command({"RPUSH", "bitmap-list-destination", "old"}),
              ":1");
    EXPECT_EQ(
        client.Command({"BITOP", "OR", "bitmap-list-destination", bitmap_a}),
        ":2");
    EXPECT_EQ(client.Command({"GET", "bitmap-list-destination"}),
              Bulk(std::string("\x0f\xf0", 2)));
    EXPECT_EQ(
        client.Command({"BITOP", "NOT", "bitmap-bad", bitmap_a, bitmap_b}),
        "-ERR BITOP NOT must be called with a single source key.");
    EXPECT_EQ(
        client.Command({"BITOP", "OR", "bitmap-bad", bitmap_a, "not-string"}),
        "-WRONGTYPE Operation against a key holding the wrong kind of value");

    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"APPEND", "exec-string", "a"}), "+QUEUED");
    EXPECT_EQ(client.Command({"INCRBY", "exec-number", "4"}), "+QUEUED");
    EXPECT_EQ(client.Command({"MSETNX", "exec-nx-a", "a", "exec-nx-b", "b"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"LCS", cross_a, cross_b, "LEN"}), "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*4\r\n:1\r\n:4\r\n:1\r\n:3");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"MSETNX", "exec-duplicate", "first",
                              "exec-duplicate", "last"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"LCS", cross_a, cross_a, "LEN"}), "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*2\r\n:1\r\n:6");
    EXPECT_EQ(client.Command({"GET", "exec-duplicate"}), Bulk("last"));
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"SETBIT", "exec-bitmap", "0", "1"}), "+QUEUED");
    EXPECT_EQ(client.Command({"BITFIELD", "exec-bitmap", "INCRBY", "u4", "0",
                              "1", "GET", "u4", "0"}),
              "+QUEUED");
    EXPECT_EQ(
        client.Command({"BITOP", "XOR", "exec-bitop", bitmap_a, bitmap_b}),
        "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*3\r\n:0\r\n*2\r\n:9\r\n:9\r\n:2");
    EXPECT_EQ(client.Command({"GET", "exec-bitop"}),
              Bulk(std::string("\x3c\xa5", 2)));
    EXPECT_EQ(client.Command({"SET", "exec-strict-expire", "safe"}), "+OK");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"EXPIRE", "exec-strict-expire", "00"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"GET", "exec-strict-expire"}), "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}),
              "*2\r\n-ERR value is not an integer or out of range\r\n" +
                  Bulk("safe"));
    ASSERT_TRUE(WaitForDurability(client));
    server.Stop();
  }
  {
    ServerProcess server(g_keylane_binary, port, data_path, log_path, 2);
    RespClient client(port);
    EXPECT_EQ(client.Command({"GET", cross_a}), Bulk("abcXYZ"));
    EXPECT_EQ(client.Command({"GET", cross_b}), Bulk("123XYZ"));
    EXPECT_EQ(client.Command({"GET", "duplicate-msetnx"}), Bulk("last"));
    EXPECT_EQ(client.Command({"GET", "exec-string"}), Bulk("a"));
    EXPECT_EQ(client.Command({"GET", "exec-nx-b"}), Bulk("b"));
    EXPECT_EQ(client.Command({"GET", "bitmap"}),
              Bulk(std::string("\xc0\x40", 2)));
    EXPECT_EQ(client.Command({"BITFIELD_RO", "field", "GET", "u8", "#2"}),
              "*1\r\n:42");
    EXPECT_EQ(client.Command({"GET", bitmap_destination}),
              Bulk(std::string("\x0f\xf0", 2)));
    EXPECT_EQ(client.Command({"GET", "exec-bitop"}),
              Bulk(std::string("\x3c\xa5", 2)));
    EXPECT_GT(std::stoll(client.Command({"PTTL", "append"}).substr(1)), 0);
    server.Stop();
  }
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
