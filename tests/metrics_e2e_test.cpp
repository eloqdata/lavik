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

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <future>
#include <iterator>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "support/test_data_path.h"

namespace {

using namespace std::chrono_literals;

std::string g_lavik_binary;

class FileCleanup {
 public:
  explicit FileCleanup(std::string path, bool preserve_on_failure = false)
      : path_(std::move(path)), preserve_on_failure_(preserve_on_failure) {}
  ~FileCleanup() {
    if (!preserve_on_failure_ || !::testing::Test::HasFailure())
      (void)::unlink(path_.c_str());
  }

 private:
  std::string path_;
  bool preserve_on_failure_ = false;
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

class RespClient {
 public:
  explicit RespClient(std::uint16_t port) {
    const auto deadline = std::chrono::steady_clock::now() + 30s;
    while (std::chrono::steady_clock::now() < deadline) {
      fd_ = ConnectSocket(port);
      if (fd_ >= 0) {
        return;
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
    SendCommand(args);
    std::string reply;
    while (!reply.ends_with("\r\n")) {
      char byte = 0;
      if (::recv(fd_, &byte, 1, 0) != 1) {
        throw std::runtime_error("failed to read RESP reply");
      }
      reply.push_back(byte);
    }
    if (!reply.starts_with('$') || reply == "$-1\r\n") {
      reply.resize(reply.size() - 2);
      return reply;
    }
    const std::size_t payload_size =
        static_cast<std::size_t>(std::stoull(reply.substr(1)));
    std::string payload(payload_size + 2, '\0');
    std::size_t received = 0;
    while (received < payload.size()) {
      const ssize_t bytes =
          ::recv(fd_, payload.data() + received, payload.size() - received, 0);
      if (bytes <= 0) {
        throw std::runtime_error("failed to read RESP bulk payload");
      }
      received += static_cast<std::size_t>(bytes);
    }
    payload.resize(payload_size);
    reply.resize(reply.size() - 2);
    return reply + "\r\n" + payload;
  }

  std::string RawCommand(const std::vector<std::string_view>& args) {
    SendCommand(args);
    return ReadRawReply();
  }

 private:
  void SendCommand(const std::vector<std::string_view>& args) {
    std::string request = "*" + std::to_string(args.size()) + "\r\n";
    for (const std::string_view arg : args) {
      request += "$" + std::to_string(arg.size()) + "\r\n";
      request.append(arg);
      request.append("\r\n");
    }
    SendAll(fd_, request);
  }

  std::string ReadLine() {
    std::string line;
    for (;;) {
      char byte = 0;
      if (::recv(fd_, &byte, 1, 0) != 1) {
        throw std::runtime_error("failed to read RESP line");
      }
      if (byte == '\r') {
        if (::recv(fd_, &byte, 1, 0) != 1 || byte != '\n') {
          throw std::runtime_error("malformed RESP line terminator");
        }
        return line;
      }
      line.push_back(byte);
    }
  }

  std::string ReadRawReply() {
    char prefix = 0;
    if (::recv(fd_, &prefix, 1, 0) != 1) {
      throw std::runtime_error("failed to read RESP type");
    }
    const std::string line = ReadLine();
    std::string result(1, prefix);
    result.append(line);
    result.append("\r\n");
    if (prefix == '*') {
      const long long count = std::stoll(line);
      for (long long index = 0; index < count; ++index) {
        result.append(ReadRawReply());
      }
      return result;
    }
    if (prefix != '$' || line == "-1") return result;
    const std::size_t length = static_cast<std::size_t>(std::stoull(line));
    std::string payload(length + 2, '\0');
    std::size_t received = 0;
    while (received < payload.size()) {
      const ssize_t bytes =
          ::recv(fd_, payload.data() + received, payload.size() - received, 0);
      if (bytes <= 0) {
        throw std::runtime_error("failed to read RESP bulk payload");
      }
      received += static_cast<std::size_t>(bytes);
    }
    if (!payload.ends_with("\r\n")) {
      throw std::runtime_error("malformed RESP bulk terminator");
    }
    result.append(payload);
    return result;
  }

  int fd_ = -1;
};

class ServerProcess {
 public:
  ServerProcess(std::string_view binary, std::uint16_t redis_port,
                std::uint16_t metrics_port, std::string_view data_path,
                std::string_view log_path, unsigned workers = 2) {
    pid_ = ::fork();
    if (pid_ < 0) {
      throw std::runtime_error("fork failed");
    }
    if (pid_ == 0) {
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
          "--logtostderr",
          "--port",
          std::to_string(redis_port),
          "--metrics-port",
          std::to_string(metrics_port),
          "--threads",
          std::to_string(workers),
          "--no-pin-workers",
          "--recv-buffers-per-worker",
          "0",
          "--max-memory",
          "1073741824",
          "--flush-max-ms",
          "20",
          "--data-file",
          std::string(data_path),
      };
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
    FAIL() << "Lavik did not stop";
  }

 private:
  pid_t pid_ = -1;
};

std::string HttpGet(std::uint16_t port, std::string_view target) {
  const auto deadline = std::chrono::steady_clock::now() + 30s;
  int fd = -1;
  while (fd < 0 && std::chrono::steady_clock::now() < deadline) {
    fd = ConnectSocket(port);
    if (fd < 0) {
      std::this_thread::sleep_for(10ms);
    }
  }
  if (fd < 0) {
    throw std::runtime_error("timed out connecting to metrics port");
  }
  SendAll(fd, "GET " + std::string(target) +
                  " HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
  std::string response;
  char buffer[4096];
  while (true) {
    const ssize_t bytes = ::recv(fd, buffer, sizeof(buffer), 0);
    if (bytes < 0 && errno == EINTR) {
      continue;
    }
    if (bytes <= 0) {
      break;
    }
    response.append(buffer, static_cast<std::size_t>(bytes));
  }
  ::close(fd);
  return response;
}

std::uint64_t MetricValue(std::string_view body, std::string_view name) {
  std::size_t begin = 0;
  while (true) {
    begin = body.find(name, begin);
    if (begin == std::string_view::npos) {
      throw std::runtime_error("metric not found: " + std::string(name));
    }
    const bool line_start = begin == 0 || body[begin - 1] == '\n';
    const std::size_t suffix = begin + name.size();
    const bool metric_suffix =
        suffix < body.size() && (body[suffix] == ' ' || body[suffix] == '{');
    if (line_start && metric_suffix) {
      break;
    }
    begin += name.size();
  }
  const std::size_t value_begin = body.find(' ', begin + name.size());
  if (value_begin == std::string_view::npos) {
    throw std::runtime_error("metric value not found: " + std::string(name));
  }
  const std::size_t end = body.find('\n', value_begin);
  return std::stoull(
      std::string(body.substr(value_begin + 1, end - value_begin - 1)));
}

TEST(MetricsE2eTest, ConcurrentInfoAndScrapesSurviveWorkerAllocationReuse) {
  ASSERT_FALSE(g_lavik_binary.empty());
  const std::string prefix = lavik::test::TestDataPath(
      "lavik-metrics-reuse-e2e-" + std::to_string(::getpid()));
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path, true);
  const int fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::posix_fallocate(fd, 0, 256ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(fd), 0);
  const auto redis_port = FindFreePort();
  auto metrics_port = FindFreePort();
  while (metrics_port == redis_port) metrics_port = FindFreePort();
  constexpr unsigned kWorkers = 4, kWriters = 4, kWrites = 64, kRounds = 16;
  ServerProcess server(g_lavik_binary, redis_port, metrics_port, data_path,
                       log_path, kWorkers);
  RespClient control(redis_port);
  ASSERT_EQ(control.Command({"PING"}), "+PONG");
  auto require = [](bool ok, std::string_view message) {
    if (!ok) throw std::runtime_error(std::string(message));
  };

  // Futures propagate a crashed server's socket errors to this test instead
  // of terminating in a std::thread destructor. Declaring the gate after the
  // futures also unblocks already-created tasks if later task creation throws.
  std::vector<std::future<void>> jobs;
  jobs.reserve(kWriters + 3);
  std::promise<void> release;
  const auto start = release.get_future().share();
  for (unsigned writer = 0; writer < kWriters; ++writer) {
    jobs.push_back(std::async(std::launch::async, [&, writer, start] {
      start.get();
      RespClient client(redis_port);
      constexpr std::size_t sizes[]{0,   7,   63,  64,   65,   127,
                                    128, 255, 511, 1023, 2047, 4095};
      for (unsigned i = 0; i < kWrites; ++i) {
        const auto key = "metrics-reuse:" + std::to_string(writer) + ":" +
                         std::to_string(i % 16);
        const auto hash = key + ":hash";
        const std::string value(sizes[(i + writer) % std::size(sizes)],
                                static_cast<char>('a' + i % 26));
        const auto bulk = "$" + std::to_string(value.size()) + "\r\n" + value;
        require(client.Command({"SET", key, value}) == "+OK", "SET failed");
        require(client.Command({"GET", key}) == bulk, "GET lost bytes");
        require(client.Command({"HSET", hash, "field", value}) ==
                    (i < 16 ? ":1" : ":0"),
                "HSET count mismatch");
        require(client.Command({"HGET", hash, "field"}) == bulk,
                "HGET lost bytes");
      }
    }));
  }
  for (unsigned reader = 0; reader < 2; ++reader) {
    jobs.push_back(std::async(std::launch::async, [&, reader, start] {
      start.get();
      constexpr std::string_view sections[]{
          "", "ALL", "STATS", "CLIENTS", "PERSISTENCE", "COMMANDSTATS"};
      constexpr std::string_view headers[]{
          "# Server\r\n",  "# Server\r\n",      "# Stats\r\n",
          "# Clients\r\n", "# Persistence\r\n", "# Commandstats\r\n"};
      for (unsigned round = 0; round < kRounds; ++round) {
        // Reconnect and vary reply sizes so the metrics coroutine shares
        // allocation reuse with connection, command and response frames.
        RespClient client(redis_port);
        const std::string echo(17 + round * 67 + reader, 'e');
        require(client.Command({"ECHO", echo}) ==
                    "$" + std::to_string(echo.size()) + "\r\n" + echo,
                "ECHO failed");
        for (unsigned offset = 0; offset < std::size(sections); ++offset) {
          const auto selected = (offset + round + reader) % std::size(sections);
          const auto info = sections[selected].empty()
                                ? client.Command({"INFO"})
                                : client.Command({"INFO", sections[selected]});
          require(info.find(headers[selected]) != std::string::npos,
                  "INFO response omitted requested section");
        }
      }
    }));
  }
  jobs.push_back(std::async(std::launch::async, [&, start] {
    start.get();
    std::uint64_t previous_commands = 0;
    for (unsigned round = 0; round < kRounds; ++round) {
      const auto response = HttpGet(metrics_port, "/metrics");
      require(response.starts_with("HTTP/1.1 200 OK\r\n"), "scrape failed");
      require(MetricValue(response, "lavik_server_ready") == 1,
              "server stopped being ready");
      const auto commands = MetricValue(response, "lavik_commands_total");
      require(commands >= previous_commands, "command counter went backwards");
      previous_commands = commands;
      for (unsigned worker = 0; worker < kWorkers; ++worker)
        require(
            response.find("lavik_worker_memory_limit_bytes{worker=\"" +
                          std::to_string(worker) + "\"}") != std::string::npos,
            "scrape omitted worker");
    }
  }));
  release.set_value();
  for (auto& job : jobs) {
    try {
      job.get();
    } catch (const std::exception& error) {
      std::ifstream log(log_path);
      ADD_FAILURE() << error.what() << "\nserver log retained at " << log_path
                    << "\n"
                    << std::string(std::istreambuf_iterator<char>(log), {});
    }
  }
  if (::testing::Test::HasFailure()) return;
  EXPECT_EQ(control.Command({"PING"}), "+PONG");
  const auto final_metrics = HttpGet(metrics_port, "/metrics");
  for (const auto* command : {"set", "get", "hset", "hget"})
    EXPECT_EQ(
        MetricValue(final_metrics, "lavik_command_calls_total{command=\"" +
                                       std::string(command) + "\"}"),
        kWriters * kWrites)
        << command;
  EXPECT_GE(MetricValue(final_metrics, "lavik_commands_total"),
            kWriters * kWrites * 4 + 2 * kRounds * 6);
  server.Stop();
}

TEST(MetricsE2eTest, ConfigResetstatClearsCommandCountersOnly) {
  ASSERT_FALSE(g_lavik_binary.empty());
  const std::string prefix = lavik::test::TestDataPath(
      "lavik-resetstat-e2e-" + std::to_string(::getpid()));
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::posix_fallocate(fd, 0, 256ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(fd), 0);

  const std::uint16_t redis_port = FindFreePort();
  std::uint16_t metrics_port = FindFreePort();
  while (metrics_port == redis_port) metrics_port = FindFreePort();
  ServerProcess server(g_lavik_binary, redis_port, metrics_port, data_path,
                       log_path);
  RespClient first(redis_port);
  RespClient second(redis_port);

  EXPECT_EQ(first.Command({"PING"}), "+PONG");
  EXPECT_EQ(second.Command({"ECHO", "before-reset"}), "$12\r\nbefore-reset");
  EXPECT_EQ(first.Command({"SET", "resetstat-key", "value"}), "+OK");
  const std::string before = second.Command({"INFO", "commandstats"});
  EXPECT_NE(before.find("cmdstat_ping:calls=1,"), std::string::npos);
  EXPECT_NE(before.find("cmdstat_echo:calls=1,"), std::string::npos);
  EXPECT_NE(before.find("cmdstat_set:calls=1,"), std::string::npos);

  EXPECT_EQ(first.Command({"CONFIG", "RESETSTAT"}), "+OK");
  const std::string reset = second.Command({"INFO", "commandstats"});
  EXPECT_EQ(reset.find("cmdstat_ping:"), std::string::npos);
  EXPECT_EQ(reset.find("cmdstat_echo:"), std::string::npos);
  EXPECT_EQ(reset.find("cmdstat_set:"), std::string::npos);
  EXPECT_NE(reset.find("cmdstat_config:calls=1,"), std::string::npos);

  // RESETSTAT resets counters, not the dataset or persistence dirty state.
  EXPECT_EQ(first.Command({"GET", "resetstat-key"}), "$5\r\nvalue");
  const std::string persistence = first.Command({"INFO", "persistence"});
  EXPECT_NE(persistence.find("rdb_changes_since_last_save:1\r\n"),
            std::string::npos);
  EXPECT_EQ(first.Command({"CONFIG", "RESETSTAT"}), "+OK");
  const std::string stats = first.Command({"INFO", "stats"});
  EXPECT_NE(stats.find("total_commands_processed:1\r\n"), std::string::npos);
  server.Stop();
}

TEST(MetricsE2eTest, SlowLogRecordsBoundsQueriesAndDisables) {
  ASSERT_FALSE(g_lavik_binary.empty());
  const std::string prefix = lavik::test::TestDataPath(
      "lavik-slowlog-e2e-" + std::to_string(::getpid()));
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::posix_fallocate(fd, 0, 256ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(fd), 0);

  const std::uint16_t redis_port = FindFreePort();
  std::uint16_t metrics_port = FindFreePort();
  while (metrics_port == redis_port) metrics_port = FindFreePort();
  ServerProcess server(g_lavik_binary, redis_port, metrics_port, data_path,
                       log_path);
  RespClient client(redis_port);

  EXPECT_EQ(client.Command({"CONFIG", "SET", "slowlog-log-slower-than", "0"}),
            "+OK");
  EXPECT_EQ(client.Command({"CONFIG", "SET", "slowlog-max-len", "3"}), "+OK");
  const std::string configuration =
      client.RawCommand({"CONFIG", "GET", "slowlog-*"});
  EXPECT_NE(configuration.find("slowlog-log-slower-than"), std::string::npos);
  EXPECT_NE(configuration.find("slowlog-max-len"), std::string::npos);
  EXPECT_EQ(client.Command({"CLIENT", "SETNAME", "slowlog-e2e"}), "+OK");
  EXPECT_EQ(client.Command({"SLOWLOG", "RESET"}), "+OK");
  EXPECT_EQ(client.Command({"PING"}), "+PONG");
  const std::string large_argument(200, 'x');
  EXPECT_EQ(client.Command({"ECHO", large_argument}),
            "$200\r\n" + large_argument);

  const std::string entries = client.RawCommand({"SLOWLOG", "GET", "-1"});
  EXPECT_TRUE(entries.starts_with("*3\r\n")) << entries;
  EXPECT_NE(entries.find("$4\r\nECHO\r\n"), std::string::npos) << entries;
  EXPECT_NE(entries.find("... (72 more bytes)"), std::string::npos) << entries;
  EXPECT_NE(entries.find("$11\r\nslowlog-e2e\r\n"), std::string::npos)
      << entries;
  EXPECT_EQ(client.Command({"SLOWLOG", "LEN"}), ":3");

  EXPECT_EQ(client.Command({"CONFIG", "SET", "slowlog-log-slower-than", "-1"}),
            "+OK");
  EXPECT_EQ(client.Command({"SLOWLOG", "RESET"}), "+OK");
  EXPECT_EQ(client.Command({"PING"}), "+PONG");
  EXPECT_EQ(client.Command({"SLOWLOG", "LEN"}), ":0");
  server.Stop();
}

TEST(MetricsE2eTest, ExposesPrometheusCommandStorageAndDefragMetrics) {
  ASSERT_FALSE(g_lavik_binary.empty());
  const std::string prefix = lavik::test::TestDataPath(
      "lavik-metrics-e2e-" + std::to_string(::getpid()));
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  // Two workers can each hold an active block while a multi-key transaction
  // also needs rollback/commit space. Leave that foreground budget in
  // addition to the fixed per-device defrag reserve.
  ASSERT_EQ(::posix_fallocate(fd, 0, 256ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(fd), 0);

  const std::uint16_t redis_port = FindFreePort();
  std::uint16_t metrics_port = FindFreePort();
  while (metrics_port == redis_port) {
    metrics_port = FindFreePort();
  }
  ServerProcess server(g_lavik_binary, redis_port, metrics_port, data_path,
                       log_path);
  RespClient client(redis_port);
  EXPECT_EQ(client.Command({"PING"}), "+PONG");
  EXPECT_EQ(client.Command({"SET", "metrics-key", "metrics-value"}), "+OK");
  EXPECT_EQ(client.Command({"GET", "metrics-key"}), "$13\r\nmetrics-value");
  EXPECT_EQ(client.Command({"SET", "del-metric-key", "value"}), "+OK");
  EXPECT_EQ(client.Command({"SET", "unlink-metric-key", "value"}), "+OK");
  EXPECT_EQ(client.Command({"DEL", "del-metric-key"}), ":1");
  EXPECT_EQ(client.Command({"UNLINK", "unlink-metric-key"}), ":1");
  EXPECT_EQ(client.Command({"APPEND", "append-metric-key", "abc"}), ":3");
  EXPECT_EQ(client.Command({"DECR", "decr-metric-key"}), ":-1");
  EXPECT_EQ(
      client.Command({"MSETNX", "lcs-metric-a", "abc", "lcs-metric-b", "xbc"}),
      ":1");
  EXPECT_EQ(client.Command({"LCS", "lcs-metric-a", "lcs-metric-b"}),
            "$2\r\nbc");
  EXPECT_EQ(client.Command({"TOUCH", "metrics-key", "missing"}), ":1");
  EXPECT_TRUE(client.Command({"RANDOMKEY"}).starts_with('$'));
  EXPECT_EQ(client.Command({"COPY", "metrics-key", "copy-metric-key"}), ":1");
  EXPECT_EQ(client.Command({"EXPIREAT", "metrics-key", "4102444800"}), ":1");
  EXPECT_EQ(client.Command({"EXPIRETIME", "metrics-key"}), ":4102444800");
  EXPECT_EQ(client.Command({"PEXPIREAT", "copy-metric-key", "4102444800000"}),
            ":1");
  EXPECT_EQ(client.Command({"PEXPIRETIME", "copy-metric-key"}),
            ":4102444800000");
  const std::string memory_info = client.Command({"INFO", "memory"});
  EXPECT_NE(memory_info.find("# Memory\r\n"), std::string::npos);
  EXPECT_NE(memory_info.find("maxmemory:1073741824\r\n"), std::string::npos);
  EXPECT_NE(memory_info.find("maxmemory_policy:noeviction\r\n"),
            std::string::npos);

  // Let the periodic flusher turn the small active block into completed
  // writes and fdatasync barriers before sampling cumulative I/O counters.
  std::this_thread::sleep_for(100ms);

  const std::string response = HttpGet(metrics_port, "/metrics");
  ASSERT_TRUE(response.starts_with("HTTP/1.1 200 OK\r\n")) << response;
  const std::size_t body_offset = response.find("\r\n\r\n");
  ASSERT_NE(body_offset, std::string::npos);
  const std::string_view body(response.data() + body_offset + 4,
                              response.size() - body_offset - 4);
  EXPECT_GE(MetricValue(body, "lavik_commands_total"), 3);
  EXPECT_EQ(MetricValue(body, "lavik_server_ready"), 1);
  const std::uint64_t connections = MetricValue(body, "lavik_connections");
  const std::uint64_t connected_clients =
      MetricValue(body, "lavik_connected_clients");
  EXPECT_GE(connections, 2);
  EXPECT_EQ(connected_clients, 1);
  EXPECT_LE(connected_clients, connections);
  EXPECT_EQ(MetricValue(body, "lavik_blocked_clients"), 0);
  EXPECT_EQ(MetricValue(body, "lavik_replication_control_connections"), 0);
  EXPECT_EQ(MetricValue(body, "lavik_replication_flow_connections"), 0);
  EXPECT_EQ(MetricValue(body, "lavik_cluster_control_connected"), 0);
  EXPECT_EQ(MetricValue(body, "lavik_cluster_control_reconnects_total"), 0);
  EXPECT_EQ(MetricValue(body, "lavik_cluster_control_protocol_errors_total"),
            0);
  EXPECT_EQ(
      MetricValue(body, "lavik_cluster_control_full_states_applied_total"), 0);
  EXPECT_EQ(MetricValue(body,
                        "lavik_cluster_control_lease_decisions_total{"
                        "decision=\"granted\"}"),
            0);
  EXPECT_EQ(
      MetricValue(
          body,
          "lavik_cluster_control_lease_decisions_total{decision=\"denied\"}"),
      0);
  EXPECT_EQ(MetricValue(body, "lavik_cluster_control_lease_expirations_total"),
            0);
  // The lazy shared backlog is enabled on the first downstream handshake.
  EXPECT_EQ(MetricValue(body, "lavik_replication_backlog_capacity_bytes"), 0);
  EXPECT_EQ(MetricValue(body, "lavik_replication_backlog_pinned_cursors"), 0);
  EXPECT_EQ(
      MetricValue(body, "lavik_replication_backlog_backpressure_waits_total"),
      0);
  EXPECT_EQ(MetricValue(body, "lavik_replication_backlog_backpressured"), 0);
  EXPECT_GT(MetricValue(body, "lavik_replication_publish_queue_capacity_bytes"),
            0);
  EXPECT_EQ(MetricValue(body, "lavik_fullsync_publish_queue_bytes"), 0);
  EXPECT_EQ(MetricValue(body, "lavik_fullsync_publish_queue_admitted_bytes"),
            0);
  EXPECT_EQ(MetricValue(body, "lavik_fullsync_publish_queue_capacity_bytes"),
            0);
  EXPECT_EQ(MetricValue(body, "lavik_fullsync_sessions"), 0);
  EXPECT_EQ(MetricValue(
                body, "lavik_fullsync_publish_queue_backpressure_waits_total"),
            0);
  EXPECT_GT(MetricValue(body, "lavik_memory_current_bytes"), 0);
  EXPECT_GT(MetricValue(body, "lavik_memory_rss_bytes"), 0);
  EXPECT_EQ(MetricValue(body, "lavik_memory_max_bytes"), 1073741824);
  EXPECT_EQ(MetricValue(body, "lavik_memory_rejected_commands_total"), 0);
  constexpr std::uint64_t kWorkerRetainedLimit =
      (1073741824ULL - 1073741824ULL / 10) / 2;
  for (unsigned worker = 0; worker < 2; ++worker) {
    const std::string label = "{worker=\"" + std::to_string(worker) + "\"}";
    EXPECT_EQ(MetricValue(body, "lavik_worker_memory_limit_bytes" + label),
              kWorkerRetainedLimit);
    EXPECT_NE(body.find("lavik_worker_retained_memory_bytes" + label + " "),
              std::string_view::npos);
    EXPECT_NE(
        body.find("lavik_worker_memory_admission_pending_bytes" + label + " "),
        std::string_view::npos);
    EXPECT_NE(
        body.find("lavik_worker_fullsync_reserved_memory_bytes" + label + " "),
        std::string_view::npos);
    EXPECT_NE(
        body.find("lavik_worker_client_buffered_request_bytes" + label + " "),
        std::string_view::npos);
  }
  EXPECT_GT(MetricValue(
                body, "lavik_storage_io_operations_total{operation=\"write\"}"),
            0);
  EXPECT_GT(
      MetricValue(body,
                  "lavik_storage_io_operations_total{operation=\"fdatasync\"}"),
      0);
  EXPECT_GT(
      MetricValue(body, "lavik_storage_io_bytes_total{operation=\"write\"}"),
      0);
  EXPECT_GT(MetricValue(
                body, "lavik_storage_io_bytes_total{operation=\"fdatasync\"}"),
            0);
  EXPECT_NE(body.find("lavik_storage_io_operations_total{operation=\"read\"} "),
            std::string_view::npos);
  EXPECT_NE(body.find("lavik_storage_io_bytes_total{operation=\"read\"} "),
            std::string_view::npos);
  EXPECT_GT(MetricValue(body, "lavik_storage_capacity_bytes"), 0);
  EXPECT_GT(MetricValue(body, "lavik_storage_available_bytes"), 0);
  EXPECT_GT(MetricValue(body, "lavik_filesystem_available_bytes"), 0);
  EXPECT_NE(body.find("lavik_command_calls_total{command=\"ping\"} 1"),
            std::string_view::npos);
  EXPECT_NE(body.find("lavik_command_calls_total{command=\"set\"} 3"),
            std::string_view::npos);
  EXPECT_NE(body.find("lavik_command_calls_total{command=\"get\"} 1"),
            std::string_view::npos);
  EXPECT_NE(body.find("lavik_command_calls_total{command=\"del\"} 1"),
            std::string_view::npos);
  EXPECT_NE(body.find("lavik_command_calls_total{command=\"unlink\"} 1"),
            std::string_view::npos);
  EXPECT_NE(body.find("lavik_command_calls_total{command=\"append\"} 1"),
            std::string_view::npos);
  EXPECT_NE(body.find("lavik_command_calls_total{command=\"decr\"} 1"),
            std::string_view::npos);
  EXPECT_NE(body.find("lavik_command_calls_total{command=\"msetnx\"} 1"),
            std::string_view::npos);
  EXPECT_NE(body.find("lavik_command_calls_total{command=\"lcs\"} 1"),
            std::string_view::npos);
  for (const char* command : {"touch", "randomkey", "copy", "expireat",
                              "expiretime", "pexpireat", "pexpiretime"}) {
    EXPECT_NE(body.find("lavik_command_calls_total{command=\"" +
                        std::string(command) + "\"} 1"),
              std::string_view::npos);
  }
  EXPECT_NE(body.find("lavik_command_duration_seconds_bucket{command=\"get\""),
            std::string_view::npos);
  EXPECT_NE(body.find("lavik_command_duration_seconds_bucket{command=\"get\","
                      "le=\"0.003\"}"),
            std::string_view::npos);
  EXPECT_NE(body.find("lavik_storage_defrag_runs_total{result=\"success\"}"),
            std::string_view::npos);
  EXPECT_NE(body.find("lavik_storage_defrag_active "), std::string_view::npos);
  EXPECT_NE(body.find("path=\"" + data_path + "\""), std::string_view::npos);

  const std::string not_found = HttpGet(metrics_port, "/unknown");
  EXPECT_TRUE(not_found.starts_with("HTTP/1.1 404 Not Found\r\n"));
  const std::string reused = HttpGet(metrics_port, "/metrics");
  EXPECT_TRUE(reused.starts_with("HTTP/1.1 200 OK\r\n"));
  server.Stop();
}

}  // namespace

int main(int argc, char** argv) {
  if (argc >= 2) {
    g_lavik_binary = argv[1];
    for (int index = 1; index + 1 < argc; ++index) {
      argv[index] = argv[index + 1];
    }
    --argc;
  }
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
