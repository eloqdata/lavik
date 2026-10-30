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

#include <array>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <future>
#include <iterator>
#include <memory>
#include <sstream>
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

  std::string RawRequest(std::string_view encoded) {
    SendAll(fd_, encoded);
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
  ServerProcess(
      std::string_view binary, std::uint16_t redis_port,
      std::uint16_t metrics_port, std::string_view data_path,
      std::string_view log_path, unsigned workers = 2,
      std::uint64_t max_memory = 1024ULL * 1024 * 1024,
      std::vector<std::string> extra = {},
      const std::vector<std::pair<std::string, std::string>>& env = {}) {
    pid_ = ::fork();
    if (pid_ < 0) {
      throw std::runtime_error("fork failed");
    }
    if (pid_ == 0) {
      for (const auto& [key, value] : env) {
        if (::setenv(key.c_str(), value.c_str(), 1) != 0) _exit(127);
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
          std::to_string(max_memory),
          "--flush-max-ms",
          "20",
          "--data-file",
          std::string(data_path),
      };
      arguments.insert(arguments.end(), extra.begin(), extra.end());
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

double MetricNumber(std::string_view body, std::string_view name) {
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
  return std::stod(
      std::string(body.substr(value_begin + 1, end - value_begin - 1)));
}

std::uint64_t MetricValue(std::string_view body, std::string_view name) {
  return static_cast<std::uint64_t>(MetricNumber(body, name));
}

struct DiagnosticMetric {
  std::string_view suffix;
  std::string_view info;
  bool counter = false;
  double divisor = 1;
};

constexpr std::array<DiagnosticMetric, 20> kDiagnostics{{
    {"tx_backlog_bytes", "tx_backlog_bytes_total"},
    {"tx_backlog_max_bytes", "tx_backlog_bytes_max"},
    {"tx_cleaner_rounds_total", "tx_cleaner_rounds", true},
    {"tx_cleaner_failures_total", "tx_cleaner_failures", true},
    {"tx_cleaner_retired_blocks_total", "tx_cleaner_retired_blocks", true},
    {"tx_cleaner_running", "tx_cleaner_running"},
    {"tx_cleaner_cooldown_seconds", "tx_cleaner_cooldown_ms", false, 1000},
    {"tx_commit_batches_total", "tx_commit_batches", true},
    {"tx_commit_batch_transactions_total", "tx_commit_batch_transactions",
     true},
    {"tx_commit_input_fences_total", "tx_commit_input_fences", true},
    {"tx_commit_merged_fences_total", "tx_commit_merged_fences", true},
    {"tx_commit_backpressure_waits_total", "tx_commit_backpressure_waits",
     true},
    {"tx_commit_queue_depth", "tx_commit_queue_depth"},
    {"tx_commit_queue_peak", "tx_commit_queue_peak"},
    {"tx_commit_queue_high_watermark", "tx_commit_queue_high_watermark"},
    {"dirty_staging_bytes", "storage_dirty_staging_bytes"},
    {"flushes_pending", "storage_flushes_pending"},
    {"tx_commits_pending", "storage_tx_commits_pending"},
    {"durability_pending", "storage_durability_pending"},
    {"expiration_pause_count", "storage_expiration_pause_count"},
}};

void CheckDiagnosticContract(std::string_view metrics, std::string_view info) {
  for (const auto& field : kDiagnostics) {
    const auto name = "lavik_storage_" + std::string(field.suffix);
    SCOPED_TRACE(name);
    EXPECT_NE(metrics.find("# HELP " + name + " "), std::string_view::npos);
    EXPECT_NE(metrics.find("# TYPE " + name +
                           (field.counter ? " counter\n" : " gauge\n")),
              std::string_view::npos);
    EXPECT_EQ(metrics.find("\n" + name + "{"), std::string_view::npos);
    const auto marker = "\r\n" + std::string(field.info) + ":";
    const auto start = info.find(marker);
    ASSERT_NE(start, std::string_view::npos);
    EXPECT_DOUBLE_EQ(
        MetricNumber(metrics, name),
        std::stod(std::string(info.substr(start + marker.size()))) /
            field.divisor);
  }
}

template <typename Predicate>
bool WaitUntil(Predicate ready) {
  const auto deadline = std::chrono::steady_clock::now() + 15s;
  while (std::chrono::steady_clock::now() < deadline) {
    if (ready()) return true;
    std::this_thread::sleep_for(10ms);
  }
  return false;
}

TEST(MetricsE2eTest, CommandStatsSeparateExecutionFromAdmission) {
  ASSERT_FALSE(g_lavik_binary.empty());
  const std::string prefix = lavik::test::TestDataPath(
      "lavik-command-results-" + std::to_string(::getpid()));
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path, true);
  const int fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::posix_fallocate(fd, 0, 256ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(fd), 0);
  const auto port = FindFreePort();
  auto metrics_port = FindFreePort();
  while (metrics_port == port) metrics_port = FindFreePort();
  ServerProcess server(g_lavik_binary, port, metrics_port, data_path, log_path);
  RespClient client(port);
  EXPECT_EQ(client.Command({"CONFIG", "RESETSTAT"}), "+OK");
  EXPECT_TRUE(client.Command({"SET"}).starts_with("-ERR wrong number"));
  EXPECT_EQ(client.Command({"SET", "result-key", "value"}), "+OK");
  const auto info = client.Command({"INFO", "commandstats"});
  const auto start = info.find("cmdstat_set:");
  ASSERT_NE(start, std::string::npos);
  const auto line = info.substr(start, info.find("\r\n", start) - start);
  EXPECT_TRUE(line.starts_with("cmdstat_set:calls=1,")) << line;
  EXPECT_TRUE(line.ends_with(",rejected_calls=1,failed_calls=0")) << line;
  const auto metrics = HttpGet(metrics_port, "/metrics");
  // Existing dispatch metrics deliberately still include the rejected call.
  EXPECT_EQ(MetricValue(metrics, "lavik_command_calls_total{command=\"set\"}"),
            2);
  EXPECT_EQ(
      MetricValue(metrics, "lavik_command_executions_total{command=\"set\"}"),
      1);
  EXPECT_EQ(MetricValue(metrics,
                        "lavik_command_rejected_calls_total{command=\"set\"}"),
            1);
  EXPECT_EQ(MetricValue(metrics, "lavik_errors_total{err=\"ERR\"}"), 1);
  server.Stop();
}

class CommandResultsE2eTest : public ::testing::Test {
 protected:
  void SetUp() override {
    const std::string prefix = lavik::test::TestDataPath(
        "lavik-results-" + std::to_string(::getpid()) + "-" +
        ::testing::UnitTest::GetInstance()->current_test_info()->name());
    data_path_ = prefix + ".data";
    log_path_ = prefix + ".log";
    const int fd =
        ::open(data_path_.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(::posix_fallocate(fd, 0, 256ULL * 1024 * 1024), 0);
    ASSERT_EQ(::close(fd), 0);
    port_ = FindFreePort();
    do {
      metrics_port_ = FindFreePort();
    } while (metrics_port_ == port_);
    server_ = std::make_unique<ServerProcess>(
        g_lavik_binary, port_, metrics_port_, data_path_, log_path_);
    client_ = std::make_unique<RespClient>(port_);
    ASSERT_EQ(client_->Command({"CONFIG", "RESETSTAT"}), "+OK");
  }
  void TearDown() override {
    client_.reset();
    if (server_) server_->Stop();
    server_.reset();
    (void)::unlink(data_path_.c_str());
    if (!HasFailure()) (void)::unlink(log_path_.c_str());
  }
  std::string Stats(std::string_view command) {
    const auto info = client_->Command({"INFO", "commandstats"});
    const auto begin = info.find("cmdstat_" + std::string(command) + ":");
    if (begin == std::string::npos) return {};
    return info.substr(begin, info.find("\r\n", begin) - begin);
  }
  std::uint64_t Counter(std::string_view command, std::string_view field) {
    const auto metrics = HttpGet(metrics_port_, "/metrics");
    const auto name = "lavik_command_" + std::string(field) +
                      "_total{command=\"" + std::string(command) + "\"}";
    // Redis omits never-observed/reset command rows.
    return metrics.find(name) == std::string::npos ? 0
                                                   : MetricValue(metrics, name);
  }
  std::uint64_t Calls(std::string_view command) {
    return Counter(command, "executions");
  }
  std::uint64_t Failed(std::string_view command) {
    return Counter(command, "failed_calls");
  }
  std::uint64_t Rejected(std::string_view command) {
    return Counter(command, "rejected_calls");
  }
  std::uint64_t Successful(std::string_view command) {
    return Calls(command) - Failed(command);
  }
  std::uint64_t Errors(std::string_view prefix) {
    return MetricValue(
        HttpGet(metrics_port_, "/metrics"),
        "lavik_errors_total{err=\"" + std::string(prefix) + "\"}");
  }
  std::string data_path_, log_path_;
  std::uint16_t port_, metrics_port_;
  std::unique_ptr<ServerProcess> server_;
  std::unique_ptr<RespClient> client_;
};

TEST_F(CommandResultsE2eTest, QueuedCommandsDoNotCountAsExecutions) {
  EXPECT_EQ(client_->Command({"MULTI"}), "+OK");
  EXPECT_EQ(client_->Command({"SET", "queued-key", "value"}), "+QUEUED");
  EXPECT_EQ(client_->Command({"DISCARD"}), "+OK");
  EXPECT_TRUE(Stats("set").empty());
  EXPECT_EQ(Successful("set"), 0);
  EXPECT_EQ(Successful("set"), 0);
}

TEST_F(CommandResultsE2eTest, WatchAbortDoesNotExecuteChildren) {
  RespClient writer(port_);
  ASSERT_EQ(client_->Command({"WATCH", "watch:key"}), "+OK");
  ASSERT_EQ(client_->Command({"MULTI"}), "+OK");
  ASSERT_EQ(client_->Command({"GET", "watch:key"}), "+QUEUED");
  ASSERT_EQ(writer.Command({"SET", "watch:key", "changed"}), "+OK");
  EXPECT_EQ(client_->RawCommand({"EXEC"}), "*-1\r\n");
  EXPECT_TRUE(Stats("get").empty());
  EXPECT_EQ(Successful("get"), 0);
  EXPECT_EQ(Successful("exec"), 1);
}

TEST_F(CommandResultsE2eTest, RestartClearsCountersAndSeenErrorSeries) {
  ASSERT_EQ(client_->Command({"SET", "restart:key", "value"}), "+OK");
  ASSERT_TRUE(client_->Command({"LPUSH", "restart:key", "x"})
                  .starts_with("-WRONGTYPE"));
  ASSERT_EQ(Errors("WRONGTYPE"), 1);
  client_.reset();
  server_->Stop();
  server_ = std::make_unique<ServerProcess>(
      g_lavik_binary, port_, metrics_port_, data_path_, log_path_);
  client_ = std::make_unique<RespClient>(port_);
  EXPECT_TRUE(Stats("lpush").empty());
  EXPECT_EQ(Failed("lpush"), 0);
  EXPECT_EQ(Errors("WRONGTYPE"), 0);
  EXPECT_EQ(
      client_->Command({"INFO", "errorstats"}).find("errorstat_WRONGTYPE"),
      std::string::npos);
  EXPECT_EQ(client_->Command({"GET", "restart:key"}), "$5\r\nvalue");
}

TEST_F(CommandResultsE2eTest, AuthenticationAndHelloCountTheirActualResults) {
  EXPECT_TRUE(
      client_->Command({"AUTH", "not-a-secret"}).starts_with("-ERR AUTH"));
  EXPECT_TRUE(client_->RawCommand({"HELLO", "2"}).starts_with('*'));
  const auto auth = Stats("auth");
  EXPECT_TRUE(auth.starts_with("cmdstat_auth:calls=1,")) << auth;
  EXPECT_TRUE(auth.ends_with(",rejected_calls=0,failed_calls=1")) << auth;
  EXPECT_TRUE(Stats("hello").starts_with("cmdstat_hello:calls=1,"));
  EXPECT_EQ(Failed("auth"), 1);
  EXPECT_EQ(Successful("hello"), 1);
}

TEST_F(CommandResultsE2eTest, SubcommandsUseBoundedCanonicalNames) {
  EXPECT_TRUE(client_->RawCommand({"CONFIG", "GET", "port"}).starts_with('*'));
  EXPECT_TRUE(
      client_->Command({"CONFIG", "invented-subcommand"}).starts_with('-'));
  EXPECT_TRUE(client_->Command({"invented-command"}).starts_with('-'));
  EXPECT_TRUE(Stats("config|get").starts_with("cmdstat_config|get:calls=1,"));
  EXPECT_TRUE(Stats("config|resetstat")
                  .starts_with("cmdstat_config|resetstat:calls=1,"));
  EXPECT_TRUE(Stats("config").empty());
  EXPECT_TRUE(Stats("unknown").empty());
  EXPECT_EQ(Successful("config|get"), 1);
  EXPECT_EQ(Errors("ERR"), 2);
  const auto metrics = HttpGet(metrics_port_, "/metrics");
  EXPECT_EQ(metrics.find("invented-command"), std::string::npos);
  EXPECT_EQ(metrics.find("invented-subcommand"), std::string::npos);
}

TEST_F(CommandResultsE2eTest,
       ExecCountsChildrenAndKeepsArrayErrorsOutOfParent) {
  ASSERT_EQ(client_->Command({"SET", "exec:type", "text"}), "+OK");
  ASSERT_EQ(client_->Command({"MULTI"}), "+OK");
  ASSERT_EQ(client_->Command({"LPUSH", "exec:type", "item"}), "+QUEUED");
  ASSERT_EQ(client_->Command({"GET", "exec:type"}), "+QUEUED");
  EXPECT_TRUE(client_->RawCommand({"EXEC"}).starts_with("*2\r\n-WRONGTYPE"));
  EXPECT_TRUE(Stats("lpush").starts_with("cmdstat_lpush:calls=1,"));
  EXPECT_TRUE(Stats("lpush").ends_with(",rejected_calls=0,failed_calls=1"));
  EXPECT_TRUE(Stats("exec").ends_with(",rejected_calls=0,failed_calls=0"));
  EXPECT_EQ(Failed("lpush"), 1);
  EXPECT_EQ(Successful("get"), 1);
  EXPECT_EQ(Successful("exec"), 1);
}

TEST_F(CommandResultsE2eTest,
       ScriptsCountCaughtAndPropagatedErrorsIndependently) {
  ASSERT_EQ(client_->Command({"SET", "script:type", "text"}), "+OK");
  EXPECT_EQ(
      client_->Command({"EVAL", "redis.pcall('LPUSH',KEYS[1],'x'); return 1",
                        "1", "script:type"}),
      ":1");
  EXPECT_TRUE(client_
                  ->Command({"EVAL", "return redis.call('LPUSH',KEYS[1],'x')",
                             "1", "script:type"})
                  .starts_with('-'));
  EXPECT_TRUE(client_
                  ->Command({"EVAL", "return redis.pcall('LPUSH',KEYS[1],'x')",
                             "1", "script:type"})
                  .starts_with('-'));
  EXPECT_TRUE(Stats("lpush").starts_with("cmdstat_lpush:calls=3,"));
  EXPECT_TRUE(Stats("lpush").ends_with(",rejected_calls=0,failed_calls=3"));
  EXPECT_TRUE(Stats("eval").starts_with("cmdstat_eval:calls=3,"));
  EXPECT_TRUE(Stats("eval").ends_with(",rejected_calls=0,failed_calls=2"));
  EXPECT_EQ(Failed("lpush"), 3);
  EXPECT_EQ(Successful("eval"), 1);
  EXPECT_EQ(Failed("eval"), 2);
  EXPECT_EQ(Errors("WRONGTYPE"), 3);
  EXPECT_EQ(Errors("ERR"), 0);
}

TEST_F(CommandResultsE2eTest, ErrorStatsAreGlobalCumulativeAndResettable) {
  EXPECT_EQ(Errors("ERR"), 0);
  EXPECT_EQ(Errors("WRONGTYPE"), 0);
  EXPECT_TRUE(client_->Command({"invented-command"}).starts_with("-ERR"));
  ASSERT_EQ(client_->Command({"SET", "global:error", "value"}), "+OK");
  EXPECT_TRUE(client_->Command({"LPUSH", "global:error", "x"})
                  .starts_with("-WRONGTYPE"));
  EXPECT_TRUE(client_
                  ->RawCommand({"EVAL",
                                "local e=redis.pcall('LPUSH',KEYS[1],'x'); "
                                "return {e,{err=e.err}}",
                                "1", "global:error"})
                  .starts_with("*2"));
  EXPECT_EQ(Errors("ERR"), 1);
  EXPECT_EQ(Errors("WRONGTYPE"), 3);
  EXPECT_EQ(Failed("lpush"), 2);
  EXPECT_EQ(Failed("eval"), 1);
  const auto info = client_->Command({"INFO", "errorstats"});
  EXPECT_NE(info.find("errorstat_ERR:count=1\r\n"), std::string::npos);
  EXPECT_NE(info.find("errorstat_WRONGTYPE:count=3\r\n"), std::string::npos);
  EXPECT_EQ(info.find("# Commandstats"), std::string::npos);
  EXPECT_EQ(info.find("errorstat_OOM"), std::string::npos);
  EXPECT_EQ(client_->Command({"INFO", "errorstats"}), info);
  EXPECT_NE(
      client_->Command({"INFO", "all"}).find("errorstat_WRONGTYPE:count=3"),
      std::string::npos);
  EXPECT_TRUE(Stats("unknown").empty());
  const auto metrics = HttpGet(metrics_port_, "/metrics");
  EXPECT_EQ(metrics.find("lavik_command_results_total"), std::string::npos);
  EXPECT_EQ(metrics.find("lavik_command_errors_total"), std::string::npos);
  EXPECT_EQ(metrics.find("context="), std::string::npos);
  ASSERT_EQ(client_->Command({"CONFIG", "RESETSTAT"}), "+OK");
  EXPECT_EQ(Errors("ERR"), 0);
  EXPECT_EQ(Errors("WRONGTYPE"), 0);
  EXPECT_EQ(client_->Command({"INFO", "errorstats"}).find("errorstat_"),
            std::string::npos);
}

TEST_F(CommandResultsE2eTest, ReusedScriptErrorFailsTheParentOnce) {
  ASSERT_EQ(client_->Command({"SET", "reused:error", "text"}), "+OK");
  EXPECT_TRUE(
      client_
          ->RawCommand({"EVAL",
                        "local e=redis.pcall('LPUSH',KEYS[1],'x'); return {e,e}",
                        "1", "reused:error"})
          .starts_with("*2\r\n-WRONGTYPE"));
  EXPECT_EQ(Calls("eval"), 1);
  EXPECT_EQ(Failed("eval"), 1);
  EXPECT_EQ(Failed("lpush"), 1);
  EXPECT_EQ(Errors("WRONGTYPE"), 1);
}

TEST_F(CommandResultsE2eTest, ErrorReasonsSurviveStorageAndReset) {
  ASSERT_EQ(client_->Command({"SET", "reason:key", "text"}), "+OK");
  EXPECT_TRUE(client_->Command({"LPUSH", "reason:key", "item"})
                  .starts_with("-WRONGTYPE"));
  EXPECT_TRUE(client_->Command({"SET", "reason:key", "value", "NX", "XX"})
                  .starts_with("-ERR syntax"));
  EXPECT_TRUE(client_->Command({"INCR", "reason:key"}).starts_with("-ERR"));
  EXPECT_EQ(Errors("WRONGTYPE"), 1);
  EXPECT_EQ(Errors("ERR"), 2);
  ASSERT_EQ(client_->Command({"CONFIG", "RESETSTAT"}), "+OK");
  EXPECT_EQ(Errors("WRONGTYPE"), 0);
  EXPECT_TRUE(Stats("lpush").empty());
  EXPECT_TRUE(Stats("config|resetstat")
                  .starts_with("cmdstat_config|resetstat:calls=1,"));
}

TEST_F(CommandResultsE2eTest, WrongTypeSourcesKeepClientErrorReasons) {
  ASSERT_EQ(client_->Command({"LPUSH", "type:list", "item"}), ":1");
  ASSERT_EQ(client_->Command({"SET", "type:string", "value"}), "+OK");
  const std::vector<std::vector<std::string_view>> commands{
      {"GET", "type:list"},
      {"STRLEN", "type:list"},
      {"GETSET", "type:list", "value"},
      {"SET", "type:list", "value", "GET"},
      {"SORT", "type:string"},
      {"SORT_RO", "type:string"}};
  for (const auto& command : commands) {
    EXPECT_EQ(client_->Command(command),
              "-WRONGTYPE Operation against a key holding the wrong kind of "
              "value");
  }
  for (const auto command :
       {"get", "strlen", "getset", "set", "sort", "sort_ro"}) {
    EXPECT_EQ(Failed(command), 1) << command;
    EXPECT_EQ(Errors("WRONGTYPE"), commands.size());
    EXPECT_TRUE(Stats(command).ends_with(",rejected_calls=0,failed_calls=1"))
        << command;
  }
}

TEST_F(CommandResultsE2eTest, AdmissionAndMalformedInputHaveDistinctCounters) {
  client_.reset();
  server_->Stop();
  server_ = std::make_unique<ServerProcess>(
      g_lavik_binary, port_, metrics_port_, data_path_, log_path_, 2,
      1024ULL * 1024 * 1024,
      std::vector<std::string>{"--requirepass", "metrics-test-secret"});
  client_ = std::make_unique<RespClient>(port_);
  ASSERT_EQ(client_->Command({"AUTH", "metrics-test-secret"}), "+OK");
  ASSERT_EQ(client_->Command({"CONFIG", "RESETSTAT"}), "+OK");
  RespClient unauthenticated(port_);
  EXPECT_TRUE(
      unauthenticated.Command({"GET", "private"}).starts_with("-NOAUTH"));
  EXPECT_TRUE(Stats("get").starts_with("cmdstat_get:calls=0,"));
  EXPECT_TRUE(Stats("get").ends_with(",rejected_calls=1,failed_calls=0"));
  EXPECT_EQ(Errors("NOAUTH"), 1);
  const int fd = ConnectSocket(port_);
  ASSERT_GE(fd, 0);
  SendAll(fd, "*not-a-number\r\n");
  char error[256];
  EXPECT_GT(::recv(fd, error, sizeof(error), 0), 0);
  ::close(fd);
  EXPECT_EQ(
      MetricValue(HttpGet(metrics_port_, "/metrics"),
                  "lavik_request_input_errors_total{reason=\"protocol\"}"),
      1);
  EXPECT_TRUE(Stats("unknown").empty());
}

TEST_F(CommandResultsE2eTest,
       BlockingTimeoutIsSuccessfulAndExcludesDormantWait) {
  EXPECT_EQ(client_->RawCommand({"BLPOP", "never-present", "0.2"}), "*-1\r\n");
  const auto stats = Stats("blpop");
  ASSERT_TRUE(stats.starts_with("cmdstat_blpop:calls=1,")) << stats;
  EXPECT_TRUE(stats.ends_with(",rejected_calls=0,failed_calls=0")) << stats;
  const auto usec = stats.find(",usec=");
  ASSERT_NE(usec, std::string::npos);
  EXPECT_LT(std::stoull(stats.substr(usec + 6)), 100000);
  EXPECT_EQ(Successful("blpop"), 1);
}

TEST_F(CommandResultsE2eTest, BlockingRoleChangesKeepRetryReasons) {
  for (const bool sorted_set : {false, true}) {
    const char* command = sorted_set ? "BZPOPMIN" : "BLPOP";
    const char* metric = sorted_set ? "bzpopmin" : "blpop";
    const char* key = sorted_set ? "retry:zset" : "retry:list";
    auto blocked = std::async(std::launch::async, [&, command, key] {
      RespClient waiting(port_);
      return waiting.Command({command, key, "5"});
    });
    const auto deadline = std::chrono::steady_clock::now() + 3s;
    bool registered = false;
    while (std::chrono::steady_clock::now() < deadline) {
      if (client_->Command({"INFO", "clients"}).find("blocked_clients:1\r\n") !=
          std::string::npos) {
        registered = true;
        break;
      }
      std::this_thread::sleep_for(5ms);
    }
    ASSERT_TRUE(registered);
    // Even a standalone NO ONE transition advances the role epoch. Wake the
    // old waiter afterward so its retry must reject the stale admission.
    ASSERT_EQ(client_->Command({"REPLICAOF", "NO", "ONE"}), "+OK");
    if (sorted_set) {
      ASSERT_EQ(client_->Command({"ZADD", key, "1", "item"}), ":1");
    } else {
      ASSERT_EQ(client_->Command({"RPUSH", key, "item"}), ":1");
    }
    EXPECT_EQ(blocked.get(),
              "-TRYAGAIN replication role changed; retry command");
    EXPECT_EQ(Errors("TRYAGAIN"), sorted_set ? 2 : 1);
    EXPECT_TRUE(Stats(metric).ends_with(",rejected_calls=0,failed_calls=1"));
    EXPECT_EQ(client_->Command({sorted_set ? "ZCARD" : "LLEN", key}), ":1");
  }
}

TEST_F(CommandResultsE2eTest, ExclusiveCommandsExcludeDatabaseDrainAdmission) {
#if !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires database gate hold hooks";
#endif
  const std::string held = data_path_ + ".held";
  const std::string marker = data_path_ + ".marker";
  FileCleanup held_cleanup(held), marker_cleanup(marker);
  auto reached = [&](const char* variable) {
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (std::chrono::steady_clock::now() < deadline) {
      std::ifstream log(log_path_);
      const std::string text((std::istreambuf_iterator<char>(log)), {});
      if (text.find(std::string("fault pause reached: ") + variable) !=
          std::string::npos)
        return true;
      std::this_thread::sleep_for(5ms);
    }
    return false;
  };
  for (const bool keys : {false, true}) {
    ASSERT_EQ(client_->Command({"SET", "held:key", "value"}), "+OK");
    client_.reset();
    server_->Stop();
    {
      std::ofstream file(held);
    }
    {
      std::ofstream file(marker);
    }
    const char* boundary = keys ? "LAVIK_KEYS_AFTER_DB_CLOSE_HOLD_FILE"
                                : "LAVIK_FLUSH_BEFORE_DRAIN_HOLD_FILE";
    ASSERT_EQ(::setenv("LAVIK_DB_OPERATION_HOLD_KEY", "held:key", 1), 0);
    ASSERT_EQ(::setenv("LAVIK_DB_OPERATION_HOLD_FILE", held.c_str(), 1), 0);
    ASSERT_EQ(::setenv(boundary, marker.c_str(), 1), 0);
    server_ = std::make_unique<ServerProcess>(
        g_lavik_binary, port_, metrics_port_, data_path_, log_path_);
    ::unsetenv("LAVIK_DB_OPERATION_HOLD_KEY");
    ::unsetenv("LAVIK_DB_OPERATION_HOLD_FILE");
    ::unsetenv(boundary);
    client_ = std::make_unique<RespClient>(port_);
    auto reader = std::async(std::launch::async, [&] {
      RespClient client(port_);
      return client.Command({"GET", "held:key"});
    });
    EXPECT_TRUE(reached("LAVIK_DB_OPERATION_HOLD_FILE"));
    auto exclusive = std::async(std::launch::async, [&] {
      RespClient client(port_);
      return keys ? client.RawCommand({"KEYS", "*"})
                  : client.RawCommand({"FLUSHDB"});
    });
    EXPECT_TRUE(reached(boundary));
    ::unlink(marker.c_str());
    // The exclusive command has entered its real DB-drain loop while GET owns
    // an active operation. That admission wait must not become execution time.
    EXPECT_EQ(exclusive.wait_for(300ms), std::future_status::timeout);
    ::unlink(held.c_str());
    EXPECT_EQ(reader.get(), "$5\r\nvalue");
    EXPECT_FALSE(exclusive.get().starts_with('-'));
    const auto stats = Stats(keys ? "keys" : "flushdb");
    const auto usec = stats.find(",usec=");
    ASSERT_NE(usec, std::string::npos) << stats;
    EXPECT_LT(std::stoull(stats.substr(usec + 6)), 150000) << stats;
  }
}

TEST_F(CommandResultsE2eTest, LuaVmAdmissionIsExcludedFromParentAndExecChild) {
#if !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires list read pause hook";
#endif
  ASSERT_EQ(client_->Command({"LPUSH", "lua:held", "value"}), ":1");
  client_.reset();
  server_->Stop();
  ASSERT_EQ(::setenv("LAVIK_LIST_READ_PAUSE_MS", "500", 1), 0);
  server_ = std::make_unique<ServerProcess>(
      g_lavik_binary, port_, metrics_port_, data_path_, log_path_, 1);
  ::unsetenv("LAVIK_LIST_READ_PAUSE_MS");
  client_ = std::make_unique<RespClient>(port_);
  for (const bool in_exec : {false, true}) {
    ASSERT_EQ(client_->Command({"CONFIG", "RESETSTAT"}), "+OK");
    auto holder = std::async(std::launch::async, [&] {
      RespClient client(port_);
      return client.Command(
          {"EVAL", "return redis.call('LINDEX',KEYS[1],0)", "1", "lua:held"});
    });
    bool running = false;
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (std::chrono::steady_clock::now() < deadline) {
      if (client_->RawCommand({"FUNCTION", "STATS"}).starts_with("-BUSY")) {
        running = true;
        break;
      }
      std::this_thread::sleep_for(1ms);
    }
    EXPECT_TRUE(running);
    const auto started = std::chrono::steady_clock::now();
    if (in_exec) {
      EXPECT_EQ(client_->Command({"MULTI"}), "+OK");
      EXPECT_EQ(client_->Command({"EVAL_RO", "return 1", "0"}), "+QUEUED");
      EXPECT_EQ(client_->RawCommand({"EXEC"}), "*1\r\n:1\r\n");
    } else {
      EXPECT_EQ(client_->Command({"EVAL_RO", "return 1", "0"}), ":1");
    }
    EXPECT_GT(std::chrono::steady_clock::now() - started, 250ms);
    EXPECT_EQ(holder.get(), "$5\r\nvalue");
    for (const auto name :
         in_exec ? std::vector<std::string_view>{"eval_ro", "exec"}
                 : std::vector<std::string_view>{"eval_ro"}) {
      const auto stats = Stats(name);
      const auto usec = stats.find(",usec=");
      ASSERT_NE(usec, std::string::npos) << stats;
      EXPECT_LT(std::stoull(stats.substr(usec + 6)), 150000) << stats;
    }
    const auto held_stats = Stats("eval");
    EXPECT_GT(std::stoull(held_stats.substr(held_stats.find(",usec=") + 6)),
              400000);
  }
}

TEST_F(CommandResultsE2eTest, FunctionsObserveCaughtScriptChildren) {
  EXPECT_FALSE(
      client_
          ->Command({"FUNCTION", "LOAD",
                     "#!lua name=metrics\nredis.register_function('caught', "
                     "function(keys,args) redis.pcall('LPUSH',keys[1],'x'); "
                     "return 1 end)"})
          .starts_with('-'));
  ASSERT_EQ(client_->Command({"SET", "function:type", "text"}), "+OK");
  EXPECT_EQ(client_->Command({"FCALL", "caught", "1", "function:type"}), ":1");
  EXPECT_EQ(Successful("fcall"), 1);
  EXPECT_EQ(Errors("WRONGTYPE"), 1);
}

TEST_F(CommandResultsE2eTest,
       SubcommandOptionErrorsExecuteAfterArityAdmission) {
  EXPECT_TRUE(
      client_->Command({"SLOWLOG", "GET", "1", "extra"}).starts_with('-'));
  EXPECT_TRUE(client_
                  ->Command({"XINFO", "STREAM", "missing", "FULL", "COUNT", "1",
                             "extra"})
                  .starts_with('-'));
  for (const auto command : {"slowlog|get", "xinfo|stream"}) {
    EXPECT_EQ(Failed(command), 1);
    EXPECT_EQ(Rejected(command), 0);
    EXPECT_TRUE(Stats(command).ends_with(",rejected_calls=0,failed_calls=1"));
  }
}

TEST_F(CommandResultsE2eTest, BlockingSortedSetErrorsKeepTheirOutcome) {
  ASSERT_EQ(client_->Command({"SET", "blocking:type", "text"}), "+OK");
  for (const auto command : {"BZPOPMIN", "BZPOPMAX"}) {
    EXPECT_TRUE(client_->Command({command, "blocking:type", "0.1"})
                    .starts_with("-WRONGTYPE"));
  }
  EXPECT_TRUE(client_->Command({"BZMPOP", "0.1", "1", "blocking:type", "MIN"})
                  .starts_with("-WRONGTYPE"));
  for (const auto command : {"bzpopmin", "bzpopmax", "bzmpop"}) {
    EXPECT_EQ(Errors("WRONGTYPE"), 3);
    EXPECT_EQ(Successful(command), 0);
    EXPECT_TRUE(Stats(command).ends_with(",rejected_calls=0,failed_calls=1"));
  }
}

TEST_F(CommandResultsE2eTest,
       StreamCompletionAndPeerCancellationAreFinalOutcomes) {
  const std::string member(1024, 'x');
  ASSERT_EQ(client_->Command({"SADD", "stream:members", member}), ":1");
  EXPECT_TRUE(client_->RawCommand({"SRANDMEMBER", "stream:members", "-2"})
                  .starts_with("*2\r\n"));
  EXPECT_EQ(Successful("srandmember"), 1);
  const int fd = ConnectSocket(port_);
  ASSERT_GE(fd, 0);
  SendAll(fd,
          "*3\r\n$11\r\nSRANDMEMBER\r\n$14\r\nstream:members\r\n$8\r\n-"
          "1000000\r\n");
  char prefix = 0;
  ASSERT_EQ(::recv(fd, &prefix, 1, 0), 1);
  EXPECT_EQ(prefix, '*');
  linger reset{1, 0};
  (void)::setsockopt(fd, SOL_SOCKET, SO_LINGER, &reset, sizeof(reset));
  ::close(fd);
  const auto deadline = std::chrono::steady_clock::now() + 2s;
  while (
      client_->Command({"INFO", "clients"}).find("connected_clients:1\r\n") ==
          std::string::npos &&
      std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(10ms);
  EXPECT_NE(
      client_->Command({"INFO", "clients"}).find("connected_clients:1\r\n"),
      std::string::npos);
  EXPECT_EQ(Calls("srandmember"), 1);
  EXPECT_EQ(Successful("srandmember"), 1);
  EXPECT_EQ(Failed("srandmember"), 0);
}

TEST_F(CommandResultsE2eTest, LateStorageFailureCountsStreamAndExecChildOnce) {
#if !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires Stream read-window failure hook";
#endif
  const std::string value(8192, 'v');
  for (unsigned i = 1; i <= 128; ++i) {
    const auto id = std::to_string(i) + "-0";
    ASSERT_TRUE(client_->Command({"XADD", "late:stream", id, "field", value})
                    .ends_with(id));
  }
  client_.reset();
  server_->Stop();
  ASSERT_EQ(
      ::setenv("LAVIK_FAIL_STREAM_READ_WINDOW_START_KEY", "late:stream", 1), 0);
  server_ = std::make_unique<ServerProcess>(
      g_lavik_binary, port_, metrics_port_, data_path_, log_path_);
  ASSERT_EQ(::unsetenv("LAVIK_FAIL_STREAM_READ_WINDOW_START_KEY"), 0);
  client_ = std::make_unique<RespClient>(port_);
  ASSERT_EQ(client_->Command({"CONFIG", "RESETSTAT"}), "+OK");
  {
    RespClient reader(port_);
    EXPECT_THROW(reader.RawCommand({"XRANGE", "late:stream", "-", "+"}),
                 std::runtime_error);
  }
  EXPECT_EQ(Failed("xrange"), 1);
  EXPECT_EQ(Successful("xrange"), 0);
  EXPECT_EQ(Errors("OOM"), 0);  // No RESP error can follow a partial array.
  {
    RespClient reader(port_);
    ASSERT_EQ(reader.Command({"MULTI"}), "+OK");
    ASSERT_EQ(reader.Command({"XRANGE", "late:stream", "-", "+"}), "+QUEUED");
    EXPECT_THROW(reader.RawCommand({"EXEC"}), std::runtime_error);
  }
  EXPECT_EQ(Failed("xrange"), 2);
  EXPECT_EQ(Failed("exec"), 1);
  EXPECT_EQ(Successful("xrange"), 0);
}

TEST_F(CommandResultsE2eTest, SubscribedCommandsKeepSessionOutcomes) {
  EXPECT_TRUE(client_->RawCommand({"SUBSCRIBE", "metrics:channel"})
                  .starts_with("*3\r\n"));
  EXPECT_TRUE(client_->RawCommand({"PING"}).starts_with("*2\r\n"));
  EXPECT_TRUE(client_->Command({"GET", "not-allowed"}).starts_with("-ERR"));
  EXPECT_TRUE(client_->RawCommand({"UNSUBSCRIBE", "metrics:channel"})
                  .starts_with("*3\r\n"));
  EXPECT_TRUE(Stats("ping").starts_with("cmdstat_ping:calls=1,"));
  EXPECT_TRUE(Stats("get").starts_with("cmdstat_get:calls=0,"));
  EXPECT_EQ(Successful("ping"), 1);
  EXPECT_EQ(Errors("ERR"), 1);
}

TEST_F(CommandResultsE2eTest, SubscribedProtocolErrorsReachGlobalErrorStats) {
  RespClient subscriber(port_);
  ASSERT_TRUE(subscriber.RawCommand({"SUBSCRIBE", "metrics:bad"})
                  .starts_with("*3\r\n"));
  EXPECT_TRUE(subscriber.RawRequest("*not-a-number\r\n").starts_with("-ERR"));
  EXPECT_EQ(Errors("ERR"), 1);
  EXPECT_EQ(
      MetricValue(HttpGet(metrics_port_, "/metrics"),
                  "lavik_request_input_errors_total{reason=\"protocol\"}"),
      1);
}

TEST_F(CommandResultsE2eTest,
       SubcommandArityRejectsBeforeExecutionAndQueueing) {
  EXPECT_TRUE(client_->Command({"CONFIG", "GET"}).starts_with('-'));
  EXPECT_TRUE(Stats("config|get").starts_with("cmdstat_config|get:calls=0,"));
  EXPECT_TRUE(
      Stats("config|get").ends_with(",rejected_calls=1,failed_calls=0"));
  ASSERT_EQ(client_->Command({"MULTI"}), "+OK");
  EXPECT_TRUE(client_->Command({"SCRIPT", "LOAD"}).starts_with('-'));
  EXPECT_TRUE(client_->Command({"EXEC"}).starts_with("-EXECABORT"));
  EXPECT_EQ(Errors("ERR"), 2);
  EXPECT_EQ(Failed("exec"), 1);
}

TEST_F(CommandResultsE2eTest, ScriptAdmissionAndReturnedErrorArraysMatchRedis) {
  EXPECT_TRUE(
      client_
          ->RawCommand({"EVAL", "return {redis.error_reply('ERR test')}", "0"})
          .starts_with("*1\r\n-ERR"));
  EXPECT_TRUE(
      client_
          ->Command({"EVAL", "return redis.call('CONFIG','GET','port')", "0"})
          .starts_with('-'));
  EXPECT_TRUE(client_
                  ->Command({"EVAL_RO", "return redis.call('SET',KEYS[1],'x')",
                             "1", "key"})
                  .starts_with('-'));
  EXPECT_EQ(Failed("eval"), 2);
  EXPECT_EQ(Failed("eval_ro"), 1);
  EXPECT_EQ(Errors("ERR"), 3);
  EXPECT_TRUE(Stats("set").starts_with("cmdstat_set:calls=0,"));
}

TEST_F(CommandResultsE2eTest, SquashedExecCountsCrossShardCommandsOnce) {
  // Redis hash tags a and b occupy slots 15495 and 3300: distinct owners
  // with the fixture's two workers, independent of connection placement.
  ASSERT_EQ(client_->Command({"MULTI"}), "+OK");
  ASSERT_EQ(client_->Command({"MSET", "{a}:left", "one", "{b}:right", "two"}),
            "+QUEUED");
  ASSERT_EQ(client_->Command({"MGET", "{a}:left", "{b}:right"}), "+QUEUED");
  ASSERT_EQ(client_->Command({"DEL", "{a}:left", "{b}:right"}), "+QUEUED");
  EXPECT_EQ(client_->RawCommand({"EXEC"}),
            "*3\r\n+OK\r\n*2\r\n$3\r\none\r\n$3\r\ntwo\r\n:2\r\n");
  for (const auto* command : {"mset", "mget", "del"}) {
    EXPECT_EQ(Successful(command), 1);
    EXPECT_TRUE(Stats(command).find(":calls=1,") != std::string::npos);
  }
}

TEST_F(CommandResultsE2eTest, SparseExecSpansKeepLogicalCountsAcrossRuns) {
  // Mix single-owner and cross-owner commands in a long squashed run. Repeated
  // keys must not create extra logical results, even as owner buffers grow.
  ASSERT_EQ(client_->Command({"SET", "{a}:key", "text"}), "+OK");
  ASSERT_EQ(client_->Command({"SET", "{b}:key", "text"}), "+OK");
  ASSERT_EQ(client_->Command({"CONFIG", "RESETSTAT"}), "+OK");
  ASSERT_EQ(client_->Command({"MULTI"}), "+OK");
  constexpr unsigned kRounds = 64;
  for (unsigned i = 0; i < kRounds; ++i) {
    ASSERT_EQ(client_->Command({"GET", "{a}:key"}), "+QUEUED");
    ASSERT_EQ(client_->Command({"LPUSH", "{b}:key", "x"}), "+QUEUED");
    ASSERT_EQ(client_->Command({"EXISTS", "{a}:key", "{b}:key", "{a}:key"}),
              "+QUEUED");
  }
  // A script splits runs, and its routed multi-key child has its own outer
  // observation; the shard callback must not add EXEC-context observations.
  ASSERT_EQ(
      client_->Command({"EVAL", "return redis.call('EXISTS',KEYS[1],KEYS[2])",
                        "2", "{a}:key", "{b}:key"}),
      "+QUEUED");
  ASSERT_EQ(client_->Command({"GET", "{b}:key"}), "+QUEUED");
  EXPECT_TRUE(client_->RawCommand({"EXEC"}).starts_with("*194\r\n"));
  EXPECT_EQ(Successful("get"), kRounds + 1);
  EXPECT_EQ(Failed("lpush"), kRounds);
  EXPECT_EQ(Successful("exists"), kRounds + 1);
  EXPECT_EQ(Successful("eval"), 1);
  EXPECT_EQ(Successful("exec"), 1);
  EXPECT_TRUE(Stats("get").starts_with("cmdstat_get:calls=65,"));
  EXPECT_TRUE(Stats("lpush").ends_with(",rejected_calls=0,failed_calls=64"));
  EXPECT_TRUE(Stats("exists").starts_with("cmdstat_exists:calls=65,"));
  for (const auto* command : {"get", "lpush", "exists", "eval"}) {
    const auto stats = Stats(command);
    const auto usec = stats.find(",usec=");
    ASSERT_NE(usec, std::string::npos);
    EXPECT_GT(std::stoull(stats.substr(usec + 6)), 0) << stats;
  }
}

TEST_F(CommandResultsE2eTest, InfoSectionsPreserveOutcomeProjection) {
  ASSERT_EQ(client_->Command({"SET", "projection:key", "text"}), "+OK");
  ASSERT_EQ(client_->Command({"CONFIG", "RESETSTAT"}), "+OK");
  EXPECT_TRUE(client_->Command({"LPUSH", "projection:key", "x"})
                  .starts_with("-WRONGTYPE"));
  EXPECT_TRUE(client_->Command({"LPUSH"}).starts_with("-ERR"));
  const auto expected = Stats("lpush");
  ASSERT_TRUE(expected.starts_with("cmdstat_lpush:calls=1,"));
  ASSERT_TRUE(expected.ends_with(",rejected_calls=1,failed_calls=1"));
  for (const auto* section : {"clients", "stats", "persistence"}) {
    const auto info = client_->Command({"INFO", section});
    EXPECT_EQ(info.find("# Commandstats"), std::string::npos);
    EXPECT_EQ(Stats("lpush"), expected);
  }
  for (const auto* section : {"default", "all", "commandstats"}) {
    EXPECT_NE(client_->Command({"INFO", section}).find(expected),
              std::string::npos);
  }
  EXPECT_EQ(Failed("lpush"), 1);
  EXPECT_EQ(Rejected("lpush"), 1);
  ASSERT_EQ(client_->Command({"CONFIG", "RESETSTAT"}), "+OK");
  EXPECT_TRUE(Stats("lpush").empty());
  EXPECT_EQ(Errors("WRONGTYPE"), 0);
}

TEST_F(CommandResultsE2eTest, SparseCollectionPreservesIdleAndResetSamples) {
  // Unused command rows are absent, while the small global prefix vocabulary
  // has a zero baseline even before the first error.
  EXPECT_EQ(Failed("get"), 0);
  EXPECT_EQ(Successful("zrange"), 0);
  ASSERT_EQ(client_->Command({"SET", "sparse:key", "text"}), "+OK");
  for (unsigned cycle = 0; cycle < 3; ++cycle) {
    ASSERT_EQ(client_->Command({"CONFIG", "RESETSTAT"}), "+OK");
    const auto reset = HttpGet(metrics_port_, "/metrics");
    EXPECT_EQ(reset.find("lavik_command_executions_total{command=\"get\"}"),
              std::string::npos);
    EXPECT_EQ(reset.find("lavik_command_calls_total{command=\"get\"}"),
              std::string::npos);
    EXPECT_EQ(client_->Command({"GET", "sparse:key"}), "$4\r\ntext");
    EXPECT_TRUE(client_->Command({"LPUSH", "sparse:key", "x"})
                    .starts_with("-WRONGTYPE"));
    const auto metrics = HttpGet(metrics_port_, "/metrics");
    EXPECT_EQ(
        MetricValue(metrics, "lavik_command_calls_total{command=\"get\"}"), 1);
    EXPECT_EQ(
        MetricValue(metrics,
                    "lavik_command_duration_seconds_count{command=\"get\"}"),
        1);
    EXPECT_EQ(
        MetricValue(metrics,
                    "lavik_command_duration_seconds_bucket{command=\"get\","
                    "le=\"+Inf\"}"),
        1);
    EXPECT_EQ(Successful("get"), 1);
    EXPECT_EQ(Errors("WRONGTYPE"), 1);
    // Taking another snapshot must neither consume counters nor duplicate
    // entries when an identity was reactivated by RESETSTAT.
    EXPECT_EQ(Successful("get"), 1);
  }
  ASSERT_EQ(client_->Command({"CONFIG", "RESETSTAT"}), "+OK");
  EXPECT_EQ(Errors("WRONGTYPE"), 0);
}

TEST_F(CommandResultsE2eTest, OutcomeMetricFamiliesAreContiguous) {
  auto check_groups = [&] {
    constexpr std::array<std::string_view, 6> families{
        "lavik_command_executions_total",
        "lavik_command_failed_calls_total",
        "lavik_command_rejected_calls_total",
        "lavik_command_execution_seconds_total",
        "lavik_errors_total",
        "lavik_request_input_errors_total"};
    std::array<unsigned, families.size()> groups{};
    std::istringstream response(HttpGet(metrics_port_, "/metrics"));
    std::string line, previous;
    while (std::getline(response, line)) {
      std::string_view text = line;
      if (text.starts_with("# HELP ") || text.starts_with("# TYPE ")) {
        text.remove_prefix(7);
      } else if (text.empty() || text.starts_with('#')) {
        continue;
      }
      const auto name = text.substr(0, text.find_first_of("{ \t\r"));
      if (name == previous) continue;
      for (std::size_t i = 0; i < families.size(); ++i)
        if (name == families[i]) ++groups[i];
      previous = name;
    }
    // A permissive Prometheus parser can ingest interleaved families. Check
    // the wire-format grouping contract, including metadata, independently.
    for (std::size_t i = 0; i < families.size(); ++i)
      EXPECT_EQ(groups[i], 1) << families[i];
  };
  check_groups();
  ASSERT_EQ(client_->Command({"SET", "grouping:key", "text"}), "+OK");
  EXPECT_EQ(client_->Command({"GET", "grouping:key"}), "$4\r\ntext");
  EXPECT_TRUE(client_->Command({"LPUSH", "grouping:key", "x"})
                  .starts_with("-WRONGTYPE"));
  check_groups();
  ASSERT_EQ(client_->Command({"CONFIG", "RESETSTAT"}), "+OK");
  check_groups();
}

TEST_F(CommandResultsE2eTest, ManagementExecCountsItsChildren) {
  ASSERT_EQ(client_->Command({"MULTI"}), "+OK");
  ASSERT_EQ(client_->Command({"CONFIG", "REWRITE"}), "+QUEUED");
  EXPECT_TRUE(client_->RawCommand({"EXEC"}).starts_with("*1\r\n-ERR"));
  EXPECT_EQ(Failed("config|rewrite"), 1);
  EXPECT_EQ(Successful("exec"), 1);
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
  // Four workers' fixed indexes and storage pools exceed the retained-memory
  // waterline at 1 GiB. Leave room for the concurrent allocation workload.
  ServerProcess server(g_lavik_binary, redis_port, metrics_port, data_path,
                       log_path, kWorkers, 2ULL * 1024 * 1024 * 1024);
  RespClient control(redis_port);
  ASSERT_EQ(control.Command({"PING"}), "+PONG");
  auto require = [](bool ok, std::string_view message) {
    if (!ok) throw std::runtime_error(std::string(message));
  };

  // Futures propagate a crashed server's socket errors to this test instead
  // of terminating in a std::thread destructor. Declaring the gate after the
  // futures also unblocks already-created tasks if later task creation throws.
  std::vector<std::future<void>> jobs;
  jobs.reserve(kWriters + 4);
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
        require(client.Command(
                    {"MSET", key + ":a", value, key + ":b", value}) == "+OK",
                "MSET failed");
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
  for (unsigned scraper = 0; scraper < 2; ++scraper) {
    jobs.push_back(std::async(std::launch::async, [&, start] {
      start.get();
      std::uint64_t previous_commands = 0;
      for (unsigned round = 0; round < kRounds; ++round) {
        const auto response = HttpGet(metrics_port, "/metrics");
        require(response.starts_with("HTTP/1.1 200 OK\r\n"), "scrape failed");
        require(MetricValue(response, "lavik_server_ready") == 1,
                "server stopped being ready");
        const auto commands = MetricValue(response, "lavik_commands_total");
        require(commands >= previous_commands,
                "command counter went backwards");
        previous_commands = commands;
        require(MetricValue(response, "lavik_storage_tx_backlog_bytes") >=
                    MetricValue(response, "lavik_storage_tx_backlog_max_bytes"),
                "backlog max exceeds sum");
        for (unsigned worker = 0; worker < kWorkers; ++worker)
          require(response.find("lavik_worker_memory_limit_bytes{worker=\"" +
                                std::to_string(worker) + "\"}") !=
                      std::string::npos,
                  "scrape omitted worker");
        // Disconnect while a snapshot may still be in flight. Its frame must
        // retain storage safely until worker callbacks have returned.
        const int abandoned = ConnectSocket(metrics_port);
        require(abandoned >= 0, "abandoned scrape connection failed");
        SendAll(abandoned, "GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n");
        ::close(abandoned);
      }
    }));
  }
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

TEST(MetricsE2eTest, StorageDiagnosticsMatchInfoDrainAndResetOnRestart) {
  const auto prefix = lavik::test::TestDataPath("lavik-storage-diagnostics-" +
                                                std::to_string(::getpid()));
  const auto data = prefix + ".data", log = prefix + ".log";
  FileCleanup data_cleanup(data), log_cleanup(log, true);
  const int fd =
      ::open(data.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::posix_fallocate(fd, 0, 512ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(fd), 0);
  const auto redis = FindFreePort();
  auto http = FindFreePort();
  while (http == redis) http = FindFreePort();
  const std::string value(65536, 'v');
  {
    ServerProcess server(g_lavik_binary, redis, http, data, log);
    RespClient client(redis);
    ASSERT_EQ(client.Command({"CONFIG", "SET", "tx-cleaner-cooldown-ms", "0"}),
              "+OK");
    CheckDiagnosticContract(HttpGet(http, "/metrics"),
                            client.Command({"INFO", "STATS"}));
    // Overwrite a bounded set of keys but roll several Tx blocks on both
    // workers.
    for (unsigned i = 0; i < 160; ++i) {
      ASSERT_EQ(client.Command({"MSET", "diag:a", value, "diag:b", value,
                                "diag:c", value, "diag:d", value}),
                "+OK");
    }
    ASSERT_TRUE(WaitUntil([&] {
      return MetricValue(HttpGet(http, "/metrics"),
                         "lavik_storage_durability_pending") == 0;
    }));
    const auto before = HttpGet(http, "/metrics");
    CheckDiagnosticContract(before, client.Command({"INFO", "STATS"}));
    EXPECT_GT(MetricValue(before, "lavik_storage_tx_backlog_bytes"), 0);
    EXPECT_GT(MetricValue(before, "lavik_storage_tx_commit_batches_total"), 0);
    EXPECT_EQ(
        MetricValue(before, "lavik_storage_tx_commit_batch_transactions_total"),
        160);
    EXPECT_GE(
        MetricValue(before, "lavik_storage_tx_commit_input_fences_total"),
        MetricValue(before, "lavik_storage_tx_commit_merged_fences_total"));
    EXPECT_EQ(MetricValue(before, "lavik_storage_tx_commit_queue_depth"), 0);
    EXPECT_GT(MetricValue(before, "lavik_storage_tx_commit_queue_peak"), 0);
    EXPECT_EQ(
        MetricValue(before, "lavik_storage_tx_commit_queue_high_watermark"),
        4096);
    ASSERT_EQ(client.Command({"CONFIG", "RESETSTAT"}), "+OK");
    const auto after = HttpGet(http, "/metrics");
    for (const auto& field : kDiagnostics) {
      const auto name = "lavik_storage_" + std::string(field.suffix);
      EXPECT_DOUBLE_EQ(MetricNumber(after, name), MetricNumber(before, name))
          << name;
    }
    server.Stop();
  }
  {
    std::vector<std::pair<std::string, std::string>> env;
#if LAVIK_TEST_FAULTS_AVAILABLE
    env.emplace_back("LAVIK_FAIL_TX_CLEANER_ONCE", "1");
#endif
    ServerProcess server(g_lavik_binary, redis, http, data, log, 2,
                         1024ULL * 1024 * 1024, {}, env);
    RespClient client(redis);
    EXPECT_EQ(client.Command({"GET", "diag:a"}), "$65536\r\n" + value);
    const auto restarted = HttpGet(http, "/metrics");
    EXPECT_EQ(MetricValue(restarted, "lavik_storage_tx_commit_batches_total"),
              0);
    EXPECT_EQ(MetricValue(restarted, "lavik_storage_tx_commit_queue_peak"), 0);
    ASSERT_EQ(
        client.Command({"CONFIG", "SET", "tx-cleaner-cooldown-ms", "250"}),
        "+OK");
    ASSERT_TRUE(WaitUntil([&] {
      const auto metrics = HttpGet(http, "/metrics");
      return MetricValue(metrics, "lavik_storage_tx_backlog_bytes") == 0 &&
             MetricValue(metrics, "lavik_storage_tx_cleaner_running") == 0 &&
             MetricValue(metrics, "lavik_storage_durability_pending") == 0;
    }));
    // Freeze the cleaner for an exact INFO/export comparison, including
    // fractional seconds.
    const auto drained = HttpGet(http, "/metrics");
    EXPECT_DOUBLE_EQ(
        MetricNumber(drained, "lavik_storage_tx_cleaner_cooldown_seconds"),
        0.25);
    EXPECT_GT(
        MetricValue(drained, "lavik_storage_tx_cleaner_retired_blocks_total"),
        0);
#if LAVIK_TEST_FAULTS_AVAILABLE
    EXPECT_EQ(MetricValue(drained, "lavik_storage_tx_cleaner_failures_total"),
              1);
#endif
    ASSERT_EQ(client.Command({"CONFIG", "SET", "tx-cleaner-cooldown-ms", "0"}),
              "+OK");
    CheckDiagnosticContract(HttpGet(http, "/metrics"),
                            client.Command({"INFO", "STATS"}));
    server.Stop();
  }
}

TEST(MetricsE2eTest, StorageDiagnosticsRemainResponsiveDuringPausedFlush) {
#if !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "Deterministic flush pause requires test fault hooks";
#else
  const auto prefix = lavik::test::TestDataPath("lavik-diagnostics-flush-" +
                                                std::to_string(::getpid()));
  const auto data = prefix + ".data", log = prefix + ".log",
             pause = prefix + ".pause";
  FileCleanup data_cleanup(data), log_cleanup(log, true), pause_cleanup(pause);
  const int fd =
      ::open(data.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::posix_fallocate(fd, 0, 256ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(fd), 0);
  {
    std::ofstream marker(pause);
  }
  const auto redis = FindFreePort();
  auto http = FindFreePort();
  while (http == redis) http = FindFreePort();
  ServerProcess server(g_lavik_binary, redis, http, data, log, 2,
                       1024ULL * 1024 * 1024, {},
                       {{"LAVIK_FLUSH_SNAPSHOT_PAUSE_FILE", pause}});
  RespClient client(redis);
  ASSERT_EQ(client.Command({"SET", "diagnostic-pause", "value"}), "+OK");
  ASSERT_TRUE(WaitUntil([&] {
    std::ifstream stream(log);
    return std::string(std::istreambuf_iterator<char>(stream), {})
               .find("fault pause reached: LAVIK_FLUSH_SNAPSHOT_PAUSE_FILE") !=
           std::string::npos;
  }));
  const auto blocked = HttpGet(http, "/metrics");
  EXPECT_GT(MetricValue(blocked, "lavik_storage_dirty_staging_bytes"), 0);
  EXPECT_EQ(MetricValue(blocked, "lavik_storage_flushes_pending"), 1);
  EXPECT_EQ(MetricValue(blocked, "lavik_storage_durability_pending"), 1);
  CheckDiagnosticContract(blocked, client.Command({"INFO", "STATS"}));
  EXPECT_EQ(client.Command({"PING"}), "+PONG");
  ASSERT_EQ(::unlink(pause.c_str()), 0);
  ASSERT_TRUE(WaitUntil([&] {
    return MetricValue(HttpGet(http, "/metrics"),
                       "lavik_storage_durability_pending") == 0;
  }));
  EXPECT_EQ(client.Command({"GET", "diagnostic-pause"}), "$5\r\nvalue");
  server.Stop();
#endif
}

TEST(MetricsE2eTest, StorageFailureExportsOnlyReadiness) {
#if !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "Deterministic storage failure requires test fault hooks";
#else
  const auto prefix = lavik::test::TestDataPath("lavik-diagnostics-unready-" +
                                                std::to_string(::getpid()));
  const auto data = prefix + ".data", log = prefix + ".log",
             failure = prefix + ".fail";
  FileCleanup data_cleanup(data), log_cleanup(log, true),
      failure_cleanup(failure);
  const int fd =
      ::open(data.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::posix_fallocate(fd, 0, 256ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(fd), 0);
  const auto redis = FindFreePort();
  auto http = FindFreePort();
  while (http == redis) http = FindFreePort();
  ServerProcess server(g_lavik_binary, redis, http, data, log, 2,
                       1024ULL * 1024 * 1024, {},
                       {{"LAVIK_EPOCH_IO_FAIL_FILE", failure},
                        {"LAVIK_EPOCH_IO_FAIL_KIND", "write"},
                        {"LAVIK_EPOCH_IO_FAIL_DEVICE", "1"}});
  RespClient client(redis);
  ASSERT_EQ(client.Command({"SET", "before-failure", "value"}), "+OK");
  ASSERT_TRUE(WaitUntil([&] {
    return MetricValue(HttpGet(http, "/metrics"),
                       "lavik_storage_durability_pending") == 0;
  }));
  {
    std::ofstream marker(failure);
  }
  // Fencing may close this connection before the command error is delivered.
  // The HTTP endpoint must remain available but stop collecting storage.
  try {
    EXPECT_TRUE(client.Command({"FLUSHALL"}).starts_with('-'));
  } catch (const std::runtime_error&) {
  }
  ASSERT_TRUE(WaitUntil([&] {
    return MetricValue(HttpGet(http, "/metrics"), "lavik_server_ready") == 0;
  }));
  const auto response = HttpGet(http, "/metrics");
  EXPECT_TRUE(response.starts_with("HTTP/1.1 200 OK\r\n"));
  EXPECT_EQ(response.find("lavik_storage_"), std::string::npos);
  std::istringstream body(response.substr(response.find("\r\n\r\n") + 4));
  std::string line;
  unsigned samples = 0;
  while (std::getline(body, line)) {
    if (!line.empty() && line.front() != '#') ++samples;
  }
  EXPECT_EQ(samples, 1);
  // The failed durability epoch remains fenced. The process fixture kills it
  // on scope exit; a graceful successful shutdown is not expected here.
#endif
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
  EXPECT_NE(reset.find("cmdstat_config|resetstat:calls=1,"), std::string::npos);

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
