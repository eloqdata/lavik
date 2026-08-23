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
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

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
    std::string request = "*" + std::to_string(args.size()) + "\r\n";
    for (const std::string_view arg : args) {
      request += "$" + std::to_string(arg.size()) + "\r\n";
      request.append(arg);
      request.append("\r\n");
    }
    SendAll(fd_, request);
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

 private:
  int fd_ = -1;
};

class ServerProcess {
 public:
  ServerProcess(std::string_view binary, std::uint16_t redis_port,
                std::uint16_t metrics_port, std::string_view data_path,
                std::string_view log_path) {
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
          "2",
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
    FAIL() << "Keylane did not stop";
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

TEST(MetricsE2eTest, ExposesPrometheusCommandStorageAndDefragMetrics) {
  ASSERT_FALSE(g_keylane_binary.empty());
  const std::string prefix =
      "/tmp/keylane-metrics-e2e-" + std::to_string(::getpid());
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
  ServerProcess server(g_keylane_binary, redis_port, metrics_port, data_path,
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
  EXPECT_GE(MetricValue(body, "keylane_commands_total"), 3);
  EXPECT_EQ(MetricValue(body, "keylane_server_ready"), 1);
  const std::uint64_t connections = MetricValue(body, "keylane_connections");
  const std::uint64_t connected_clients =
      MetricValue(body, "keylane_connected_clients");
  EXPECT_GE(connections, 2);
  EXPECT_EQ(connected_clients, 1);
  EXPECT_LE(connected_clients, connections);
  EXPECT_EQ(MetricValue(body, "keylane_blocked_clients"), 0);
  EXPECT_EQ(MetricValue(body, "keylane_replication_control_connections"), 0);
  EXPECT_EQ(MetricValue(body, "keylane_replication_flow_connections"), 0);
  // The lazy shared backlog is enabled on the first downstream handshake.
  EXPECT_EQ(MetricValue(body, "keylane_replication_backlog_capacity_bytes"), 0);
  EXPECT_EQ(MetricValue(body, "keylane_replication_backlog_pinned_cursors"), 0);
  EXPECT_GT(
      MetricValue(body, "keylane_replication_publish_queue_capacity_bytes"), 0);
  EXPECT_EQ(MetricValue(body, "keylane_fullsync_publish_queue_bytes"), 0);
  EXPECT_EQ(MetricValue(body, "keylane_fullsync_publish_queue_admitted_bytes"),
            0);
  EXPECT_EQ(MetricValue(body, "keylane_fullsync_publish_queue_capacity_bytes"),
            0);
  EXPECT_EQ(MetricValue(body, "keylane_fullsync_sessions"), 0);
  EXPECT_EQ(
      MetricValue(body,
                  "keylane_fullsync_publish_queue_backpressure_waits_total"),
      0);
  EXPECT_GT(MetricValue(body, "keylane_memory_current_bytes"), 0);
  EXPECT_GT(MetricValue(body, "keylane_memory_rss_bytes"), 0);
  EXPECT_EQ(MetricValue(body, "keylane_memory_max_bytes"), 1073741824);
  EXPECT_EQ(MetricValue(body, "keylane_memory_rejected_commands_total"), 0);
  EXPECT_GT(
      MetricValue(body,
                  "keylane_storage_io_operations_total{operation=\"write\"}"),
      0);
  EXPECT_GT(
      MetricValue(
          body, "keylane_storage_io_operations_total{operation=\"fdatasync\"}"),
      0);
  EXPECT_GT(
      MetricValue(body, "keylane_storage_io_bytes_total{operation=\"write\"}"),
      0);
  EXPECT_GT(
      MetricValue(body,
                  "keylane_storage_io_bytes_total{operation=\"fdatasync\"}"),
      0);
  EXPECT_NE(
      body.find("keylane_storage_io_operations_total{operation=\"read\"} "),
      std::string_view::npos);
  EXPECT_NE(body.find("keylane_storage_io_bytes_total{operation=\"read\"} "),
            std::string_view::npos);
  EXPECT_GT(MetricValue(body, "keylane_storage_capacity_bytes"), 0);
  EXPECT_GT(MetricValue(body, "keylane_storage_available_bytes"), 0);
  EXPECT_GT(MetricValue(body, "keylane_filesystem_available_bytes"), 0);
  EXPECT_NE(body.find("keylane_command_calls_total{command=\"ping\"} 1"),
            std::string_view::npos);
  EXPECT_NE(body.find("keylane_command_calls_total{command=\"set\"} 3"),
            std::string_view::npos);
  EXPECT_NE(body.find("keylane_command_calls_total{command=\"get\"} 1"),
            std::string_view::npos);
  EXPECT_NE(body.find("keylane_command_calls_total{command=\"del\"} 1"),
            std::string_view::npos);
  EXPECT_NE(body.find("keylane_command_calls_total{command=\"unlink\"} 1"),
            std::string_view::npos);
  EXPECT_NE(body.find("keylane_command_calls_total{command=\"append\"} 1"),
            std::string_view::npos);
  EXPECT_NE(body.find("keylane_command_calls_total{command=\"decr\"} 1"),
            std::string_view::npos);
  EXPECT_NE(body.find("keylane_command_calls_total{command=\"msetnx\"} 1"),
            std::string_view::npos);
  EXPECT_NE(body.find("keylane_command_calls_total{command=\"lcs\"} 1"),
            std::string_view::npos);
  for (const char* command : {"touch", "randomkey", "copy", "expireat",
                              "expiretime", "pexpireat", "pexpiretime"}) {
    EXPECT_NE(body.find("keylane_command_calls_total{command=\"" +
                        std::string(command) + "\"} 1"),
              std::string_view::npos);
  }
  EXPECT_NE(
      body.find("keylane_command_duration_seconds_bucket{command=\"get\""),
      std::string_view::npos);
  EXPECT_NE(body.find("keylane_command_duration_seconds_bucket{command=\"get\","
                      "le=\"0.003\"}"),
            std::string_view::npos);
  EXPECT_NE(body.find("keylane_storage_defrag_runs_total{result=\"success\"}"),
            std::string_view::npos);
  EXPECT_NE(body.find("keylane_storage_defrag_active "),
            std::string_view::npos);
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
    g_keylane_binary = argv[1];
    for (int index = 1; index + 1 < argc; ++index) {
      argv[index] = argv[index + 1];
    }
    --argc;
  }
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
