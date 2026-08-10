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
    for (std::string_view arg : args) {
      request += "$" + std::to_string(arg.size()) + "\r\n";
      request.append(arg);
      request += "\r\n";
    }
    SendAll(fd_, request);
    std::string response;
    while (!response.ends_with("\r\n")) {
      char byte = 0;
      const ssize_t received = ::recv(fd_, &byte, 1, 0);
      if (received < 0 && errno == EINTR) {
        continue;
      }
      if (received != 1) {
        throw std::runtime_error("failed to read RESP response");
      }
      response.push_back(byte);
    }
    response.resize(response.size() - 2);
    return response;
  }

 private:
  int fd_ = -1;
};

class ServerProcess {
 public:
  ServerProcess(std::string_view binary, std::uint16_t port,
                std::string_view data_path, std::string_view log_path,
                unsigned threads) {
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
          "--port",
          std::to_string(port),
          "--threads",
          std::to_string(threads),
          "--recv-buffers",
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
