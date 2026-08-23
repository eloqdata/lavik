#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <charconv>
#include <array>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <iostream>
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

void WriteFile(const std::string& path, std::string_view contents) {
  const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
  if (fd < 0) Fail("failed to create " + path);
  while (!contents.empty()) {
    const ssize_t written = ::write(fd, contents.data(), contents.size());
    if (written < 0 && errno == EINTR) continue;
    if (written <= 0) Fail("failed to write " + path);
    contents.remove_prefix(static_cast<std::size_t>(written));
  }
  if (::close(fd) != 0) Fail("failed to close " + path);
}

void CreateDataFile(const std::string& path) {
  const int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_EXCL, 0600);
  if (fd < 0) Fail("failed to create data file");
  const int allocated = ::posix_fallocate(fd, 0, 128ULL * 1024 * 1024);
  const int closed = ::close(fd);
  if (allocated != 0 || closed != 0) Fail("failed to allocate data file");
}

class TempDirectory {
 public:
  TempDirectory() {
    char pattern[] = "/tmp/keylane-sentinel-e2e-XXXXXX";
    char* created = ::mkdtemp(pattern);
    if (created == nullptr) Fail("mkdtemp failed");
    path_ = created;
  }
  ~TempDirectory() {
    if (std::uncaught_exceptions() == 0) {
      std::filesystem::remove_all(path_);
    } else {
      std::cerr << "sentinel e2e artifacts retained at " << path_ << '\n';
    }
  }
  const std::string& path() const { return path_; }

 private:
  std::string path_;
};

class ChildProcess {
 public:
  ChildProcess(std::vector<std::string> args, const std::string& log) {
    pid_ = ::fork();
    if (pid_ < 0) Fail("fork failed");
    if (pid_ != 0) return;
    const int log_fd = ::open(log.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0600);
    if (log_fd >= 0) {
      (void)::dup2(log_fd, STDOUT_FILENO);
      (void)::dup2(log_fd, STDERR_FILENO);
      ::close(log_fd);
    }
    std::vector<char*> argv;
    for (std::string& arg : args) argv.push_back(arg.data());
    argv.push_back(nullptr);
    ::execv(args.front().c_str(), argv.data());
    _exit(127);
  }
  ChildProcess(const ChildProcess&) = delete;
  ChildProcess& operator=(const ChildProcess&) = delete;
  ~ChildProcess() { Stop(SIGKILL); }

  void Stop(int signal = SIGINT) {
    if (pid_ <= 0) return;
    (void)::kill(pid_, signal);
    (void)::waitpid(pid_, nullptr, 0);
    pid_ = -1;
  }

 private:
  pid_t pid_ = -1;
};

class RespClient {
 public:
  explicit RespClient(int fd) : fd_(fd) {}
  RespClient(const RespClient&) = delete;
  RespClient& operator=(const RespClient&) = delete;
  RespClient(RespClient&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }
  ~RespClient() {
    if (fd_ >= 0) ::close(fd_);
  }

  std::string Command(const std::vector<std::string_view>& args) {
    std::string wire = "*" + std::to_string(args.size()) + "\r\n";
    for (std::string_view arg : args) {
      wire += "$" + std::to_string(arg.size()) + "\r\n";
      wire.append(arg);
      wire += "\r\n";
    }
    SendAll(wire);
    return ReadReply();
  }

 private:
  static std::size_t Length(std::string_view line) {
    std::size_t value = 0;
    const auto parsed =
        std::from_chars(line.data() + 1, line.data() + line.size(), value);
    if (parsed.ec != std::errc{} || parsed.ptr != line.data() + line.size()) {
      Fail("invalid RESP length");
    }
    return value;
  }
  void SendAll(std::string_view bytes) {
    while (!bytes.empty()) {
      const ssize_t sent = ::send(fd_, bytes.data(), bytes.size(), MSG_NOSIGNAL);
      if (sent < 0 && errno == EINTR) continue;
      if (sent <= 0) Fail("send failed");
      bytes.remove_prefix(static_cast<std::size_t>(sent));
    }
  }
  void ReadExact(char* output, std::size_t bytes) {
    while (bytes != 0) {
      const ssize_t count = ::recv(fd_, output, bytes, 0);
      if (count < 0 && errno == EINTR) continue;
      if (count <= 0) Fail("connection closed while reading RESP");
      output += count;
      bytes -= static_cast<std::size_t>(count);
    }
  }
  std::string ReadLine() {
    std::string line;
    while (!line.ends_with("\r\n")) {
      char byte = 0;
      ReadExact(&byte, 1);
      line.push_back(byte);
      if (line.size() > 64 * 1024) Fail("RESP line too long");
    }
    line.resize(line.size() - 2);
    return line;
  }
  std::string ReadReply() {
    const std::string line = ReadLine();
    if (line.empty()) Fail("empty RESP reply");
    if (line[0] == '+' || line[0] == '-' || line[0] == ':') return line;
    if (line[0] == '$') {
      if (line == "$-1") return line;
      std::string body(Length(line) + 2, '\0');
      ReadExact(body.data(), body.size());
      body.resize(body.size() - 2);
      return line + "\r\n" + body;
    }
    if (line[0] == '*') {
      if (line == "*-1") return line;
      std::string result = line;
      for (std::size_t i = 0; i < Length(line); ++i) {
        result += "\r\n" + ReadReply();
      }
      return result;
    }
    Fail("unsupported RESP reply");
  }

  int fd_ = -1;
};

std::uint16_t FreePort() {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (fd < 0) Fail("socket failed");
  sockaddr_in address{.sin_family = AF_INET,
                      .sin_port = 0,
                      .sin_addr = {.s_addr = htonl(INADDR_LOOPBACK)}};
  if (::bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
    Fail("bind failed");
  }
  socklen_t size = sizeof(address);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &size) != 0) {
    Fail("getsockname failed");
  }
  ::close(fd);
  return ntohs(address.sin_port);
}

RespClient Connect(std::uint16_t port) {
  const auto deadline = std::chrono::steady_clock::now() + 20s;
  while (std::chrono::steady_clock::now() < deadline) {
    const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    timeval timeout{.tv_sec = 10, .tv_usec = 0};
    (void)::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    sockaddr_in address{.sin_family = AF_INET,
                        .sin_port = htons(port),
                        .sin_addr = {.s_addr = htonl(INADDR_LOOPBACK)}};
    if (::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) ==
        0) {
      return RespClient(fd);
    }
    ::close(fd);
    std::this_thread::sleep_for(20ms);
  }
  Fail("timed out connecting to port " + std::to_string(port));
}

void WaitUntil(std::string_view label, std::chrono::seconds timeout,
               const std::function<bool()>& ready) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    try {
      if (ready()) return;
    } catch (const std::exception&) {
    }
    std::this_thread::sleep_for(100ms);
  }
  Fail("timed out waiting for " + std::string(label));
}

std::vector<std::string> KeylaneArgs(const std::string& binary,
                                     const std::string& config,
                                     const std::string& data) {
  return {binary,
          config,
          "--data-file",
          data,
          "--threads",
          "1",
          "--no-pin-workers",
          "--logtostderr",
          "--recv-buffers-per-worker",
          "0"};
}

std::string Bulk(std::string_view value) {
  return "$" + std::to_string(value.size()) + "\r\n" + std::string(value);
}

std::size_t Count(std::string_view haystack, std::string_view needle) {
  std::size_t count = 0;
  for (std::size_t offset = 0;
       (offset = haystack.find(needle, offset)) != std::string_view::npos;
       offset += needle.size()) {
    ++count;
  }
  return count;
}

}  // namespace

int main(int argc, char** argv) {
  try {
    if (argc != 3) {
      Fail("usage: sentinel_e2e_test KEYLANE_BINARY REDIS_SERVER_BINARY");
    }
    TempDirectory root;
    const std::uint16_t master_port = FreePort();
    const std::uint16_t preferred_port = FreePort();
    const std::uint16_t other_port = FreePort();
    const std::array<std::uint16_t, 3> sentinel_ports{
        FreePort(), FreePort(), FreePort()};

    const std::string master_config = root.path() + "/master.conf";
    const std::string preferred_config = root.path() + "/preferred.conf";
    const std::string other_config = root.path() + "/other.conf";
    WriteFile(master_config, "port " + std::to_string(master_port) + "\n");
    WriteFile(preferred_config,
              "port " + std::to_string(preferred_port) + "\nreplicaof 127.0.0.1 " +
                  std::to_string(master_port) + "\nreplica-priority 10\n");
    WriteFile(other_config,
              "port " + std::to_string(other_port) + "\nreplicaof 127.0.0.1 " +
                  std::to_string(master_port) + "\nreplica-priority 100\n");
    const std::string master_data = root.path() + "/master.data";
    const std::string preferred_data = root.path() + "/preferred.data";
    const std::string other_data = root.path() + "/other.data";
    CreateDataFile(master_data);
    CreateDataFile(preferred_data);
    CreateDataFile(other_data);

    ChildProcess master(KeylaneArgs(argv[1], master_config, master_data),
                        root.path() + "/master.log");
    ChildProcess preferred(
        KeylaneArgs(argv[1], preferred_config, preferred_data),
        root.path() + "/preferred.log");
    ChildProcess other(KeylaneArgs(argv[1], other_config, other_data),
                       root.path() + "/other.log");

    WaitUntil("both replicas online", 60s, [&] {
      RespClient client = Connect(master_port);
      const std::string info = client.Command({"INFO", "replication"});
      return info.find("connected_slaves:2") != std::string::npos;
    });
    {
      RespClient client = Connect(master_port);
      if (client.Command({"SET", "sentinel-key", "before"}) != "+OK") {
        Fail("failed to write pre-failover value");
      }
    }
    WaitUntil("pre-failover value on both replicas", 30s, [&] {
      for (const std::uint16_t port : {preferred_port, other_port}) {
        RespClient client = Connect(port);
        (void)client.Command({"READONLY"});
        if (client.Command({"GET", "sentinel-key"}) != Bulk("before")) {
          return false;
        }
      }
      return true;
    });

    std::array<std::string, 3> sentinel_configs;
    for (std::size_t index = 0; index < sentinel_ports.size(); ++index) {
      sentinel_configs[index] =
          root.path() + "/sentinel-" + std::to_string(index) + ".conf";
      WriteFile(sentinel_configs[index],
                "port " + std::to_string(sentinel_ports[index]) + "\n" +
                    "dir " + root.path() + "\n" +
                    "sentinel monitor keylane 127.0.0.1 " +
                    std::to_string(master_port) + " 2\n" +
                    "sentinel down-after-milliseconds keylane 1000\n" +
                    "sentinel failover-timeout keylane 15000\n" +
                    "sentinel parallel-syncs keylane 1\n");
    }
    ChildProcess sentinel0(
        {argv[2], sentinel_configs[0], "--sentinel"},
        root.path() + "/sentinel-0.log");
    ChildProcess sentinel1(
        {argv[2], sentinel_configs[1], "--sentinel"},
        root.path() + "/sentinel-1.log");
    ChildProcess sentinel2(
        {argv[2], sentinel_configs[2], "--sentinel"},
        root.path() + "/sentinel-2.log");
    WaitUntil("all Sentinels discover replicas and each other", 45s, [&] {
      for (const std::uint16_t sentinel_port : sentinel_ports) {
        RespClient client = Connect(sentinel_port);
        const std::string replicas =
            client.Command({"SENTINEL", "REPLICAS", "keylane"});
        if (replicas.find(std::to_string(preferred_port)) == std::string::npos ||
            replicas.find(std::to_string(other_port)) == std::string::npos ||
            Count(replicas, "$18\r\nmaster-link-status\r\n$2\r\nok") != 2) {
          return false;
        }
        const std::string peers =
            client.Command({"SENTINEL", "SENTINELS", "keylane"});
        if (!peers.starts_with("*2\r\n")) return false;
      }
      return true;
    });
    WaitUntil("three Sentinel named pubsub connections", 30s, [&] {
      RespClient client = Connect(master_port);
      const std::string list =
          client.Command({"CLIENT", "LIST", "TYPE", "pubsub"});
      return Count(list, "name=sentinel-") == 3 &&
             Count(list, "flags=P") == 3;
    });

    // Exercise actual quorum-based failure detection, not just the manual
    // SENTINEL FAILOVER path. Two of three Sentinels must agree the primary is
    // objectively down before one can win the leader election.
    master.Stop(SIGKILL);
    const std::string preferred_address =
        "*2\r\n$9\r\n127.0.0.1\r\n$" +
        std::to_string(std::to_string(preferred_port).size()) + "\r\n" +
        std::to_string(preferred_port);
    WaitUntil("quorum priority-selected master", 60s, [&] {
      for (const std::uint16_t sentinel_port : sentinel_ports) {
        RespClient client = Connect(sentinel_port);
        if (client.Command(
                {"SENTINEL", "GET-MASTER-ADDR-BY-NAME", "keylane"}) !=
            preferred_address) {
          return false;
        }
      }
      return true;
    });
    WaitUntil("promoted Keylane role", 30s, [&] {
      RespClient client = Connect(preferred_port);
      return client.Command({"ROLE"}).starts_with("*3\r\n$6\r\nmaster");
    });
    WaitUntil("remaining replica follows promoted master", 60s, [&] {
      RespClient client = Connect(other_port);
      const std::string info = client.Command({"INFO", "replication"});
      return info.find("master_port:" + std::to_string(preferred_port)) !=
                 std::string::npos &&
             info.find("master_link_status:up") != std::string::npos;
    });
    {
      RespClient promoted = Connect(preferred_port);
      if (promoted.Command({"GET", "sentinel-key"}) != Bulk("before")) {
        Fail("promoted replica lost pre-failover data");
      }
      if (promoted.Command({"SET", "sentinel-after", "ok"}) != "+OK") {
        Fail("promoted replica did not accept writes");
      }
    }
    WaitUntil("post-failover replication", 30s, [&] {
      RespClient client = Connect(other_port);
      (void)client.Command({"READONLY"});
      return client.Command({"GET", "sentinel-after"}) == Bulk("ok");
    });

    sentinel2.Stop();
    sentinel1.Stop();
    sentinel0.Stop();
    other.Stop();
    preferred.Stop();
    master.Stop();
    std::cout << "sentinel e2e passed\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "sentinel e2e failed: " << error.what() << '\n';
    return 1;
  }
}
