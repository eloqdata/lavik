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
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>
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
          binary,
          "--logtostderr",
          "--port",
          std::to_string(port),
          "--threads",
          "1",
          "--recv-buffers-per-worker",
          "0",
          "--flush-max-ms",
          "20",
          "--data-file",
          data_path,
          "--tomb-raider-interval-ms",
          "500",
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
  if (!reply.starts_with(':'))
    Fail(std::string(operation) + " was not integer");
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

long long StatField(RespClient& client, std::string_view field) {
  const std::string info = client.Command({"INFO", "stats"});
  const std::string needle = std::string(field) + ":";
  const std::size_t at = info.find(needle);
  if (at == std::string::npos) Fail("INFO missing " + std::string(field));
  long long value = 0;
  const char* begin = info.data() + at + needle.size();
  const auto [parsed_end, error] =
      std::from_chars(begin, info.data() + info.size(), value);
  if (error != std::errc{} || parsed_end == begin) {
    Fail("INFO field malformed: " + std::string(field));
  }
  return value;
}

// Waits until tomb_raider_rounds advances past `floor`, so an assertion
// about reap totals is made only after a full round observed the state the
// test just arranged.
long long AwaitRoundBeyond(RespClient& client, long long floor) {
  const auto deadline = std::chrono::steady_clock::now() + 30s;
  while (std::chrono::steady_clock::now() < deadline) {
    const long long rounds = StatField(client, "tomb_raider_rounds");
    if (rounds > floor) return rounds;
    std::this_thread::sleep_for(50ms);
  }
  Fail("tomb raider round did not complete in time");
}

std::string LocalTimeAfter(std::chrono::seconds offset) {
  const std::time_t target = std::time(nullptr) + offset.count();
  std::tm local{};
  if (::localtime_r(&target, &local) == nullptr) {
    Fail("localtime_r failed");
  }
  char buffer[9]{};
  if (std::snprintf(buffer, sizeof(buffer), "%02d:%02d:%02d", local.tm_hour,
                    local.tm_min, local.tm_sec) != 8) {
    Fail("daily time formatting failed");
  }
  return buffer;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "usage: tomb_raider_e2e_test /path/to/keylane\n";
    return 2;
  }
  const std::string prefix =
      "/tmp/keylane-tombraider-" + std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  (void)::unlink(data_path.c_str());
  (void)::unlink(log_path.c_str());

  try {
    const std::uint16_t port = FindFreePort();
    CreateDataFile(data_path, 192ULL * 1024 * 1024);
    {
      ServerProcess server(argv[1], port, data_path, log_path);
      RespClient client = Connect(port);
      Expect(client.Command({"PING"}), "+PONG", "PING");
      Expect(client.Command(
                 {"CONFIG", "SET", "defrag-max-active-per-device", "1"}),
             "+OK", "CONFIG defrag max active");
      Expect(client.Command({"CONFIG", "SET", "defrag-sleep-ms", "25"}),
             "+OK", "CONFIG defrag block sleep");
      Expect(
          client.Command({"CONFIG", "SET", "defrag-record-sleep-us", "7"}),
          "+OK", "CONFIG defrag record sleep");
      Expect(client.Command({"CONFIG", "SET", "defrag-paused", "yes"}),
             "+OK", "CONFIG defrag paused");
      const std::string defrag_status =
          client.Command({"DEFRAG", "STATUS"});
      if (defrag_status.find("paused=1") == std::string::npos ||
          defrag_status.find("max_active_per_device=1") == std::string::npos ||
          defrag_status.find("block_sleep_ms=25") == std::string::npos ||
          defrag_status.find("record_sleep_us=7") == std::string::npos) {
        Fail("DEFRAG STATUS did not report runtime settings");
      }
      if (StatField(client, "defrag_max_active_per_device") != 1 ||
          StatField(client, "defrag_paused") != 1 ||
          StatField(client, "defrag_block_sleep_ms") != 25 ||
          StatField(client, "defrag_record_sleep_us") != 7) {
        Fail("INFO stats did not report defrag runtime settings");
      }
      Expect(client.Command({"DEFRAG", "RESUME"}), "+OK",
             "DEFRAG RESUME");
      Expect(client.Command({"DEFRAG", "MAX-ACTIVE", "0"}),
             "-ERR value is not an integer or out of range",
             "DEFRAG zero concurrency");
      Expect(client.Command({"DEFRAG", "INVALID", "1"}),
             "-ERR syntax error", "DEFRAG invalid setting");
      Expect(client.Command({"DEFRAG", "BLOCK-SLEEP-MS", "0"}), "+OK",
             "DEFRAG reset block sleep");
      Expect(client.Command({"DEFRAG", "RECORD-SLEEP-US", "0"}), "+OK",
             "DEFRAG reset record sleep");
      Expect(client.Command({"CONFIG", "SET", "tomb-raider-mode", "off"}),
             "+OK", "CONFIG tomb raider off");
      const long long disabled_rounds = StatField(client, "tomb_raider_rounds");
      std::this_thread::sleep_for(700ms);
      if (StatField(client, "tomb_raider_rounds") != disabled_rounds) {
        Fail("tomb raider ran while disabled");
      }
      if (StatField(client, "tomb_raider_enabled") != 0) {
        Fail("tomb raider did not report disabled");
      }
      if (client.Command({"TOMBRAIDER", "STATUS"}).find("mode=off") ==
          std::string::npos) {
        Fail("TOMBRAIDER STATUS did not report off mode");
      }
      Expect(client.Command({"TOMBRAIDER", "INVALID"}), "-ERR syntax error",
             "TOMBRAIDER invalid mode");
      Expect(client.Command({"TOMBRAIDER", "INTERVAL", "0"}),
             "-ERR value is not an integer or out of range",
             "TOMBRAIDER zero interval");
      Expect(client.Command(
                 {"CONFIG", "SET", "tomb-raider-sleep-ms", "0"}),
             "+OK", "CONFIG tomb raider block sleep");
      if (StatField(client, "tomb_raider_block_sleep_ms") != 0) {
        Fail("tomb raider did not update block sleep");
      }

      const std::string daily = LocalTimeAfter(2s);
      Expect(client.Command(
                 {"CONFIG", "SET", "tomb-raider-daily-time", daily}),
             "+OK", "CONFIG tomb raider daily");
      if (client.Command({"TOMBRAIDER", "STATUS"}).find("mode=daily") ==
          std::string::npos) {
        Fail("TOMBRAIDER STATUS did not report daily mode");
      }
      const long long daily_rounds = AwaitRoundBeyond(client, disabled_rounds);
      std::this_thread::sleep_for(1200ms);
      if (StatField(client, "tomb_raider_rounds") != daily_rounds) {
        Fail("daily tomb raider ran more than once");
      }
      Expect(client.Command({"TOMBRAIDER", "OFF"}), "+OK",
             "TOMBRAIDER daily OFF");
      Expect(client.Command({"TOMBRAIDER", "ON"}), "+OK", "TOMBRAIDER ON");
      if (client.Command({"TOMBRAIDER", "STATUS"}).find("mode=daily") ==
          std::string::npos) {
        Fail("TOMBRAIDER ON did not restore daily mode");
      }
      Expect(client.Command(
                 {"CONFIG", "SET", "tomb-raider-interval-ms", "500"}),
             "+OK", "CONFIG tomb raider interval");
      if (StatField(client, "tomb_raider_enabled") != 1) {
        Fail("tomb raider did not report enabled");
      }

      // Reapable: the only older record expires on its own, after which
      // nothing on disk needs the tombstone.
      Expect(client.Command({"SET", "reapable", "v", "PX", "100"}), "+OK",
             "reapable SET");
      Expect(client.Command({"DEL", "reapable"}), ":1", "reapable DEL");
      // Not reapable: the buried value never expires, so the tombstone is
      // the only thing standing between it and resurrection.
      Expect(client.Command({"SET", "kept", "v"}), "+OK", "kept SET");
      Expect(client.Command({"DEL", "kept"}), ":1", "kept DEL");
      // Untouched live key, as a control across the restart below.
      Expect(client.Command({"SET", "control", "c"}), "+OK", "control SET");

      // Let the buried TTL lapse, then require a round that started after
      // that: its sweep must see the value as expired and reap exactly the
      // one tombstone.
      std::this_thread::sleep_for(200ms);
      long long rounds = AwaitRoundBeyond(client, 0);
      rounds = AwaitRoundBeyond(client, rounds);
      const auto deadline = std::chrono::steady_clock::now() + 30s;
      while (StatField(client, "tomb_raider_reaped") < 1) {
        if (std::chrono::steady_clock::now() > deadline) {
          Fail("reapable tombstone was never reaped");
        }
        std::this_thread::sleep_for(50ms);
      }

      // Two more full rounds: the reaped total must stay at exactly one —
      // the permanent value keeps claiming its tombstone every sweep.
      rounds = AwaitRoundBeyond(client, rounds);
      (void)AwaitRoundBeyond(client, rounds);
      const long long reaped = StatField(client, "tomb_raider_reaped");
      if (reaped != 1) {
        Fail("expected exactly one reap, saw " + std::to_string(reaped));
      }

      Expect(client.Command({"GET", "reapable"}), "$-1", "reapable GET");
      Expect(client.Command({"GET", "kept"}), "$-1", "kept GET");
      Expect(client.Command({"GET", "control"}), "$1\r\nc", "control GET");
      server.Stop();
    }

    // The kept tombstone must have survived to suppress the permanent
    // value across recovery; the reaped one must stay gone without it.
    {
      ServerProcess server(argv[1], port, data_path, log_path);
      RespClient client = Connect(port);
      Expect(client.Command({"GET", "kept"}), "$-1", "restart kept GET");
      Expect(client.Command({"GET", "reapable"}), "$-1",
             "restart reapable GET");
      Expect(client.Command({"GET", "control"}), "$1\r\nc",
             "restart control GET");
      server.Stop();
    }
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    std::cerr << "--- Keylane log ---\n" << ReadFile(log_path);
    (void)::unlink(data_path.c_str());
    (void)::unlink(log_path.c_str());
    return 1;
  }
  (void)::unlink(data_path.c_str());
  (void)::unlink(log_path.c_str());
  std::cout << "tomb raider e2e passed\n";
  return 0;
}
