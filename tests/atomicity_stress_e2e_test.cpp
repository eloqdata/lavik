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

// Serializability stress: writers atomically MSET overlapping key pairs
// while readers snapshot the keys with MGET and MULTI/EXEC. Any torn or
// reordered transaction breaks the invariants below.
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <exception>
#include <fstream>
#include <iostream>
#include <iterator>
#include <latch>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "support/test_data_path.h"

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
    return ReadReply();
  }

  // Splits a flat array-of-bulks reply into element payloads ("" for nil).
  static std::vector<std::string> ParseFlatArray(const std::string& reply) {
    std::vector<std::string> values;
    std::size_t pos = reply.find("\r\n");
    if (pos == std::string::npos) {
      Fail("not an array: " + reply);
    }
    pos += 2;
    while (pos < reply.size()) {
      if (reply.compare(pos, 3, "$-1") == 0) {
        values.emplace_back();
        pos += 5;
        continue;
      }
      if (reply[pos] != '$') {
        Fail("unexpected array element in: " + reply);
      }
      const std::size_t line_end = reply.find("\r\n", pos);
      std::size_t size = 0;
      std::from_chars(reply.data() + pos + 1, reply.data() + line_end, size);
      values.emplace_back(reply.substr(line_end + 2, size));
      pos = line_end + 2 + size + 2;
    }
    return values;
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
  Fail("timed out connecting to Lavik");
}

RespClient ConnectReady(std::uint16_t port) {
  const auto deadline = std::chrono::steady_clock::now() + 20s;
  while (std::chrono::steady_clock::now() < deadline) {
    try {
      RespClient client = Connect(port);
      if (client.Command({"PING"}) == "+PONG") return client;
    } catch (const std::exception&) {
      // Rapid same-port restarts can complete a loopback handshake against
      // the previous process generation. Reconnect until the command path
      // proves this socket belongs to the ready server.
    }
    std::this_thread::sleep_for(10ms);
  }
  Fail("timed out waiting for Lavik readiness");
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
          "4",
          "--no-pin-workers",
          "--recv-buffers-per-worker",
          "0",
          "--flush-max-ms",
          "1",
          "--flush-size-kb",
          "4",
          "--data-file",
          data_path,
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
          Fail("Lavik exited unsuccessfully");
        }
        return;
      }
      if (result < 0) Fail("waitpid failed");
      std::this_thread::sleep_for(10ms);
    }
    Fail("Lavik did not stop");
  }

 private:
  pid_t pid_ = -1;
};

std::string ReadFile(const std::string& path) {
  std::ifstream input(path);
  return std::string(std::istreambuf_iterator<char>(input),
                     std::istreambuf_iterator<char>());
}

// Bound the append workload independently of host throughput. A timed loop
// can exhaust the fixed device with outstanding transaction generations before
// cleaning catches up, turning the snapshot test into a capacity race. Four
// writers each perform this many acknowledged mutations while readers contend.
constexpr std::uint64_t kWritesPerWriter = 2000;
std::atomic<bool> stop_flag{false};
std::mutex failure_mutex;
std::string failure_message;

void ReportFailure(const std::string& message) {
  {
    std::lock_guard<std::mutex> lock(failure_mutex);
    if (failure_message.empty()) {
      failure_message = message;
    }
  }
  stop_flag.store(true, std::memory_order_release);
}

struct StressStart {
  std::latch readers_connected_{4};
  std::latch run_{1};
  std::latch readers_observed_{4};
};

// Each reader owns one arrival at each checkpoint. Unwinding a failed reader
// releases its arrivals after ReportFailure sets stop_flag, so neither main
// nor a writer can hang waiting for a connection or snapshot that will not
// come.
class ReaderProgress {
 public:
  explicit ReaderProgress(StressStart& start) : start_(start) {}
  ReaderProgress(const ReaderProgress&) = delete;
  ReaderProgress& operator=(const ReaderProgress&) = delete;
  ~ReaderProgress() {
    Connected();
    Observed();
  }
  void Connected() {
    if (!std::exchange(connected_, true))
      start_.readers_connected_.count_down();
  }
  void Observed() {
    if (!std::exchange(observed_, true)) start_.readers_observed_.count_down();
  }

 private:
  StressStart& start_;
  bool connected_ = false;
  bool observed_ = false;
};

// W1 atomically writes {a, b}, W2 atomically writes {b, c}; each value is
// writer-tagged. Any snapshot where b carries W1's tag must show a == b, and
// any snapshot where b carries W2's tag must show c == b.
void Writer(std::uint16_t port, const char* tag, const char* first,
            const char* second, StressStart& start) {
  try {
    RespClient client = Connect(port);
    start.run_.wait();
    for (std::uint64_t i = 1;
         i <= kWritesPerWriter && !stop_flag.load(std::memory_order_acquire);
         ++i) {
      const std::string value = std::string(tag) + std::to_string(i);
      const std::string reply =
          client.Command({"MSET", first, value, second, value});
      if (reply != "+OK") {
        ReportFailure("writer MSET failed: " + reply);
        return;
      }
      // Keep the write interval open until every connected reader has
      // actually completed a snapshot, even if that reader is descheduled.
      if (i == 1) start.readers_observed_.wait();
    }
  } catch (const std::exception& error) {
    ReportFailure(std::string("writer: ") + error.what());
  }
}

void CheckSnapshot(const std::vector<std::string>& abc, const char* context) {
  if (abc.size() != 3) {
    ReportFailure(std::string(context) + ": expected 3 values");
    return;
  }
  const std::string& a = abc[0];
  const std::string& b = abc[1];
  const std::string& c = abc[2];
  if (b.starts_with("W1:") && a != b) {
    ReportFailure(std::string(context) + ": torn W1 write: a='" + a + "' b='" +
                  b + "' c='" + c + "'");
  } else if (b.starts_with("W2:") && c != b) {
    ReportFailure(std::string(context) + ": torn W2 write: a='" + a + "' b='" +
                  b + "' c='" + c + "'");
  }
}

void MgetReader(std::uint16_t port, std::uint64_t& snapshots,
                StressStart& start) {
  ReaderProgress progress(start);
  try {
    RespClient client = Connect(port);
    progress.Connected();
    start.run_.wait();
    while (!stop_flag.load(std::memory_order_acquire)) {
      const std::string reply = client.Command({"MGET", "sa", "sb", "sc"});
      CheckSnapshot(RespClient::ParseFlatArray(reply), "MGET");
      ++snapshots;
      progress.Observed();
    }
  } catch (const std::exception& error) {
    ReportFailure(std::string("mget reader: ") + error.what());
  }
}

void ExecReader(std::uint16_t port, std::uint64_t& snapshots,
                StressStart& start) {
  ReaderProgress progress(start);
  try {
    RespClient client = Connect(port);
    progress.Connected();
    start.run_.wait();
    while (!stop_flag.load(std::memory_order_acquire)) {
      if (client.Command({"MULTI"}) != "+OK") {
        ReportFailure("exec reader MULTI failed");
        return;
      }
      client.Command({"GET", "sa"});
      client.Command({"GET", "sb"});
      client.Command({"GET", "sc"});
      const std::string reply = client.Command({"EXEC"});
      CheckSnapshot(RespClient::ParseFlatArray(reply), "EXEC");
      ++snapshots;
      progress.Observed();
    }
  } catch (const std::exception& error) {
    ReportFailure(std::string("exec reader: ") + error.what());
  }
}

// Two writers hammer the same pair with their own tags; every snapshot must
// show both keys equal.
void PairWriter(std::uint16_t port, const char* tag, StressStart& start) {
  try {
    RespClient client = Connect(port);
    start.run_.wait();
    for (std::uint64_t i = 1;
         i <= kWritesPerWriter && !stop_flag.load(std::memory_order_acquire);
         ++i) {
      const std::string value = std::string(tag) + std::to_string(i);
      const std::string reply =
          client.Command({"MSET", "ha", value, "hb", value});
      if (reply != "+OK") {
        ReportFailure("pair writer MSET failed: " + reply);
        return;
      }
      // Keep the write interval open until every connected reader has
      // actually completed a snapshot, even if that reader is descheduled.
      if (i == 1) start.readers_observed_.wait();
    }
  } catch (const std::exception& error) {
    ReportFailure(std::string("pair writer: ") + error.what());
  }
}

void PairReader(std::uint16_t port, std::uint64_t& snapshots,
                StressStart& start) {
  ReaderProgress progress(start);
  try {
    RespClient client = Connect(port);
    progress.Connected();
    start.run_.wait();
    while (!stop_flag.load(std::memory_order_acquire)) {
      const std::string reply = client.Command({"MGET", "ha", "hb"});
      const auto values = RespClient::ParseFlatArray(reply);
      if (values.size() != 2) {
        ReportFailure("pair MGET: expected 2 values");
        return;
      }
      if (values[0] != values[1]) {
        ReportFailure("torn pair: ha='" + values[0] + "' hb='" + values[1] +
                      "'");
      }
      ++snapshots;
      progress.Observed();
    }
  } catch (const std::exception& error) {
    ReportFailure(std::string("pair reader: ") + error.what());
  }
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "usage: atomicity_stress_e2e_test /path/to/lavik\n";
    return 1;
  }
  const std::string suffix = std::to_string(::getpid());
  const std::string data_path =
      lavik::test::TestDataPath("lavik-stress-" + suffix + ".data");
  const std::string log_path =
      lavik::test::TestDataPath("lavik-stress-" + suffix + ".log");
  (void)::unlink(data_path.c_str());
  (void)::unlink(log_path.c_str());

  int exit_code = 0;
  try {
    const std::uint16_t port = FindFreePort();
    CreateDataFile(data_path, 512ULL * 1024 * 1024);
    std::vector<std::string> final_values;
    {
      ServerProcess server(argv[1], port, data_path, log_path);
      {
        RespClient seed = Connect(port);
        // Exercise online cleaning without rotating sparse per-worker 8 MiB
        // transaction blocks every millisecond. Rotation can otherwise exhaust
        // this small device before old generations become reclaimable, even
        // with a bounded number of writes.
        if (seed.Command({"CONFIG", "SET", "tx-cleaner-cooldown-ms", "100"}) !=
            "+OK") {
          Fail("enabling transaction cleaner failed");
        }
        if (seed.Command({"MSET", "sa", "W1:0", "sb", "W1:0", "ha", "H0", "hb",
                          "H0"}) != "+OK") {
          Fail("seed MSET failed");
        }
      }

      // Each reader owns one counter; main inspects it only after joining.
      // Connection readiness and actual snapshot progress are separate gates.
      StressStart start;
      std::array<std::uint64_t, 4> snapshots{};
      std::vector<std::thread> writers, readers;
      writers.emplace_back([&] { Writer(port, "W1:", "sa", "sb", start); });
      writers.emplace_back([&] { Writer(port, "W2:", "sb", "sc", start); });
      writers.emplace_back([&] { PairWriter(port, "P1:", start); });
      writers.emplace_back([&] { PairWriter(port, "P2:", start); });
      readers.emplace_back([&] { MgetReader(port, snapshots[0], start); });
      readers.emplace_back([&] { MgetReader(port, snapshots[1], start); });
      readers.emplace_back([&] { ExecReader(port, snapshots[2], start); });
      readers.emplace_back([&] { PairReader(port, snapshots[3], start); });

      start.readers_connected_.wait();
      start.run_.count_down();
      for (auto& writer : writers) writer.join();
      stop_flag.store(true, std::memory_order_release);
      for (auto& reader : readers) reader.join();
      if (!failure_message.empty()) {
        Fail(failure_message);
      }
      for (auto count : snapshots) {
        if (count == 0) Fail("a reader did not observe any snapshots");
      }
      std::cout << "completed " << 4 * kWritesPerWriter
                << " writes; reader snapshots=" << snapshots[0] << ','
                << snapshots[1] << ',' << snapshots[2] << ',' << snapshots[3]
                << '\n';

      RespClient final = Connect(port);
      final_values = RespClient::ParseFlatArray(
          final.Command({"MGET", "sa", "sb", "sc", "ha", "hb"}));
      if (final_values.size() != 5) {
        Fail("final MGET did not return five values");
      }
      server.Stop();
    }

    {
      ServerProcess server(argv[1], port, data_path, log_path);
      RespClient recovered = ConnectReady(port);
      const auto recovered_values = RespClient::ParseFlatArray(
          recovered.Command({"MGET", "sa", "sb", "sc", "ha", "hb"}));
      if (recovered_values != final_values) {
        Fail("graceful restart did not recover the last acknowledged values");
      }
    }
  } catch (const std::exception& error) {
    std::cerr << error.what() << "\n--- Lavik log ---\n"
              << ReadFile(log_path) << std::flush;
    exit_code = 1;
  }

  (void)::unlink(data_path.c_str());
  (void)::unlink(log_path.c_str());
  std::cout << (exit_code == 0 ? "atomicity stress passed\n" : "")
            << std::flush;
  return exit_code;
}
