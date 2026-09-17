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
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <future>
#include <iostream>
#include <iterator>
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

  bool WaitForClose(std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
      char byte = 0;
      const ssize_t received = ::recv(fd_, &byte, 1, MSG_PEEK | MSG_DONTWAIT);
      if (received == 0) return true;
      if (received < 0 && errno != EINTR && errno != EAGAIN &&
          errno != EWOULDBLOCK) {
        Fail("recv failed while waiting for close: " +
             std::string(std::strerror(errno)));
      }
      std::this_thread::sleep_for(10ms);
    }
    return false;
  }

  // Send one wire batch before reading, then retain replies in wire order.
  std::vector<std::string> Pipeline(
      const std::vector<std::vector<std::string_view>>& commands) {
    std::string wire;
    for (const auto& args : commands) {
      wire += "*" + std::to_string(args.size()) + "\r\n";
      for (const auto arg : args) {
        wire += "$" + std::to_string(arg.size()) + "\r\n";
        wire.append(arg);
        wire += "\r\n";
      }
    }
    SendAll(wire);
    std::vector<std::string> replies;
    for (std::size_t i = 0; i < commands.size(); ++i) {
      replies.push_back(ReadReply());
    }
    return replies;
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
                const std::string& data_path, const std::string& log_path,
                std::string_view fail_tx_write = {},
                std::string_view tx_active_pause_ms = {},
                bool fail_tx_cleaner_once = false, unsigned threads = 4,
                std::string_view order_hold_ms = {},
                std::string_view standby_pause_ms = {},
                std::string_view fail_replication_transaction_containing = {},
                bool shutdown_checkpoint = false) {
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
      if (!fail_tx_write.empty()) {
        (void)::setenv("LAVIK_FAIL_TX_WRITE",
                       std::string(fail_tx_write).c_str(), 1);
      }
      if (!tx_active_pause_ms.empty()) {
        (void)::setenv("LAVIK_TX_ACTIVE_BLOCK_PAUSE_MS",
                       std::string(tx_active_pause_ms).c_str(), 1);
      }
      if (!order_hold_ms.empty()) {
        (void)::setenv("LAVIK_REPLICATION_ORDER_HOLD_MS",
                       std::string(order_hold_ms).c_str(), 1);
      }
      if (!standby_pause_ms.empty()) {
        (void)::setenv("LAVIK_STANDBY_PREFETCH_PAUSE_MS",
                       std::string(standby_pause_ms).c_str(), 1);
      }
      if (!fail_replication_transaction_containing.empty()) {
        (void)::setenv(
            "LAVIK_FAIL_REPLICATION_TRANSACTION_CONTAINING_ONCE",
            std::string(fail_replication_transaction_containing).c_str(), 1);
      }
      if (fail_tx_cleaner_once) {
        (void)::setenv("LAVIK_FAIL_TX_CLEANER_ONCE", "1", 1);
      }
      std::vector<std::string> arguments{
          binary,
          "--logtostderr",
          "--port",
          std::to_string(port),
          "--threads",
          std::to_string(threads),
          "--no-pin-workers",
          "--recv-buffers-per-worker",
          "0",
          "--flush-max-ms",
          "20",
          "--data-file",
          data_path,
      };
      if (shutdown_checkpoint) {
        arguments.push_back("--shutdown-checkpoint");
      }
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

bool WaitForLogMarker(const std::string& path, std::string_view marker,
                      std::chrono::seconds timeout = 20s) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (ReadFile(path).find(marker) != std::string::npos) return true;
    std::this_thread::sleep_for(10ms);
  }
  return false;
}

std::size_t CountOccurrences(std::string_view haystack,
                             std::string_view needle) {
  std::size_t count = 0;
  for (std::size_t offset = 0;
       (offset = haystack.find(needle, offset)) != std::string_view::npos;
       offset += needle.size()) {
    ++count;
  }
  return count;
}

bool WaitForLogMarkerCount(const std::string& path, std::string_view marker,
                           std::size_t expected,
                           std::chrono::seconds timeout = 20s) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (CountOccurrences(ReadFile(path), marker) >= expected) return true;
    std::this_thread::sleep_for(10ms);
  }
  return false;
}

std::uint64_t InfoUnsigned(RespClient& client, std::string_view section,
                           std::string_view marker) {
  const std::string info = client.Command({"INFO", section});
  const std::size_t begin = info.find(marker);
  if (begin == std::string::npos) Fail("INFO field is missing");
  const std::size_t value_begin = begin + marker.size();
  const std::size_t value_end = info.find("\r\n", value_begin);
  if (value_end == std::string::npos) Fail("malformed INFO field");
  std::uint64_t value = 0;
  const char* first = info.data() + value_begin;
  const char* last = info.data() + value_end;
  const auto [parsed, error] = std::from_chars(first, last, value);
  if (error != std::errc{} || parsed != last) {
    Fail("invalid INFO counter");
  }
  return value;
}

std::uint64_t InfoStat(RespClient& client, std::string_view marker) {
  return InfoUnsigned(client, "STATS", marker);
}

std::uint64_t TxCleanerRetiredGenerations(RespClient& client) {
  return InfoStat(client, "tx_cleaner_retired_generations:");
}

bool WaitForCleanerStat(RespClient& client, std::string_view marker,
                        std::uint64_t baseline,
                        std::chrono::seconds timeout = 30s) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (InfoStat(client, marker) > baseline) return true;
    std::this_thread::sleep_for(20ms);
  }
  return false;
}

bool WaitForInfoStat(RespClient& client, std::string_view marker,
                     std::uint64_t expected,
                     std::chrono::seconds timeout = 30s) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (InfoStat(client, marker) == expected) return true;
    std::this_thread::sleep_for(20ms);
  }
  return false;
}

bool WaitForCleanerRetirement(RespClient& client, std::uint64_t baseline) {
  return WaitForCleanerStat(client,
                            "tx_cleaner_retired_generations:", baseline);
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "usage: multikey_e2e_test /path/to/lavik\n";
    return 1;
  }
  const std::string suffix = std::to_string(::getpid());
  const std::string data_path =
      lavik::test::TestDataPath("lavik-multikey-" + suffix + ".data");
  const std::string log_path =
      lavik::test::TestDataPath("lavik-multikey-" + suffix + ".log");
  const std::string source_data = lavik::test::TestDataPath(
      "lavik-multikey-repl-source-" + suffix + ".data");
  const std::string replica_data = lavik::test::TestDataPath(
      "lavik-multikey-repl-replica-" + suffix + ".data");
  const std::string gate_source_data = lavik::test::TestDataPath(
      "lavik-multikey-gate-source-" + suffix + ".data");
  const std::string gate_replica_data = lavik::test::TestDataPath(
      "lavik-multikey-gate-replica-" + suffix + ".data");
  const std::string standby_data =
      lavik::test::TestDataPath("lavik-multikey-standby-" + suffix + ".data");
  const std::string gate_log_path =
      lavik::test::TestDataPath("lavik-multikey-gate-" + suffix + ".log");
  (void)::unlink(data_path.c_str());
  (void)::unlink(log_path.c_str());
  (void)::unlink(source_data.c_str());
  (void)::unlink(replica_data.c_str());
  (void)::unlink(gate_source_data.c_str());
  (void)::unlink(gate_replica_data.c_str());
  (void)::unlink(standby_data.c_str());
  (void)::unlink(gate_log_path.c_str());

  int exit_code = 0;
  try {
    const std::uint16_t port = FindFreePort();
    CreateDataFile(data_path, 512ULL * 1024 * 1024);
    ServerProcess server(argv[1], port, data_path, log_path);
    RespClient client = Connect(port);
    Expect(client.Command({"PING"}), "+PONG", "PING");
    Expect(client.Command({"CONFIG", "GET", "shutdown-checkpoint"}),
           "*2\r\n" + Bulk("shutdown-checkpoint") + "\r\n" + Bulk("no"),
           "shutdown checkpoint defaults off");
    Expect(client.Command({"CONFIG", "SET", "shutdown-checkpoint", "yes"}),
           "+OK", "enable shutdown checkpoint at runtime");
    Expect(client.Command({"CONFIG", "GET", "shutdown-checkpoint"}),
           "*2\r\n" + Bulk("shutdown-checkpoint") + "\r\n" + Bulk("yes"),
           "read enabled shutdown checkpoint");
    Expect(client.Command({"CONFIG", "SET", "shutdown-checkpoint", "maybe"}),
           "-ERR value must be 'yes' or 'no'",
           "reject invalid shutdown checkpoint setting");
    Expect(client.Command({"CONFIG", "SET", "shutdown-checkpoint", "no"}),
           "+OK", "disable shutdown checkpoint at runtime");

    Expect(client.Command({"RANDOMKEY"}), "$-1", "RANDOMKEY empty database");
    Expect(client.Command({"SELECT", "15"}), "+OK", "RANDOMKEY select db15");
    Expect(client.Command({"SET", "only-random-key", "v"}), "+OK",
           "RANDOMKEY seed");
    Expect(client.Command({"RANDOMKEY"}), Bulk("only-random-key"),
           "RANDOMKEY single key");
    Expect(client.Command({"PEXPIRE", "only-random-key", "0"}), ":1",
           "RANDOMKEY expire seed");
    Expect(client.Command({"RANDOMKEY"}), "$-1",
           "RANDOMKEY ignores expired key");
    Expect(client.Command({"SELECT", "0"}), "+OK", "RANDOMKEY back to db0");

    Expect(client.Command({"MSET", "touch-a", "1", "touch-b", "2"}), "+OK",
           "TOUCH seed");
    Expect(
        client.Command({"TOUCH", "touch-a", "missing", "touch-a", "touch-b"}),
        ":3", "TOUCH counts duplicate live keys");

    Expect(client.Command({"SET", "copy-source", "source", "EX", "60"}), "+OK",
           "COPY string seed");
    Expect(client.Command({"COPY", "copy-source", "copy-destination"}), ":1",
           "COPY string");
    Expect(client.Command({"GET", "copy-destination"}), Bulk("source"),
           "COPY string value");
    Expect(client.Command({"PERSIST", "copy-destination"}), ":1",
           "COPY preserves TTL");
    Expect(client.Command({"SET", "copy-destination", "old"}), "+OK",
           "COPY existing destination");
    Expect(client.Command({"COPY", "copy-source", "copy-destination"}), ":0",
           "COPY without REPLACE");
    Expect(client.Command({"GET", "copy-destination"}), Bulk("old"),
           "COPY leaves destination without REPLACE");
    Expect(
        client.Command({"COPY", "copy-source", "copy-destination", "REPLACE"}),
        ":1", "COPY REPLACE");
    Expect(client.Command({"GET", "copy-destination"}), Bulk("source"),
           "COPY REPLACE value");
    Expect(
        client.Command({"COPY", "missing-copy", "copy-destination", "REPLACE"}),
        ":0", "COPY missing source");
    Expect(client.Command({"COPY", "copy-source", "copy-source"}),
           "-ERR source and destination objects are the same",
           "COPY same object");

    Expect(client.Command({"LPUSH", "copy-list", "a", "b"}), ":2",
           "COPY list seed");
    Expect(client.Command({"COPY", "copy-list", "copy-list-destination"}), ":1",
           "COPY list");
    Expect(client.Command({"LLEN", "copy-list-destination"}), ":2",
           "COPY preserves collection type");

    Expect(client.Command({"COPY", "copy-source", "copy-db", "DB", "2"}), ":1",
           "COPY cross database");
    Expect(client.Command({"SELECT", "2"}), "+OK", "COPY select destination");
    Expect(client.Command({"GET", "copy-db"}), Bulk("source"),
           "COPY cross database value");
    Expect(client.Command({"PERSIST", "copy-db"}), ":1",
           "COPY cross database TTL");
    Expect(client.Command({"SELECT", "0"}), "+OK", "COPY return to db0");
    Expect(client.Command({"COPY", "copy-source", "x", "DB", "16"}),
           "-ERR DB index is out of range", "COPY invalid DB");
    Expect(client.Command({"COPY", "copy-source", "x", "DB", "01"}),
           "-ERR value is not an integer or out of range",
           "COPY rejects a non-canonical DB index");
    Expect(client.Command({"COPY", "copy-source", "x", "DB", "+1"}),
           "-ERR value is not an integer or out of range",
           "COPY rejects a signed positive DB index");
    Expect(client.Command({"COPY", "copy-source", "x", "UNKNOWN"}),
           "-ERR syntax error", "COPY invalid option");

    // Cross-shard MSET/MGET: values come back in request order regardless of
    // which worker owns each key.
    Expect(client.Command({"MSET", "mk0", "v0", "mk1", "v1", "mk2", "v2", "mk3",
                           "v3", "mk4", "v4", "mk5", "v5", "mk6", "v6", "mk7",
                           "v7"}),
           "+OK", "cross-shard MSET");
    Expect(client.Command({"MGET", "mk5", "mk0", "missing", "mk7", "mk2"}),
           "*5\r\n" + Bulk("v5") + "\r\n" + Bulk("v0") + "\r\n$-1\r\n" +
               Bulk("v7") + "\r\n" + Bulk("v2"),
           "shuffled MGET");
    Expect(client.Command({"GET", "mk3"}), Bulk("v3"), "single GET after MSET");

    // Progress regression for the multi-shard scheduler. These writers use
    // disjoint key sets, so their granted intents are conflict-free even when
    // lower-txid transactions are still waiting on unrelated shards. A
    // head-only queue can form a cross-shard wait cycle here and stop all
    // clients; every command must instead finish within one shared deadline.
    {
      constexpr int kWriters = 48;
      constexpr int kRounds = 64;
      constexpr int kKeysPerCommand = 8;
      if (!WaitForInfoStat(client, "storage_tx_commits_pending:", 0)) {
        Fail("initial MSET commit did not drain");
      }
      const std::uint64_t batches_before =
          InfoStat(client, "tx_commit_batches:");
      const std::uint64_t transactions_before =
          InfoStat(client, "tx_commit_batch_transactions:");
      const std::uint64_t input_fences_before =
          InfoStat(client, "tx_commit_input_fences:");
      const std::uint64_t merged_fences_before =
          InfoStat(client, "tx_commit_merged_fences:");
      std::vector<std::future<void>> writers;
      writers.reserve(kWriters);
      for (int writer = 0; writer < kWriters; ++writer) {
        writers.push_back(std::async(std::launch::async, [port, writer] {
          RespClient stress = Connect(port);
          std::vector<std::string> keys;
          keys.reserve(kKeysPerCommand);
          for (int key = 0; key < kKeysPerCommand; ++key) {
            keys.push_back("tx-progress:" + std::to_string(writer) + ":" +
                           std::to_string(key));
          }
          for (int round = 0; round < kRounds; ++round) {
            std::string value = "writer:" + std::to_string(writer) +
                                ":round:" + std::to_string(round);
            value.resize(256, static_cast<char>('a' + writer % 26));
            std::vector<std::string_view> command{"MSET"};
            command.reserve(1 + 2 * kKeysPerCommand);
            for (const std::string& key : keys) {
              command.push_back(key);
              command.push_back(value);
            }
            Expect(stress.Command(command), "+OK",
                   "conflict-free concurrent MSET");
          }
        }));
      }
      const auto progress_deadline = std::chrono::steady_clock::now() + 45s;
      for (auto& writer : writers) {
        if (writer.wait_until(progress_deadline) != std::future_status::ready) {
          Fail("conflict-free concurrent MSET made no progress");
        }
      }
      for (auto& writer : writers) writer.get();
      if (!WaitForInfoStat(client, "storage_tx_commits_pending:", 0)) {
        Fail("batched MSET commits did not drain");
      }
      const std::uint64_t batches =
          InfoStat(client, "tx_commit_batches:") - batches_before;
      const std::uint64_t transactions =
          InfoStat(client, "tx_commit_batch_transactions:") -
          transactions_before;
      const std::uint64_t input_fences =
          InfoStat(client, "tx_commit_input_fences:") - input_fences_before;
      const std::uint64_t merged_fences =
          InfoStat(client, "tx_commit_merged_fences:") - merged_fences_before;
      if (transactions != kWriters * kRounds) {
        Fail("commit coordinator lost a concurrent MSET receipt");
      }
      if (batches >= transactions) {
        Fail("commit coordinator did not batch concurrent MSET receipts");
      }
      if (merged_fences >= input_fences) {
        Fail("commit coordinator did not merge shared durability fences");
      }
    }

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
    Expect(client.Command({"DEL", "mk0", "missing", "mk5", "mk7", "mk7"}), ":3",
           "cross-shard DEL");
    Expect(client.Command({"MGET", "mk0", "mk5", "mk7", "mk1"}),
           "*4\r\n$-1\r\n$-1\r\n$-1\r\n" + Bulk("v1"), "MGET after DEL");

    // UNLINK shares DEL's deferred tombstone retirement but remains a
    // distinct command at dispatch and metrics boundaries.
    Expect(client.Command({"MSET", "unlink-a", "1", "unlink-b", "2"}), "+OK",
           "UNLINK seed");
    Expect(client.Command(
               {"UNLINK", "unlink-a", "missing", "unlink-b", "unlink-b"}),
           ":2", "cross-shard UNLINK");
    Expect(client.Command({"MGET", "unlink-a", "unlink-b"}), "*2\r\n$-1\r\n$-1",
           "MGET after UNLINK");

    // Hashtag keys share one slot: the whole command stays on a single shard
    // (fast path) and must behave identically.
    Expect(
        client.Command({"MSET", "{tag}a", "1", "{tag}b", "2", "{tag}c", "3"}),
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
      Expect(client.Command({"SET", bin_key, bin_value}), "+OK", "binary SET");
      Expect(client.Command({"GET", bin_key}), Bulk(bin_value), "binary GET");
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
    const auto streamed_pipeline = client.Pipeline(
        {{"GET", "large"}, {"PING"}, {"GET", "large"}, {"PING"}});
    if (streamed_pipeline !=
        std::vector<std::string>{Bulk(large), "+PONG", Bulk(large), "+PONG"}) {
      Fail("mixed streamed/encoded pipeline reordered replies");
    }
    Expect(client.Command({"DEL", "large", "small"}), ":2", "DEL large");

    // ---- KEYS / SCAN TYPE ----
    Expect(client.Command(
               {"MSET", "kx:1", "a", "kx:2", "b", "kx:3", "c", "other", "1"}),
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

    // Full Redis glob: character classes, ranges, negation, and escapes.
    expect_members(client.Command({"KEYS", "kx:[12]"}), 2, {"kx:1", "kx:2"},
                   "KEYS char class");
    expect_members(client.Command({"KEYS", "kx:[1-2]"}), 2, {"kx:1", "kx:2"},
                   "KEYS class range");
    expect_members(client.Command({"KEYS", "kx:[^1]"}), 2, {"kx:2", "kx:3"},
                   "KEYS negated class");
    Expect(client.Command({"SET", "lit*eral", "x"}), "+OK", "escape seed");
    expect_members(client.Command({"KEYS", "lit\\*eral"}), 1, {"lit*eral"},
                   "KEYS escaped star");
    Expect(client.Command({"KEYS", "lit\\?eral"}), "*0",
           "KEYS escaped question mark");

    // Large key names: the total far exceeds one 64 KiB stream chunk, so
    // the reply must arrive complete across several bounded chunks.
    {
      std::vector<std::string> long_names;
      for (int i = 0; i < 48; ++i) {
        std::string name = "longname:" + std::to_string(i) + ":";
        name.append(3500, 'x');
        Expect(client.Command({"SET", name, "v"}), "+OK", "long name SET");
        long_names.push_back(std::move(name));
      }
      expect_members(client.Command({"KEYS", "longname:*"}), long_names.size(),
                     long_names, "KEYS long names");
      for (const std::string& name : long_names) {
        Expect(client.Command({"DEL", name}), ":1", "long name DEL");
      }
    }
    Expect(client.Command({"DEL", "lit*eral"}), ":1", "escape cleanup");

    // Streaming stays bounded: several hundred keys still arrive with an
    // exact element count.
    std::vector<std::string> volume_storage;
    for (unsigned batch = 0; batch < 6; ++batch) {
      std::vector<std::string_view> mset_args;
      volume_storage.clear();
      mset_args.push_back("MSET");
      for (unsigned i = 0; i < 50; ++i) {
        volume_storage.push_back("vol:" + std::to_string(batch * 50 + i));
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
            {"SCAN", cursor, "MATCH", "kx:*", "COUNT", "1000", "TYPE", type});
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

    // Force one logical partition owned by each of the four server workers to
    // grow beyond one bucket. Its stateless hash-table cursor uses low
    // bucket-index bits, which the global SCAN cursor must preserve while
    // crossing worker ownership boundaries between client calls.
    {
      constexpr std::size_t kPackedCursorKeysPerWorker = 128;
      constexpr std::uint64_t kPackedLocalMask =
          (std::uint64_t{1} << (64 - 14)) - 1;
      // Standard Redis slots for b, c, d, and a are 3300, 7365, 11298, and
      // 15495 respectively, selecting owners 0 through 3 modulo four.
      constexpr std::array<std::string_view, 4> kWorkerTags{"{b}", "{c}", "{d}",
                                                            "{a}"};
      std::vector<std::string> packed_cursor_keys;
      packed_cursor_keys.reserve(kPackedCursorKeysPerWorker *
                                 kWorkerTags.size());
      for (std::string_view tag : kWorkerTags) {
        for (std::size_t i = 0; i < kPackedCursorKeysPerWorker; ++i) {
          packed_cursor_keys.push_back("scan-packed:" + std::string(tag) + ":" +
                                       std::to_string(i));
        }
      }
      for (const std::string& key : packed_cursor_keys) {
        Expect(client.Command({"SET", key, "v"}), "+OK", "packed-cursor seed");
      }

      std::string collected;
      std::string cursor = "0";
      std::array<bool, 4> saw_local_cursor{};
      std::size_t calls = 0;
      do {
        const std::string reply = client.Command(
            {"SCAN", cursor, "MATCH", "scan-packed:*", "COUNT", "1"});
        if (reply.starts_with("-")) {
          Fail("packed-cursor SCAN failed: " + reply);
        }
        const std::size_t cursor_bulk = reply.find("\r\n") + 2;
        const std::size_t cursor_start = reply.find("\r\n", cursor_bulk) + 2;
        const std::size_t cursor_end = reply.find("\r\n", cursor_start);
        cursor = reply.substr(cursor_start, cursor_end - cursor_start);
        std::uint64_t numeric_cursor = 0;
        const auto [parsed_cursor, error] = std::from_chars(
            cursor.data(), cursor.data() + cursor.size(), numeric_cursor);
        if (error != std::errc{} ||
            parsed_cursor != cursor.data() + cursor.size()) {
          Fail("packed-cursor SCAN returned malformed cursor: " + cursor);
        }
        if ((numeric_cursor & kPackedLocalMask) != 0) {
          const std::uint64_t partition_id = numeric_cursor >> (64 - 14);
          saw_local_cursor[partition_id % saw_local_cursor.size()] = true;
        }
        collected += reply.substr(cursor_end);
        if (++calls >= 1000) {
          Fail("packed-cursor SCAN did not terminate");
        }
      } while (cursor != "0");

      for (std::size_t worker = 0; worker < saw_local_cursor.size(); ++worker) {
        if (!saw_local_cursor[worker]) {
          Fail(
              "packed-cursor SCAN never returned local bucket state for "
              "worker " +
              std::to_string(worker));
        }
      }
      for (const std::string& key : packed_cursor_keys) {
        if (collected.find(Bulk(key)) == std::string::npos) {
          Fail("packed-cursor SCAN missed key: " + key);
        }
      }

      std::vector<std::string_view> delete_args{"DEL"};
      delete_args.reserve(packed_cursor_keys.size() + 1);
      for (const std::string& key : packed_cursor_keys) {
        delete_args.push_back(key);
      }
      Expect(client.Command(delete_args),
             ":" + std::to_string(packed_cursor_keys.size()),
             "packed-cursor cleanup");
    }

    // SCAN MATCH speaks the same glob dialect.
    {
      std::string collected;
      std::string cursor = "0";
      do {
        const std::string reply = client.Command(
            {"SCAN", cursor, "MATCH", "kx:[13]", "COUNT", "100000"});
        const std::size_t cursor_start = reply.find("\r\n") + 2;
        const std::size_t digits = reply.find("\r\n", cursor_start) + 2;
        const std::size_t digits_end = reply.find("\r\n", digits);
        cursor = reply.substr(digits, digits_end - digits);
        collected += reply.substr(digits_end);
      } while (cursor != "0");
      if (collected.find("kx:1") == std::string::npos ||
          collected.find("kx:3") == std::string::npos ||
          collected.find("kx:2") != std::string::npos) {
        Fail("SCAN MATCH character class mismatch: " + collected);
      }
    }

    // Transaction generations are rotated and cleaned by whichever periodic
    // worker wins the process-wide guard. Wait for an observed retirement so
    // this verifies the cleaner itself rather than merely sleeping.
    Expect(client.Command({"CONFIG", "SET", "tx-cleaner-cooldown-ms", "20"}),
           "+OK", "enable tx cleaner");
    Expect(client.Command({"CONFIG", "GET", "tx-cleaner-cooldown-ms"}),
           "*2\r\n" + Bulk("tx-cleaner-cooldown-ms") + "\r\n" + Bulk("20"),
           "read tx cleaner cooldown");
    const std::uint64_t cleaner_baseline = TxCleanerRetiredGenerations(client);
    Expect(
        client.Command({"MSET", "cleaner-a", "after-a", "cleaner-b", "after-b",
                        "cleaner-c", "after-c", "cleaner-d", "after-d"}),
        "+OK", "tx cleaner seed");
    if (!WaitForCleanerRetirement(client, cleaner_baseline)) {
      Fail("transaction cleaner did not retire a generation");
    }
    Expect(client.Command(
               {"MGET", "cleaner-d", "cleaner-a", "cleaner-c", "cleaner-b"}),
           "*4\r\n" + Bulk("after-d") + "\r\n" + Bulk("after-a") + "\r\n" +
               Bulk("after-c") + "\r\n" + Bulk("after-b"),
           "values after tx cleaner retirement");
    Expect(client.Command({"MSET", "{disk-batch}a", "batch-a", "{disk-batch}b",
                           "batch-b", "{disk-batch}c", "batch-c",
                           "{disk-batch}d", "batch-d"}),
           "+OK", "same-shard disk batch seed");

    // Leave one committed tagged generation for the shutdown-only cleaner.
    // Its promoted ordinary records are appended after the first storage
    // freeze, so loading them from the checkpoint after restart specifically
    // exercises the required second seal-and-drain round.
    Expect(client.Command({"CONFIG", "SET", "tx-cleaner-cooldown-ms", "0"}),
           "+OK", "defer transaction cleaning until shutdown");
    // These keys are also used by the order-gate coverage below because they
    // deterministically span workers when the server runs with four workers.
    Expect(client.Command(
               {"MSET", "order-a", "shutdown-a", "order-b", "shutdown-b"}),
           "+OK", "shutdown-only cleaner seed");

    // Enabling immediately before the drain must be enough to publish a
    // checkpoint even though the process started with the default disabled.
    Expect(client.Command({"CONFIG", "SET", "shutdown-checkpoint", "yes"}),
           "+OK", "enable checkpoint for the next shutdown");
    server.Stop();
    if (ReadFile(log_path).find("published shutdown checkpoint generation=") ==
        std::string::npos) {
      Fail("runtime-enabled shutdown did not publish a checkpoint");
    }
    constexpr std::string_view kWorkerReadyMarker =
        "direct-IO storage initialized";
    const std::size_t ready_workers_before_restart =
        CountOccurrences(ReadFile(log_path), kWorkerReadyMarker);
    ServerProcess recovered_server(argv[1], port, data_path, log_path, {}, {},
                                   false, 4, {}, {}, {}, true);
    // Opening the listener precedes recovery. Wait for the recovery decision
    // and every worker's ready boundary rather than treating a successful TCP
    // connect as storage readiness.
    if (!WaitForLogMarker(log_path, "loaded shutdown checkpoint generation=")) {
      Fail("startup-enabled recovery did not load the runtime checkpoint");
    }
    if (!WaitForLogMarkerCount(log_path, kWorkerReadyMarker,
                               ready_workers_before_restart + 4)) {
      Fail("startup-enabled checkpoint recovery did not become ready");
    }
    RespClient recovered = Connect(port);
    Expect(recovered.Command({"CONFIG", "GET", "shutdown-checkpoint"}),
           "*2\r\n" + Bulk("shutdown-checkpoint") + "\r\n" + Bulk("yes"),
           "startup checkpoint setting initializes runtime state");
    Expect(recovered.Command({"CONFIG", "SET", "shutdown-checkpoint", "no"}),
           "+OK", "disable the recovered server's next checkpoint");
    Expect(recovered.Command(
               {"MGET", "cleaner-a", "cleaner-b", "cleaner-c", "cleaner-d"}),
           "*4\r\n" + Bulk("after-a") + "\r\n" + Bulk("after-b") + "\r\n" +
               Bulk("after-c") + "\r\n" + Bulk("after-d"),
           "promoted values after restart");
    Expect(recovered.Command({"MGET", "{disk-batch}d", "{disk-batch}b",
                              "{disk-batch}a", "{disk-batch}c"}),
           "*4\r\n" + Bulk("batch-d") + "\r\n" + Bulk("batch-b") + "\r\n" +
               Bulk("batch-a") + "\r\n" + Bulk("batch-c"),
           "same-shard batched disk MGET after restart");
    Expect(recovered.Command({"MGET", "order-a", "order-b"}),
           "*2\r\n" + Bulk("shutdown-a") + "\r\n" + Bulk("shutdown-b"),
           "shutdown-cleaner values loaded from checkpoint");
    // Recovered values exercise disk-backed replies. Encoded replies preceding
    // them must flush first, and an empty batch must not suppress a disk reply.
    for (int repeat = 0; repeat < 3; ++repeat) {
      const auto replies = recovered.Pipeline({{"GET", "{disk-batch}a"},
                                               {"PING"},
                                               {"GET", "{disk-batch}b"},
                                               {"GET", "{disk-batch}c"},
                                               {"GET", "pipeline-missing-key"},
                                               {"GET", "{disk-batch}d"},
                                               {"PING"}});
      const std::vector<std::string> expected{
          Bulk("batch-a"), "+PONG",         Bulk("batch-b"), Bulk("batch-c"),
          "$-1",           Bulk("batch-d"), "+PONG"};
      if (replies != expected) {
        Fail("mixed disk/encoded pipeline reordered replies");
      }
    }
    Expect(recovered.Command({"CONFIG", "SET", "tx-cleaner-cooldown-ms", "0"}),
           "+OK", "disable tx cleaner before recovery fixture");
    Expect(recovered.Command({"MSET", "cleaner-recovery-a", "disk-a",
                              "cleaner-recovery-b", "disk-b"}),
           "+OK", "persist a closed generation for recovery");
    recovered_server.Stop();

    ServerProcess generation_recovery_server(argv[1], port, data_path,
                                             log_path);
    RespClient generation_recovery = ConnectReady(port);
    const std::uint64_t recovered_cleaner_baseline =
        TxCleanerRetiredGenerations(generation_recovery);
    Expect(generation_recovery.Command(
               {"CONFIG", "SET", "tx-cleaner-cooldown-ms", "20"}),
           "+OK", "enable tx cleaner after generation recovery");
    if (!WaitForCleanerRetirement(generation_recovery,
                                  recovered_cleaner_baseline)) {
      Fail("recovered transaction generation was not retired");
    }
    Expect(generation_recovery.Command(
               {"MGET", "cleaner-recovery-a", "cleaner-recovery-b"}),
           "*2\r\n" + Bulk("disk-a") + "\r\n" + Bulk("disk-b"),
           "recovered generation values after retirement");

    // FLUSHDB invalidates tagged winners by advancing the database epoch.
    // The cleaner must not promote them into the new epoch; detached-index
    // reclaim instead drops their tagged-byte accounting so the complete
    // transaction generation can still be retired.
    Expect(generation_recovery.Command(
               {"CONFIG", "SET", "tx-cleaner-cooldown-ms", "0"}),
           "+OK", "disable tx cleaner before FLUSHDB fixture");
    Expect(generation_recovery.Command({"MSET", "cleaner-flush-a", "old-a",
                                        "cleaner-flush-b", "old-b"}),
           "+OK", "persist tagged values before FLUSHDB");
    Expect(generation_recovery.Command({"FLUSHDB", "SYNC"}), "+OK",
           "flush tagged transaction generation");
    const std::uint64_t flushed_cleaner_baseline =
        TxCleanerRetiredGenerations(generation_recovery);
    Expect(generation_recovery.Command(
               {"CONFIG", "SET", "tx-cleaner-cooldown-ms", "20"}),
           "+OK", "enable tx cleaner after FLUSHDB");
    if (!WaitForCleanerRetirement(generation_recovery,
                                  flushed_cleaner_baseline)) {
      Fail("FLUSHDB-invalidated transaction generation was not retired");
    }
    Expect(generation_recovery.Command(
               {"EXISTS", "cleaner-flush-a", "cleaner-flush-b"}),
           ":0", "FLUSHDB values after transaction generation retirement");
    generation_recovery_server.Stop();

#if LAVIK_TEST_FAULTS_AVAILABLE
    // A failed transaction keeps its generation lease through rollback. Once
    // UNDO has restored every old value, dependency pins drop and the same
    // cleaner can retire the aborted tagged records safely.
    ServerProcess rollback_server(argv[1], port, data_path, log_path,
                                  "cleaner-undo-d");
    RespClient rollback = ConnectReady(port);
    Expect(rollback.Command({"CONFIG", "SET", "tx-cleaner-cooldown-ms", "20"}),
           "+OK", "enable tx cleaner during rollback");
    for (std::string_view key : {"cleaner-undo-a", "cleaner-undo-b",
                                 "cleaner-undo-c", "cleaner-undo-d"}) {
      Expect(rollback.Command({"SET", key, "old", "EX", "600"}), "+OK",
             "tx cleaner rollback seed");
    }
    const std::uint64_t rollback_cleaner_baseline =
        TxCleanerRetiredGenerations(rollback);
    const std::string failed = rollback.Command(
        {"MSET", "cleaner-undo-a", "new-a", "cleaner-undo-b", "new-b",
         "cleaner-undo-c", "new-c", "cleaner-undo-d", "new-d"});
    if (!failed.starts_with("-ERR injected transaction write fault")) {
      Fail("fault-injected MSET unexpectedly returned: " + failed);
    }
    Expect(rollback.Command({"MGET", "cleaner-undo-a", "cleaner-undo-b",
                             "cleaner-undo-c", "cleaner-undo-d"}),
           "*4\r\n" + Bulk("old") + "\r\n" + Bulk("old") + "\r\n" +
               Bulk("old") + "\r\n" + Bulk("old"),
           "UNDO values while tx cleaner is enabled");
    Expect(rollback.Command({"EXPIRE", "cleaner-undo-a", "600", "NX"}), ":0",
           "UNDO restores TTL representation");
    if (!WaitForCleanerRetirement(rollback, rollback_cleaner_baseline)) {
      Fail("transaction cleaner did not retire the rolled-back generation");
    }
    rollback_server.Stop();

    ServerProcess rollback_recovered_server(argv[1], port, data_path, log_path);
    RespClient rollback_recovered = ConnectReady(port);
    Expect(
        rollback_recovered.Command({"MGET", "cleaner-undo-a", "cleaner-undo-b",
                                    "cleaner-undo-c", "cleaner-undo-d"}),
        "*4\r\n" + Bulk("old") + "\r\n" + Bulk("old") + "\r\n" + Bulk("old") +
            "\r\n" + Bulk("old"),
        "UNDO values after cleaner restart");
    Expect(
        rollback_recovered.Command({"EXPIRE", "cleaner-undo-a", "600", "NX"}),
        ":0", "UNDO TTL survives recovery");
    rollback_recovered_server.Stop();

#if LAVIK_TEST_FAULTS_AVAILABLE
    // The first transaction holds the generation's allocation gate while the
    // test hook suspends physical allocation. A same-worker peer in that
    // generation must remain queued: completing early would mean rollover
    // fanned out into a second allocation. Both writes must resume once the
    // elected allocator publishes the shared stream.
    ServerProcess allocation_server(argv[1], port, data_path, log_path, {},
                                    "1000");
    RespClient allocation_control = ConnectReady(port);
    auto elected_write = std::async(std::launch::async, [port] {
      RespClient client = Connect(port);
      return client.Command({"MSET", "allocation-leader-a{tx-stream}",
                             "leader-a", "allocation-leader-b{tx-stream}",
                             "leader-b"});
    });
    const auto allocation_marker_deadline =
        std::chrono::steady_clock::now() + 30s;
    bool allocation_marker_seen = false;
    while (!allocation_marker_seen &&
           std::chrono::steady_clock::now() < allocation_marker_deadline) {
      allocation_marker_seen =
          ReadFile(log_path).find("LAVIK_TX_ACTIVE_BLOCK_PAUSE_MS pausing") !=
          std::string::npos;
      if (!allocation_marker_seen) std::this_thread::sleep_for(20ms);
    }
    if (!allocation_marker_seen) {
      Fail("transaction append allocator pause did not engage");
    }
    auto waiting_write = std::async(std::launch::async, [port] {
      RespClient client = Connect(port);
      return client.Command({"MSET", "allocation-follower-a{tx-stream}",
                             "follower-a", "allocation-follower-b{tx-stream}",
                             "follower-b"});
    });
    if (waiting_write.wait_for(200ms) == std::future_status::ready) {
      Fail("same-stream transaction bypassed the allocation gate");
    }
    if (elected_write.wait_for(5s) != std::future_status::ready ||
        waiting_write.wait_for(5s) != std::future_status::ready) {
      Fail("transaction allocation gate stranded a writer");
    }
    Expect(elected_write.get(), "+OK", "elected allocation writer");
    Expect(waiting_write.get(), "+OK", "waiting allocation writer");
    Expect(allocation_control.Command({"MGET", "allocation-leader-a{tx-stream}",
                                       "allocation-leader-b{tx-stream}",
                                       "allocation-follower-a{tx-stream}",
                                       "allocation-follower-b{tx-stream}"}),
           "*4\r\n" + Bulk("leader-a") + "\r\n" + Bulk("leader-b") + "\r\n" +
               Bulk("follower-a") + "\r\n" + Bulk("follower-b"),
           "values after transaction allocation single-flight");
    allocation_server.Stop();

    // Ordinary rollover shares its allocation gate with the prefetch task.
    // Publishing the first active block requests its successor immediately;
    // holding that task proves a writer cannot bypass it and allocate a
    // duplicate. Installing the successor requests another standby, so
    // graceful shutdown also exercises returning an unused reservation.
    // The suite's shared fixture can be physically full by this point. A
    // fresh device makes readiness mean "the writer bypassed the gate"
    // instead of also allowing an immediate out-of-space reply.
    CreateDataFile(standby_data, 128ULL * 1024 * 1024);
    ServerProcess standby_server(argv[1], port, standby_data, log_path, {}, {},
                                 false, 4, {}, "1000");
    RespClient standby_control = ConnectReady(port);
    const std::string standby_payload(7 * 1024 * 1024, 's');
    Expect(standby_control.Command(
               {"SET", "standby-leader{standby}", standby_payload}),
           "+OK", "create ordinary stream and request its standby");
    const auto standby_marker_deadline = std::chrono::steady_clock::now() + 30s;
    bool standby_marker_seen = false;
    while (!standby_marker_seen &&
           std::chrono::steady_clock::now() < standby_marker_deadline) {
      standby_marker_seen =
          ReadFile(log_path).find("LAVIK_STANDBY_PREFETCH_PAUSE_MS pausing") !=
          std::string::npos;
      if (!standby_marker_seen) std::this_thread::sleep_for(20ms);
    }
    if (!standby_marker_seen) Fail("standby prefetch pause did not engage");
    auto standby_waiter =
        std::async(std::launch::async, [port, &standby_payload] {
          RespClient client = Connect(port);
          return client.Command(
              {"SET", "standby-follower{standby}", standby_payload});
        });
    if (standby_waiter.wait_for(200ms) == std::future_status::ready) {
      Fail("ordinary rollover bypassed an in-flight standby prefetch");
    }
    if (standby_waiter.wait_for(5s) != std::future_status::ready) {
      Fail("standby prefetch stranded an ordinary rollover");
    }
    Expect(standby_waiter.get(), "+OK", "ordinary standby rollover");
    Expect(standby_control.Command({"STRLEN", "standby-leader{standby}"}),
           ":7340032", "standby leader value length");
    Expect(standby_control.Command({"STRLEN", "standby-follower{standby}"}),
           ":7340032", "standby follower value length");
    standby_server.Stop();
#endif

    // A retryable cleaner failure is observable but must not terminate the
    // periodic flush coroutine or report a shutdown drain as complete. The
    // same process must run a later round and retire the generation.
    ServerProcess retry_server(argv[1], port, data_path, log_path, {}, {},
                               true);
    RespClient retry = ConnectReady(port);
    const std::uint64_t failure_baseline =
        InfoStat(retry, "tx_cleaner_failures:");
    const std::uint64_t retry_retired_baseline =
        TxCleanerRetiredGenerations(retry);
    Expect(retry.Command({"MSET", "cleaner-retry-a{tx}", "durable-a",
                          "cleaner-retry-b{tx}", "durable-b"}),
           "+OK", "seed retryable cleaner failure");
    Expect(retry.Command({"CONFIG", "SET", "tx-cleaner-cooldown-ms", "20"}),
           "+OK", "enable retryable cleaner fixture");
    if (!WaitForCleanerStat(retry, "tx_cleaner_failures:", failure_baseline,
                            10s)) {
      Fail("injected cleaner failure was not recorded");
    }
    if (!WaitForCleanerRetirement(retry, retry_retired_baseline)) {
      Fail("periodic flush stopped after a retryable cleaner failure");
    }
    Expect(
        retry.Command({"MGET", "cleaner-retry-a{tx}", "cleaner-retry-b{tx}"}),
        "*2\r\n" + Bulk("durable-a") + "\r\n" + Bulk("durable-b"),
        "value after cleaner retry");
    retry_server.Stop();

    ServerProcess retry_recovered_server(argv[1], port, data_path, log_path);
    RespClient retry_recovered = ConnectReady(port);
    Expect(retry_recovered.Command(
               {"MGET", "cleaner-retry-a{tx}", "cleaner-retry-b{tx}"}),
           "*2\r\n" + Bulk("durable-a") + "\r\n" + Bulk("durable-b"),
           "cleaner retry value after graceful shutdown");
    retry_recovered_server.Stop();
#endif

    // A reservation that is never given back stands against its worker's
    // publish-queue waterline for the life of the process, so a long run of
    // wide writes against a deliberately small waterline wedges if any
    // participant is ever missed. The replica must also converge on exactly
    // the effects the source applied, in the order it applied them.
    {
      // The dataset here is a few megabytes; size the pair for that rather
      // than for the whole-suite fixture above.
      CreateDataFile(source_data, 256ULL * 1024 * 1024);
      CreateDataFile(replica_data, 256ULL * 1024 * 1024);
      const std::uint16_t source_port = FindFreePort();
      std::uint16_t replica_port = FindFreePort();
      while (replica_port == source_port) replica_port = FindFreePort();
      // Both log to log_path: the likeliest failures here are replica-side,
      // and that is the log the failure handler prints.
      ServerProcess replication_source(argv[1], source_port, source_data,
                                       log_path, {}, {}, false, 4, {}, {},
                                       "catalog_failure");
      ServerProcess replication_replica(argv[1], replica_port, replica_data,
                                        log_path, {}, {}, false, 2);
      RespClient source_client = Connect(source_port);
      RespClient replica_client = Connect(replica_port);
      Expect(source_client.Command({"CONFIG", "SET",
                                    "replication-publish-queue-mb-per-worker",
                                    "1"}),
             "+OK", "shrink the publisher waterline");
      Expect(replica_client.Command(
                 {"REPLICAOF", "127.0.0.1", std::to_string(source_port)}),
             "+OK", "attach replica for wide multi-key writes");
      const auto online_deadline = std::chrono::steady_clock::now() + 120s;
      bool online = false;
      while (!online && std::chrono::steady_clock::now() < online_deadline) {
        online =
            replica_client.Command({"INFO", "replication"})
                .find("lavik_replication_state:online") != std::string::npos;
        if (!online) std::this_thread::sleep_for(20ms);
      }
      if (!online) Fail("replica did not come online for wide writes");
      // A replica redirects keyed reads to its upstream unless the connection
      // opts into serving them locally.
      Expect(replica_client.Command({"READONLY"}), "+OK",
             "serve reads from the replica");

      // Pin flow zero as the minimum acknowledged cursor by advancing every
      // other source flow. The mixed EXEC below can then advance the scalar
      // minimum only if its keyless catalog mutation contributes a real flow
      // zero participant marker to the same replication transaction.
      Expect(source_client.Command({"CLUSTER", "KEYSLOT", "bar"}), ":5061",
             "catalog transaction worker-one key");
      Expect(source_client.Command({"CLUSTER", "KEYSLOT", "foo"}), ":12182",
             "catalog transaction worker-two key");
      Expect(
          source_client.Command({"CLUSTER", "KEYSLOT", "{user1000}.following"}),
          ":3443", "catalog transaction worker-three key");
      std::uint64_t catalog_flow_floor =
          InfoUnsigned(source_client, "replication", "master_repl_offset:");
      bool flow_zero_is_floor = false;
      for (unsigned attempt = 0; attempt < 8 && !flow_zero_is_floor;
           ++attempt) {
        const std::string value = "floor:" + std::to_string(attempt);
        Expect(source_client.Command({"MSET", "bar", value, "foo", value,
                                      "{user1000}.following", value}),
               "+OK", "advance non-catalog replication flows");
        Expect(source_client.Command({"WAIT", "1", "30000"}), ":1",
               "ack non-catalog replication flows");
        const std::uint64_t next_floor =
            InfoUnsigned(source_client, "replication", "master_repl_offset:");
        flow_zero_is_floor = next_floor == catalog_flow_floor;
        catalog_flow_floor = next_floor;
      }
      if (!flow_zero_is_floor) {
        Fail("could not isolate flow zero as the acknowledged cursor floor");
      }

      constexpr std::string_view catalog_transaction_library =
          "#!lua name=catalog_transaction\n"
          "redis.register_function{function_name='catalog_transaction', "
          "callback=function() return 1 end, flags={'no-writes'}}";
      Expect(source_client.Command({"MULTI"}), "+OK",
             "begin mixed catalog transaction");
      Expect(source_client.Command({"MSET", "bar", "mixed", "foo", "mixed",
                                    "{user1000}.following", "mixed"}),
             "+QUEUED", "queue keyed catalog transaction child");
      Expect(source_client.Command(
                 {"FUNCTION", "LOAD", catalog_transaction_library}),
             "+QUEUED", "queue Function catalog transaction child");
      Expect(source_client.Command({"EXEC"}),
             "*2\r\n+OK\r\n" + Bulk("catalog_transaction"),
             "commit keyed Function catalog transaction");
      Expect(source_client.Command({"WAIT", "1", "30000"}), ":1",
             "ack keyed Function catalog transaction");
      const std::uint64_t catalog_transaction_floor =
          InfoUnsigned(source_client, "replication", "master_repl_offset:");
      if (catalog_transaction_floor != catalog_flow_floor + 1) {
        Fail("keyed Function EXEC did not publish one marker on every flow");
      }
      Expect(replica_client.Command({"FCALL_RO", "catalog_transaction", "0"}),
             ":1", "replica applied Function child of keyed EXEC");

      // The configured one-megabyte publisher waterline admits an oversized
      // event only while it is exclusive. Waiting for a publication fence
      // while retaining that reservation deadlocks against the fence's own
      // admission. This mixed single-owner EXEC must release its outer
      // reservation after every marker is committed and then finish normally.
      std::string large_catalog_transaction_library =
          "#!lua name=large_catalog_transaction\n--";
      large_catalog_transaction_library.append(1200 * 1024, 'x');
      large_catalog_transaction_library.append(
          "\nredis.register_function{function_name="
          "'large_catalog_transaction', callback=function() return 2 end, "
          "flags={'no-writes'}}");
      Expect(source_client.Command({"MULTI"}), "+OK",
             "begin oversized catalog transaction");
      Expect(source_client.Command({"SET", "bar", "oversized"}), "+QUEUED",
             "queue oversized catalog transaction key");
      Expect(source_client.Command(
                 {"FUNCTION", "LOAD", large_catalog_transaction_library}),
             "+QUEUED", "queue oversized Function catalog child");
      Expect(source_client.Command({"EXEC"}),
             "*2\r\n+OK\r\n" + Bulk("large_catalog_transaction"),
             "commit oversized keyed Function catalog transaction");
      Expect(source_client.Command({"WAIT", "1", "30000"}), ":1",
             "ack oversized keyed Function catalog transaction");
      Expect(replica_client.Command(
                 {"FCALL_RO", "large_catalog_transaction", "0"}),
             ":2", "replica applied oversized Function child");

      constexpr int kWideKeys = 24;
      constexpr int kWideRounds = 120;
      std::vector<std::string> wide_keys;
      wide_keys.reserve(kWideKeys);
      for (int key = 0; key < kWideKeys; ++key) {
        wide_keys.push_back("wide-multikey:" + std::to_string(key));
      }
      const std::string payload(1024, 'w');

      // Concurrent wide transactions used to spend their entire storage
      // lifetime behind the source-wide replication order slot. Releasing the
      // slot after every participant marker is queued must preserve a common
      // flow order while allowing the storage callbacks to overlap. Hammer the
      // same participant set so any early release before the final marker can
      // still produce the classic cross-flow arrival/ACK cycle.
      constexpr int kConcurrentWriters = 4;
      constexpr int kConcurrentRounds = 32;
      std::vector<std::future<void>> writers;
      writers.reserve(kConcurrentWriters);
      for (int writer = 0; writer < kConcurrentWriters; ++writer) {
        writers.push_back(std::async(std::launch::async, [source_port, writer,
                                                          &wide_keys]() {
          RespClient client = Connect(source_port);
          for (int round = 0; round < kConcurrentRounds; ++round) {
            const std::string value = "concurrent:" + std::to_string(writer) +
                                      ":" + std::to_string(round);
            std::vector<std::string_view> command{"MSET"};
            command.reserve(1 + 2 * wide_keys.size());
            for (const std::string& key : wide_keys) {
              command.push_back(key);
              command.push_back(value);
            }
            Expect(client.Command(command), "+OK",
                   "concurrent replicated MSET");
          }
        }));
      }
      for (auto& writer : writers) writer.get();

      const std::string concurrent_final =
          source_client.Command({"GET", wide_keys.front()});
      for (const std::string& key : wide_keys) {
        Expect(source_client.Command({"GET", key}), concurrent_final,
               "atomic source state after concurrent MSET");
      }
      const auto concurrent_deadline = std::chrono::steady_clock::now() + 30s;
      for (const std::string& key : wide_keys) {
        std::string replicated;
        do {
          replicated = replica_client.Command({"GET", key});
          if (replicated == concurrent_final) break;
          std::this_thread::sleep_for(10ms);
        } while (std::chrono::steady_clock::now() < concurrent_deadline);
        Expect(replicated, concurrent_final,
               "replica state after concurrent MSET");
      }

      // Exercise changing participant sets across enough concurrent
      // transactions to cross several per-flow wire-batch boundaries. A
      // stop-and-wait sender can form an arrival/ACK cycle here: one flow
      // waits for a transaction whose missing participant is just beyond a
      // different flow's arbitrary batch boundary. ONLINE replication must
      // keep sending while its independent ACK receiver advances retention.
      constexpr int kVariedWriters = 12;
      constexpr int kVariedRounds = 192;
      const std::string varied_payload(256, 'd');
      writers.clear();
      writers.reserve(kVariedWriters);
      for (int writer = 0; writer < kVariedWriters; ++writer) {
        writers.push_back(std::async(std::launch::async, [source_port, writer,
                                                          &varied_payload]() {
          RespClient client = Connect(source_port);
          for (int round = 0; round < kVariedRounds; ++round) {
            const int key_count = 2 + (writer * 5 + round * 3) % 7;
            std::vector<std::string> keys;
            keys.reserve(key_count);
            std::vector<std::string_view> command{"MSET"};
            command.reserve(1 + 2 * key_count);
            for (int key = 0; key < key_count; ++key) {
              keys.push_back("duplex-multikey:" + std::to_string(writer) + ":" +
                             std::to_string(round) + ":" + std::to_string(key));
              command.push_back(keys.back());
              command.push_back(varied_payload);
            }
            Expect(client.Command(command), "+OK",
                   "varied-participant replicated MSET");
          }
        }));
      }
      for (auto& writer : writers) writer.get();
      const std::string varied_source_size = source_client.Command({"DBSIZE"});
      const auto varied_deadline = std::chrono::steady_clock::now() + 30s;
      std::string varied_replica_size;
      do {
        varied_replica_size = replica_client.Command({"DBSIZE"});
        if (varied_replica_size == varied_source_size) break;
        std::this_thread::sleep_for(10ms);
      } while (std::chrono::steady_clock::now() < varied_deadline);
      Expect(varied_replica_size, varied_source_size,
             "replica convergence after varied-participant MSET");

      std::vector<std::string> values(kWideKeys);
      for (int round = 0; round < kWideRounds; ++round) {
        std::vector<std::string_view> command{"MSET"};
        command.reserve(1 + 2 * kWideKeys);
        for (int key = 0; key < kWideKeys; ++key) {
          values[key] =
              std::to_string(round) + ":" + std::to_string(key) + ":" + payload;
          command.push_back(wide_keys[key]);
          command.push_back(values[key]);
        }
        Expect(source_client.Command(command), "+OK",
               "wide MSET round " + std::to_string(round));
      }
      // A multi-key DEL settles the same per-worker journals. Deleting only
      // half the keys makes the replica's final state prove ordering: had the
      // DEL been applied before the last MSET round, those keys would still
      // hold values.
      std::vector<std::string_view> wide_delete{"DEL"};
      for (int key = 0; key < kWideKeys; key += 2) {
        wide_delete.push_back(wide_keys[key]);
      }
      Expect(source_client.Command(wide_delete),
             ":" + std::to_string(kWideKeys / 2), "wide multi-key DEL");

      std::vector<std::string_view> wide_read{"MGET"};
      std::string expected = "*" + std::to_string(kWideKeys) + "\r\n";
      for (int key = 0; key < kWideKeys; ++key) {
        wide_read.push_back(wide_keys[key]);
        if (key != 0) expected += "\r\n";
        expected += key % 2 == 0 ? std::string("$-1") : Bulk(values[key]);
      }
      Expect(source_client.Command(wide_read), expected,
             "source state after wide multi-key writes");
      const auto converge_deadline = std::chrono::steady_clock::now() + 120s;
      std::string replicated;
      while (std::chrono::steady_clock::now() < converge_deadline) {
        replicated = replica_client.Command(wide_read);
        if (replicated == expected) break;
        std::this_thread::sleep_for(20ms);
      }
      Expect(replicated, expected, "replicated wide multi-key effects");
      // Whatever the run reserved has to have been given back: the source
      // must still admit a further write rather than sit permanently wedged
      // against its own waterline. A wedge surfaces as the client's socket
      // read timing out, not as an error reply, since a write that cannot be
      // admitted simply never answers.
      Expect(source_client.Command({"SET", "wide-multikey:after", "ok"}), "+OK",
             "source write after the wide multi-key burst");

#if LAVIK_TEST_FAULTS_AVAILABLE
      // Once a Function child has durably installed a catalog, losing any
      // participant's transaction marker makes the source history unsafe.
      // Inject that exact failure and require a top-level fail-closed EXEC;
      // the process-wide LOADING fence must reject a fresh connection too.
      constexpr std::string_view catalog_failure_library =
          "#!lua name=catalog_failure\n"
          "redis.register_function{function_name='catalog_failure', "
          "callback=function() return 3 end, flags={'no-writes'}}";
      Expect(source_client.Command({"MULTI"}), "+OK",
             "begin failing catalog transaction");
      Expect(source_client.Command({"SET", "bar", "catalog-failure"}),
             "+QUEUED", "queue failing catalog transaction key");
      Expect(
          source_client.Command({"FUNCTION", "LOAD", catalog_failure_library}),
          "+QUEUED", "queue failing Function catalog child");
      const std::string failed_catalog_exec = source_client.Command({"EXEC"});
      if (!failed_catalog_exec.starts_with(
              "-ERR durable Function catalog committed but EXEC replication "
              "failed:")) {
        Fail("catalog publication failure returned '" + failed_catalog_exec +
             "'");
      }
      if (!source_client.WaitForClose(5s)) {
        Fail("catalog publication failure did not close the client");
      }
      RespClient fenced_source_client = Connect(source_port);
      Expect(fenced_source_client.Command({"GET", "bar"}),
             "-LOADING Lavik is loading the dataset from the primary",
             "process fence after catalog publication failure");
#endif
    }

#if LAVIK_TEST_FAULTS_AVAILABLE
    // Replication transaction order gate admission regression: with an ONLINE
    // replica, a cross-shard MSET acquires the global order gate while the
    // test-only LAVIK_REPLICATION_ORDER_HOLD_MS hook holds it for 10 s. A
    // same-shard hashtag MSET (proven single-participant via
    // kCmdKeyViewComplete) must skip the gate and finish well inside that
    // window; a second cross-shard MSET must keep waiting on the gate and
    // complete only after the hold ends.
    {
      CreateDataFile(gate_source_data, 256ULL * 1024 * 1024);
      CreateDataFile(gate_replica_data, 256ULL * 1024 * 1024);
      const std::uint16_t gate_source_port = FindFreePort();
      std::uint16_t gate_replica_port = FindFreePort();
      while (gate_replica_port == gate_source_port)
        gate_replica_port = FindFreePort();
      ServerProcess gate_source(argv[1], gate_source_port, gate_source_data,
                                gate_log_path, {}, {}, false, 4, "10000");
      ServerProcess gate_replica(argv[1], gate_replica_port, gate_replica_data,
                                 gate_log_path, {}, {}, false, 2);
      RespClient gate_source_client = Connect(gate_source_port);
      RespClient gate_replica_client = Connect(gate_replica_port);
      Expect(gate_replica_client.Command(
                 {"REPLICAOF", "127.0.0.1", std::to_string(gate_source_port)}),
             "+OK", "attach replica for order-gate regression");
      const auto gate_online_deadline = std::chrono::steady_clock::now() + 120s;
      bool gate_online = false;
      while (!gate_online &&
             std::chrono::steady_clock::now() < gate_online_deadline) {
        gate_online =
            gate_replica_client.Command({"INFO", "replication"})
                .find("lavik_replication_state:online") != std::string::npos;
        if (!gate_online) std::this_thread::sleep_for(20ms);
      }
      if (!gate_online) Fail("replica did not come online for the gate test");
      Expect(gate_replica_client.Command({"READONLY"}), "+OK",
             "serve gate-test reads from the replica");

      // The 10 s hold is claimed once per process by the first order-gate
      // acquisition; this MSET is the first client write on the source.
      // "order-a"/"order-b" hash to different shards with four workers, so
      // this request must take the global order gate.
      auto paused_cross = std::async(std::launch::async, [gate_source_port] {
        RespClient client = Connect(gate_source_port);
        return client.Command(
            {"MSET", "order-a", "paused-1", "order-b", "paused-2"});
      });
      // Do not guess with sleeps: the gate hold is in effect once the hook
      // logs its marker.
      const auto marker_deadline = std::chrono::steady_clock::now() + 30s;
      bool marker_seen = false;
      while (!marker_seen &&
             std::chrono::steady_clock::now() < marker_deadline) {
        marker_seen = ReadFile(gate_log_path)
                          .find("LAVIK_REPLICATION_ORDER_HOLD_MS holding") !=
                      std::string::npos;
        if (!marker_seen) std::this_thread::sleep_for(20ms);
      }
      if (!marker_seen) Fail("order-gate hold did not engage for the MSET");

      // Hashtag keys share one slot: this MSET is proven single-shard by
      // kCmdKeyViewComplete and must skip the held global gate.
      auto same_shard = std::async(std::launch::async, [gate_source_port] {
        RespClient client = Connect(gate_source_port);
        return client.Command({"MSET", "{og}a", "fast-1", "{og}b", "fast-2"});
      });
      if (same_shard.wait_for(5s) != std::future_status::ready) {
        Fail("same-shard MSET blocked behind the replication order gate");
      }
      Expect(same_shard.get(), "+OK", "same-shard MSET during held gate");

      // A second cross-shard MSET still serializes on the gate: it must be
      // incomplete at the 5 s checkpoint and finish once the 10 s hold (and
      // with it the first MSET's gate acquisition) ends.
      auto blocked_cross = std::async(std::launch::async, [gate_source_port] {
        RespClient client = Connect(gate_source_port);
        return client.Command(
            {"MSET", "order-c", "late-1", "order-d", "late-2"});
      });
      if (blocked_cross.wait_for(5s) == std::future_status::ready) {
        Fail("cross-shard MSET completed while the order gate was held");
      }
      Expect(paused_cross.get(), "+OK", "held cross-shard MSET resumed");
      if (blocked_cross.wait_for(20s) != std::future_status::ready) {
        Fail("cross-shard MSET did not complete after the gate released");
      }
      Expect(blocked_cross.get(), "+OK", "queued cross-shard MSET");

      const std::string gate_expected =
          "*6\r\n" + Bulk("paused-1") + "\r\n" + Bulk("paused-2") + "\r\n" +
          Bulk("fast-1") + "\r\n" + Bulk("fast-2") + "\r\n" + Bulk("late-1") +
          "\r\n" + Bulk("late-2");
      const auto gate_converge_deadline =
          std::chrono::steady_clock::now() + 60s;
      std::string gate_replicated;
      while (std::chrono::steady_clock::now() < gate_converge_deadline) {
        gate_replicated =
            gate_replica_client.Command({"MGET", "order-a", "order-b", "{og}a",
                                         "{og}b", "order-c", "order-d"});
        if (gate_replicated == gate_expected) break;
        std::this_thread::sleep_for(20ms);
      }
      Expect(gate_replicated, gate_expected, "replicated gate-test effects");
    }
#endif
  } catch (const std::exception& error) {
    std::cerr << error.what() << "\n--- Lavik log ---\n"
              << ReadFile(log_path) << "\n--- gate-test log ---\n"
              << ReadFile(gate_log_path) << std::flush;
    exit_code = 1;
  }

  (void)::unlink(data_path.c_str());
  (void)::unlink(log_path.c_str());
  (void)::unlink(source_data.c_str());
  (void)::unlink(replica_data.c_str());
  (void)::unlink(gate_source_data.c_str());
  (void)::unlink(gate_replica_data.c_str());
  (void)::unlink(standby_data.c_str());
  (void)::unlink(gate_log_path.c_str());
  std::cout << (exit_code == 0 ? "multikey e2e passed\n" : "") << std::flush;
  return exit_code;
}
