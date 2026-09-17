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

// External values are stored in dedicated extent blocks, referenced by a
// manifest in the record. Recovery has to match every manifest against the
// blocks it points at, and the owner of an extent block after a worker-count
// change is unrelated to the owner of the block holding the manifest. This
// test restarts with a different worker count and expects the data back.
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <fstream>
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

class RespClient {
 public:
  explicit RespClient(int fd) : fd_(fd) {}
  RespClient(const RespClient&) = delete;
  RespClient& operator=(const RespClient&) = delete;
  RespClient(RespClient&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }
  RespClient& operator=(RespClient&&) = delete;
  ~RespClient() {
    if (fd_ >= 0) {
      ::close(fd_);
    }
  }

  std::string Command(const std::vector<std::string_view>& args) {
    Send(args);
    return ReadLine();
  }

  // Reads a bulk reply in full, returning only its length so multi-megabyte
  // values do not have to be held twice.
  std::int64_t CommandBulkLength(const std::vector<std::string_view>& args,
                                 char expected_fill) {
    Send(args);
    const std::string head = ReadLine();
    if (head.empty() || head[0] != '$') {
      Fail("expected a bulk reply, got '" + head + "'");
    }
    const std::int64_t length = std::stoll(head.substr(1));
    if (length < 0) {
      return length;
    }
    std::size_t remaining = static_cast<std::size_t>(length) + 2;
    std::vector<char> chunk(1 << 16);
    while (remaining > 0) {
      const ssize_t received =
          ::recv(fd_, chunk.data(), std::min(chunk.size(), remaining), 0);
      if (received < 0) {
        if (errno == EINTR) {
          continue;
        }
        Fail("recv failed: " + std::string(std::strerror(errno)));
      }
      if (received == 0) {
        Fail("server closed the connection mid-bulk");
      }
      const std::size_t payload = std::min(static_cast<std::size_t>(received),
                                           remaining > 2 ? remaining - 2 : 0);
      for (std::size_t i = 0; i < payload; ++i) {
        if (chunk[i] != expected_fill) {
          Fail("bulk payload byte mismatch");
        }
      }
      remaining -= static_cast<std::size_t>(received);
    }
    return length;
  }

 private:
  void Send(const std::vector<std::string_view>& args) {
    std::string request = "*" + std::to_string(args.size()) + "\r\n";
    for (std::string_view arg : args) {
      request += "$" + std::to_string(arg.size()) + "\r\n";
      request.append(arg);
      request += "\r\n";
    }
    SendAll(request);
  }

  void SendAll(std::string_view bytes) {
    while (!bytes.empty()) {
      const ssize_t sent =
          ::send(fd_, bytes.data(), bytes.size(), MSG_NOSIGNAL);
      if (sent < 0) {
        if (errno == EINTR) {
          continue;
        }
        Fail("send failed: " + std::string(std::strerror(errno)));
      }
      if (sent == 0) {
        Fail("send returned zero bytes");
      }
      bytes.remove_prefix(static_cast<std::size_t>(sent));
    }
  }

  std::string ReadLine() {
    std::string response;
    while (!response.ends_with("\r\n")) {
      char byte = 0;
      const ssize_t received = ::recv(fd_, &byte, 1, 0);
      if (received < 0) {
        if (errno == EINTR) {
          continue;
        }
        Fail("recv failed: " + std::string(std::strerror(errno)));
      }
      if (received == 0) {
        Fail("server closed the connection");
      }
      response.push_back(byte);
      if (response.size() > 4096) {
        Fail("unexpectedly long RESP status line");
      }
    }
    response.resize(response.size() - 2);
    return response;
  }

  int fd_ = -1;
};

std::uint16_t FindFreePort() {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (fd < 0) {
    Fail("socket failed while selecting a port");
  }
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  if (::bind(fd, reinterpret_cast<const sockaddr*>(&address),
             sizeof(address)) != 0) {
    ::close(fd);
    Fail("bind failed while selecting a port");
  }
  socklen_t address_bytes = sizeof(address);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&address),
                    &address_bytes) != 0) {
    ::close(fd);
    Fail("getsockname failed while selecting a port");
  }
  ::close(fd);
  return ntohs(address.sin_port);
}

void CreateDataFile(const std::string& path, std::uint64_t bytes) {
  const int fd =
      ::open(path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  if (fd < 0) {
    Fail("failed to create test data file");
  }
  const int allocated = ::posix_fallocate(fd, 0, static_cast<off_t>(bytes));
  const int close_error = ::close(fd);
  if (allocated != 0 || close_error != 0) {
    Fail("failed to size test data file");
  }
}

RespClient Connect(std::uint16_t port) {
  const auto deadline = std::chrono::steady_clock::now() + 30s;
  while (std::chrono::steady_clock::now() < deadline) {
    const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) {
      Fail("client socket failed");
    }
    timeval timeout{.tv_sec = 60, .tv_usec = 0};
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
  const auto deadline = std::chrono::steady_clock::now() + 30s;
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
  ServerProcess(std::string binary, std::uint16_t port,
                const std::string& data_path, const std::string& log_path,
                unsigned threads) {
    pid_ = ::fork();
    if (pid_ < 0) {
      Fail("fork failed");
    }
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
          std::to_string(threads),
          "--no-pin-workers",
          "--recv-buffers-per-worker",
          "0",
          "--data-file",
          data_path,
      };
      std::vector<char*> child_argv;
      child_argv.reserve(arguments.size() + 1);
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
    if (pid_ <= 0) {
      return;
    }
    if (::kill(pid_, SIGINT) != 0 && errno != ESRCH) {
      Fail("failed to signal Lavik");
    }
    const auto deadline = std::chrono::steady_clock::now() + 60s;
    while (std::chrono::steady_clock::now() < deadline) {
      int status = 0;
      const pid_t waited = ::waitpid(pid_, &status, WNOHANG);
      if (waited == pid_) {
        pid_ = -1;
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
          Fail("Lavik exited unsuccessfully");
        }
        return;
      }
      if (waited < 0) {
        Fail("waitpid failed");
      }
      std::this_thread::sleep_for(10ms);
    }
    Fail("Lavik did not stop after SIGINT");
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

void ExpectEventually(RespClient& client,
                      const std::vector<std::string_view>& command,
                      std::string_view expected, std::string_view operation) {
  const auto deadline = std::chrono::steady_clock::now() + 30s;
  std::string actual;
  do {
    actual = client.Command(command);
    if (actual == expected) {
      return;
    }
    std::this_thread::sleep_for(10ms);
  } while (std::chrono::steady_clock::now() < deadline);
  Expect(actual, expected, operation);
}

// Comfortably past the inline limit of one block minus its header, so each
// value lands in dedicated extent blocks.
constexpr std::size_t kExternalBytes = 9ULL * 1024 * 1024;
constexpr int kExternalKeys = 3;

std::string ExternalKey(int index) {
  // Exercise external keys and external values together. The key is well
  // above the fixed record-header key limit.
  return std::string(5000, static_cast<char>('a' + index)) + "-external";
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "usage: extent_recovery_e2e_test /path/to/lavik\n";
    return 2;
  }
  try {
    const std::string prefix = lavik::test::TestDataPath(
        "lavik-extent-recovery-" + std::to_string(::getpid()));
    const std::string data_path = prefix + ".data";
    const std::string log_path = prefix + ".log";
    (void)::unlink(data_path.c_str());
    (void)::unlink(log_path.c_str());
    CreateDataFile(data_path, 768ULL * 1024 * 1024);

    const std::string value(kExternalBytes, 'X');
    const std::string inline_combined_key(6ULL * 1024 * 1024, 'i');
    const std::string inline_combined_value(1ULL * 1024 * 1024, 'I');
    const std::string shared_extent_key(6ULL * 1024 * 1024, 's');
    const std::string shared_extent_value(6ULL * 1024 * 1024, 'S');
    const std::uint16_t port = FindFreePort();

    // Written under four workers.
    {
      ServerProcess server(argv[1], port, data_path, log_path, 4);
      RespClient client = Connect(port);
      for (int i = 0; i < kExternalKeys; ++i) {
        Expect(client.Command({"SET", ExternalKey(i), value}), "+OK",
               "external SET");
        Expect(client.Command({"STRLEN", ExternalKey(i)}),
               ":" + std::to_string(kExternalBytes), "external STRLEN");
      }
      Expect(
          client.Command({"SET", inline_combined_key, inline_combined_value}),
          "+OK", "inline combined SET");
      Expect(client.Command({"STRLEN", inline_combined_key}),
             ":" + std::to_string(inline_combined_value.size()),
             "inline combined STRLEN");
      Expect(client.Command({"SET", shared_extent_key, shared_extent_value}),
             "+OK", "shared extent SET");
      Expect(client.Command({"STRLEN", shared_extent_key}),
             ":" + std::to_string(shared_extent_value.size()),
             "shared extent STRLEN");
      server.Stop();
    }

    // Recovered under two. Every extent block is reassigned by a hash of its
    // own block id, so its owner no longer follows the manifest's owner.
    {
      ServerProcess server(argv[1], port, data_path, log_path, 2);
      RespClient client = ConnectReady(port);
      for (int i = 0; i < kExternalKeys; ++i) {
        Expect(client.Command({"STRLEN", ExternalKey(i)}),
               ":" + std::to_string(kExternalBytes),
               "recovered external STRLEN");
        const std::int64_t length =
            client.CommandBulkLength({"GET", ExternalKey(i)}, 'X');
        if (length != static_cast<std::int64_t>(kExternalBytes)) {
          Fail("recovered external GET returned " + std::to_string(length));
        }
      }
      if (client.CommandBulkLength({"GET", inline_combined_key}, 'I') !=
          static_cast<std::int64_t>(inline_combined_value.size())) {
        Fail("recovered inline combined GET returned the wrong value");
      }
      if (client.CommandBulkLength({"GET", shared_extent_key}, 'S') !=
          static_cast<std::int64_t>(shared_extent_value.size())) {
        Fail("recovered shared extent GET returned the wrong value");
      }
      // Overwriting retires the extents, which reclaims them through their
      // owning worker.
      for (int i = 0; i < kExternalKeys; ++i) {
        Expect(client.Command({"SET", ExternalKey(i), "small"}), "+OK",
               "external overwrite");
        Expect(client.Command({"STRLEN", ExternalKey(i)}), ":5",
               "overwritten STRLEN");
      }
      Expect(client.Command({"SET", inline_combined_key, "small"}), "+OK",
             "inline combined overwrite");
      Expect(client.Command({"SET", shared_extent_key, "small"}), "+OK",
             "shared extent overwrite");
      server.Stop();
    }

    // Restart after overwrite at a third worker count. Old roots may remain
    // in partially live record blocks, so a shared key/value extent chain
    // must survive until the root block is durably retired.
    {
      ServerProcess server(argv[1], port, data_path, log_path, 3);
      RespClient client = ConnectReady(port);
      for (int i = 0; i < kExternalKeys; ++i) {
        const std::string key = ExternalKey(i);
        ExpectEventually(client, {"STRLEN", key}, ":5", "post-reclaim STRLEN");
      }
      ExpectEventually(client, {"STRLEN", inline_combined_key}, ":5",
                       "post-reclaim inline combined STRLEN");
      ExpectEventually(client, {"STRLEN", shared_extent_key}, ":5",
                       "post-reclaim shared extent STRLEN");
      server.Stop();
    }

    (void)::unlink(data_path.c_str());
    (void)::unlink(log_path.c_str());
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << "\n";
    const std::string log_path = lavik::test::TestDataPath(
        "lavik-extent-recovery-" + std::to_string(::getpid()) + ".log");
    std::ifstream log(log_path);
    if (log) {
      std::string contents((std::istreambuf_iterator<char>(log)),
                           std::istreambuf_iterator<char>());
      if (!contents.empty()) {
        std::cerr << "--- Lavik log ---\n" << contents;
      }
    }
    return 1;
  }
}
