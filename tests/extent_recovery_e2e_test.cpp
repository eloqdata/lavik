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
// Inline records also exercise a recycled staging buffer across full and
// partial flushes, followed by cold recovery without an index checkpoint.
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

#include "../include/lavik/storage/format.h"
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

  void SetTimeout(int seconds) {
    timeval timeout{.tv_sec = seconds, .tv_usec = 0};
    if (::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) !=
            0 ||
        ::setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) !=
            0) {
      Fail("failed to set client socket timeout");
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
  [[noreturn]] void Fail(std::string message) const {
    ::Fail(operation_.empty() ? std::move(message)
                              : operation_ + ": " + message);
  }

  void Send(const std::vector<std::string_view>& args) {
    operation_ = std::string(args.front());
    if (args.size() > 1) {
      operation_ += " key_bytes=" + std::to_string(args[1].size());
    }
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
  std::string operation_;
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

// Inspect persisted flags so round trips cannot hide a writer that ignores
// the startup option. A recovered append block may contain both policies.
unsigned RecordCrcPolicies(const std::string& path) {
  using namespace lavik::storage;
  std::ifstream input(path, std::ios::binary | std::ios::ate);
  const auto blocks =
      static_cast<std::uint64_t>(input.tellg()) / kStorageBlockBytes;
  unsigned policies = 0;
  std::array<std::byte, kBlockHeaderBytes> pages{};
  for (std::uint64_t id = DataBlockBegin(blocks); id < blocks; ++id) {
    input.seekg(id * kStorageBlockBytes);
    input.read(reinterpret_cast<char*>(pages.data()), pages.size());
    if (!input) Fail("reading mixed CRC block header failed");
    BlockHeader block;
    if (!DecodeBlockHeaderPages(pages, &block) ||
        block.kind_ != BlockKind::kRecords || block.record_count_ == 0)
      continue;
    std::vector<std::byte> body(block.committed_bytes_ - kBlockHeaderBytes);
    input.read(reinterpret_cast<char*>(body.data()), body.size());
    if (!input) Fail("reading mixed CRC records failed");
    std::size_t offset = 0;
    unsigned records = 0;
    while (offset + kRecordHeaderBaseBytes <= body.size()) {
      std::uint64_t magic = 0;
      std::memcpy(&magic, body.data() + offset, sizeof(magic));
      if (magic == 0) {
        offset += kRecordAlignment;
        continue;
      }
      RecordHeader record;
      std::string_view key;
      if (!DecodeRecordHeader(std::span(body).subspan(offset), &record, &key) ||
          record.total_disk_bytes_ > body.size() - offset)
        Fail("invalid mixed CRC record header");
      policies |= record.no_crc_ ? 1 : 2;
      offset += record.total_disk_bytes_;
      ++records;
    }
    if (records != block.record_count_) Fail("mixed CRC record count mismatch");
  }
  return policies;
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
  // Recovery and allocator collection can take longer on the arm64 CI host.
  // Bound each readiness probe separately so a connection accepted before
  // startup finishes cannot consume the entire readiness deadline.
  const auto deadline = std::chrono::steady_clock::now() + 120s;
  while (std::chrono::steady_clock::now() < deadline) {
    try {
      RespClient client = Connect(port);
      client.SetTimeout(2);
      if (client.Command({"PING"}) == "+PONG") {
        client.SetTimeout(60);
        return client;
      }
    } catch (const std::exception&) {
      // Rapid same-port restarts can complete a loopback handshake against
      // the previous process generation. Reconnect until the command path
      // proves this socket belongs to the ready server.
    }
    std::this_thread::sleep_for(10ms);
  }
  Fail("timed out waiting for Lavik readiness after 120 seconds");
}

class ServerProcess {
 public:
  ServerProcess(std::string binary, std::uint16_t port,
                const std::string& data_path, const std::string& log_path,
                unsigned threads,
                const std::vector<std::string>& extra_arguments = {}) {
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
      arguments.insert(arguments.end(), extra_arguments.begin(),
                       extra_arguments.end());
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
    // This test checks durability across restarts, not a 60-second shutdown
    // limit. Give a contended CI file backend time to drain, while retaining
    // a finite bound that still reports a stuck shutdown.
    const auto deadline = std::chrono::steady_clock::now() + 120s;
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
    Fail("Lavik did not stop within 120 seconds after SIGINT");
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

// Large Strings exercise many grouped segments while the parent UUID is
// shared. The separate 9 MiB key below exercises KeyRecord payload extents.
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
    const std::uint16_t port = FindFreePort();
    (void)::unlink(data_path.c_str());
    (void)::unlink(log_path.c_str());

    // Alternate startup policies on one device. Previously written records,
    // extent payloads and indirect keys retain their own verification policy.
    // Keep defrag paused so relocation cannot convert every older record to
    // the current policy before inspecting the mixed flags. The final boot
    // reads both policies with CRC generation enabled again.
    CreateDataFile(data_path, 512ULL * 1024 * 1024);
    std::vector<std::pair<std::string, std::string>> mixed_records;
    for (int boot = 0; boot < 4; ++boot) {
      std::vector<std::string> arguments{"--defrag-paused"};
      if (boot % 2 != 0) arguments.emplace_back("--record-crc");
      ServerProcess server(argv[1], port, data_path, log_path, 2, arguments);
      RespClient client = ConnectReady(port);
      if (boot == 0) {
        // Catalog extents use the shared writer but must keep mandatory CRCs.
        if (client.CommandBulkLength(
                {"FUNCTION", "LOAD",
                 "#!lua name=cccc\n"
                 "redis.register_function('crc_policy_probe', "
                 "function(keys, args) return 42 end)"},
                'c') != 4) {
          Fail("loading CRC policy probe function failed");
        }
      }
      Expect(client.Command({"FCALL", "crc_policy_probe", "0"}), ":42",
             "catalog recovery across CRC policy switches");
      for (const auto& [key, payload] : mixed_records) {
        if (client.CommandBulkLength({"GET", key}, payload.front()) !=
            static_cast<std::int64_t>(payload.size())) {
          Fail("mixed CRC record changed after policy switch");
        }
      }
      if (boot < 3) {
        const std::string prefix = "crc-boot-" + std::to_string(boot);
        for (const auto& [key, payload] :
             std::vector<std::pair<std::string, std::string>>{
                 {prefix, std::string(200, 'i')},
                 {prefix + "-extent", std::string(9 * 1024 * 1024, 'e')},
                 {prefix + std::string(3000, 'k'), std::string(200, 'l')},
                 {prefix + std::string(9 * 1024 * 1024, 'K'),
                  std::string(200, 'L')}}) {
          Expect(client.Command({"SET", key, payload}), "+OK", "mixed CRC SET");
          mixed_records.emplace_back(key, payload);
        }
      }
      server.Stop();
      const unsigned policies = RecordCrcPolicies(data_path);
      if (policies != (boot == 0 ? 1U : 3U)) {
        Fail("persisted record CRC flags at boot " + std::to_string(boot) +
             " do not match startup policies: " + std::to_string(policies));
      }
    }
    (void)::unlink(data_path.c_str());
    (void)::unlink(log_path.c_str());

    // Two staging buffers must be reused across these four large values.
    // Nonzero prior contents must not escape into the next block's headers,
    // record alignment bytes, or the page tails of periodic partial flushes.
    // Disable checkpoints so the restart validates the actual record stream.
    // With reclamation paused, transaction blocks and pressure-sealed short
    // blocks also remain on disk. Leave room for them through the tail writes.
    CreateDataFile(data_path, 1024ULL * 1024 * 1024);
    std::vector<std::pair<std::string, std::string>> reused_records;
    for (int i = 0; i < 4; ++i) {
      const std::string key = "reuse-full-" + std::to_string(i);
      const std::size_t bytes = lavik::storage::kStorageBlockBytes -
                                lavik::storage::kBlockHeaderBytes -
                                lavik::storage::RecordHeaderBytes(key.size());
      reused_records.emplace_back(key, std::string(bytes, 'F'));
    }
    for (int i = 0; i < 32; ++i) {
      const std::size_t bytes = i < 8 ? i : 4093 + i;
      reused_records.emplace_back("reuse-tail-" + std::to_string(i),
                                  std::string(bytes, 'T'));
    }
    reused_records.emplace_back(std::string(5001, 'K'), std::string(12, 'E'));
    {
      ServerProcess server(
          argv[1], port, data_path, log_path, 1,
          {"--storage-write-buffers-per-worker=2", "--flush-max-ms=10",
           "--defrag-paused", "--no-shutdown-checkpoint"});
      RespClient client = ConnectReady(port);
      for (std::size_t i = 0; i < reused_records.size(); ++i) {
        const auto& [key, payload] = reused_records[i];
        Expect(client.Command({"SET", key, payload}), "+OK",
               "recycled SET " + std::to_string(i));
        if (i >= 4 && i % 4 == 0) {
          std::this_thread::sleep_for(25ms);
        }
      }
      server.Stop();
    }
    {
      ServerProcess server(argv[1], port, data_path, log_path, 2,
                           {"--no-shutdown-checkpoint"});
      RespClient client = ConnectReady(port);
      for (const auto& [key, payload] : reused_records) {
        const char fill = payload.empty() ? '\0' : payload.front();
        if (client.CommandBulkLength({"GET", key}, fill) !=
            static_cast<std::int64_t>(payload.size())) {
          Fail("recycled staging record changed after cold recovery");
        }
      }
      server.Stop();
    }
    (void)::unlink(data_path.c_str());
    (void)::unlink(log_path.c_str());
    CreateDataFile(data_path, 768ULL * 1024 * 1024);

    const std::string value(kExternalBytes, 'X');
    const std::string inline_combined_key(6ULL * 1024 * 1024, 'i');
    const std::string inline_combined_value(1ULL * 1024 * 1024, 'I');
    const std::string shared_extent_key(9ULL * 1024 * 1024, 's');
    const std::string shared_extent_value(6ULL * 1024 * 1024, 'S');

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
      // Overwriting retires value groups. The original key and its extents
      // stay reachable through the UUID while any old or new record needs it.
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
    // in partially live record blocks. UUIDs and their KeyRecord extents
    // survive until every referencing allocation is durably retired.
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
