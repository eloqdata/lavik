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

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
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

#include "lavik/storage/format.h"
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
    std::string request = "*" + std::to_string(args.size()) + "\r\n";
    for (std::string_view arg : args) {
      request += "$" + std::to_string(arg.size()) + "\r\n";
      request.append(arg);
      request += "\r\n";
    }
    SendAll(request);
    return ReadLine();
  }

 private:
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
  const auto deadline = std::chrono::steady_clock::now() + 20s;
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
      try {
        RespClient client(fd);
        if (client.Command({"PING"}) == "+PONG") {
          return client;
        }
      } catch (const std::exception&) {
        // The listener is created before recovery finishes. A successful TCP
        // connect is therefore not sufficient to prove that a worker is ready
        // to serve requests; retry until PING completes too.
      }
    } else {
      ::close(fd);
    }
    std::this_thread::sleep_for(10ms);
  }
  Fail("timed out connecting to Lavik");
}

class ServerProcess {
 public:
  ServerProcess(std::string binary, std::uint16_t port,
                std::vector<std::string> data_paths, std::string log_path,
                unsigned flush_max_ms = 1000) {
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
          "1",
          "--recv-buffers-per-worker",
          "0",
          "--flush-max-ms",
          std::to_string(flush_max_ms),
      };
      for (const std::string& data_path : data_paths) {
        arguments.emplace_back("--data-file");
        arguments.push_back(data_path);
      }
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

  void Crash() {
    if (pid_ <= 0) {
      return;
    }
    if (::kill(pid_, SIGKILL) != 0 && errno != ESRCH) {
      Fail("failed to kill Lavik");
    }
    int status = 0;
    if (::waitpid(pid_, &status, 0) != pid_) {
      Fail("waitpid failed after SIGKILL");
    }
    pid_ = -1;
  }

  // Waits for the server to exit on its own (an armed crash point) and
  // returns its exit code.
  int AwaitExit(std::chrono::seconds timeout) {
    if (pid_ <= 0) {
      Fail("no server process to await");
    }
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
      int status = 0;
      const pid_t waited = ::waitpid(pid_, &status, WNOHANG);
      if (waited == pid_) {
        pid_ = -1;
        if (!WIFEXITED(status)) {
          Fail("Lavik terminated without an exit status");
        }
        return WEXITSTATUS(status);
      }
      if (waited < 0) {
        Fail("waitpid failed");
      }
      std::this_thread::sleep_for(10ms);
    }
    Fail("Lavik did not exit before the deadline");
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

std::string ReadFile(const std::string& path) {
  std::ifstream input(path);
  return std::string(std::istreambuf_iterator<char>(input),
                     std::istreambuf_iterator<char>());
}

std::vector<std::uint64_t> ReadAllocatedRecordBlocks(const std::string& path) {
  const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    Fail("failed to open data file for block scan");
  }
  off_t bytes = ::lseek(fd, 0, SEEK_END);
  if (bytes < 0) {
    ::close(fd);
    Fail("failed to size data file for block scan");
  }
  const std::uint64_t capacity_blocks =
      static_cast<std::uint64_t>(bytes) / lavik::storage::kStorageBlockBytes;
  const std::uint32_t begin = lavik::storage::DataBlockBegin(capacity_blocks);
  alignas(lavik::storage::kDirectIoAlignment)
      std::array<std::byte, lavik::storage::kBlockHeaderBytes>
          header{};
  std::vector<std::uint64_t> blocks;
  for (std::uint32_t local = begin; local < capacity_blocks; ++local) {
    const off_t offset =
        static_cast<off_t>(local) * lavik::storage::kStorageBlockBytes;
    const ssize_t read = ::pread(fd, header.data(), header.size(), offset);
    if (read != static_cast<ssize_t>(header.size())) {
      ::close(fd);
      Fail("failed to read block header during test scan");
    }
    lavik::storage::BlockHeader decoded{};
    if (lavik::storage::DecodeBlockHeaderPages(header, &decoded) &&
        decoded.kind_ == lavik::storage::BlockKind::kRecords) {
      blocks.push_back(decoded.block_id_);
    }
  }
  ::close(fd);
  return blocks;
}

bool BlockBitmapBitIsClear(const std::string& path, std::uint64_t block_id) {
  const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    Fail("failed to open data file while reading allocation bitmap");
  }
  const std::uint32_t local_block = lavik::storage::LocalBlockId(block_id);
  const std::size_t byte_index = local_block / 8;
  const std::uint32_t page_index = static_cast<std::uint32_t>(
      byte_index / lavik::storage::kMetadataPagePayloadBytes);
  const std::size_t payload_byte =
      byte_index % lavik::storage::kMetadataPagePayloadBytes;
  alignas(lavik::storage::kDirectIoAlignment)
      std::array<std::byte, lavik::storage::kDirectIoAlignment>
          page{};
  std::array<std::byte, lavik::storage::kMetadataPagePayloadBytes>
      selected_payload{};
  std::uint64_t selected_generation = 0;
  for (unsigned slot = 0; slot < 2; ++slot) {
    const off_t offset =
        static_cast<off_t>(lavik::storage::MetadataPageSlotOffset(
            lavik::storage::kScanBitmapMetadataOffset, page_index, slot));
    const ssize_t read = ::pread(fd, page.data(), page.size(), offset);
    if (read != static_cast<ssize_t>(page.size())) {
      ::close(fd);
      Fail("failed to read allocation bitmap page");
    }
    std::array<std::byte, lavik::storage::kMetadataPagePayloadBytes> payload{};
    std::uint64_t generation = 0;
    if (lavik::storage::DecodeMetadataPage(
            page, lavik::storage::MetadataPageKind::kScanBitmap, page_index,
            &generation, payload) &&
        generation > selected_generation) {
      selected_generation = generation;
      selected_payload = payload;
    }
  }
  ::close(fd);
  if (selected_generation == 0) {
    Fail("allocation bitmap has no valid metadata page");
  }
  const unsigned bit = local_block % 8;
  return (std::to_integer<unsigned>(selected_payload[payload_byte]) &
          (1U << bit)) == 0;
}

std::uint64_t CopyCommittedHeaderToUnusedAllocatedBlock(
    const std::string& path) {
  const int fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
  if (fd < 0) {
    Fail("failed to open data file while injecting a stale block header");
  }
  const off_t bytes = ::lseek(fd, 0, SEEK_END);
  if (bytes < 0) {
    ::close(fd);
    Fail("failed to size data file while injecting a stale block header");
  }
  const std::uint64_t capacity_blocks =
      static_cast<std::uint64_t>(bytes) / lavik::storage::kStorageBlockBytes;
  const std::uint32_t begin = lavik::storage::DataBlockBegin(capacity_blocks);
  alignas(lavik::storage::kDirectIoAlignment)
      std::array<std::byte, lavik::storage::kBlockHeaderBytes>
          candidate{};
  std::array<std::byte, lavik::storage::kBlockHeaderBytes> committed{};
  std::uint32_t committed_local = 0;
  std::uint32_t unused_local = 0;
  for (std::uint32_t local = begin; local < capacity_blocks; ++local) {
    const off_t offset =
        static_cast<off_t>(local) * lavik::storage::kStorageBlockBytes;
    const ssize_t read =
        ::pread(fd, candidate.data(), candidate.size(), offset);
    if (read != static_cast<ssize_t>(candidate.size())) {
      ::close(fd);
      Fail("failed to scan headers while injecting a stale block header");
    }
    lavik::storage::BlockHeader decoded{};
    if (lavik::storage::DecodeBlockHeaderPages(candidate, &decoded) &&
        decoded.kind_ == lavik::storage::BlockKind::kRecords) {
      if (committed_local == 0) {
        committed = candidate;
        committed_local = local;
      }
      continue;
    }
    if (unused_local == 0 &&
        std::all_of(candidate.begin(), candidate.end(),
                    [](std::byte byte) { return byte == std::byte{0}; })) {
      unused_local = local;
    }
  }
  if (committed_local == 0 || unused_local == 0) {
    ::close(fd);
    Fail("test data file lacks a committed and an unused block");
  }
  const std::uint64_t target = lavik::storage::MakeBlockId(0, unused_local);
  if (BlockBitmapBitIsClear(path, target)) {
    ::close(fd);
    Fail("stale-header target was not activated in the allocation bitmap");
  }
  const off_t target_offset =
      static_cast<off_t>(unused_local) * lavik::storage::kStorageBlockBytes;
  const ssize_t written =
      ::pwrite(fd, committed.data(), committed.size(), target_offset);
  const int sync_error =
      written == static_cast<ssize_t>(committed.size()) ? ::fdatasync(fd) : -1;
  const int close_error = ::close(fd);
  if (written != static_cast<ssize_t>(committed.size()) || sync_error != 0 ||
      close_error != 0) {
    Fail("failed to inject and persist a stale block header");
  }
  return target;
}

// These fixtures target ordinary-block defrag, including its PAUSE contract.
// Oversized parent keys keep large Strings in that layout; short-key grouped
// String expiration/reclamation is covered by the grouped write suite.
std::string OrdinaryKey(std::string_view suffix) {
  return std::string(8193, 'k') + std::string(suffix);
}

}  // namespace

int main(int argc, char** argv) {
  const bool stale_header_only =
      argc == 3 && std::string_view(argv[2]) == "--stale-header-only";
  const bool paused_defrag_only =
      argc == 3 && std::string_view(argv[2]) == "--paused-defrag-only";
  if (argc != 2 && !stale_header_only && !paused_defrag_only) {
    std::cerr << "usage: flushdb_reclaim_e2e_test /path/to/lavik "
                 "[--stale-header-only|--paused-defrag-only]\n";
    return 2;
  }

  const std::string prefix = lavik::test::TestDataPath(
      "lavik-flushdb-reclaim-" + std::to_string(::getpid()));
  const std::string data_path = prefix + ".data";
  const std::string unequal_path_a = prefix + "-unequal-a.data";
  const std::string unequal_path_b = prefix + "-unequal-b.data";
  const std::string expiry_full_path = prefix + "-expiry-full.data";
  const std::string defrag_crash_path = prefix + "-defrag-crash.data";
  const std::string stale_header_path = prefix + "-stale-header.data";
  const std::string log_path = prefix + ".log";
  (void)::unlink(data_path.c_str());
  (void)::unlink(unequal_path_a.c_str());
  (void)::unlink(unequal_path_b.c_str());
  (void)::unlink(expiry_full_path.c_str());
  (void)::unlink(defrag_crash_path.c_str());
  (void)::unlink(stale_header_path.c_str());
  (void)::unlink(log_path.c_str());

  try {
    const std::uint16_t port = FindFreePort();
    const std::string value(900 * 1024, 'v');
    CreateDataFile(data_path, 96ULL * 1024 * 1024);

    {
      ServerProcess server(argv[1], port, {data_path}, log_path);
      RespClient client = Connect(port);
      Expect(client.Command({"PING"}), "+PONG", "PING");

      unsigned inserted = 0;
      bool observed_full = false;
      for (unsigned i = 0; i < 32; ++i) {
        const std::string key = OrdinaryKey("old-" + std::to_string(i));
        const std::string response = client.Command({"SET", key, value});
        if (response == "+OK") {
          ++inserted;
          continue;
        }
        if (response.starts_with("-ERR ") &&
            response.find("out of disk space") != std::string::npos) {
          observed_full = true;
          break;
        }
        Fail("SET returned an unexpected response: " + response);
      }
      if (inserted == 0 || !observed_full) {
        Fail("test device did not reach foreground block exhaustion");
      }

      // A paused defrag queue cannot make allocation progress. Treating its
      // pending entries as reclaim work makes a full-device SET wait forever
      // for work that is explicitly disabled instead of reporting FULL.
      Expect(client.Command({"DEFRAG", "PAUSE"}), "+OK", "DEFRAG PAUSE");
      Expect(client.Command({"FLUSHDB"}), "+OK", "FLUSHDB");
      const std::string paused_response =
          client.Command({"SET", OrdinaryKey("paused-fresh"), value});
      if (!paused_response.starts_with("-ERR ") ||
          paused_response.find("out of disk space") == std::string::npos) {
        Fail("paused defrag did not report stable device exhaustion: " +
             paused_response);
      }
      Expect(client.Command({"DEFRAG", "RESUME"}), "+OK", "DEFRAG RESUME");
      // The active block has become entirely dead. Once resumed, this write
      // waits while defrag returns it to the ready pool.
      Expect(client.Command({"SET", OrdinaryKey("fresh"), value}), "+OK",
             "post-FLUSHDB SET");
      Expect(client.Command({"DBSIZE"}), ":1", "DBSIZE");
      Expect(client.Command(
                 {"EXISTS", OrdinaryKey("old-0"), OrdinaryKey("fresh")}),
             ":1", "EXISTS before restart");
      server.Stop();
    }

    if (paused_defrag_only) {
      (void)::unlink(data_path.c_str());
      (void)::unlink(log_path.c_str());
      return 0;
    }

    // Fill a device after placing several unshielded, short-lived records in
    // its first records block. Once their TTLs elapse, appending a tombstone
    // has no foreground block available. Active expiration must be able to
    // retire those records in memory, allowing defrag to reclaim their block
    // and restore write availability.
    if (!stale_header_only) {
      // Keep one foreground block after the fixed defrag reserve. This
      // scenario needs every expiring record in the same reclaim candidate;
      // it must not depend on an empty Function catalog consuming capacity.
      CreateDataFile(expiry_full_path, 80ULL * 1024 * 1024);
      {
        ServerProcess server(argv[1], port, {expiry_full_path}, log_path);
        RespClient client = Connect(port);
        constexpr unsigned kExpiringKeys = 7;
        for (unsigned i = 0; i < kExpiringKeys; ++i) {
          // Keep the keys in one low-numbered partition so the assertion
          // measures retirement and reclaim, not a complete partition sweep.
          const std::string key =
              OrdinaryKey("{expiry-387}" + std::to_string(i));
          Expect(client.Command({"SET", key, value, "PX", "5000"}), "+OK",
                 "full-device expiring SET");
        }
        bool observed_full = false;
        for (unsigned i = 0; i < 64; ++i) {
          const std::string key = OrdinaryKey("full-live-" + std::to_string(i));
          const std::string response = client.Command({"SET", key, value});
          if (response == "+OK") continue;
          if (response.starts_with("-ERR ") &&
              response.find("out of disk space") != std::string::npos) {
            observed_full = true;
            break;
          }
          Fail("full-device SET returned an unexpected response: " + response);
        }
        if (!observed_full) {
          Fail("expiration test device did not reach foreground exhaustion");
        }

        // Active expiration advances through every logical (partition, DB)
        // map at background priority. Allow more than one complete sweep so
        // keys visited just before their TTL elapsed are revisited reliably
        // on slower/debug test runs.
        const auto reclaim_deadline = std::chrono::steady_clock::now() + 60s;
        bool write_recovered = false;
        while (std::chrono::steady_clock::now() < reclaim_deadline) {
          const std::string response =
              client.Command({"SET", OrdinaryKey("after-full-expiry"), value});
          if (response == "+OK") {
            write_recovered = true;
            break;
          }
          if (!response.starts_with("-ERR ") ||
              response.find("out of disk space") == std::string::npos) {
            Fail("post-expiration SET returned an unexpected response: " +
                 response);
          }
          std::this_thread::sleep_for(100ms);
        }
        if (!write_recovered) {
          Fail("expired records did not restore full-device write capacity");
        }
        std::vector<std::string_view> exists{"EXISTS"};
        std::vector<std::string> expiring_names;
        expiring_names.reserve(kExpiringKeys);
        for (unsigned i = 0; i < kExpiringKeys; ++i) {
          expiring_names.push_back(
              OrdinaryKey("{expiry-387}" + std::to_string(i)));
        }
        for (const std::string& key : expiring_names) exists.push_back(key);
        Expect(client.Command(exists), ":0", "full-device expired EXISTS");
        server.Stop();
      }
      {
        ServerProcess server(argv[1], port, {expiry_full_path}, log_path);
        RespClient client = Connect(port);
        Expect(client.Command({"EXISTS", OrdinaryKey("{expiry-387}0")}), ":0",
               "full-device expired key after restart");
        Expect(client.Command({"EXISTS", OrdinaryKey("after-full-expiry")}),
               ":1", "full-device recovered write after restart");
        server.Stop();
      }
    }

    // The bitmap means activated, not committed. Model an affected on-disk
    // image by putting a CRC-valid committed header in another activated but
    // unwritten physical block. Recovery must ignore the block-id mismatch,
    // retain current data, and leave the bitmap unchanged.
    CreateDataFile(stale_header_path, 96ULL * 1024 * 1024);
    {
      ServerProcess server(argv[1], port, {stale_header_path}, log_path, 10);
      RespClient client = Connect(port);
      Expect(client.Command({"SET", "fresh-format", "fresh-value"}), "+OK",
             "stale-header initial SET");
      server.Stop();
    }
    const std::uint64_t stale_header_target =
        CopyCommittedHeaderToUnusedAllocatedBlock(stale_header_path);
    {
      ServerProcess server(argv[1], port, {stale_header_path}, log_path, 10);
      RespClient client = Connect(port);
      Expect(client.Command({"DBSIZE"}), ":1", "stale-header DBSIZE");
      Expect(client.Command({"EXISTS", "missing", "fresh-format"}), ":1",
             "stale-header EXISTS");
      Expect(client.Command({"GET", "fresh-format"}), "$11",
             "stale-header GET");
      server.Stop();
    }
    if (BlockBitmapBitIsClear(stale_header_path, stale_header_target)) {
      Fail("recovery cleared the stale-header allocation bit");
    }
    if (stale_header_only) {
      (void)::unlink(data_path.c_str());
      (void)::unlink(stale_header_path.c_str());
      (void)::unlink(log_path.c_str());
      return 0;
    }

    {
      ServerProcess server(argv[1], port, {data_path}, log_path);
      RespClient client = Connect(port);
      Expect(client.Command({"PING"}), "+PONG", "restart PING");
      Expect(client.Command({"DBSIZE"}), ":1", "restart DBSIZE");
      Expect(client.Command(
                 {"EXISTS", OrdinaryKey("old-0"), OrdinaryKey("fresh")}),
             ":1", "EXISTS after restart");
      server.Stop();
    }

    CreateDataFile(unequal_path_a, 88ULL * 1024 * 1024);
    CreateDataFile(unequal_path_b, 96ULL * 1024 * 1024);
    {
      ServerProcess server(argv[1], port, {unequal_path_a, unequal_path_b},
                           log_path);
      RespClient client = Connect(port);
      Expect(client.Command({"PING"}), "+PONG", "unequal-device PING");
      for (unsigned i = 0; i < 20; ++i) {
        const std::string key = "unequal-" + std::to_string(i);
        Expect(client.Command({"SET", key, value}), "+OK",
               "unequal-device SET");
      }
      Expect(client.Command({"DBSIZE"}), ":20", "unequal-device DBSIZE");
      server.Stop();
    }

    // A defrag relocation updates the in-memory index before its destination
    // block is necessarily flushed. Keep periodic flush far away and arm the
    // "defrag-source-retired" crash point: the server dies the instant a
    // source block's cleared allocation bit becomes durable, with its stale
    // records still on disk. Recovery must skip that stale source header and
    // find every key through relocation records committed before the bitmap
    // update.
    constexpr unsigned kDefragKeys = 12000;
    CreateDataFile(defrag_crash_path, 128ULL * 1024 * 1024);
    {
      ::setenv("LAVIK_CRASH_POINT", "defrag-source-retired", 1);
      ServerProcess server(argv[1], port, {defrag_crash_path}, log_path, 60000);
      ::unsetenv("LAVIK_CRASH_POINT");
      RespClient client = Connect(port);
      const std::string small_value(2000, 'd');
      for (unsigned i = 0; i < kDefragKeys; ++i) {
        const std::string key = "defrag-crash-" + std::to_string(i);
        Expect(client.Command({"SET", key, small_value}), "+OK",
               "defrag crash initial SET");
      }

      const auto scan_deadline = std::chrono::steady_clock::now() + 20s;
      bool sources_durable = false;
      while (std::chrono::steady_clock::now() < scan_deadline) {
        if (ReadAllocatedRecordBlocks(defrag_crash_path).size() >= 2) {
          sources_durable = true;
          break;
        }
        std::this_thread::sleep_for(10ms);
      }
      if (!sources_durable) {
        Fail("initial record blocks did not become durable");
      }

      // Replace 80% of every original block's sequential key population. The
      // original blocks fall well below the 50% live-ratio threshold, defrag
      // retires one of them, and the armed crash point fires — possibly
      // while these writes are still in flight, so connection loss here is
      // the expected outcome, not an error.
      try {
        for (unsigned i = 0; i < kDefragKeys; ++i) {
          if (i % 5 == 0) {
            continue;
          }
          const std::string key = "defrag-crash-" + std::to_string(i);
          Expect(client.Command({"SET", key, small_value}), "+OK",
                 "defrag crash overwrite SET");
        }
      } catch (const std::exception&) {
        // The server died mid-write; AwaitExit verifies it was the armed
        // crash point and not an accident.
      }
      if (server.AwaitExit(60s) != 86) {
        Fail("server did not die at the armed defrag crash point");
      }
    }
    // Post-mortem: the crash left at least one retired source block —
    // allocation bit durably clear while its stale header is still on disk.
    bool retired_source_found = false;
    for (const std::uint64_t block_id :
         ReadAllocatedRecordBlocks(defrag_crash_path)) {
      if (BlockBitmapBitIsClear(defrag_crash_path, block_id)) {
        retired_source_found = true;
        break;
      }
    }
    if (!retired_source_found) {
      Fail("crash point fired without a durably retired source block");
    }
    {
      ServerProcess server(argv[1], port, {defrag_crash_path}, log_path);
      RespClient client = Connect(port);
      Expect(client.Command({"PING"}), "+PONG", "defrag crash restart PING");
      Expect(client.Command({"DBSIZE"}), ":" + std::to_string(kDefragKeys),
             "defrag crash restart DBSIZE");
      for (unsigned i : {0U, 1U, kDefragKeys / 2, kDefragKeys - 1}) {
        const std::string key = "defrag-crash-" + std::to_string(i);
        Expect(client.Command({"STRLEN", key}), ":2000",
               "defrag crash restart STRLEN");
      }
      server.Stop();
    }
    {
      ServerProcess server(argv[1], port, {unequal_path_a, unequal_path_b},
                           log_path);
      RespClient client = Connect(port);
      Expect(client.Command({"PING"}), "+PONG", "unequal-device restart PING");
      Expect(client.Command({"DBSIZE"}), ":20",
             "unequal-device restart DBSIZE");
      Expect(client.Command({"EXISTS", "unequal-0", "unequal-19"}), ":2",
             "unequal-device restart EXISTS");
      server.Stop();
    }

    (void)::unlink(data_path.c_str());
    (void)::unlink(unequal_path_a.c_str());
    (void)::unlink(unequal_path_b.c_str());
    (void)::unlink(expiry_full_path.c_str());
    (void)::unlink(defrag_crash_path.c_str());
    (void)::unlink(stale_header_path.c_str());
    (void)::unlink(log_path.c_str());
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    const std::string log = ReadFile(log_path);
    if (!log.empty()) {
      std::cerr << "--- Lavik log ---\n" << log;
    }
    (void)::unlink(data_path.c_str());
    (void)::unlink(unequal_path_a.c_str());
    (void)::unlink(unequal_path_b.c_str());
    (void)::unlink(expiry_full_path.c_str());
    (void)::unlink(defrag_crash_path.c_str());
    (void)::unlink(stale_header_path.c_str());
    (void)::unlink(log_path.c_str());
    return 1;
  }
}
