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
#include <chrono>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <iterator>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "keylane/storage/format.h"

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
  RespClient(RespClient&& other) noexcept : fd_(other.fd_) {
    other.fd_ = -1;
  }
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
  const int fd = ::open(path.c_str(),
                        O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  if (fd < 0) {
    Fail("failed to create test data file");
  }
  const int allocated =
      ::posix_fallocate(fd, 0, static_cast<off_t>(bytes));
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
    (void)::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                       sizeof(timeout));
    (void)::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout,
                       sizeof(timeout));
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
  Fail("timed out connecting to Keylane");
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
      const int log_fd = ::open(log_path.c_str(),
                                O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC,
                                0600);
      if (log_fd >= 0) {
        (void)::dup2(log_fd, STDOUT_FILENO);
        (void)::dup2(log_fd, STDERR_FILENO);
        ::close(log_fd);
      }
      std::vector<std::string> arguments{
          binary,
          "--port", std::to_string(port),
          "--threads", "1",
          "--recv-buffers", "0",
          "--flush-max-ms", std::to_string(flush_max_ms),
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
      Fail("failed to signal Keylane");
    }
    const auto deadline = std::chrono::steady_clock::now() + 60s;
    while (std::chrono::steady_clock::now() < deadline) {
      int status = 0;
      const pid_t waited = ::waitpid(pid_, &status, WNOHANG);
      if (waited == pid_) {
        pid_ = -1;
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
          Fail("Keylane exited unsuccessfully");
        }
        return;
      }
      if (waited < 0) {
        Fail("waitpid failed");
      }
      std::this_thread::sleep_for(10ms);
    }
    Fail("Keylane did not stop after SIGINT");
  }

  void Crash() {
    if (pid_ <= 0) {
      return;
    }
    if (::kill(pid_, SIGKILL) != 0 && errno != ESRCH) {
      Fail("failed to kill Keylane");
    }
    int status = 0;
    if (::waitpid(pid_, &status, 0) != pid_) {
      Fail("waitpid failed after SIGKILL");
    }
    pid_ = -1;
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

std::vector<std::uint64_t> ReadAllocatedRecordBlocks(
    const std::string& path) {
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
      static_cast<std::uint64_t>(bytes) / keylane::storage::kStorageBlockBytes;
  const std::uint32_t begin =
      keylane::storage::DataBlockBegin(capacity_blocks);
  alignas(keylane::storage::kDirectIoAlignment)
      std::array<std::byte, keylane::storage::kBlockHeaderBytes> header{};
  std::vector<std::uint64_t> blocks;
  for (std::uint32_t local = begin; local < capacity_blocks; ++local) {
    const off_t offset = static_cast<off_t>(local) *
                         keylane::storage::kStorageBlockBytes;
    const ssize_t read = ::pread(fd, header.data(), header.size(), offset);
    if (read != static_cast<ssize_t>(header.size())) {
      ::close(fd);
      Fail("failed to read block header during test scan");
    }
    keylane::storage::BlockHeader decoded{};
    if (keylane::storage::DecodeBlockHeaderPages(header, &decoded) &&
        decoded.kind == keylane::storage::BlockKind::kRecords) {
      blocks.push_back(decoded.block_id);
    }
  }
  ::close(fd);
  return blocks;
}

bool BlockBitmapBitIsClear(const std::string& path,
                           std::uint64_t block_id) {
  const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    Fail("failed to open data file while reading allocation bitmap");
  }
  const std::uint32_t local_block =
      keylane::storage::LocalBlockId(block_id);
  const std::size_t byte_index = local_block / 8;
  const std::uint32_t page_index = static_cast<std::uint32_t>(
      byte_index / keylane::storage::kMetadataPagePayloadBytes);
  const std::size_t payload_byte =
      byte_index % keylane::storage::kMetadataPagePayloadBytes;
  alignas(keylane::storage::kDirectIoAlignment)
      std::array<std::byte, keylane::storage::kDirectIoAlignment> page{};
  std::array<std::byte, keylane::storage::kMetadataPagePayloadBytes>
      selected_payload{};
  std::uint64_t selected_generation = 0;
  for (unsigned slot = 0; slot < 2; ++slot) {
    const off_t offset = static_cast<off_t>(
        keylane::storage::MetadataPageSlotOffset(
            keylane::storage::kScanBitmapMetadataOffset, page_index, slot));
    const ssize_t read = ::pread(fd, page.data(), page.size(), offset);
    if (read != static_cast<ssize_t>(page.size())) {
      ::close(fd);
      Fail("failed to read allocation bitmap page");
    }
    std::array<std::byte, keylane::storage::kMetadataPagePayloadBytes>
        payload{};
    std::uint64_t generation = 0;
    if (keylane::storage::DecodeMetadataPage(
            page, keylane::storage::MetadataPageKind::kScanBitmap,
            page_index, &generation, payload) &&
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

bool BlockHeaderIsNonZero(const std::string& path, std::uint64_t block_id) {
  const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    Fail("failed to open data file while checking retired block header");
  }
  alignas(keylane::storage::kDirectIoAlignment)
      std::array<std::byte, keylane::storage::kBlockHeaderBytes> header{};
  const off_t offset = static_cast<off_t>(
      keylane::storage::LocalBlockOffset(block_id));
  const ssize_t read = ::pread(fd, header.data(), header.size(), offset);
  ::close(fd);
  if (read != static_cast<ssize_t>(header.size())) {
    Fail("failed to read retired block header");
  }
  return std::any_of(header.begin(), header.end(),
                     [](std::byte byte) { return byte != std::byte{0}; });
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "usage: flushdb_reclaim_e2e_test /path/to/keylane\n";
    return 2;
  }

  const std::string prefix =
      "/tmp/keylane-flushdb-reclaim-" + std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string unequal_path_a = prefix + "-unequal-a.data";
  const std::string unequal_path_b = prefix + "-unequal-b.data";
  const std::string defrag_crash_path = prefix + "-defrag-crash.data";
  const std::string log_path = prefix + ".log";
  (void)::unlink(data_path.c_str());
  (void)::unlink(unequal_path_a.c_str());
  (void)::unlink(unequal_path_b.c_str());
  (void)::unlink(defrag_crash_path.c_str());
  (void)::unlink(log_path.c_str());

  try {
    const std::uint16_t port = FindFreePort();
    const std::string value(900 * 1024, 'v');
    CreateDataFile(data_path, 80ULL * 1024 * 1024);

    {
      ServerProcess server(argv[1], port, {data_path}, log_path);
      RespClient client = Connect(port);
      Expect(client.Command({"PING"}), "+PONG", "PING");

      unsigned inserted = 0;
      bool observed_full = false;
      for (unsigned i = 0; i < 32; ++i) {
        const std::string key = "old-" + std::to_string(i);
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

      Expect(client.Command({"FLUSHDB"}), "+OK", "FLUSHDB");
      // The active block has become entirely dead. This write must wait while
      // FLUSHDB seals and flushes it and defrag returns it to the ready pool.
      Expect(client.Command({"SET", "fresh", value}), "+OK",
             "post-FLUSHDB SET");
      Expect(client.Command({"DBSIZE"}), ":1", "DBSIZE");
      Expect(client.Command({"EXISTS", "old-0", "fresh"}), ":1",
             "EXISTS before restart");
      server.Stop();
    }

    {
      ServerProcess server(argv[1], port, {data_path}, log_path);
      RespClient client = Connect(port);
      Expect(client.Command({"PING"}), "+PONG", "restart PING");
      Expect(client.Command({"DBSIZE"}), ":1", "restart DBSIZE");
      Expect(client.Command({"EXISTS", "old-0", "fresh"}), ":1",
             "EXISTS after restart");
      server.Stop();
    }

    CreateDataFile(unequal_path_a, 80ULL * 1024 * 1024);
    CreateDataFile(unequal_path_b, 88ULL * 1024 * 1024);
    {
      ServerProcess server(argv[1], port,
                           {unequal_path_a, unequal_path_b}, log_path);
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
    // block is necessarily flushed. Keep periodic flush far away, wait until
    // defrag has durably cleared an original source allocation bit, then crash
    // immediately. Recovery must skip that stale source header and find every
    // key through relocation records committed before the bitmap update.
    constexpr unsigned kDefragKeys = 12000;
    CreateDataFile(defrag_crash_path, 128ULL * 1024 * 1024);
    std::vector<std::uint64_t> original_blocks;
    {
      ServerProcess server(argv[1], port, {defrag_crash_path}, log_path,
                           60000);
      RespClient client = Connect(port);
      const std::string small_value(2000, 'd');
      for (unsigned i = 0; i < kDefragKeys; ++i) {
        const std::string key = "defrag-crash-" + std::to_string(i);
        Expect(client.Command({"SET", key, small_value}), "+OK",
               "defrag crash initial SET");
      }

      const auto scan_deadline = std::chrono::steady_clock::now() + 20s;
      while (std::chrono::steady_clock::now() < scan_deadline) {
        original_blocks = ReadAllocatedRecordBlocks(defrag_crash_path);
        if (original_blocks.size() >= 2) {
          break;
        }
        std::this_thread::sleep_for(10ms);
      }
      if (original_blocks.size() < 2) {
        Fail("initial record blocks did not become durable");
      }

      // Replace 80% of every original block's sequential key population. The
      // original blocks fall well below the 50% live-ratio threshold, while
      // the relocation destination remains partial with periodic flush
      // disabled.
      for (unsigned i = 0; i < kDefragKeys; ++i) {
        if (i % 5 == 0) {
          continue;
        }
        const std::string key = "defrag-crash-" + std::to_string(i);
        Expect(client.Command({"SET", key, small_value}), "+OK",
               "defrag crash overwrite SET");
      }

      std::optional<std::uint64_t> source_cleared;
      const auto defrag_deadline = std::chrono::steady_clock::now() + 30s;
      while (std::chrono::steady_clock::now() < defrag_deadline &&
             !source_cleared.has_value()) {
        for (const std::uint64_t block_id : original_blocks) {
          if (BlockBitmapBitIsClear(defrag_crash_path, block_id)) {
            source_cleared = block_id;
            break;
          }
        }
        if (!source_cleared.has_value()) {
          std::this_thread::sleep_for(1ms);
        }
      }
      if (!source_cleared.has_value()) {
        Fail("defrag did not clear an original source bitmap bit");
      }
      if (!BlockHeaderIsNonZero(defrag_crash_path, *source_cleared)) {
        Fail("defrag unexpectedly zeroed a retired source header");
      }
      server.Crash();
    }
    {
      ServerProcess server(argv[1], port, {defrag_crash_path}, log_path);
      RespClient client = Connect(port);
      Expect(client.Command({"PING"}), "+PONG", "defrag crash restart PING");
      Expect(client.Command({"DBSIZE"}),
             ":" + std::to_string(kDefragKeys),
             "defrag crash restart DBSIZE");
      for (unsigned i : {0U, 1U, kDefragKeys / 2, kDefragKeys - 1}) {
        const std::string key = "defrag-crash-" + std::to_string(i);
        Expect(client.Command({"STRLEN", key}), ":2000",
               "defrag crash restart STRLEN");
      }
      server.Stop();
    }
    {
      ServerProcess server(argv[1], port,
                           {unequal_path_a, unequal_path_b}, log_path);
      RespClient client = Connect(port);
      Expect(client.Command({"PING"}), "+PONG",
             "unequal-device restart PING");
      Expect(client.Command({"DBSIZE"}), ":20",
             "unequal-device restart DBSIZE");
      Expect(client.Command({"EXISTS", "unequal-0", "unequal-19"}), ":2",
             "unequal-device restart EXISTS");
      server.Stop();
    }

    (void)::unlink(data_path.c_str());
    (void)::unlink(unequal_path_a.c_str());
    (void)::unlink(unequal_path_b.c_str());
    (void)::unlink(defrag_crash_path.c_str());
    (void)::unlink(log_path.c_str());
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    const std::string log = ReadFile(log_path);
    if (!log.empty()) {
      std::cerr << "--- Keylane log ---\n" << log;
    }
    (void)::unlink(data_path.c_str());
    (void)::unlink(unequal_path_a.c_str());
    (void)::unlink(unequal_path_b.c_str());
    (void)::unlink(defrag_crash_path.c_str());
    (void)::unlink(log_path.c_str());
    return 1;
  }
}
