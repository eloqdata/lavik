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

#include "bycorf/net/server.h"
#include "lavik/memory.h"
#include "lavik/metrics.h"
#include "lavik/storage/engine.h"
#include "lavik/tx/tx_shard.h"
#include "support/test_data_path.h"

namespace {

using namespace std::chrono_literals;

constexpr std::uint64_t kMiB = 1024 * 1024;

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

class TombRaiderQuiesceService final : public bycorf::Service {
 public:
  explicit TombRaiderQuiesceService(lavik::storage::StorageEngine* storage)
      : storage_(storage) {}

  void Prepare(unsigned thread_count) override {
    if (thread_count != 1) {
      Fail("tomb raider quiesce test requires one worker");
    }
  }

  bycorf::Task<absl::Status> Run(bycorf::Worker& worker,
                                 bycorf::ServiceContext) override {
    lavik::BindMemoryAccountingShard(worker.id());
    lavik::tx::TxRuntime::Get()->shard(worker.id()).Bind(worker);
    result_ = co_await storage_->InitializeWorker(worker);
    if (result_.ok()) {
      auto seeded = co_await storage_->Set(0, "quiesce-running-round", "v");
      if (!seeded.ok()) result_ = seeded.status();
    }

    const auto running_deadline = std::chrono::steady_clock::now() + 10s;
    while (result_.ok() && !storage_->TombRaiderStats().running_ &&
           std::chrono::steady_clock::now() < running_deadline) {
      result_ = co_await bycorf::SleepFor(worker, 1ms);
    }
    if (result_.ok() && !storage_->TombRaiderStats().running_) {
      result_ = absl::Status(absl::StatusCode::kDeadlineExceeded,
                             "tomb raider round did not start");
    }

    // User OFF only closes scheduler admission; it intentionally leaves the
    // current maintenance round alone. Replica quiesce is the stronger path.
    if (result_.ok()) {
      result_ = co_await storage_->ConfigureTombRaider(
          {.action_ = lavik::storage::TombRaiderConfigAction::kOff});
    }
    const auto user_off = storage_->TombRaiderStats();
    if (result_.ok() && (user_off.enabled_ || !user_off.running_)) {
      result_ = absl::Status(
          absl::StatusCode::kFailedPrecondition,
          "user tomb raider OFF changed in-flight round semantics");
    }

    const std::uint64_t rounds_before = storage_->TombRaiderStats().rounds_;
    const auto quiesce_started = std::chrono::steady_clock::now();
    if (result_.ok()) {
      result_ = co_await storage_->QuiesceTombRaiderForReplica();
    }
    const auto quiesce_elapsed =
        std::chrono::steady_clock::now() - quiesce_started;
    const auto quiesced = storage_->TombRaiderStats();
    if (result_.ok() && quiesce_elapsed > 2s) {
      result_ = absl::Status(absl::StatusCode::kDeadlineExceeded,
                             "tomb raider quiesce was not bounded");
    }
    if (result_.ok() && (quiesced.enabled_ || quiesced.running_ ||
                         quiesced.rounds_ != rounds_before)) {
      result_ = absl::Status(
          absl::StatusCode::kFailedPrecondition,
          "quiesce did not forfeit the running round and disable scheduling");
    }

    if (result_.ok()) result_ = co_await bycorf::SleepFor(worker, 100ms);
    const auto stayed_quiesced = storage_->TombRaiderStats();
    if (result_.ok() && (stayed_quiesced.enabled_ || stayed_quiesced.running_ ||
                         stayed_quiesced.rounds_ != rounds_before)) {
      result_ = absl::Status(absl::StatusCode::kFailedPrecondition,
                             "tomb raider restarted after replica quiesce");
    }
    worker.RequestStop();
    co_return result_;
  }

  void Stop() noexcept override {}

  const absl::Status& result() const noexcept { return result_; }

 private:
  lavik::storage::StorageEngine* storage_ = nullptr;
  absl::Status result_ =
      absl::UnknownError("tomb raider quiesce service did not run");
};

void VerifyReplicaQuiesce(const std::string& path) {
  lavik::storage::StorageEngineOptions options;
  options.data_files_ = {path};
  options.buffers_.registered_bytes_ = 64 * kMiB;
  options.tomb_raider_interval_ms_ = 1;
  // A normal round would remain in this pacing sleep for a minute. Quiesce
  // must wake at a bounded checkpoint rather than waiting for it to finish.
  options.tomb_raider_sleep_ms_ = 60'000;
  lavik::storage::StorageEngine storage(std::move(options));
  lavik::InitWorkerMetrics(1);
  const absl::Status memory = lavik::InitMemoryLimit(512 * kMiB, 1);
  if (!memory.ok()) Fail(std::string(memory.message()));
  const absl::Status prepared = storage.Prepare(1);
  if (!prepared.ok()) Fail(std::string(prepared.message()));
  lavik::tx::TxRuntime::Create(1);

  TombRaiderQuiesceService service(&storage);
  bycorf::Server server;
  server.AddService(&service);
  bycorf::ServerOptions runtime;
  runtime.thread_count_ = 1;
  runtime.pin_workers_ = false;
  runtime.recv_buffer_count_ = 0;
  const absl::Status started = server.Start(runtime);
  if (!started.ok()) Fail(std::string(started.message()));
  server.WaitUntilStopped();
  if (!service.result().ok()) Fail(std::string(service.result().message()));
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
      try {
        RespClient client(fd);
        if (client.Command({"PING"}) == "+PONG") return client;
      } catch (const std::exception&) {
        // The listener may be bound before storage recovery has installed the
        // worker services. That connection is reset during initialization;
        // reconnect until the command path itself is ready.
      }
      std::this_thread::sleep_for(10ms);
      continue;
    }
    ::close(fd);
    std::this_thread::sleep_for(10ms);
  }
  Fail("timed out connecting to Lavik");
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

long long InfoField(RespClient& client, std::string_view section,
                    std::string_view field) {
  const std::string info = client.Command({"INFO", section});
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

long long StatField(RespClient& client, std::string_view field) {
  return InfoField(client, "stats", field);
}

void VerifyTtlReapingReducesMemory(RespClient& client) {
  // Fill the index with exactly 100 MiB of inline key bytes, independently
  // of value sizes, so reclamation must release both buckets and entry spans.
  const std::size_t key_bytes = 1024;
  const std::size_t expiring_keys = 100 * kMiB / key_bytes;
  const long long minimum_memory_growth = 100 * kMiB;
  const auto ttl = 15s;
  const auto reap_timeout = 300s;
  constexpr long long kRemainingAllowance = 8 * 1024;
  Expect(client.Command({"TOMBRAIDER", "OFF"}), "+OK",
         "memory test raider OFF");
  const auto stopped_deadline = std::chrono::steady_clock::now() + 10s;
  while (StatField(client, "tomb_raider_running") != 0) {
    if (std::chrono::steady_clock::now() > stopped_deadline) {
      Fail("memory test could not drain the previous tomb raider round");
    }
    std::this_thread::sleep_for(10ms);
  }
  Expect(client.Command({"DEFRAG", "PAUSE"}), "+OK",
         "memory test defrag PAUSE");
  const long long reaped_before = StatField(client, "tomb_raider_reaped");
  auto key_for = [key_bytes](std::size_t index) {
    constexpr std::string_view prefix = "{ttl-memory}:";
    const std::string suffix = std::to_string(index);
    return std::string(prefix) +
           std::string(key_bytes - prefix.size() - suffix.size(), '0') + suffix;
  };
  const std::string survivor = key_for(expiring_keys);
  Expect(client.Command({"SET", survivor, "alive"}), "+OK",
         "memory survivor SET");
  // INFO's retained-memory gauge is published by the 100 ms health loop.
  // Warm its first arena span before measuring growth; the survivor keeps a
  // span allocated after reaping. Defrag is paused so disk-block reclamation
  // cannot supply an unrelated memory drop. RSS is deliberately not an
  // assertion: mimalloc may retain freed pages even when Lavik releases
  // ownership.
  std::this_thread::sleep_for(300ms);
  const long long warm_memory = InfoField(client, "memory", "used_memory");
  const std::string ttl_ms = std::to_string(
      std::chrono::duration_cast<std::chrono::milliseconds>(ttl).count());
  std::cout << "TTL memory fixture: keys=" << expiring_keys
            << " key-bytes=" << key_bytes
            << " total-key-bytes=" << expiring_keys * key_bytes
            << " warm=" << warm_memory << std::endl;
  for (std::size_t i = 0; i < expiring_keys; ++i) {
    const std::string key = key_for(i);
    Expect(client.Command({"SET", key, "v", "PX", ttl_ms}), "+OK",
           "memory TTL SET");
    if ((i + 1) % 16384 == 0) {
      std::cout << "TTL memory fixture written=" << i + 1 << std::endl;
    }
  }
  // The same hash tag concentrates the bucket growth in one index.
  // Requiring it to return near its warm footprint catches a missing shrink
  // even if Erase still frees entries and individual overflow buckets.
  const auto grown_deadline = std::chrono::steady_clock::now() + 5s;
  long long populated_memory = 0;
  do {
    populated_memory = InfoField(client, "memory", "used_memory");
    if (populated_memory >= warm_memory + minimum_memory_growth) break;
    if (std::chrono::steady_clock::now() > grown_deadline) {
      Fail("TTL fixture did not grow retained memory: warm=" +
           std::to_string(warm_memory) +
           " populated=" + std::to_string(populated_memory));
    }
    std::this_thread::sleep_for(50ms);
  } while (true);
  std::cout << "TTL memory fixture populated=" << populated_memory << std::endl;

  const std::string last_key = key_for(expiring_keys - 1);
  const auto expired_deadline = std::chrono::steady_clock::now() + ttl + 5s;
  while (client.Command({"GET", last_key}) != "$-1") {
    if (std::chrono::steady_clock::now() > expired_deadline) {
      Fail("memory fixture TTL did not expire");
    }
    std::this_thread::sleep_for(50ms);
  }
  // Reads enqueue lazy-expiration candidates; active expiration writes their
  // tombstones. No explicit DEL or FLUSH may stand in for that lifecycle.
  for (std::size_t i = 0; i < expiring_keys; ++i) {
    const std::string key = key_for(i);
    Expect(client.Command({"GET", key}), "$-1", "memory expired GET");
  }
  if (StatField(client, "tomb_raider_reaped") != reaped_before) {
    Fail("memory fixture was reaped before tomb raider was enabled");
  }
  Expect(client.Command({"TOMBRAIDER", "BLOCK-SLEEP", "0"}), "+OK",
         "memory test raider pacing");
  Expect(client.Command({"TOMBRAIDER", "INTERVAL", "10"}), "+OK",
         "memory test raider interval");
  const auto reaped_deadline = std::chrono::steady_clock::now() + reap_timeout;
  auto next_progress = std::chrono::steady_clock::now();
  while (true) {
    const long long reaped =
        StatField(client, "tomb_raider_reaped") - reaped_before;
    if (reaped >= static_cast<long long>(expiring_keys)) break;
    if (std::chrono::steady_clock::now() > reaped_deadline) {
      Fail("memory fixture TTL tombstones were not all reaped: " +
           std::to_string(reaped) + "/" + std::to_string(expiring_keys));
    }
    if (std::chrono::steady_clock::now() >= next_progress) {
      std::cout << "TTL memory fixture reaped=" << reaped << "/"
                << expiring_keys
                << " used_memory=" << InfoField(client, "memory", "used_memory")
                << std::endl;
      next_progress = std::chrono::steady_clock::now() + 10s;
    }
    std::this_thread::sleep_for(50ms);
  }
  // Poll only administrative counters after reaping: no key lookup or write
  // should be needed to finish the final incremental shrink in the background.
  const auto reclaimed_deadline = std::chrono::steady_clock::now() + 30s;
  long long reclaimed_memory = 0;
  do {
    reclaimed_memory = InfoField(client, "memory", "used_memory");
    if (reclaimed_memory <= warm_memory + kRemainingAllowance &&
        reclaimed_memory <= populated_memory - minimum_memory_growth) {
      break;
    }
    if (std::chrono::steady_clock::now() > reclaimed_deadline) {
      Fail("TTL reaping did not reclaim index memory: warm=" +
           std::to_string(warm_memory) +
           " populated=" + std::to_string(populated_memory) +
           " reclaimed=" + std::to_string(reclaimed_memory));
    }
    std::this_thread::sleep_for(50ms);
  } while (true);
  Expect(client.Command({"GET", survivor}), "$5\r\nalive",
         "memory survivor GET");
  std::cout << "TTL tomb raider memory: warm=" << warm_memory
            << " populated=" << populated_memory
            << " reclaimed=" << reclaimed_memory << " bytes\n";
  Expect(client.Command({"DEFRAG", "RESUME"}), "+OK",
         "memory test defrag RESUME");
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
    std::cerr << "usage: tomb_raider_e2e_test /path/to/lavik\n";
    return 2;
  }
  const std::string prefix = lavik::test::TestDataPath(
      "lavik-tombraider-" + std::to_string(::getpid()));
  const std::string data_path = prefix + ".data";
  const std::string quiesce_path = prefix + ".quiesce.data";
  const std::string log_path = prefix + ".log";
  (void)::unlink(data_path.c_str());
  (void)::unlink(quiesce_path.c_str());
  (void)::unlink(log_path.c_str());

  try {
    const std::uint16_t port = FindFreePort();
    // The fixture needs room for both 100 MiB key populations: values
    // and their later tombstones, while defrag is deliberately paused.
    CreateDataFile(data_path, 1024 * kMiB);
    CreateDataFile(quiesce_path, 192ULL * 1024 * 1024);
    {
      ServerProcess server(argv[1], port, data_path, log_path);
      RespClient client = Connect(port);
      Expect(client.Command({"PING"}), "+PONG", "PING");
      Expect(client.Command(
                 {"CONFIG", "SET", "defrag-max-active-per-device", "1"}),
             "+OK", "CONFIG defrag max active");
      Expect(client.Command({"CONFIG", "SET", "defrag-sleep-ms", "25"}), "+OK",
             "CONFIG defrag block sleep");
      Expect(client.Command({"CONFIG", "SET", "defrag-record-sleep-us", "7"}),
             "+OK", "CONFIG defrag record sleep");
      Expect(client.Command({"CONFIG", "SET", "defrag-paused", "yes"}), "+OK",
             "CONFIG defrag paused");
      const std::string defrag_status = client.Command({"DEFRAG", "STATUS"});
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
      Expect(client.Command({"DEFRAG", "RESUME"}), "+OK", "DEFRAG RESUME");
      Expect(client.Command({"DEFRAG", "MAX-ACTIVE", "0"}),
             "-ERR value is not an integer or out of range",
             "DEFRAG zero concurrency");
      Expect(client.Command({"DEFRAG", "INVALID", "1"}), "-ERR syntax error",
             "DEFRAG invalid setting");
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
      Expect(client.Command({"CONFIG", "SET", "tomb-raider-sleep-ms", "0"}),
             "+OK", "CONFIG tomb raider block sleep");
      if (StatField(client, "tomb_raider_block_sleep_ms") != 0) {
        Fail("tomb raider did not update block sleep");
      }

      const std::string daily = LocalTimeAfter(2s);
      Expect(client.Command({"CONFIG", "SET", "tomb-raider-daily-time", daily}),
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
      Expect(
          client.Command({"CONFIG", "SET", "tomb-raider-interval-ms", "500"}),
          "+OK", "CONFIG tomb raider interval");
      if (StatField(client, "tomb_raider_enabled") != 1) {
        Fail("tomb raider did not report enabled");
      }

      // Reapable: the only older record expires on its own, after which
      // nothing on disk needs the tombstone. Leave room for CI scheduling
      // delays between SET and DEL so expiration does not win the deletion.
      constexpr auto reapable_ttl = 5000ms;
      const std::string reapable_ttl_ms = std::to_string(reapable_ttl.count());
      Expect(client.Command({"SET", "reapable", "v", "PX", reapable_ttl_ms}),
             "+OK", "reapable SET");
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
      std::this_thread::sleep_for(reapable_ttl + 100ms);
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
      VerifyTtlReapingReducesMemory(client);
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
    VerifyReplicaQuiesce(quiesce_path);
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    std::cerr << "--- Lavik log ---\n" << ReadFile(log_path);
    (void)::unlink(data_path.c_str());
    (void)::unlink(quiesce_path.c_str());
    (void)::unlink(log_path.c_str());
    return 1;
  }
  (void)::unlink(data_path.c_str());
  (void)::unlink(quiesce_path.c_str());
  (void)::unlink(log_path.c_str());
  std::cout << "tomb raider e2e passed\n";
  return 0;
}
