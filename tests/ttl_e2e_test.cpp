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
#include <cstring>
#include <filesystem>
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

#include "absl/status/status.h"
#include "bycorf/net/server.h"
#include "lavik/memory.h"
#include "lavik/metrics.h"
#include "lavik/storage/engine.h"
#include "lavik/tx/tx_shard.h"
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

 private:
  std::string ReadReply() {
    const std::string line = ReadLine();
    if (line.starts_with('*') || line.starts_with('%')) {
      if (line == "*-1") return line;
      std::size_t count = 0;
      const auto [end, error] =
          std::from_chars(line.data() + 1, line.data() + line.size(), count);
      if (error != std::errc{} || end != line.data() + line.size()) {
        Fail("malformed aggregate reply length");
      }
      if (line.starts_with('%')) count *= 2;
      std::string reply = line;
      for (std::size_t i = 0; i < count; ++i) reply += "\r\n" + ReadReply();
      return reply;
    }
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
      try {
        RespClient client(fd);
        if (client.Command({"PING"}) == "+PONG") return client;
      } catch (const std::exception&) {
        // Rapid same-port restarts can complete a loopback handshake against
        // the previous process generation. Reconnect until the command path
        // proves this socket belongs to the ready server.
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
  ServerProcess(const std::string& binary, std::uint16_t port,
                const std::string& data_path, const std::string& log_path,
                std::vector<std::string> extra_arguments = {},
                unsigned thread_count = 1) {
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
          std::to_string(thread_count),
          "--no-pin-workers",
          "--recv-buffers-per-worker",
          "0",
          "--flush-max-ms",
          "20",
          "--data-file",
          data_path,
      };
      arguments.insert(arguments.end(),
                       std::make_move_iterator(extra_arguments.begin()),
                       std::make_move_iterator(extra_arguments.end()));
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

std::string BulkPayload(std::string_view reply, std::string_view operation) {
  if (!reply.starts_with('$')) {
    Fail(std::string(operation) + " was not a bulk string");
  }
  const std::size_t separator = reply.find("\r\n");
  if (separator == std::string_view::npos) {
    Fail(std::string(operation) + " was malformed");
  }
  std::size_t size = 0;
  const auto [parsed_end, error] =
      std::from_chars(reply.data() + 1, reply.data() + separator, size);
  if (error != std::errc{} || parsed_end != reply.data() + separator ||
      reply.size() - separator - 2 != size) {
    Fail(std::string(operation) + " had an invalid bulk length");
  }
  return std::string(reply.substr(separator + 2));
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

std::string ConfigPair(std::string_view name, std::string_view value,
                       bool resp3 = false) {
  return std::string(resp3 ? "%1" : "*2") + "\r\n$" +
         std::to_string(name.size()) + "\r\n" + std::string(name) + "\r\n$" +
         std::to_string(value.size()) + "\r\n" + std::string(value);
}

void VerifySetPayloads(RespClient& client, bool write) {
  // Exercise empty payloads, copy/CRC tails, and external-key prefixes. Read
  // again after restart so the decoder checks the persisted CRC independently
  // of the in-memory value and the encoder's copy implementation.
  for (std::size_t key_bytes : {16, 5001}) {
    for (std::size_t value_bytes : {0, 1, 15, 16, 17, 255, 256, 257, 1023,
                                    1024, 4095, 4096, 8191, 8192, 8193}) {
      const std::string key = "copy-crc-" + std::to_string(value_bytes) +
                              std::string(key_bytes, 'k');
      std::string value(value_bytes, '\0');
      for (std::size_t i = 0; i < value.size(); ++i) {
        value[i] = static_cast<char>((i * 37) % 256);
      }
      if (write) {
        // Unconditional SET must replace another type and clear its TTL even
        // though it no longer inspects that old value before append.
        Expect(client.Command({"RPUSH", key, "old"}), ":1", "payload seed");
        Expect(client.Command({"PEXPIRE", key, "60000"}), ":1",
               "payload seed TTL");
        Expect(client.Command({"SET", key, value}), "+OK", "payload SET");
      }
      Expect(client.Command({"GET", key}),
             "$" + std::to_string(value_bytes) + "\r\n" + value,
             "payload GET");
      Expect(client.Command({"PTTL", key}), ":-1", "payload TTL cleared");
    }
  }
}

void VerifyExpirationConfig(RespClient& client, std::uint16_t port) {
  struct Setting {
    std::string_view name;
    std::string_view initial;
    std::string_view updated;
  };
  constexpr Setting settings[]{
      {"active-expiration-interval-ms", "10", "20"},
      {"active-expiration-map-steps-per-cycle", "256", "512"},
      {"active-expiration-deletes-per-cycle", "64", "8"},
      {"active-expiration-index-maintenance-steps-per-cycle", "256", "128"},
  };
  std::string defaults = "*8";
  for (const auto& setting : settings) {
    defaults += ConfigPair(setting.name, setting.initial).substr(2);
  }
  Expect(client.Command({"CONFIG", "GET", "ACTIVE-EXPIRATION-*"}), defaults,
         "expiration wildcard defaults");

  RespClient other = Connect(port);
  if (!other.Command({"HELLO", "3"}).starts_with('%')) {
    Fail("HELLO 3 did not return a map");
  }
  for (const auto& setting : settings) {
    Expect(client.Command({"CONFIG", "SET", setting.name, setting.updated}),
           "+OK", "set expiration config");
    Expect(other.Command({"CONFIG", "GET", setting.name}),
           ConfigPair(setting.name, setting.updated, true),
           "shared expiration config in RESP3");
    for (std::string_view invalid :
         {"0", "-1", "1.5", "", "abc", "4294967296", "18446744073709551616"}) {
      if (!client.Command({"CONFIG", "SET", setting.name, invalid})
               .starts_with("-ERR")) {
        Fail("invalid expiration config was accepted");
      }
      Expect(client.Command({"CONFIG", "GET", setting.name}),
             ConfigPair(setting.name, setting.updated),
             "invalid expiration config preserves previous value");
    }
    Expect(client.Command({"CONFIG", "SET", setting.name, "1"}), "+OK",
           "minimum expiration config");
    Expect(client.Command({"CONFIG", "GET", setting.name}),
           ConfigPair(setting.name, "1"), "minimum expiration config GET");
    Expect(client.Command({"CONFIG", "SET", setting.name, setting.initial}),
           "+OK", "restore expiration config");
  }
  Expect(other.Command({"CONFIG", "GET", "active-expiration-*"}),
         "%4" + defaults.substr(2), "expiration wildcard RESP3");
  Expect(
      client.Command({"config", "set", "ACTIVE-EXPIRATION-INTERVAL-MS", "10"}),
      "+OK", "case-insensitive expiration SET");
}

class ExpirationAuthorityService final : public bycorf::Service {
 public:
  explicit ExpirationAuthorityService(lavik::storage::StorageEngine* storage)
      : storage_(storage) {}

  void Prepare(unsigned thread_count) override {
    if (thread_count != 1) {
      Fail("expiration authority test requires one worker");
    }
  }

  bycorf::Task<absl::Status> Run(bycorf::Worker& worker,
                                 bycorf::ServiceContext) override {
    lavik::BindMemoryAccountingShard(worker.id());
    lavik::tx::TxRuntime::Get()->shard(worker.id()).Bind(worker);
    result_ = co_await storage_->InitializeWorker(worker);
    if (!result_.ok()) {
      worker.RequestStop();
      co_return result_;
    }
    if (storage_->LocalSize(0) != 1) {
      result_ = absl::FailedPreconditionError(
          "recovery without authority did not retain the expired winner");
    }
    if (result_.ok()) {
      auto hidden = co_await storage_->Get(0, "authority-deferred");
      if (hidden.ok() ||
          hidden.status().code() != absl::StatusCode::kNotFound) {
        result_ = absl::FailedPreconditionError(
            "read exposed the expired winner without authority");
      }
    }
    if (result_.ok()) {
      result_ = co_await bycorf::SleepFor(worker, 100ms);
    }
    if (result_.ok() && storage_->LocalSize(0) != 1) {
      result_ = absl::FailedPreconditionError(
          "active expiration ran without authority");
    }

    if (result_.ok()) {
      // Change pacing on the already-running coroutine. With one full map
      // pass per cycle it can discover the recovered key without a read
      // queuing a candidate, including after authority was initially withheld.
      using Key = lavik::storage::ActiveExpirationConfigKey;
      result_ = storage_->ConfigureActiveExpiration(Key::kIntervalMs, 1);
      if (result_.ok()) {
        result_ = storage_->ConfigureActiveExpiration(Key::kMapStepsPerCycle,
                                                      16'384 * 16);
      }
      if (result_.ok()) {
        result_ = storage_->ConfigureActiveExpiration(Key::kDeletesPerCycle, 1);
      }
      if (result_.ok()) {
        result_ = storage_->ConfigureActiveExpiration(
            Key::kIndexMaintenanceStepsPerCycle, 1);
      }
    }
    if (result_.ok()) {
      storage_->SetExpirationAuthority(true);
    }
    for (unsigned attempt = 0;
         result_.ok() && storage_->LocalSize(0) != 0 && attempt < 5'000;
         ++attempt) {
      result_ = co_await bycorf::SleepFor(worker, 1ms);
    }
    if (result_.ok() && storage_->LocalSize(0) != 0) {
      result_ = absl::DeadlineExceededError(
          "active expiration did not retire the recovered winner");
    }
    worker.RequestStop();
    co_return result_;
  }

  void Stop() noexcept override {}

  void FinalizeWorker(bycorf::Worker& worker) noexcept override {
    storage_->FinalizeWorker(worker);
  }

  const absl::Status& result() const noexcept { return result_; }

 private:
  lavik::storage::StorageEngine* storage_ = nullptr;
  absl::Status result_ =
      absl::UnknownError("expiration authority test did not run");
};

void VerifyDeferredExpirationAuthority(const std::string& data_path) {
  lavik::storage::StorageEngineOptions options;
  options.data_files_ = {data_path};
  options.buffers_.registered_bytes_ = 64ULL * 1024 * 1024;
  options.expiration_authority_ = false;
  options.tomb_raider_interval_ms_ = 0;
  options.tx_cleaner_cooldown_ms_ = 0;
  lavik::storage::StorageEngine storage(std::move(options));
  // Exercise the largest supported settings before workers can consume them;
  // a live UINT32_MAX interval would intentionally leave a very long sleep.
  using Key = lavik::storage::ActiveExpirationConfigKey;
  constexpr std::uint64_t maximum = std::numeric_limits<std::uint32_t>::max();
  for (Key key : {Key::kIntervalMs, Key::kMapStepsPerCycle,
                  Key::kDeletesPerCycle, Key::kIndexMaintenanceStepsPerCycle}) {
    const auto initial = storage.ActiveExpirationConfigValue(key);
    if (!storage.ConfigureActiveExpiration(key, maximum).ok() ||
        storage.ActiveExpirationConfigValue(key) != maximum) {
      Fail("maximum expiration config was not retained");
    }
    for (std::uint64_t invalid : {std::uint64_t{0}, maximum + 1,
                                  std::numeric_limits<std::uint64_t>::max()}) {
      if (storage.ConfigureActiveExpiration(key, invalid).ok() ||
          storage.ActiveExpirationConfigValue(key) != maximum) {
        Fail("invalid expiration config changed storage settings");
      }
    }
    if (!storage.ConfigureActiveExpiration(key, initial).ok()) {
      Fail("failed to restore expiration config before recovery");
    }
  }
  lavik::InitWorkerMetrics(1);
  absl::Status status = lavik::InitMemoryLimit(512ULL * 1024 * 1024, 1);
  if (!status.ok()) Fail(std::string(status.message()));
  status = storage.Prepare(1);
  if (!status.ok()) Fail(std::string(status.message()));
  lavik::tx::TxRuntime::Create(1);

  ExpirationAuthorityService service(&storage);
  bycorf::Server server;
  server.AddService(&service);
  bycorf::ServerOptions runtime;
  runtime.thread_count_ = 1;
  runtime.pin_workers_ = false;
  runtime.recv_buffer_count_ = 0;
  status = server.Start(runtime);
  if (!status.ok()) Fail(std::string(status.message()));
  server.WaitUntilStopped();
  if (!service.result().ok()) {
    Fail(std::string(service.result().message()));
  }
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "usage: ttl_e2e_test /path/to/lavik\n";
    return 2;
  }
  const std::string prefix =
      lavik::test::TestDataPath("lavik-ttl-" + std::to_string(::getpid()));
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  const std::string no_authority_data_path = prefix + "-no-authority.data";
  const std::string no_authority_log_path = prefix + "-no-authority.log";
  (void)::unlink(data_path.c_str());
  (void)::unlink(log_path.c_str());
  (void)::unlink(no_authority_data_path.c_str());
  (void)::unlink(no_authority_log_path.c_str());

  try {
    const std::uint16_t port = FindFreePort();
    CreateDataFile(data_path, 192ULL * 1024 * 1024);
    {
      ServerProcess server(argv[1], port, data_path, log_path);
      RespClient client = Connect(port);
      Expect(client.Command({"PING"}), "+PONG", "PING");
      VerifyExpirationConfig(client, port);

      Expect(client.Command({"SET", "conditional", "old"}), "+OK",
             "initial SET");
      Expect(client.Command({"SET", "conditional", "new", "NX", "GET"}),
             "$3\r\nold", "SET NX GET failure");
      Expect(client.Command({"GET", "conditional"}), "$3\r\nold",
             "GET after failed NX");
      Expect(client.Command({"SET", "conditional", "new", "XX", "GET"}),
             "$3\r\nold", "SET XX GET success");
      Expect(client.Command({"GET", "conditional"}), "$3\r\nnew",
             "GET after successful XX");
      Expect(client.Command({"SET", "missing", "new", "XX"}), "$-1",
             "SET XX missing");
      Expect(client.Command({"SET", "missing", "new", "NX", "GET"}), "$-1",
             "SET NX GET create");
      Expect(client.Command({"GET", "missing"}), "$3\r\nnew",
             "GET created value");

      Expect(client.Command({"RPUSH", "set-get-type", "old"}), ":1",
             "SET GET wrong-type seed");
      Expect(client.Command({"SET", "set-get-type", "new", "GET"}),
             "-WRONGTYPE Operation against a key holding the wrong kind of value",
             "SET GET preserves type check");
      Expect(client.Command({"LINDEX", "set-get-type", "0"}), "$3\r\nold",
             "SET GET wrong-type preserves value");

      Expect(client.Command({"SET", "ttl", "one", "PX", "2000"}), "+OK",
             "SET PX");
      ExpectRange(IntegerReply(client.Command({"PTTL", "ttl"}), "PTTL"), 1,
                  2000, "PTTL after SET PX");
      Expect(client.Command({"SET", "ttl", "two", "KEEPTTL"}), "+OK",
             "SET KEEPTTL");
      ExpectRange(IntegerReply(client.Command({"PTTL", "ttl"}), "PTTL"), 1,
                  2000, "PTTL after KEEPTTL");
      Expect(client.Command({"SET", "ttl", "three"}), "+OK", "SET clears TTL");
      Expect(client.Command({"TTL", "ttl"}), ":-1", "TTL persistent");
      Expect(client.Command({"TTL", "does-not-exist"}), ":-2", "TTL missing");

      const auto unix_ms =
          std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now().time_since_epoch())
              .count();
      const std::string future_ms = std::to_string(unix_ms + 2000);
      Expect(client.Command({"SET", "pxat", "v", "PXAT", future_ms}), "+OK",
             "SET PXAT");
      ExpectRange(IntegerReply(client.Command({"PTTL", "pxat"}), "PXAT PTTL"),
                  1, 2000, "PXAT deadline");
      const std::string future_seconds = std::to_string(unix_ms / 1000 + 3);
      Expect(client.Command({"SET", "exat", "v", "EXAT", future_seconds}),
             "+OK", "SET EXAT");
      ExpectRange(IntegerReply(client.Command({"PTTL", "exat"}), "EXAT PTTL"),
                  1, 3000, "EXAT deadline");
      Expect(client.Command({"SET", "past", "v", "PXAT", "1"}), "+OK",
             "SET past PXAT");
      Expect(client.Command({"GET", "past"}), "$-1", "past PXAT hidden");

      const std::string expireat_seconds = std::to_string(unix_ms / 1000 + 60);
      Expect(client.Command({"SET", "expireat", "v"}), "+OK", "EXPIREAT seed");
      Expect(client.Command({"EXPIREAT", "expireat", expireat_seconds}), ":1",
             "EXPIREAT");
      Expect(client.Command({"EXPIRETIME", "expireat"}), ":" + expireat_seconds,
             "EXPIRETIME");
      Expect(client.Command({"PEXPIRETIME", "expireat"}),
             ":" + std::to_string(std::stoll(expireat_seconds) * 1000),
             "PEXPIRETIME after EXPIREAT");

      const std::string pexpireat_ms = std::to_string(unix_ms + 61'234);
      Expect(client.Command({"SET", "pexpireat", "v"}), "+OK",
             "PEXPIREAT seed");
      Expect(client.Command({"PEXPIREAT", "pexpireat", pexpireat_ms}), ":1",
             "PEXPIREAT");
      Expect(client.Command({"PEXPIRETIME", "pexpireat"}), ":" + pexpireat_ms,
             "PEXPIRETIME exact milliseconds");
      Expect(client.Command({"EXPIRETIME", "pexpireat"}),
             ":" + std::to_string((std::stoll(pexpireat_ms) + 500) / 1000),
             "EXPIRETIME rounded seconds");
      Expect(client.Command({"EXPIRETIME", "missing-expiretime"}), ":-2",
             "EXPIRETIME missing");
      Expect(client.Command({"PEXPIRETIME", "conditional"}), ":-1",
             "PEXPIRETIME persistent");

      Expect(client.Command({"SET", "expireat-past", "v"}), "+OK",
             "past EXPIREAT seed");
      Expect(client.Command({"EXPIREAT", "expireat-past", "1"}), ":1",
             "past EXPIREAT deletes");
      Expect(client.Command({"GET", "expireat-past"}), "$-1",
             "past EXPIREAT hidden");
      Expect(client.Command({"EXPIREAT", "missing-expireat", "1"}), ":0",
             "EXPIREAT missing");
      Expect(client.Command({"EXPIREAT", "expireat", "9223372036854775807"}),
             "-ERR invalid expire time in 'expireat' command",
             "EXPIREAT seconds overflow");
      Expect(client.Command({"PEXPIREAT", "expireat", "bad"}),
             "-ERR value is not an integer or out of range",
             "PEXPIREAT invalid integer");

      Expect(client.Command({"SET", "persistent-conditions", "v"}), "+OK",
             "SET persistent-conditions");
      Expect(client.Command({"EXPIRE", "persistent-conditions", "10", "GT"}),
             ":0", "EXPIRE GT treats persistence as infinity");
      Expect(client.Command({"EXPIRE", "persistent-conditions", "10", "LT"}),
             ":1", "EXPIRE LT on persistent key");
      Expect(client.Command({"EXPIRE", "persistent-conditions", "20", "XX"}),
             ":1", "EXPIRE XX on expiring key");

      Expect(client.Command({"SET", "expire-options", "v"}), "+OK",
             "SET expire-options");
      Expect(client.Command({"EXPIRE", "expire-options", "10", "NX"}), ":1",
             "EXPIRE NX");
      Expect(client.Command({"EXPIRE", "expire-options", "20", "NX"}), ":0",
             "EXPIRE NX failure");
      Expect(client.Command({"EXPIRE", "expire-options", "20", "GT"}), ":1",
             "EXPIRE GT");
      Expect(client.Command({"EXPIRE", "expire-options", "30", "LT"}), ":0",
             "EXPIRE LT failure");
      Expect(client.Command({"PERSIST", "expire-options"}), ":1", "PERSIST");
      Expect(client.Command({"PERSIST", "expire-options"}), ":0",
             "PERSIST without TTL");
      Expect(client.Command({"SET", "strict-expire", "safe"}), "+OK",
             "strict expiration seed");
      for (const auto& command : std::vector<std::vector<std::string_view>>{
               {"EXPIRE", "strict-expire", "-0"},
               {"PEXPIRE", "strict-expire", "00"},
               {"EXPIREAT", "strict-expire", "010"},
               {"PEXPIREAT", "strict-expire", "-0"}}) {
        Expect(client.Command(command),
               "-ERR value is not an integer or out of range",
               "non-canonical expiration rejected");
        Expect(client.Command({"GET", "strict-expire"}), "$4\r\nsafe",
               "rejected expiration preserves key");
      }
      Expect(client.Command({"PEXPIRE", "expire-options", "0"}), ":1",
             "PEXPIRE immediate delete");
      Expect(client.Command({"GET", "expire-options"}), "$-1",
             "GET immediate deletion");

      Expect(client.Command({"SET", "counter", "1", "PX", "2000"}), "+OK",
             "counter SET");
      Expect(client.Command({"INCR", "counter"}), ":2", "counter INCR");
      ExpectRange(
          IntegerReply(client.Command({"PTTL", "counter"}), "counter PTTL"), 1,
          2000, "INCR preserves TTL");

      Expect(client.Command({"SET", "race", "old", "PX", "1"}), "+OK",
             "race old SET");
      std::this_thread::sleep_for(3ms);
      Expect(client.Command({"GET", "race"}), "$-1", "lazy expiration");
      Expect(client.Command({"SET", "race", "new"}), "+OK",
             "race replacement SET");
      std::this_thread::sleep_for(50ms);
      Expect(client.Command({"GET", "race"}), "$3\r\nnew",
             "stale expiration candidate");

      Expect(client.Command({"SET", "bad", "v", "NX", "XX"}),
             "-ERR syntax error", "conflicting SET conditions");
      Expect(client.Command({"SET", "bad", "v", "EX", "0"}),
             "-ERR invalid expire time in 'set' command",
             "invalid SET expiration");

      Expect(client.Command({"DUMP", "missing-dump"}), "$-1",
             "DUMP missing key");
      Expect(
          client.Command({"SET", "dump-source", "serialized", "PX", "60000"}),
          "+OK", "DUMP source SET");
      const std::string dump =
          BulkPayload(client.Command({"DUMP", "dump-source"}), "DUMP");
      Expect(client.Command({"RESTORE", "restored-persistent", "0", dump}),
             "+OK", "RESTORE persistent");
      Expect(client.Command({"GET", "restored-persistent"}),
             "$10\r\nserialized", "RESTORE value");
      Expect(client.Command({"PTTL", "restored-persistent"}), ":-1",
             "DUMP excludes TTL");
      Expect(client.Command({"RESTORE", "restored-relative", "60000", dump}),
             "+OK", "RESTORE relative TTL");
      ExpectRange(IntegerReply(client.Command({"PTTL", "restored-relative"}),
                               "RESTORE relative PTTL"),
                  1, 60000, "RESTORE relative TTL");
      Expect(client.Command(
                 {"RESTORE", "restored-persistent", "invalid", "invalid"}),
             "-BUSYKEY Target key name already exists.",
             "RESTORE BUSYKEY precedence");
      Expect(client.Command(
                 {"RESTORE", "restored-persistent", "0", dump, "REPLACE"}),
             "+OK", "RESTORE REPLACE");
      std::string corrupt_dump = dump;
      corrupt_dump.back() ^= 1;
      Expect(client.Command({"RESTORE", "restore-corrupt", "0", corrupt_dump}),
             "-ERR DUMP payload version or checksum are wrong",
             "RESTORE checksum");
      Expect(client.Command(
                 {"RESTORE", "restore-idle", "0", dump, "IDLETIME", "1"}),
             "-ERR RESTORE IDLETIME and FREQ are not supported",
             "RESTORE unsupported IDLETIME");
      Expect(client.Command({"SET", "restore-expired", "old"}), "+OK",
             "RESTORE expired seed");
      Expect(client.Command({"RESTORE", "restore-expired", "1", dump, "REPLACE",
                             "ABSTTL"}),
             "+OK", "RESTORE expired ABSTTL");
      Expect(client.Command({"GET", "restore-expired"}), "$-1",
             "RESTORE expired replacement deletes");

      Expect(client.Command({"RPUSH", "dump-list", "a", "b", "c"}), ":3",
             "DUMP List seed");
      const std::string list_dump =
          BulkPayload(client.Command({"DUMP", "dump-list"}), "DUMP List");
      Expect(client.Command({"RESTORE", "restored-list", "0", list_dump}),
             "+OK", "RESTORE List");
      Expect(client.Command({"LLEN", "restored-list"}), ":3",
             "RESTORE List length");
      Expect(client.Command({"LINDEX", "restored-list", "1"}), "$1\r\nb",
             "RESTORE List contents");

      // TTL updates rewrite the record with the value loaded back from
      // storage. Wait out the periodic flush (20ms here) so the records are
      // disk-resident, then verify EXPIRE/PERSIST preserve the payload
      // byte-for-byte. Several back-to-back keys give the records non-zero
      // offsets within their pages, so a mislocated value start (e.g. the
      // raw aligned read buffer instead of the decoded value) surfaces as a
      // mismatch here.
      std::vector<std::string> flushed_values;
      for (int i = 0; i < 4; ++i) {
        flushed_values.emplace_back(200 + 17 * i, static_cast<char>('a' + i));
        Expect(client.Command(
                   {"SET", "flushed-" + std::to_string(i), flushed_values[i]}),
               "+OK", "flushed SET");
      }
      Expect(client.Command({"SET", "flushed-ttl", "keepme", "PX", "60000"}),
             "+OK", "flushed-ttl SET");
      std::this_thread::sleep_for(2s);
      for (int i = 0; i < 4; ++i) {
        const std::string key = "flushed-" + std::to_string(i);
        Expect(client.Command({"EXPIRE", key, "1000"}), ":1",
               "disk-resident EXPIRE");
        Expect(client.Command({"GET", key}),
               "$" + std::to_string(flushed_values[i].size()) + "\r\n" +
                   flushed_values[i],
               "value intact after disk-resident EXPIRE");
        ExpectRange(IntegerReply(client.Command({"TTL", key}),
                                 "disk-resident EXPIRE TTL"),
                    1, 1000, "disk-resident EXPIRE TTL");
      }
      Expect(client.Command({"PERSIST", "flushed-ttl"}), ":1",
             "disk-resident PERSIST");
      Expect(client.Command({"GET", "flushed-ttl"}), "$6\r\nkeepme",
             "value intact after disk-resident PERSIST");
      Expect(client.Command({"TTL", "flushed-ttl"}), ":-1",
             "disk-resident PERSIST TTL");

      // Generous TTL: the restart below includes full recovery, which takes
      // several seconds under sanitizer builds; the key must outlive it.
      Expect(client.Command({"SET", "restart-live", "v", "PX", "60000"}), "+OK",
             "restart-live SET");
      Expect(client.Command({"SET", "restart-dead", "v", "PX", "50"}), "+OK",
             "restart-dead SET");
      const std::string large_value(9ULL * 1024 * 1024, 'L');
      Expect(client.Command({"SET", "restart-large", large_value}), "+OK",
             "large SET");
      Expect(client.Command({"STRLEN", "restart-large"}), ":9437184",
             "large STRLEN");
      Expect(client.Command({"GET", "restart-large"}),
             "$9437184\r\n" + large_value, "large GET");
      Expect(client.Command(
                 {"CONFIG", "SET", "active-expiration-interval-ms", "37"}),
             "+OK", "change expiration config before restart");
      VerifySetPayloads(client, true);
      server.Stop();
    }

    std::this_thread::sleep_for(100ms);
    {
      ServerProcess server(argv[1], port, data_path, log_path);
      RespClient client = Connect(port);
      Expect(client.Command({"CONFIG", "GET", "active-expiration-interval-ms"}),
             ConfigPair("active-expiration-interval-ms", "10"),
             "expiration config resets on restart");
      Expect(client.Command({"GET", "restart-dead"}), "$-1",
             "expired while stopped");
      Expect(client.Command({"TTL", "restart-dead"}), ":-2",
             "restart expired TTL");
      Expect(client.Command({"GET", "restart-live"}), "$1\r\nv",
             "restart live value");
      VerifySetPayloads(client, false);
      ExpectRange(IntegerReply(client.Command({"PTTL", "restart-live"}),
                               "restart-live PTTL"),
                  1, 60000, "restart live TTL");
      Expect(client.Command({"STRLEN", "restart-large"}), ":9437184",
             "recovered large STRLEN");
      Expect(client.Command({"SET", "restart-large", "small"}), "+OK",
             "large to small overwrite");
      Expect(client.Command({"GET", "restart-large"}), "$5\r\nsmall",
             "large to small GET");
      server.Stop();
    }

    {
      ServerProcess server(argv[1], port, data_path, log_path, {}, 2);
      RespClient client = Connect(port);
      VerifyExpirationConfig(client, port);
      Expect(client.Command({"GET", "restart-large"}), "$5\r\nsmall",
             "large extent reclaim restart");
      server.Stop();
    }

    // Recovery without expiration authority must keep the expired winner on
    // disk and in the index. Reads still hide it by its absolute deadline, but
    // only a later authority grant may mint the durable tombstone. The direct
    // storage fixture below observes both sides of that authority transition.
    const std::uint16_t no_authority_port = FindFreePort();
    CreateDataFile(no_authority_data_path, 192ULL * 1024 * 1024);
    {
      ServerProcess server(argv[1], no_authority_port, no_authority_data_path,
                           no_authority_log_path);
      RespClient client = Connect(no_authority_port);
      Expect(
          client.Command({"SET", "authority-deferred", "value", "PX", "3000"}),
          "+OK", "no-authority recovery seed");
      server.Stop();
    }
    std::this_thread::sleep_for(3100ms);
    VerifyDeferredExpirationAuthority(no_authority_data_path);

    (void)::unlink(data_path.c_str());
    (void)::unlink(log_path.c_str());
    (void)::unlink(no_authority_data_path.c_str());
    (void)::unlink(no_authority_log_path.c_str());
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    const std::string log = ReadFile(log_path);
    if (!log.empty()) std::cerr << "--- Lavik log ---\n" << log;
    (void)::unlink(data_path.c_str());
    (void)::unlink(log_path.c_str());
    (void)::unlink(no_authority_data_path.c_str());
    (void)::unlink(no_authority_log_path.c_str());
    return 1;
  }
}
