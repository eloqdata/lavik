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
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <future>
#include <iterator>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "lavik/command_table.h"
#include "lavik/rdb.h"
#include "lavik/storage/format.h"
#include "support/test_data_path.h"

namespace {

using namespace std::chrono_literals;

std::string g_lavik_binary;

class FileCleanup {
 public:
  explicit FileCleanup(std::string path) : path_(std::move(path)) {}
  ~FileCleanup() { (void)::unlink(path_.c_str()); }

 private:
  std::string path_;
};

void SendAll(int fd, std::string_view bytes) {
  while (!bytes.empty()) {
    const ssize_t sent = ::send(fd, bytes.data(), bytes.size(), MSG_NOSIGNAL);
    if (sent < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw std::runtime_error("send failed: " +
                               std::string(std::strerror(errno)));
    }
    if (sent == 0) {
      throw std::runtime_error("send returned zero bytes");
    }
    bytes.remove_prefix(static_cast<std::size_t>(sent));
  }
}

std::string EncodeCommand(const std::vector<std::string_view>& args) {
  std::string request = "*" + std::to_string(args.size()) + "\r\n";
  for (std::string_view arg : args) {
    request += "$" + std::to_string(arg.size()) + "\r\n";
    request.append(arg);
    request += "\r\n";
  }
  return request;
}

std::string ReadRespLine(int fd) {
  std::string line;
  while (!line.ends_with("\r\n")) {
    char byte = 0;
    const ssize_t received = ::recv(fd, &byte, 1, 0);
    if (received < 0 && errno == EINTR) continue;
    if (received <= 0) {
      throw std::runtime_error("failed to read RESP line");
    }
    line.push_back(byte);
  }
  line.resize(line.size() - 2);
  return line;
}

std::string ReadRespBulk(int fd) {
  const std::string header = ReadRespLine(fd);
  if (header.empty() || header.front() != '$')
    throw std::runtime_error("expected RESP bulk string, got: " + header);
  std::size_t size = 0;
  const auto parsed =
      std::from_chars(header.data() + 1, header.data() + header.size(), size);
  if (parsed.ec != std::errc{} || parsed.ptr != header.data() + header.size())
    throw std::runtime_error("invalid RESP bulk length: " + header);
  std::string payload(size + 2, '\0');
  std::size_t received_total = 0;
  while (received_total < payload.size()) {
    const ssize_t received = ::recv(fd, payload.data() + received_total,
                                    payload.size() - received_total, 0);
    if (received < 0 && errno == EINTR) continue;
    if (received <= 0) throw std::runtime_error("failed to read RESP bulk");
    received_total += static_cast<std::size_t>(received);
  }
  if (!payload.ends_with("\r\n"))
    throw std::runtime_error("RESP bulk string lacks terminator");
  payload.resize(size);
  return payload;
}

std::vector<std::string> ReadRespCommand(int fd) {
  const std::string header = ReadRespLine(fd);
  if (header.empty() || header.front() != '*') {
    throw std::runtime_error("expected RESP array, got: " + header);
  }
  std::size_t count = 0;
  const auto parsed =
      std::from_chars(header.data() + 1, header.data() + header.size(), count);
  if (parsed.ec != std::errc{} || parsed.ptr != header.data() + header.size()) {
    throw std::runtime_error("invalid RESP array length: " + header);
  }
  std::vector<std::string> command;
  command.reserve(count);
  for (std::size_t index = 0; index < count; ++index) {
    command.push_back(ReadRespBulk(fd));
  }
  return command;
}

class RedisPsyncSource {
 public:
  RedisPsyncSource(std::string rdb, std::string command_stream) {
    const int listen_fd =
        ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);
    if (listen_fd < 0) throw std::runtime_error("socket failed");
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (::bind(listen_fd, reinterpret_cast<const sockaddr*>(&address),
               sizeof(address)) != 0 ||
        ::listen(listen_fd, 1) != 0) {
      ::close(listen_fd);
      throw std::runtime_error("failed to listen for Redis PSYNC test source");
    }
    socklen_t address_size = sizeof(address);
    if (::getsockname(listen_fd, reinterpret_cast<sockaddr*>(&address),
                      &address_size) != 0) {
      ::close(listen_fd);
      throw std::runtime_error("failed to resolve Redis PSYNC test port");
    }
    port_ = ntohs(address.sin_port);

    pid_ = ::fork();
    if (pid_ < 0) {
      ::close(listen_fd);
      throw std::runtime_error("fork failed");
    }
    if (pid_ == 0) {
      try {
        const int probe_fd =
            ::accept4(listen_fd, nullptr, nullptr, SOCK_CLOEXEC);
        if (probe_fd < 0) _exit(125);
        const auto identity = ReadRespCommand(probe_fd);
        if (identity.empty() || identity.front() != "INFO") _exit(125);
        SendAll(probe_fd, "$0\r\n\r\n");
        const auto probe = ReadRespCommand(probe_fd);
        if (probe.empty() || probe.front() != "LVPSYNC") _exit(125);
        SendAll(probe_fd, "-ERR unknown command 'LVPSYNC'\r\n");
        const int discovery_fd = probe_fd;
        if (discovery_fd < 0) _exit(125);
        const std::vector<std::string> discovery =
            ReadRespCommand(discovery_fd);
        if (discovery.empty() || discovery.front() != "CLUSTER") _exit(125);
        SendAll(discovery_fd,
                "-ERR This instance has cluster support disabled\r\n");
        ::close(discovery_fd);

        const int client_fd =
            ::accept4(listen_fd, nullptr, nullptr, SOCK_CLOEXEC);
        ::close(listen_fd);
        if (client_fd < 0) _exit(125);
        const auto expect = [client_fd](std::string_view name) {
          const std::vector<std::string> command = ReadRespCommand(client_fd);
          if (command.empty() || command.front() != name) {
            throw std::runtime_error("unexpected Redis replication handshake");
          }
        };
        expect("INFO");
        SendAll(client_fd, "$0\r\n\r\n");
        expect("LVPSYNC");
        SendAll(client_fd, "-ERR unknown command 'LVPSYNC'\r\n");
        expect("CLUSTER");
        SendAll(client_fd,
                "-ERR This instance has cluster support disabled\r\n");
        expect("PING");
        SendAll(client_fd, "+PONG\r\n");
        expect("REPLCONF");
        SendAll(client_fd, "+OK\r\n");
        expect("REPLCONF");
        SendAll(client_fd, "+OK\r\n");
        expect("PSYNC");
        SendAll(client_fd,
                "+FULLRESYNC 0123456789012345678901234567890123456789 0\r\n$" +
                    std::to_string(rdb.size()) + "\r\n");
        SendAll(client_fd, rdb);
        SendAll(client_fd, command_stream);
        char buffer[256];
        while (::recv(client_fd, buffer, sizeof(buffer), 0) > 0) {
        }
        ::close(client_fd);
        _exit(0);
      } catch (const std::exception&) {
        _exit(125);
      }
    }
    ::close(listen_fd);
  }

  RedisPsyncSource(const RedisPsyncSource&) = delete;
  RedisPsyncSource& operator=(const RedisPsyncSource&) = delete;
  ~RedisPsyncSource() { Stop(); }

  std::uint16_t port() const noexcept { return port_; }

  void Stop() noexcept {
    if (pid_ <= 0) return;
    (void)::kill(pid_, SIGTERM);
    int status = 0;
    while (::waitpid(pid_, &status, 0) < 0 && errno == EINTR) {
    }
    pid_ = -1;
  }

 private:
  pid_t pid_ = -1;
  std::uint16_t port_ = 0;
};

int ConnectSocket(std::uint16_t port);

void CloseStreamingReplyAfterHeader(std::uint16_t port,
                                    const std::vector<std::string_view>& args,
                                    std::string_view expected_header) {
  const int fd = ConnectSocket(port);
  if (fd < 0) throw std::runtime_error("failed to connect streaming client");
  SendAll(fd, EncodeCommand(args));
  const std::string header = ReadRespLine(fd);
  if (header != expected_header) {
    ::close(fd);
    throw std::runtime_error("unexpected streaming header: " + header);
  }
  linger reset{.l_onoff = 1, .l_linger = 0};
  (void)::setsockopt(fd, SOL_SOCKET, SO_LINGER, &reset, sizeof(reset));
  ::close(fd);
}

void CloseExecStreamingReplyAfterHeader(
    std::uint16_t port, const std::vector<std::string_view>& args,
    std::string_view expected_header) {
  const int fd = ConnectSocket(port);
  if (fd < 0) throw std::runtime_error("failed to connect streaming client");
  SendAll(fd, EncodeCommand({"MULTI"}));
  if (ReadRespLine(fd) != "+OK") {
    ::close(fd);
    throw std::runtime_error("MULTI failed for streaming EXEC");
  }
  SendAll(fd, EncodeCommand(args));
  if (ReadRespLine(fd) != "+QUEUED") {
    ::close(fd);
    throw std::runtime_error("random command was not queued");
  }
  SendAll(fd, EncodeCommand({"EXEC"}));
  if (ReadRespLine(fd) != "*1" || ReadRespLine(fd) != expected_header) {
    ::close(fd);
    throw std::runtime_error("unexpected streaming EXEC header");
  }
  linger reset{.l_onoff = 1, .l_linger = 0};
  (void)::setsockopt(fd, SOL_SOCKET, SO_LINGER, &reset, sizeof(reset));
  ::close(fd);
}

std::uint16_t FindFreePort() {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (fd < 0) {
    throw std::runtime_error("socket failed while selecting a port");
  }
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  if (::bind(fd, reinterpret_cast<const sockaddr*>(&address),
             sizeof(address)) != 0) {
    ::close(fd);
    throw std::runtime_error("bind failed while selecting a port");
  }
  socklen_t size = sizeof(address);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &size) != 0) {
    ::close(fd);
    throw std::runtime_error("getsockname failed while selecting a port");
  }
  ::close(fd);
  return ntohs(address.sin_port);
}

int ConnectSocket(std::uint16_t port) {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (fd < 0) {
    return -1;
  }
  timeval timeout{.tv_sec = 30, .tv_usec = 0};
  (void)::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
  (void)::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = htons(port);
  if (::connect(fd, reinterpret_cast<const sockaddr*>(&address),
                sizeof(address)) != 0) {
    ::close(fd);
    return -1;
  }
  return fd;
}

std::string HttpGet(std::uint16_t port, std::string_view target) {
  const auto deadline = std::chrono::steady_clock::now() + 30s;
  int fd = -1;
  while (fd < 0 && std::chrono::steady_clock::now() < deadline) {
    fd = ConnectSocket(port);
    if (fd < 0) std::this_thread::sleep_for(10ms);
  }
  if (fd < 0) throw std::runtime_error("timed out connecting to HTTP port");

  SendAll(fd, "GET " + std::string(target) +
                  " HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
  std::string response;
  char buffer[4096];
  while (true) {
    const ssize_t received = ::recv(fd, buffer, sizeof(buffer), 0);
    if (received < 0 && errno == EINTR) continue;
    if (received <= 0) break;
    response.append(buffer, static_cast<std::size_t>(received));
  }
  ::close(fd);
  return response;
}

class RespClient {
 public:
  explicit RespClient(std::uint16_t port) {
    const auto deadline = std::chrono::steady_clock::now() + 30s;
    while (std::chrono::steady_clock::now() < deadline) {
      fd_ = ConnectSocket(port);
      if (fd_ >= 0) {
        try {
          if (Command({"PING"}) == "+PONG") return;
        } catch (const std::exception&) {
        }
        ::close(fd_);
        fd_ = -1;
      }
      std::this_thread::sleep_for(10ms);
    }
    throw std::runtime_error("timed out connecting to Redis port");
  }

  ~RespClient() {
    if (fd_ >= 0) {
      ::close(fd_);
    }
  }

  std::string Command(const std::vector<std::string_view>& args) {
    SendAll(fd_, EncodeCommand(args));
    try {
      return ReadReply();
    } catch (const std::exception& error) {
      std::string command;
      for (std::string_view argument : args) {
        if (!command.empty()) command.push_back(' ');
        command.append(argument);
      }
      throw std::runtime_error(command + ": " + error.what());
    }
  }

  std::string ReadPush() { return ReadReply(); }

 private:
  std::string ReadReply() {
    const std::string line = ReadLine();
    switch (line.empty() ? '\0' : line.front()) {
      case '+':
      case '-':
      case ':':
        return line;
      case '$': {
        if (line == "$-1") return line;
        const std::size_t size = ParseLength(line);
        std::string payload(size + 2, '\0');
        ReadExact(payload.data(), payload.size());
        if (!payload.ends_with("\r\n")) {
          throw std::runtime_error("malformed bulk terminator");
        }
        payload.resize(size);
        return line + "\r\n" + payload;
      }
      case '*': {
        if (line == "*-1") return line;
        const std::size_t count = ParseLength(line);
        std::string reply = line;
        for (std::size_t i = 0; i < count; ++i) {
          reply += "\r\n" + ReadReply();
        }
        return reply;
      }
      default:
        throw std::runtime_error("unexpected RESP type: " + line);
    }
  }

  static std::size_t ParseLength(const std::string& line) {
    std::size_t size = 0;
    const char* begin = line.data() + 1;
    const char* end = line.data() + line.size();
    const auto [parsed, error] = std::from_chars(begin, end, size);
    if (error != std::errc{} || parsed != end) {
      throw std::runtime_error("malformed RESP length: " + line);
    }
    return size;
  }

  void ReadExact(char* output, std::size_t size) {
    while (size != 0) {
      const ssize_t received = ::recv(fd_, output, size, 0);
      if (received < 0 && errno == EINTR) continue;
      if (received <= 0) {
        throw std::runtime_error("failed to read RESP response");
      }
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
    }
    response.resize(response.size() - 2);
    return response;
  }

  int fd_ = -1;
};

bool WaitForReply(RespClient& client,
                  const std::vector<std::string_view>& command,
                  std::string_view expected) {
  const auto deadline = std::chrono::steady_clock::now() + 30s;
  while (std::chrono::steady_clock::now() < deadline) {
    const std::string reply = client.Command(command);
    if (reply == expected) return true;
    if (!reply.starts_with("-TRYAGAIN") && !reply.starts_with("-BUSY")) {
      return false;
    }
    std::this_thread::sleep_for(10ms);
  }
  return false;
}

// Unlike WaitForReply, an ordinary mismatch is not a failure here but a retry:
// a replica read legitimately disagrees until the effect arrives.
bool WaitForEventualReply(RespClient& client,
                          const std::vector<std::string_view>& command,
                          std::string_view expected,
                          std::chrono::seconds timeout = 60s) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  do {
    if (client.Command(command) == expected) return true;
    std::this_thread::sleep_for(10ms);
  } while (std::chrono::steady_clock::now() < deadline);
  return false;
}

std::optional<std::uint64_t> InfoUnsigned(std::string_view info,
                                          std::string_view field) {
  const std::string needle = std::string(field) + ":";
  const std::size_t value_begin = info.find(needle);
  if (value_begin == std::string_view::npos) return std::nullopt;
  const std::size_t begin = value_begin + needle.size();
  const std::size_t end = info.find("\r\n", begin);
  if (end == std::string_view::npos) return std::nullopt;
  std::uint64_t value = 0;
  const auto parsed =
      std::from_chars(info.data() + begin, info.data() + end, value);
  if (parsed.ec != std::errc{} || parsed.ptr != info.data() + end) {
    return std::nullopt;
  }
  return value;
}

bool WaitForLog(std::string_view path, std::string_view needle,
                std::chrono::seconds timeout = 10s) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  do {
    std::ifstream input{std::string(path)};
    const std::string contents((std::istreambuf_iterator<char>(input)),
                               std::istreambuf_iterator<char>());
    if (contents.find(needle) != std::string::npos) return true;
    std::this_thread::sleep_for(10ms);
  } while (std::chrono::steady_clock::now() < deadline);
  return false;
}

std::string KeyForWorker(std::string_view prefix, unsigned worker,
                         unsigned worker_count) {
  for (std::uint64_t candidate = 0;; ++candidate) {
    std::string key = std::string(prefix) + "-" + std::to_string(candidate);
    if (lavik::storage::StorageShardForKey(key) % worker_count == worker) {
      return key;
    }
  }
}

std::string KeyForPartition(std::string_view prefix,
                            std::uint16_t partition_id) {
  for (std::uint64_t candidate = 0;; ++candidate) {
    std::string key = std::string(prefix) + "-" + std::to_string(candidate);
    if (lavik::storage::RedisSlot(key) == partition_id) return key;
  }
}

struct ParsedRespValue {
  std::string scalar_;
  std::vector<ParsedRespValue> elements_;
};

ParsedRespValue ParseEncodedResp(std::string_view input, std::size_t* offset) {
  std::size_t line_end = input.find("\r\n", *offset);
  if (line_end == std::string_view::npos) line_end = input.size();
  if (line_end == *offset) {
    throw std::runtime_error("malformed encoded RESP value at offset " +
                             std::to_string(*offset) + ": " +
                             std::string(input));
  }
  const char type = input[*offset];
  const std::string_view body =
      input.substr(*offset + 1, line_end - *offset - 1);
  ParsedRespValue result;
  if (type == '$') {
    if (line_end == input.size()) {
      throw std::runtime_error("RESP bulk string lacks payload separator");
    }
    *offset = line_end + 2;
    std::size_t length = 0;
    const auto parsed =
        std::from_chars(body.data(), body.data() + body.size(), length);
    if (parsed.ec != std::errc{} || parsed.ptr != body.data() + body.size() ||
        length > input.size() - *offset) {
      throw std::runtime_error("malformed encoded RESP bulk string");
    }
    result.scalar_.assign(input.substr(*offset, length));
    *offset += length;
    return result;
  }
  if (type != '*') {
    result.scalar_.assign(body);
    // Scalar replies have no bytes after their value in RespClient's encoded
    // representation. Leave the inter-element CRLF for the parent array.
    *offset = line_end;
    return result;
  }
  std::size_t count = 0;
  const auto parsed =
      std::from_chars(body.data(), body.data() + body.size(), count);
  if (parsed.ec != std::errc{} || parsed.ptr != body.data() + body.size()) {
    throw std::runtime_error("malformed encoded RESP array");
  }
  if (count == 0) {
    *offset = line_end;
    return result;
  }
  if (line_end == input.size()) {
    throw std::runtime_error("RESP array lacks element separator");
  }
  *offset = line_end + 2;
  result.elements_.reserve(count);
  for (std::size_t i = 0; i < count; ++i) {
    result.elements_.push_back(ParseEncodedResp(input, offset));
    if (i + 1 < count) {
      if (input.substr(*offset, 2) != "\r\n") {
        throw std::runtime_error("malformed encoded RESP array separator");
      }
      *offset += 2;
    }
  }
  return result;
}

std::pair<std::string, std::vector<std::string>> ParseScanReply(
    std::string_view encoded) {
  std::size_t offset = 0;
  ParsedRespValue parsed = ParseEncodedResp(encoded, &offset);
  if (offset != encoded.size() || parsed.elements_.size() != 2) {
    throw std::runtime_error("malformed SCAN reply");
  }
  std::vector<std::string> values;
  for (ParsedRespValue& value : parsed.elements_[1].elements_) {
    values.push_back(std::move(value.scalar_));
  }
  return {std::move(parsed.elements_[0].scalar_), std::move(values)};
}

bool WaitForDurability(RespClient& client) {
  const auto deadline = std::chrono::steady_clock::now() + 30s;
  while (std::chrono::steady_clock::now() < deadline) {
    const std::string info = client.Command({"INFO", "STATS"});
    if (info.find("storage_durability_pending:0\r\n") != std::string::npos) {
      return true;
    }
    std::this_thread::sleep_for(10ms);
  }
  return false;
}

std::uint64_t TxCleanerRetiredGenerations(RespClient& client) {
  constexpr std::string_view marker = "tx_cleaner_retired_generations:";
  const std::string info = client.Command({"INFO", "STATS"});
  const std::size_t begin = info.find(marker);
  if (begin == std::string::npos) {
    throw std::runtime_error("tx cleaner INFO field is missing");
  }
  const std::size_t value_begin = begin + marker.size();
  const std::size_t value_end = info.find("\r\n", value_begin);
  if (value_end == std::string::npos) {
    throw std::runtime_error("malformed tx cleaner INFO field");
  }
  std::uint64_t retired = 0;
  const char* first = info.data() + value_begin;
  const char* last = info.data() + value_end;
  const auto [parsed, error] = std::from_chars(first, last, retired);
  if (error != std::errc{} || parsed != last) {
    throw std::runtime_error("invalid tx cleaner INFO counter");
  }
  return retired;
}

bool WaitForTxCleanerRetirement(RespClient& client, std::uint64_t baseline) {
  const auto deadline = std::chrono::steady_clock::now() + 30s;
  while (std::chrono::steady_clock::now() < deadline) {
    if (TxCleanerRetiredGenerations(client) > baseline) return true;
    std::this_thread::sleep_for(10ms);
  }
  return false;
}

class ServerProcess {
 public:
  ServerProcess(
      std::string_view binary, std::uint16_t port, std::string_view data_path,
      std::string_view log_path, unsigned threads,
      std::string_view crash_point = {},
      std::vector<std::string> extra_arguments = {},
      std::vector<std::pair<std::string, std::string>> environment = {},
      std::string_view config_path = {},
      std::optional<rlim_t> nofile_limit = std::nullopt) {
    pid_ = ::fork();
    if (pid_ < 0) {
      throw std::runtime_error("fork failed");
    }
    if (pid_ == 0) {
      if (nofile_limit.has_value()) {
        const rlimit limit{.rlim_cur = *nofile_limit,
                           .rlim_max = *nofile_limit};
        if (::setrlimit(RLIMIT_NOFILE, &limit) != 0) _exit(126);
      }
      if (!crash_point.empty()) {
        (void)::setenv("LAVIK_CRASH_POINT", std::string(crash_point).c_str(),
                       1);
      }
      for (const auto& [name, value] : environment) {
        (void)::setenv(name.c_str(), value.c_str(), 1);
      }
      const int log_fd =
          ::open(std::string(log_path).c_str(),
                 O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0600);
      if (log_fd >= 0) {
        (void)::dup2(log_fd, STDOUT_FILENO);
        (void)::dup2(log_fd, STDERR_FILENO);
        ::close(log_fd);
      }
      std::string max_memory = "8589934592";
      std::string recv_buffers = "0";
      for (std::size_t i = 0; i + 1 < extra_arguments.size();) {
        if (extra_arguments[i] == "--max-memory") {
          max_memory = std::move(extra_arguments[i + 1]);
        } else if (extra_arguments[i] == "--recv-buffers-per-worker") {
          recv_buffers = std::move(extra_arguments[i + 1]);
        } else {
          ++i;
          continue;
        }
        extra_arguments.erase(extra_arguments.begin() + i,
                              extra_arguments.begin() + i + 2);
      }
      std::vector<std::string> arguments{
          std::string(binary),
          "--logtostderr",
          "--port",
          std::to_string(port),
          "--threads",
          std::to_string(threads),
          "--no-pin-workers",
          "--recv-buffers-per-worker",
          std::move(recv_buffers),
          "--max-memory",
          std::move(max_memory),
          "--flush-max-ms",
          "20",
          "--data-file",
          std::string(data_path),
      };
      if (!config_path.empty()) {
        arguments.insert(arguments.begin() + 1, std::string(config_path));
      }
      arguments.insert(arguments.end(),
                       std::make_move_iterator(extra_arguments.begin()),
                       std::make_move_iterator(extra_arguments.end()));
      std::vector<char*> argv;
      for (std::string& argument : arguments) {
        argv.push_back(argument.data());
      }
      argv.push_back(nullptr);
      ::execv(argv[0], argv.data());
      _exit(127);
    }
  }

  ~ServerProcess() {
    if (pid_ > 0) {
      (void)::kill(pid_, SIGKILL);
      (void)::waitpid(pid_, nullptr, 0);
    }
  }

  void Stop() {
    ASSERT_GT(pid_, 0);
    ASSERT_EQ(::kill(pid_, SIGINT), 0);
    const auto deadline = std::chrono::steady_clock::now() + 30s;
    while (std::chrono::steady_clock::now() < deadline) {
      int status = 0;
      const pid_t waited = ::waitpid(pid_, &status, WNOHANG);
      if (waited == pid_) {
        pid_ = -1;
        ASSERT_TRUE(WIFEXITED(status));
        ASSERT_EQ(WEXITSTATUS(status), 0);
        return;
      }
      ASSERT_GE(waited, 0);
      std::this_thread::sleep_for(10ms);
    }
    FAIL() << "Lavik did not stop";
  }

  void Kill() {
    ASSERT_GT(pid_, 0);
    ASSERT_EQ(::kill(pid_, SIGKILL), 0);
    int status = 0;
    ASSERT_EQ(::waitpid(pid_, &status, 0), pid_);
    ASSERT_TRUE(WIFSIGNALED(status));
    ASSERT_EQ(WTERMSIG(status), SIGKILL);
    pid_ = -1;
  }

  void Pause() {
    ASSERT_GT(pid_, 0);
    ASSERT_EQ(::kill(pid_, SIGSTOP), 0);
    int status = 0;
    ASSERT_EQ(::waitpid(pid_, &status, WUNTRACED), pid_);
    ASSERT_TRUE(WIFSTOPPED(status));
  }

  void Resume() {
    ASSERT_GT(pid_, 0);
    ASSERT_EQ(::kill(pid_, SIGCONT), 0);
  }

  void WaitForCrash() {
    ASSERT_GT(pid_, 0);
    const auto deadline = std::chrono::steady_clock::now() + 30s;
    while (std::chrono::steady_clock::now() < deadline) {
      int status = 0;
      const pid_t waited = ::waitpid(pid_, &status, WNOHANG);
      if (waited == pid_) {
        pid_ = -1;
        ASSERT_TRUE(WIFEXITED(status));
        ASSERT_EQ(WEXITSTATUS(status), 86);
        return;
      }
      ASSERT_GE(waited, 0);
      std::this_thread::sleep_for(10ms);
    }
    FAIL() << "Lavik did not reach the armed crash point";
  }

 private:
  pid_t pid_ = -1;
};

std::string Bulk(std::string_view value) {
  return "$" + std::to_string(value.size()) + "\r\n" + std::string(value);
}

std::string Moved(std::string_view key, std::uint16_t master_port) {
  return "-MOVED " + std::to_string(lavik::storage::RedisSlot(key)) +
         " 127.0.0.1:" + std::to_string(master_port);
}

std::string BulkArray(const std::vector<std::string_view>& values) {
  std::string reply = "*" + std::to_string(values.size());
  for (std::string_view value : values) reply += "\r\n" + Bulk(value);
  return reply;
}

TEST(ListE2eTest, FreshMinimumStorageUsesImplicitEmptyCatalog) {
  ASSERT_FALSE(g_lavik_binary.empty());
  const std::string prefix = lavik::test::TestDataPathPrefix() +
                             "lavik-minimum-storage-e2e-" +
                             std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::posix_fallocate(fd, 0, 80ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(fd), 0);

  const std::uint16_t port = FindFreePort();
  {
    ServerProcess server(g_lavik_binary, port, data_path, log_path, 2);
    RespClient client(port);
    EXPECT_EQ(client.Command({"PING"}), "+PONG");
    EXPECT_EQ(InfoUnsigned(client.Command({"INFO", "replication"}),
                           "lavik_function_catalog_generation"),
              0);

    constexpr std::string_view library =
        "#!lua name=minimum_capacity\n"
        "redis.register_function('minimum_capacity_value', function(keys, "
        "args) return 'visible' end)";
    const std::string load =
        client.Command({"FUNCTION", "LOAD", std::string(library)});
    EXPECT_TRUE(load.starts_with("-ERR")) << load;
    EXPECT_NE(client.Command({"FCALL", "minimum_capacity_value", "0"})
                  .find("Function not found"),
              std::string::npos);
    EXPECT_EQ(InfoUnsigned(client.Command({"INFO", "replication"}),
                           "lavik_function_catalog_generation"),
              0);
    server.Stop();
  }
  {
    ServerProcess server(g_lavik_binary, port, data_path, log_path, 2);
    RespClient client(port);
    EXPECT_EQ(client.Command({"PING"}), "+PONG");
    EXPECT_EQ(InfoUnsigned(client.Command({"INFO", "replication"}),
                           "lavik_function_catalog_generation"),
              0);
    EXPECT_NE(client.Command({"FCALL", "minimum_capacity_value", "0"})
                  .find("Function not found"),
              std::string::npos);
    server.Stop();
  }
}

TEST(ListE2eTest, FunctionCatalogCrashRecoverySelectsCommittedRoot) {
#if defined(NDEBUG) && !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires a Debug or fault-instrumented server";
#endif
  ASSERT_FALSE(g_lavik_binary.empty());
  constexpr std::string_view old_library =
      "#!lua name=crash_catalog\n"
      "redis.register_function('crash_catalog_value', function(keys, args) "
      "return 'old' end)";
  constexpr std::string_view new_library =
      "#!lua name=crash_catalog\n"
      "redis.register_function('crash_catalog_value', function(keys, args) "
      "return 'new' end)";
  struct CrashCase {
    std::string_view point_;
    std::string_view recovered_;
    unsigned devices_ = 1;
  };
  constexpr CrashCase cases[] = {
      {"function-catalog-body-durable", "old", 1},
      {"system-state-device-root-durable", "new", 1},
      {"system-state-device-root-durable", "old", 2},
      {"function-catalog-before-runtime-swap", "new", 1},
      {"function-catalog-after-runtime-swap", "new", 1},
  };

  for (std::size_t case_index = 0; case_index < std::size(cases);
       ++case_index) {
    const std::string prefix =
        lavik::test::TestDataPathPrefix() + "lavik-function-catalog-crash-" +
        std::to_string(::getpid()) + "-" + std::to_string(case_index);
    const std::string first_path = prefix + "-0.data";
    const std::string second_path = prefix + "-1.data";
    const std::string log_path = prefix + ".log";
    FileCleanup first_cleanup(first_path);
    FileCleanup second_cleanup(second_path);
    FileCleanup log_cleanup(log_path);
    for (unsigned device = 0; device < cases[case_index].devices_; ++device) {
      const std::string& path = device == 0 ? first_path : second_path;
      const int fd =
          ::open(path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
      ASSERT_GE(fd, 0);
      ASSERT_EQ(::posix_fallocate(fd, 0, 128ULL * 1024 * 1024), 0);
      ASSERT_EQ(::close(fd), 0);
    }
    std::vector<std::string> extra_arguments;
    if (cases[case_index].devices_ == 2) {
      extra_arguments = {"--data-file", second_path};
    }
    const std::uint16_t port = FindFreePort();
    {
      ServerProcess server(g_lavik_binary, port, first_path, log_path, 2, {},
                           extra_arguments);
      RespClient client(port);
      ASSERT_EQ(client.Command({"FUNCTION", "LOAD", old_library}),
                Bulk("crash_catalog"));
      server.Stop();
    }
    {
      ServerProcess server(g_lavik_binary, port, first_path, log_path, 2,
                           cases[case_index].point_, extra_arguments);
      RespClient ready(port);
      const int crash_fd = ConnectSocket(port);
      ASSERT_GE(crash_fd, 0);
      SendAll(crash_fd,
              EncodeCommand({"FUNCTION", "LOAD", "REPLACE", new_library}));
      server.WaitForCrash();
      ASSERT_EQ(::close(crash_fd), 0);
    }
    {
      ServerProcess server(g_lavik_binary, port, first_path, log_path, 2, {},
                           extra_arguments);
      RespClient client(port);
      EXPECT_EQ(client.Command({"FCALL", "crash_catalog_value", "0"}),
                Bulk(cases[case_index].recovered_))
          << "crash point " << cases[case_index].point_ << " with "
          << cases[case_index].devices_ << " device(s)";
      server.Stop();
    }
  }
}

TEST(ListE2eTest, AmbiguousCatalogRootCommitFencesAllClientsUntilRestart) {
#if defined(NDEBUG) && !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires a Debug or fault-instrumented server";
#endif
  ASSERT_FALSE(g_lavik_binary.empty());
  constexpr std::string_view old_library =
      "#!lua name=ambiguous_catalog\n"
      "redis.register_function('ambiguous_catalog_value', function(keys, "
      "args) return 'old' end)";
  constexpr std::string_view new_library =
      "#!lua name=ambiguous_catalog\n"
      "redis.register_function('ambiguous_catalog_value', function(keys, "
      "args) return 'new' end)";
  const std::string prefix = lavik::test::TestDataPathPrefix() +
                             "lavik-function-catalog-ambiguous-" +
                             std::to_string(::getpid());
  const std::string first_path = prefix + "-0.data";
  const std::string second_path = prefix + "-1.data";
  const std::string log_path = prefix + ".log";
  FileCleanup first_cleanup(first_path);
  FileCleanup second_cleanup(second_path);
  FileCleanup log_cleanup(log_path);
  for (const std::string* path : {&first_path, &second_path}) {
    const int fd =
        ::open(path->c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(::posix_fallocate(fd, 0, 128ULL * 1024 * 1024), 0);
    ASSERT_EQ(::close(fd), 0);
  }

  const std::uint16_t port = FindFreePort();
  {
    // A fresh set has implicit generation zero. The initial LOAD commits
    // generation one, so inject ambiguity into the REPLACE at generation two.
    ServerProcess server(g_lavik_binary, port, first_path, log_path, 2, {},
                         {"--data-file", second_path},
                         {{"LAVIK_FAIL_SYSTEM_STATE_ROOT_ONCE", "1:2"}});
    RespClient mutation_client(port);
    RespClient observer_client(port);
    ASSERT_EQ(mutation_client.Command({"FUNCTION", "LOAD", old_library}),
              Bulk("ambiguous_catalog"));
    const std::string failed =
        mutation_client.Command({"FUNCTION", "LOAD", "REPLACE", new_library});
    EXPECT_TRUE(failed.starts_with("-ERR Error registering functions:"))
        << failed;
    EXPECT_TRUE(observer_client.Command({"SET", "fenced-write", "value"})
                    .starts_with("-LOADING"));
    EXPECT_TRUE(
        observer_client.Command({"FCALL", "ambiguous_catalog_value", "0"})
            .starts_with("-LOADING"));
    server.Kill();
  }
  {
    ServerProcess server(g_lavik_binary, port, first_path, log_path, 2, {},
                         {"--data-file", second_path});
    RespClient client(port);
    EXPECT_EQ(client.Command({"FCALL", "ambiguous_catalog_value", "0"}),
              Bulk("old"));
    EXPECT_EQ(client.Command({"GET", "fenced-write"}), "$-1");
    server.Stop();
  }
}

TEST(ListE2eTest, PersistsStreamApproximateTrimNodeBoundaries) {
  ASSERT_FALSE(g_lavik_binary.empty());
  const std::string prefix = lavik::test::TestDataPathPrefix() +
                             "lavik-stream-trim-e2e-" +
                             std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int data_fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(data_fd, 0);
  ASSERT_EQ(::posix_fallocate(data_fd, 0, 128ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(data_fd), 0);

  const std::uint16_t port = FindFreePort();
  {
    ServerProcess server(g_lavik_binary, port, data_path, log_path, 2);
    RespClient client(port);
    EXPECT_EQ(
        client.Command({"CONFIG", "SET", "stream-node-max-entries", "10"}),
        "+OK");
    for (unsigned index = 1; index <= 100; ++index) {
      const std::string id = std::to_string(index) + "-0";
      EXPECT_EQ(client.Command({"XADD", "trim-stream", id, "f", "v"}),
                Bulk(id));
    }
    EXPECT_EQ(client.Command({"XADD", "trim-stream", "MAXLEN", "~", "55",
                              "LIMIT", "30", "101-0", "f", "v"}),
              Bulk("101-0"));
    EXPECT_EQ(client.Command({"XLEN", "trim-stream"}), ":71");
    server.Stop();
  }
  {
    ServerProcess server(g_lavik_binary, port, data_path, log_path, 2);
    RespClient client(port);
    EXPECT_EQ(client.Command({"XLEN", "trim-stream"}), ":71");
    EXPECT_EQ(client.Command({"XADD", "trim-stream", "MAXLEN", "~", "55",
                              "LIMIT", "30", "102-0", "f", "v"}),
              Bulk("102-0"));
    EXPECT_EQ(client.Command({"XLEN", "trim-stream"}), ":62");
    EXPECT_EQ(client.Command({"XTRIM", "trim-stream", "MAXLEN", "=", "55"}),
              ":7");
    EXPECT_EQ(client.Command({"XTRIM", "trim-stream", "MAXLEN", "~", "44"}),
              ":3");
    EXPECT_EQ(client.Command({"XLEN", "trim-stream"}), ":52");
    server.Stop();
  }
}

TEST(ListE2eTest, PersistsLogicalLengthSeparatelyFromSerializedBytes) {
  ASSERT_FALSE(g_lavik_binary.empty());
  const std::string prefix = lavik::test::TestDataPathPrefix() +
                             "lavik-list-e2e-" + std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::posix_fallocate(fd, 0, 256ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(fd), 0);

  const std::uint16_t port = FindFreePort();
  // One list element contributes a four-byte length after the eight-byte list
  // header. This makes the first encoding exactly three full extents; pushing
  // an empty element adds four bytes and forces a fourth extent.
  constexpr std::size_t kThreeExtentElementBytes =
      3 * lavik::storage::kExtentPayloadBytes - 12;
  const std::string large_element(kThreeExtentElementBytes, 'x');
  std::string binary_element;
  binary_element.push_back('\0');
  binary_element.push_back(static_cast<char>(0xff));
  binary_element.append("\r\n");
  const std::string external_key(5000, 'k');
  {
    ServerProcess server(g_lavik_binary, port, data_path, log_path, 2);
    RespClient client(port);
    EXPECT_EQ(client.Command({"LPUSH", "list", "a", "bb", "ccc"}), ":3");
    EXPECT_EQ(client.Command({"LPUSH", "list", "z"}), ":4");
    EXPECT_EQ(client.Command({"LPUSH", "list", "", binary_element}), ":6");
    EXPECT_EQ(client.Command({"LPOS", "list", "", "MAXLEN", "0"}), ":1");
    EXPECT_EQ(
        client.Command({"LPOS", "list", "", "RANK", "-9223372036854775808"}),
        "-ERR value is out of range");
    EXPECT_EQ(client.Command({"LPOS", "list", "", "RANK", "0"}),
              "-ERR RANK can't be zero: use 1 to start from the first match, "
              "2 from the second ... or use negative to start from the end "
              "of the list");
    EXPECT_EQ(client.Command({"PEXPIRE", "list", "600000"}), ":1");
    EXPECT_EQ(client.Command({"LPUSH", "list", "keeps-ttl"}), ":7");
    const std::string ttl = client.Command({"PTTL", "list"});
    ASSERT_TRUE(ttl.starts_with(':'));
    EXPECT_GT(std::stoll(ttl.substr(1)), 0);
    EXPECT_EQ(client.Command({"GET", "list"}),
              "-WRONGTYPE Operation against a key holding the wrong kind of "
              "value");
    EXPECT_EQ(client.Command({"SET", "string", "value"}), "+OK");
    EXPECT_EQ(client.Command({"LPUSH", "string", "x"}),
              "-WRONGTYPE Operation against a key holding the wrong kind of "
              "value");
    EXPECT_EQ(client.Command({"LPUSH", "", ""}), ":1");
    EXPECT_EQ(client.Command({"LPUSH", external_key, binary_element}), ":1");
    EXPECT_EQ(client.Command({"LPUSH", "multi-extent", large_element}), ":1");
    EXPECT_EQ(client.Command({"LPUSH", "multi-extent", ""}), ":2");
    const std::string binary_blocking_key = "binary\r\n*1\r\nkey";
    EXPECT_EQ(client.Command({"RPUSH", binary_blocking_key, binary_element}),
              ":1");
    EXPECT_EQ(
        client.Command({"BLPOP", binary_blocking_key, "0.1"}),
        "*2\r\n" + Bulk(binary_blocking_key) + "\r\n" + Bulk(binary_element));
    const std::string oversized_key(lavik::storage::kStorageBlockBytes + 4096,
                                    'q');
    EXPECT_EQ(client.Command({"SET", oversized_key, "string-value"}), "+OK");
    EXPECT_EQ(client.Command({"DEL", oversized_key}), ":1");
    EXPECT_EQ(client.Command({"RPUSH", oversized_key, "list-value"}), ":1");
    EXPECT_EQ(client.Command({"LLEN", oversized_key}), ":1");
    server.Stop();
  }

  {
    ServerProcess server(g_lavik_binary, port, data_path, log_path, 3);
    RespClient client(port);
    EXPECT_EQ(client.Command({"LPUSH", "list", "after-restart"}), ":8");
    EXPECT_EQ(client.Command({"LPUSH", "", "after-restart"}), ":2");
    EXPECT_EQ(client.Command({"LPUSH", external_key, "after-restart"}), ":2");
    EXPECT_EQ(client.Command(
                  {"LPUSH", "multi-extent", binary_element, "after-restart"}),
              ":4");
    EXPECT_EQ(client.Command({"STRLEN", "multi-extent"}),
              "-WRONGTYPE Operation against a key holding the wrong kind of "
              "value");
    server.Stop();
  }
}

TEST(ListE2eTest, BlockingAndStreamedCommandsDoNotHoldFlushDbGate) {
  ASSERT_FALSE(g_lavik_binary.empty());
  const std::string prefix = lavik::test::TestDataPathPrefix() +
                             "lavik-db-gate-e2e-" + std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int data_fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(data_fd, 0);
  ASSERT_EQ(::posix_fallocate(data_fd, 0, 128ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(data_fd), 0);

  const std::uint16_t port = FindFreePort();
  ServerProcess server(g_lavik_binary, port, data_path, log_path, 2);
  RespClient client(port);

  auto blocked = std::async(std::launch::async, [port] {
    RespClient waiting(port);
    return waiting.Command({"BLPOP", "gate-blocked-list", "0.3"});
  });
  std::this_thread::sleep_for(30ms);
  auto flushed = std::async(std::launch::async, [port] {
    RespClient flushing(port);
    return flushing.Command({"FLUSHDB", "SYNC"});
  });
  ASSERT_EQ(flushed.wait_for(2s), std::future_status::ready);
  EXPECT_EQ(flushed.get(), "+OK");
  EXPECT_EQ(client.Command({"SET", "after-blocking-flush", "alive"}), "+OK");
  EXPECT_EQ(blocked.get(), "*-1");

  auto blocked_stream = std::async(std::launch::async, [port] {
    RespClient waiting(port);
    return waiting.Command(
        {"XREAD", "BLOCK", "0", "STREAMS", "gate-blocked-stream", "0-0"});
  });
  std::this_thread::sleep_for(30ms);
  auto stream_read_flush = std::async(std::launch::async, [port] {
    RespClient flushing(port);
    return flushing.Command({"FLUSHDB", "SYNC"});
  });
  ASSERT_EQ(stream_read_flush.wait_for(2s), std::future_status::ready);
  EXPECT_EQ(stream_read_flush.get(), "+OK");
  EXPECT_EQ(
      client.Command({"XADD", "gate-blocked-stream", "1-0", "field", "value"}),
      Bulk("1-0"));
  ASSERT_EQ(blocked_stream.wait_for(2s), std::future_status::ready);
  EXPECT_EQ(blocked_stream.get(), "*1\r\n*2\r\n" + Bulk("gate-blocked-stream") +
                                      "\r\n*1\r\n*2\r\n" + Bulk("1-0") +
                                      "\r\n*2\r\n" + Bulk("field") + "\r\n" +
                                      Bulk("value"));

  ASSERT_EQ(client.Command({"XGROUP", "CREATE", "gate-blocked-group", "g", "$",
                            "MKSTREAM"}),
            "+OK");
  auto blocked_group = std::async(std::launch::async, [port] {
    RespClient waiting(port);
    return waiting.Command({"XREADGROUP", "GROUP", "g", "consumer", "BLOCK",
                            "0", "STREAMS", "gate-blocked-group", ">"});
  });
  std::this_thread::sleep_for(30ms);
  auto group_read_flush = std::async(std::launch::async, [port] {
    RespClient flushing(port);
    return flushing.Command({"FLUSHDB", "SYNC"});
  });
  ASSERT_EQ(group_read_flush.wait_for(2s), std::future_status::ready);
  EXPECT_EQ(group_read_flush.get(), "+OK");
  ASSERT_EQ(blocked_group.wait_for(2s), std::future_status::ready);
  EXPECT_TRUE(blocked_group.get().starts_with("-NOGROUP"));

  const std::string member(4096, 's');
  ASSERT_EQ(client.Command({"SADD", "gate-stream-set", member}), ":1");
  const int locked_stream_fd = ConnectSocket(port);
  ASSERT_GE(locked_stream_fd, 0);
  SendAll(locked_stream_fd,
          EncodeCommand({"SRANDMEMBER", "gate-stream-set", "-2000"}));
  ASSERT_EQ(ReadRespLine(locked_stream_fd), "*2000");
  // The stalled stream holds neither the process-wide DB gate nor its key lock
  // while its generated chunk waits for socket backpressure.
  EXPECT_NE(client.Command({"INFO", "STATS"})
                .find("storage_expiration_pause_count:0\r\n"),
            std::string::npos);
  EXPECT_EQ(client.Command({"SET", "stream-unrelated-write", "alive"}), "+OK");
  auto removal = std::async(std::launch::async, [port, member] {
    RespClient writer(port);
    return writer.Command({"SREM", "gate-stream-set", member});
  });
  ASSERT_EQ(removal.wait_for(2s), std::future_status::ready);
  EXPECT_EQ(removal.get(), ":1");
  for (std::size_t i = 0; i < 2000; ++i) {
    EXPECT_EQ(ReadRespBulk(locked_stream_fd), member);
  }
  SendAll(locked_stream_fd, EncodeCommand({"PING"}));
  EXPECT_EQ(ReadRespLine(locked_stream_fd), "+PONG");
  linger reset{.l_onoff = 1, .l_linger = 0};
  (void)::setsockopt(locked_stream_fd, SOL_SOCKET, SO_LINGER, &reset,
                     sizeof(reset));
  ::close(locked_stream_fd);

  ASSERT_EQ(client.Command({"SADD", "gate-stream-set", member}), ":1");
  const int flush_stream_fd = ConnectSocket(port);
  ASSERT_GE(flush_stream_fd, 0);
  SendAll(flush_stream_fd,
          EncodeCommand({"SRANDMEMBER", "gate-stream-set", "-1000000000"}));
  ASSERT_EQ(ReadRespLine(flush_stream_fd), "*1000000000");
  std::this_thread::sleep_for(30ms);
  auto stream_flush = std::async(std::launch::async, [port] {
    RespClient flushing(port);
    return flushing.Command({"FLUSHDB", "SYNC"});
  });
  ASSERT_EQ(stream_flush.wait_for(2s), std::future_status::ready);
  EXPECT_EQ(stream_flush.get(), "+OK");
  (void)::setsockopt(flush_stream_fd, SOL_SOCKET, SO_LINGER, &reset,
                     sizeof(reset));
  ::close(flush_stream_fd);
  EXPECT_EQ(client.Command({"SET", "after-stream-flush", "alive"}), "+OK");
  server.Stop();
}

TEST(ListE2eTest, StreamBlockingRegistryBroadcastsAndKeepsGroupFifo) {
  ASSERT_FALSE(g_lavik_binary.empty());
  const std::string prefix = lavik::test::TestDataPathPrefix() +
                             "lavik-stream-wait-e2e-" +
                             std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int data_fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(data_fd, 0);
  ASSERT_EQ(::posix_fallocate(data_fd, 0, 128ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(data_fd), 0);

  const std::uint16_t port = FindFreePort();
  ServerProcess server(g_lavik_binary, port, data_path, log_path, 3);
  RespClient client(port);

  auto xread = [port] {
    RespClient waiting(port);
    return waiting.Command(
        {"XREAD", "BLOCK", "5000", "STREAMS", "broadcast-stream", "0-0"});
  };
  // Thread launch order is not waiter registration order. Observe each
  // registration before publishing data or adding the next FIFO consumer.
  auto wait_for_blocked_clients = [&](unsigned expected) {
    const std::string field =
        "blocked_clients:" + std::to_string(expected) + "\r\n";
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (std::chrono::steady_clock::now() < deadline) {
      if (client.Command({"INFO", "CLIENTS"}).find(field) != std::string::npos)
        return true;
      std::this_thread::sleep_for(1ms);
    }
    return false;
  };
  auto first_reader = std::async(std::launch::async, xread);
  auto second_reader = std::async(std::launch::async, xread);
  ASSERT_TRUE(wait_for_blocked_clients(2));
  EXPECT_EQ(
      client.Command({"XADD", "broadcast-stream", "1-0", "field", "value"}),
      Bulk("1-0"));
  ASSERT_EQ(first_reader.wait_for(1s), std::future_status::ready);
  ASSERT_EQ(second_reader.wait_for(1s), std::future_status::ready);
  const std::string broadcast_reply =
      "*1\r\n*2\r\n" + Bulk("broadcast-stream") + "\r\n*1\r\n*2\r\n" +
      Bulk("1-0") + "\r\n*2\r\n" + Bulk("field") + "\r\n" + Bulk("value");
  EXPECT_EQ(first_reader.get(), broadcast_reply);
  EXPECT_EQ(second_reader.get(), broadcast_reply);

  EXPECT_EQ(
      client.Command({"XGROUP", "CREATE", "fifo-stream", "g", "$", "MKSTREAM"}),
      "+OK");
  auto first_group = std::async(std::launch::async, [port] {
    RespClient waiting(port);
    return waiting.Command({"XREADGROUP", "GROUP", "g", "first", "COUNT", "1",
                            "BLOCK", "5000", "STREAMS", "fifo-stream", ">"});
  });
  ASSERT_TRUE(wait_for_blocked_clients(1));
  auto second_group = std::async(std::launch::async, [port] {
    RespClient waiting(port);
    return waiting.Command({"XREADGROUP", "GROUP", "g", "second", "COUNT", "1",
                            "BLOCK", "5000", "STREAMS", "fifo-stream", ">"});
  });
  ASSERT_TRUE(wait_for_blocked_clients(2));
  EXPECT_EQ(client.Command({"XADD", "fifo-stream", "1-0", "f", "one"}),
            Bulk("1-0"));
  ASSERT_EQ(first_group.wait_for(1s), std::future_status::ready);
  EXPECT_EQ(second_group.wait_for(100ms), std::future_status::timeout);
  EXPECT_TRUE(first_group.get().starts_with("*1\r\n"));
  EXPECT_EQ(client.Command({"XADD", "fifo-stream", "2-0", "f", "two"}),
            Bulk("2-0"));
  ASSERT_EQ(second_group.wait_for(1s), std::future_status::ready);
  EXPECT_TRUE(second_group.get().starts_with("*1\r\n"));

  EXPECT_EQ(client.Command({"XADD", "rewind-wake", "1-0", "f", "v"}),
            Bulk("1-0"));
  EXPECT_EQ(
      client.Command({"XGROUP", "CREATE", "rewind-wake", "rewind-group", "$"}),
      "+OK");
  auto rewound_group = std::async(std::launch::async, [port] {
    RespClient waiting(port);
    return waiting.Command({"XREADGROUP", "GROUP", "rewind-group", "reader",
                            "BLOCK", "5000", "STREAMS", "rewind-wake", ">"});
  });
  ASSERT_TRUE(wait_for_blocked_clients(1));
  EXPECT_EQ(
      client.Command({"XGROUP", "SETID", "rewind-wake", "rewind-group", "0"}),
      "+OK");
  ASSERT_EQ(rewound_group.wait_for(1s), std::future_status::ready);
  EXPECT_TRUE(rewound_group.get().starts_with("*1\r\n"));

  server.Stop();
}

TEST(ListE2eTest, ClientUnblockFindsBlockedClientsAcrossWorkers) {
  ASSERT_FALSE(g_lavik_binary.empty());
  const std::string prefix = lavik::test::TestDataPathPrefix() +
                             "lavik-client-unblock-e2e-" +
                             std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int data_fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(data_fd, 0);
  ASSERT_EQ(::posix_fallocate(data_fd, 0, 128ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(data_fd), 0);

  const std::uint16_t port = FindFreePort();
  ServerProcess server(g_lavik_binary, port, data_path, log_path, 3);
  RespClient client(port);
  auto wait_until_blocked = [&](std::string_view id) {
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (std::chrono::steady_clock::now() < deadline) {
      const std::string listing = client.Command({"CLIENT", "LIST", "ID", id});
      if (listing.find("id=" + std::string(id) + " ") != std::string::npos &&
          listing.find(" flags=b ") != std::string::npos) {
        return true;
      }
      std::this_thread::sleep_for(10ms);
    }
    return false;
  };
  auto client_id = [](std::string encoded) {
    if (!encoded.starts_with(':')) {
      throw std::runtime_error("CLIENT ID did not return an integer");
    }
    return encoded.substr(1);
  };

  const std::string help = client.Command({"CLIENT", "HELP"});
  EXPECT_NE(
      help.find(
          "+CLIENT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"),
      std::string::npos);
  EXPECT_NE(help.find("+SETINFO <option> <value>"), std::string::npos);
  EXPECT_NE(help.find("+    Print this help."), std::string::npos);
  EXPECT_EQ(client.Command({"CLIENT", "HELP", "extra"}),
            "-ERR wrong number of arguments for 'client|help' command");

  std::string info = client.Command({"CLIENT", "INFO"});
  EXPECT_NE(info.find(" cmd=client|info "), std::string::npos);
  EXPECT_NE(info.find(" lib-name= lib-ver="), std::string::npos);
  EXPECT_EQ(client.Command({"CLIENT", "INFO", "extra"}),
            "-ERR wrong number of arguments for 'client|info' command");
  EXPECT_EQ(client.Command({"CLIENT", "SETINFO", "lib-name", "redis.py"}),
            "+OK");
  EXPECT_EQ(client.Command({"CLIENT", "SETINFO", "LIB-VER", "1.2.3"}), "+OK");
  info = client.Command({"CLIENT", "INFO"});
  EXPECT_NE(info.find(" lib-name=redis.py lib-ver=1.2.3"), std::string::npos);
  const std::string metadata_id = client_id(client.Command({"CLIENT", "ID"}));
  const std::string metadata_listing =
      client.Command({"CLIENT", "LIST", "ID", metadata_id});
  EXPECT_NE(metadata_listing.find(" lib-name=redis.py lib-ver=1.2.3"),
            std::string::npos);
  EXPECT_EQ(
      client.Command({"CLIENT", "SETINFO", "lib-name", "redis py"}),
      "-ERR lib-name cannot contain spaces, newlines or special characters.");
  EXPECT_EQ(
      client.Command({"CLIENT", "SETINFO", "lib-ver", "1.2\n3"}),
      "-ERR lib-ver cannot contain spaces, newlines or special characters.");
  EXPECT_EQ(client.Command({"CLIENT", "SETINFO", "badger", "hamster"}),
            "-ERR Unrecognized option 'badger'");
  EXPECT_EQ(client.Command({"CLIENT", "SETINFO", "lib-name"}),
            "-ERR wrong number of arguments for 'client|setinfo' command");
  EXPECT_EQ(client.Command({"RESET"}), "+RESET");
  info = client.Command({"CLIENT", "INFO"});
  EXPECT_NE(info.find(" lib-name=redis.py lib-ver=1.2.3"), std::string::npos);
  EXPECT_EQ(client.Command({"CLIENT", "SETINFO", "lib-name", ""}), "+OK");
  info = client.Command({"CLIENT", "INFO"});
  EXPECT_NE(info.find(" lib-name= lib-ver=1.2.3"), std::string::npos);

  EXPECT_EQ(client.Command({"CLIENT", "UNBLOCK", "not-an-id"}),
            "-ERR value is not an integer or out of range");
  EXPECT_EQ(client.Command({"CLIENT", "UNBLOCK", "1", "invalid"}),
            "-ERR CLIENT UNBLOCK reason should be TIMEOUT or ERROR");
  const std::string active_id = client_id(client.Command({"CLIENT", "ID"}));
  EXPECT_EQ(client.Command({"CLIENT", "UNBLOCK", active_id}), ":0");

  std::promise<std::string> timeout_id_promise;
  std::future<std::string> timeout_id = timeout_id_promise.get_future();
  auto timeout_waiter =
      std::async(std::launch::async, [port, &timeout_id_promise, &client_id] {
        RespClient waiting(port);
        timeout_id_promise.set_value(
            client_id(waiting.Command({"CLIENT", "ID"})));
        return waiting.Command({"BLPOP", "client-unblock-timeout", "5"});
      });
  const std::string timeout_client_id = timeout_id.get();
  ASSERT_TRUE(wait_until_blocked(timeout_client_id));
  EXPECT_EQ(client.Command({"CLIENT", "UNBLOCK", timeout_client_id}), ":1");
  ASSERT_EQ(timeout_waiter.wait_for(1s), std::future_status::ready);
  EXPECT_EQ(timeout_waiter.get(), "*-1");
  EXPECT_EQ(client.Command({"CLIENT", "UNBLOCK", timeout_client_id}), ":0");

  std::promise<std::string> error_id_promise;
  std::future<std::string> error_id = error_id_promise.get_future();
  auto error_waiter = std::async(std::launch::async, [port, &error_id_promise,
                                                      &client_id] {
    RespClient waiting(port);
    error_id_promise.set_value(client_id(waiting.Command({"CLIENT", "ID"})));
    return waiting.Command({"BLPOP", "client-unblock-error", "5"});
  });
  const std::string error_client_id = error_id.get();
  ASSERT_TRUE(wait_until_blocked(error_client_id));
  EXPECT_EQ(client.Command({"CLIENT", "UNBLOCK", error_client_id, "ERROR"}),
            ":1");
  ASSERT_EQ(error_waiter.wait_for(1s), std::future_status::ready);
  EXPECT_EQ(error_waiter.get(),
            "-UNBLOCKED client unblocked via CLIENT UNBLOCK");

  std::promise<std::string> stream_id_promise;
  std::future<std::string> stream_id = stream_id_promise.get_future();
  auto stream_waiter = std::async(std::launch::async, [port, &stream_id_promise,
                                                       &client_id] {
    RespClient waiting(port);
    stream_id_promise.set_value(client_id(waiting.Command({"CLIENT", "ID"})));
    return waiting.Command(
        {"XREAD", "BLOCK", "5000", "STREAMS", "client-unblock-stream", "0-0"});
  });
  const std::string stream_client_id = stream_id.get();
  ASSERT_TRUE(wait_until_blocked(stream_client_id));
  EXPECT_EQ(client.Command({"CLIENT", "UNBLOCK", stream_client_id, "ERROR"}),
            ":1");
  ASSERT_EQ(stream_waiter.wait_for(1s), std::future_status::ready);
  EXPECT_EQ(stream_waiter.get(),
            "-UNBLOCKED client unblocked via CLIENT UNBLOCK");

  server.Stop();
}

TEST(ListE2eTest, DisconnectCancelsActiveBlockingWait) {
  ASSERT_FALSE(g_lavik_binary.empty());
  const std::string prefix = lavik::test::TestDataPathPrefix() +
                             "lavik-blocking-disconnect-e2e-" +
                             std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int data_fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(data_fd, 0);
  ASSERT_EQ(::posix_fallocate(data_fd, 0, 128ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(data_fd), 0);

  const std::uint16_t port = FindFreePort();
  ServerProcess server(g_lavik_binary, port, data_path, log_path, 3);
  RespClient control(port);
  auto wait_for_blocked_clients = [&](std::uint64_t expected) {
    const std::string field =
        "blocked_clients:" + std::to_string(expected) + "\r\n";
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (std::chrono::steady_clock::now() < deadline) {
      if (control.Command({"INFO", "CLIENTS"}).find(field) !=
          std::string::npos) {
        return true;
      }
      std::this_thread::sleep_for(1ms);
    }
    return false;
  };

  const int blocked = ConnectSocket(port);
  ASSERT_GE(blocked, 0);
  SendAll(blocked, EncodeCommand({"BLPOP", "disconnect-blocked-client", "0"}));
  ASSERT_TRUE(wait_for_blocked_clients(1));
  ASSERT_EQ(::close(blocked), 0);
  EXPECT_TRUE(wait_for_blocked_clients(0));

  // The dead waiter's key registration must be gone as well as its metric.
  EXPECT_EQ(control.Command({"LPUSH", "disconnect-blocked-client", "value"}),
            ":1");
  EXPECT_EQ(control.Command({"LPOP", "disconnect-blocked-client"}),
            "$5\r\nvalue");
  server.Stop();
}

TEST(ListE2eTest, PipelineFlushesRepliesBeforeBlockingCommand) {
  ASSERT_FALSE(g_lavik_binary.empty());
  const std::string prefix = lavik::test::TestDataPathPrefix() +
                             "lavik-blocking-pipeline-e2e-" +
                             std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int data_fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(data_fd, 0);
  ASSERT_EQ(::posix_fallocate(data_fd, 0, 128ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(data_fd), 0);

  const std::uint16_t port = FindFreePort();
  ServerProcess server(g_lavik_binary, port, data_path, log_path, 3);
  RespClient control(port);
  auto wait_for_blocked_clients = [&](std::uint64_t expected) {
    const std::string field =
        "blocked_clients:" + std::to_string(expected) + "\r\n";
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (std::chrono::steady_clock::now() < deadline) {
      if (control.Command({"INFO", "CLIENTS"}).find(field) !=
          std::string::npos) {
        return true;
      }
      std::this_thread::sleep_for(1ms);
    }
    return false;
  };

  constexpr std::string_view key = "pipeline-blocking-fairness";
  auto first_waiter = std::async(std::launch::async, [port, key] {
    RespClient waiting(port);
    return waiting.Command({"BLPOP", key, "2"});
  });
  ASSERT_TRUE(wait_for_blocked_clients(1));

  const int pipelined = ConnectSocket(port);
  ASSERT_GE(pipelined, 0);
  timeval short_timeout{.tv_sec = 2, .tv_usec = 0};
  ASSERT_EQ(::setsockopt(pipelined, SOL_SOCKET, SO_RCVTIMEO, &short_timeout,
                         sizeof(short_timeout)),
            0);
  std::string commands = EncodeCommand({"LPUSH", key, "first"});
  commands += EncodeCommand({"BLPOP", key, "2"});
  SendAll(pipelined, commands);

  ASSERT_EQ(first_waiter.wait_for(1s), std::future_status::ready);
  EXPECT_EQ(first_waiter.get(),
            "*2\r\n$26\r\npipeline-blocking-fairness\r\n"
            "$5\r\nfirst");
  // This reply must be on the wire even though the next pipelined command is
  // now blocked on the same connection.
  EXPECT_EQ(ReadRespLine(pipelined), ":1");
  ASSERT_TRUE(wait_for_blocked_clients(1));

  EXPECT_EQ(control.Command({"LPUSH", key, "second"}), ":1");
  ASSERT_TRUE(wait_for_blocked_clients(0));
  EXPECT_EQ(ReadRespLine(pipelined), "*2");
  EXPECT_EQ(ReadRespBulk(pipelined), key);
  EXPECT_EQ(ReadRespBulk(pipelined), "second");
  ASSERT_EQ(::close(pipelined), 0);
  server.Stop();
}

TEST(ListE2eTest, ExecWakesBlockersOnlyForFinalValueTypes) {
  ASSERT_FALSE(g_lavik_binary.empty());
  const std::string prefix = lavik::test::TestDataPathPrefix() +
                             "lavik-exec-final-type-e2e-" +
                             std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int data_fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(data_fd, 0);
  ASSERT_EQ(::posix_fallocate(data_fd, 0, 160ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(data_fd), 0);

  constexpr unsigned kWorkerCount = 3;
  const std::uint16_t port = FindFreePort();
  ServerProcess server(g_lavik_binary, port, data_path, log_path, kWorkerCount);
  RespClient client(port);
  const std::string list_key = KeyForWorker("exec-final-list", 0, kWorkerCount);
  const std::string extra_list_key =
      KeyForWorker("exec-final-list-extra", 1, kWorkerCount);
  const std::string zset_key = KeyForWorker("exec-final-zset", 1, kWorkerCount);
  const std::string stream_key =
      KeyForWorker("exec-final-stream", 2, kWorkerCount);
  auto wait_for_blocked_clients = [&](std::uint64_t expected) {
    const std::string field =
        "blocked_clients:" + std::to_string(expected) + "\r\n";
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (std::chrono::steady_clock::now() < deadline) {
      if (client.Command({"INFO", "CLIENTS"}).find(field) !=
          std::string::npos) {
        return true;
      }
      std::this_thread::sleep_for(10ms);
    }
    return false;
  };

  auto list_waiter =
      std::async(std::launch::async, [port, list_key, extra_list_key] {
        RespClient waiting(port);
        return waiting.Command({"BLPOP", list_key, extra_list_key, "2"});
      });
  auto zset_waiter = std::async(std::launch::async, [port, zset_key] {
    RespClient waiting(port);
    return waiting.Command({"BZPOPMIN", zset_key, "2"});
  });
  auto stream_waiter = std::async(std::launch::async, [port, stream_key] {
    RespClient waiting(port);
    return waiting.Command(
        {"XREAD", "BLOCK", "2000", "STREAMS", stream_key, "0-0"});
  });
  ASSERT_TRUE(wait_for_blocked_clients(3));

  EXPECT_EQ(client.Command({"MULTI"}), "+OK");
  EXPECT_EQ(client.Command({"RPUSH", list_key, "transient"}), "+QUEUED");
  EXPECT_EQ(client.Command({"DEL", list_key}), "+QUEUED");
  EXPECT_EQ(client.Command({"SET", list_key, "final-string"}), "+QUEUED");
  EXPECT_EQ(client.Command({"ZADD", zset_key, "1", "transient"}), "+QUEUED");
  EXPECT_EQ(client.Command({"DEL", zset_key}), "+QUEUED");
  EXPECT_EQ(client.Command({"SET", zset_key, "final-string"}), "+QUEUED");
  EXPECT_EQ(client.Command({"XADD", stream_key, "1-0", "field", "transient"}),
            "+QUEUED");
  EXPECT_EQ(client.Command({"DEL", stream_key}), "+QUEUED");
  EXPECT_EQ(client.Command({"SET", stream_key, "final-string"}), "+QUEUED");
  EXPECT_EQ(client.Command({"EXEC"}),
            "*9\r\n:1\r\n:1\r\n+OK\r\n:1\r\n:1\r\n+OK\r\n" + Bulk("1-0") +
                "\r\n:1\r\n+OK");

  EXPECT_EQ(list_waiter.wait_for(100ms), std::future_status::timeout);
  EXPECT_EQ(zset_waiter.wait_for(100ms), std::future_status::timeout);
  EXPECT_EQ(stream_waiter.wait_for(100ms), std::future_status::timeout);

  EXPECT_EQ(client.Command({"MULTI"}), "+OK");
  EXPECT_EQ(client.Command({"DEL", list_key, zset_key, stream_key}), "+QUEUED");
  EXPECT_EQ(client.Command({"RPUSH", list_key, "ready"}), "+QUEUED");
  EXPECT_EQ(client.Command({"ZADD", zset_key, "2", "ready"}), "+QUEUED");
  EXPECT_EQ(client.Command({"XADD", stream_key, "2-0", "field", "ready"}),
            "+QUEUED");
  EXPECT_EQ(client.Command({"EXEC"}), "*4\r\n:3\r\n:1\r\n:1\r\n" + Bulk("2-0"));

  ASSERT_EQ(list_waiter.wait_for(1s), std::future_status::ready);
  EXPECT_EQ(list_waiter.get(),
            "*2\r\n" + Bulk(list_key) + "\r\n" + Bulk("ready"));
  ASSERT_EQ(zset_waiter.wait_for(1s), std::future_status::ready);
  EXPECT_EQ(zset_waiter.get(), "*3\r\n" + Bulk(zset_key) + "\r\n" +
                                   Bulk("ready") + "\r\n" + Bulk("2"));
  ASSERT_EQ(stream_waiter.wait_for(1s), std::future_status::ready);
  EXPECT_EQ(stream_waiter.get(), "*1\r\n*2\r\n" + Bulk(stream_key) +
                                     "\r\n*1\r\n*2\r\n" + Bulk("2-0") +
                                     "\r\n*2\r\n" + Bulk("field") + "\r\n" +
                                     Bulk("ready"));
  EXPECT_TRUE(wait_for_blocked_clients(0));

  server.Stop();
}

TEST(ListE2eTest, CircularBlockingMovesDrainBeforeTriggerReply) {
  ASSERT_FALSE(g_lavik_binary.empty());
  const std::string prefix = lavik::test::TestDataPathPrefix() +
                             "lavik-circular-blocking-move-e2e-" +
                             std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int data_fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(data_fd, 0);
  ASSERT_EQ(::posix_fallocate(data_fd, 0, 128ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(data_fd), 0);

  const std::uint16_t port = FindFreePort();
  ServerProcess server(g_lavik_binary, port, data_path, log_path, 3);
  RespClient client(port);
  const std::string first = "circular-first{nested}";
  const std::string second = "circular-second{nested}";
  auto wait_for_blocked_clients = [&](std::uint64_t expected) {
    const std::string field =
        "blocked_clients:" + std::to_string(expected) + "\r\n";
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (std::chrono::steady_clock::now() < deadline) {
      if (client.Command({"INFO", "CLIENTS"}).find(field) !=
          std::string::npos) {
        return true;
      }
      std::this_thread::sleep_for(10ms);
    }
    return false;
  };

  auto run_cycle = [&](bool transaction) {
    const std::string deleted = client.Command({"DEL", first, second});
    EXPECT_TRUE(deleted == ":0" || deleted == ":1");
    auto forward = std::async(std::launch::async, [port, first, second] {
      RespClient waiting(port);
      return waiting.Command({"BRPOPLPUSH", first, second, "2"});
    });
    ASSERT_TRUE(wait_for_blocked_clients(1));
    auto backward = std::async(std::launch::async, [port, first, second] {
      RespClient waiting(port);
      return waiting.Command({"BRPOPLPUSH", second, first, "2"});
    });
    ASSERT_TRUE(wait_for_blocked_clients(2));

    if (transaction) {
      EXPECT_EQ(client.Command({"MULTI"}), "+OK");
      EXPECT_EQ(client.Command({"RPUSH", first, "foo"}), "+QUEUED");
      EXPECT_EQ(client.Command({"EXEC"}), "*1\r\n:1");
    } else {
      EXPECT_EQ(client.Command({"RPUSH", first, "foo"}), ":1");
    }

    // Redis drains ready keys, including nested wakes, before accepting the
    // next command. The value must already have completed both moves here.
    EXPECT_EQ(client.Command({"LRANGE", first, "0", "-1"}), BulkArray({"foo"}));
    EXPECT_EQ(client.Command({"LRANGE", second, "0", "-1"}), "*0");
    ASSERT_EQ(forward.wait_for(1s), std::future_status::ready);
    ASSERT_EQ(backward.wait_for(1s), std::future_status::ready);
    EXPECT_EQ(forward.get(), Bulk("foo"));
    EXPECT_EQ(backward.get(), Bulk("foo"));
    EXPECT_TRUE(wait_for_blocked_clients(0));
  };

  run_cycle(false);
  run_cycle(true);
  server.Stop();
}

TEST(ListE2eTest, BlockingMovesDoNotDirtyWatchBeforeWakeAndCountChanges) {
  ASSERT_FALSE(g_lavik_binary.empty());
  const std::string prefix = lavik::test::TestDataPathPrefix() +
                             "lavik-blocking-watch-dirty-e2e-" +
                             std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int data_fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(data_fd, 0);
  ASSERT_EQ(::posix_fallocate(data_fd, 0, 128ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(data_fd), 0);

  const std::uint16_t port = FindFreePort();
  ServerProcess server(g_lavik_binary, port, data_path, log_path, 3);
  RespClient trigger(port);
  auto blocked_clients = [&](std::uint64_t expected) {
    const std::string field =
        "blocked_clients:" + std::to_string(expected) + "\r\n";
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (std::chrono::steady_clock::now() < deadline) {
      if (trigger.Command({"INFO", "CLIENTS"}).find(field) !=
          std::string::npos) {
        return true;
      }
      std::this_thread::sleep_for(1ms);
    }
    return false;
  };
  auto dirty = [&] {
    constexpr std::string_view marker = "rdb_changes_since_last_save:";
    const std::string info = trigger.Command({"INFO", "PERSISTENCE"});
    const std::size_t begin = info.find(marker);
    if (begin == std::string::npos) {
      throw std::runtime_error("RDB dirty counter is missing");
    }
    const std::size_t value_begin = begin + marker.size();
    const std::size_t value_end = info.find("\r\n", value_begin);
    std::uint64_t value = 0;
    const auto [parsed, error] = std::from_chars(
        info.data() + value_begin, info.data() + value_end, value);
    if (error != std::errc{} || parsed != info.data() + value_end) {
      throw std::runtime_error("RDB dirty counter is malformed");
    }
    return value;
  };

  const std::string source = "watch-source{blocking-watch}";
  const std::string destination = "watch-destination{blocking-watch}";
  const std::string value_key = "watch-value{blocking-watch}";
  for (unsigned iteration = 0; iteration < 32; ++iteration) {
    const std::string deleted =
        trigger.Command({"DEL", source, destination, value_key});
    ASSERT_TRUE(deleted == ":0" || deleted == ":1" || deleted == ":2" ||
                deleted == ":3");
    ASSERT_EQ(trigger.Command({"SET", value_key, "somevalue"}), "+OK");
    auto blocked = std::async(std::launch::async, [port, source, destination] {
      RespClient client(port);
      return client.Command({"BRPOPLPUSH", source, destination, "2"});
    });
    ASSERT_TRUE(blocked_clients(1));

    RespClient watcher(port);
    ASSERT_EQ(watcher.Command({"WATCH", destination}), "+OK");
    ASSERT_EQ(watcher.Command({"MULTI"}), "+OK");
    ASSERT_EQ(watcher.Command({"GET", value_key}), "+QUEUED");
    // Complete EXEC while BRPOPLPUSH is still blocked. Ordering two
    // independent TCP streams by client-side send time is not a server-side
    // synchronization primitive; reading the reply makes the intended
    // pre-wake ordering explicit.
    EXPECT_EQ(watcher.Command({"EXEC"}), "*1\r\n" + Bulk("somevalue"));
    ASSERT_EQ(trigger.Command({"LPUSH", source, "element"}), ":1");
    ASSERT_EQ(blocked.wait_for(1s), std::future_status::ready);
    EXPECT_EQ(blocked.get(), Bulk("element"));
  }

  const std::string pop_key = "dirty-pop{blocking-dirty}";
  auto pop = std::async(std::launch::async, [port, pop_key] {
    RespClient client(port);
    return client.Command({"BLPOP", pop_key, "2"});
  });
  ASSERT_TRUE(blocked_clients(1));
  const std::uint64_t before_pop = dirty();
  ASSERT_EQ(trigger.Command({"LPUSH", pop_key, "a"}), ":1");
  ASSERT_EQ(pop.wait_for(1s), std::future_status::ready);
  EXPECT_EQ(pop.get(), "*2\r\n" + Bulk(pop_key) + "\r\n" + Bulk("a"));
  EXPECT_EQ(dirty(), before_pop + 2);

  auto move = std::async(std::launch::async, [port, source, destination] {
    RespClient client(port);
    return client.Command({"BLMOVE", source, destination, "LEFT", "LEFT", "2"});
  });
  ASSERT_TRUE(blocked_clients(1));
  const std::uint64_t before_move = dirty();
  ASSERT_EQ(trigger.Command({"LPUSH", source, "a"}), ":1");
  ASSERT_EQ(move.wait_for(1s), std::future_status::ready);
  EXPECT_EQ(move.get(), Bulk("a"));
  EXPECT_EQ(dirty(), before_move + 2);

  server.Stop();
}

TEST(ListE2eTest, CommandsLargeKeyTransactionsAndCrashRecovery) {
  ASSERT_FALSE(g_lavik_binary.empty());
  const std::string prefix = lavik::test::TestDataPathPrefix() +
                             "lavik-list-complete-e2e-" +
                             std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::posix_fallocate(fd, 0, 512ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(fd), 0);

  const std::uint16_t port = FindFreePort();
  const std::string lua_recovery_a = KeyForWorker("lua-exec-recovery-a", 0, 3);
  const std::string lua_recovery_b = KeyForWorker("lua-exec-recovery-b", 1, 3);
  std::vector<std::string> large_values;
  large_values.reserve(1200);
  for (int i = 0; i < 1200; ++i) {
    large_values.push_back("element-" + std::to_string(i) + "-" +
                           std::string(1024, static_cast<char>('a' + i % 26)));
  }

  {
    ServerProcess server(g_lavik_binary, port, data_path, log_path, 4);
    RespClient client(port);

    const auto expect_arity_error = [&](std::vector<std::string_view> args,
                                        std::string_view command) {
      EXPECT_EQ(client.Command(args), "-ERR wrong number of arguments for '" +
                                          std::string(command) + "' command");
      EXPECT_EQ(client.Command({"PING"}), "+PONG");
    };
    expect_arity_error({"SISMEMBER", "s"}, "sismember");
    expect_arity_error({"SSCAN", "s"}, "sscan");
    expect_arity_error({"HGET", "h"}, "hget");
    expect_arity_error({"HSCAN", "h"}, "hscan");
    expect_arity_error({"LINDEX", "list"}, "lindex");
    expect_arity_error({"HSETNX", "h"}, "hsetnx");
    expect_arity_error({"SMISMEMBER", "s"}, "smismember");
    expect_arity_error({"HDEL", "h"}, "hdel");
    expect_arity_error({"SADD", "s"}, "sadd");
    expect_arity_error({"SPOP", "s", "1", "2"}, "spop");
    expect_arity_error({"SRANDMEMBER", "s", "1", "2"}, "srandmember");

    EXPECT_EQ(client.Command({"LPUSHX", "missing", "x"}), ":0");
    EXPECT_EQ(client.Command({"RPUSHX", "missing", "x"}), ":0");
    EXPECT_EQ(client.Command({"RPUSH", "list", "a", "b", "c", "b", "d"}), ":5");
    EXPECT_EQ(client.Command({"LPUSH", "list", "z", "y"}), ":7");
    EXPECT_EQ(client.Command({"LRANGE", "list", "0", "-1"}),
              BulkArray({"y", "z", "a", "b", "c", "b", "d"}));
    EXPECT_EQ(client.Command({"LINDEX", "list", "-2"}), Bulk("b"));
    EXPECT_EQ(client.Command({"LSET", "list", "2", "A"}), "+OK");
    EXPECT_EQ(client.Command({"LINSERT", "list", "BEFORE", "c", "before"}),
              ":8");
    EXPECT_EQ(client.Command({"LINSERT", "list", "AFTER", "absent", "x"}),
              ":-1");
    EXPECT_EQ(client.Command({"LPOS", "list", "b"}), ":3");
    EXPECT_EQ(client.Command({"LPOS", "list", "b", "RANK", "-1"}), ":6");
    EXPECT_EQ(client.Command({"LPOS", "list", "b", "COUNT", "0"}),
              "*2\r\n:3\r\n:6");
    EXPECT_EQ(client.Command({"LREM", "list", "-1", "b"}), ":1");
    EXPECT_EQ(client.Command({"RPUSH", "min-remove-compact", "target", "keep",
                              "target", "target"}),
              ":4");
    EXPECT_EQ(client.Command({"LREM", "min-remove-compact",
                              "-9223372036854775808", "target"}),
              ":3");
    EXPECT_EQ(client.Command({"LRANGE", "min-remove-compact", "0", "-1"}),
              BulkArray({"keep"}));
    EXPECT_EQ(client.Command({"LTRIM", "list", "1", "-2"}), "+OK");
    EXPECT_EQ(client.Command({"LLEN", "list"}), ":5");
    EXPECT_EQ(client.Command({"LPOP", "list", "2"}), BulkArray({"z", "A"}));
    EXPECT_EQ(client.Command({"RPOP", "list"}), Bulk("c"));
    EXPECT_EQ(client.Command({"LRANGE", "list", "0", "-1"}),
              BulkArray({"b", "before"}));

    EXPECT_EQ(client.Command({"RPUSH", "source", "s1", "s2"}), ":2");
    EXPECT_EQ(client.Command({"RPUSH", "destination", "d1"}), ":1");
    EXPECT_EQ(
        client.Command({"LMOVE", "source", "destination", "RIGHT", "LEFT"}),
        Bulk("s2"));
    EXPECT_EQ(client.Command({"RPOPLPUSH", "source", "destination"}),
              Bulk("s1"));
    EXPECT_EQ(client.Command({"LRANGE", "destination", "0", "-1"}),
              BulkArray({"s1", "s2", "d1"}));
    EXPECT_EQ(client.Command({"RPUSH", "mpop-two", "m1", "m2", "m3"}), ":3");
    EXPECT_EQ(client.Command({"LMPOP", "2", "mpop-one", "mpop-two", "RIGHT",
                              "COUNT", "2"}),
              "*2\r\n" + Bulk("mpop-two") + "\r\n" + BulkArray({"m3", "m2"}));

    EXPECT_EQ(client.Command({"RPUSH", "blocking", "v1", "v2"}), ":2");
    EXPECT_EQ(client.Command({"BLPOP", "blocking", "0.1"}),
              "*2\r\n" + Bulk("blocking") + "\r\n" + Bulk("v1"));
    EXPECT_EQ(client.Command({"BRPOP", "blocking", "0.1"}),
              "*2\r\n" + Bulk("blocking") + "\r\n" + Bulk("v2"));
    const auto timeout_started = std::chrono::steady_clock::now();
    EXPECT_EQ(client.Command({"BLPOP", "blocking-missing", "0.01"}), "*-1");
    EXPECT_GE(std::chrono::steady_clock::now() - timeout_started, 8ms);
    EXPECT_EQ(client.Command({"BLPOP", "blocking-missing", "-1"}),
              "-ERR timeout is negative");
    // This value rounds to 2^63 nanoseconds in double precision. It must be
    // rejected before integer conversion/time_point addition rather than
    // overflowing into an immediate timeout.
    EXPECT_EQ(
        client.Command({"BLPOP", "blocking-missing", "9223372036.854776"}),
        "-ERR timeout is out of range");
    EXPECT_EQ(client.Command({"BLPOP", "blocking-missing", "0x7FFFFFFFFFFFFF"}),
              "-ERR timeout is out of range");
    auto blocked = std::async(std::launch::async, [port] {
      RespClient waiting(port);
      return waiting.Command({"BLPOP", "blocking-wakeup", "1"});
    });
    std::this_thread::sleep_for(20ms);
    EXPECT_EQ(client.Command({"RPUSH", "blocking-wakeup", "awake"}), ":1");
    EXPECT_EQ(blocked.get(),
              "*2\r\n" + Bulk("blocking-wakeup") + "\r\n" + Bulk("awake"));

    auto first_waiter = std::async(std::launch::async, [port] {
      RespClient waiting(port);
      return waiting.Command({"BLPOP", "blocking-fifo", "2"});
    });
    std::this_thread::sleep_for(20ms);
    auto second_waiter = std::async(std::launch::async, [port] {
      RespClient waiting(port);
      return waiting.Command({"BLPOP", "blocking-fifo", "2"});
    });
    std::this_thread::sleep_for(20ms);
    EXPECT_EQ(client.Command({"RPUSH", "blocking-fifo", "first"}), ":1");
    EXPECT_EQ(first_waiter.get(),
              "*2\r\n" + Bulk("blocking-fifo") + "\r\n" + Bulk("first"));
    EXPECT_EQ(second_waiter.wait_for(20ms), std::future_status::timeout);
    EXPECT_EQ(client.Command({"RPUSH", "blocking-fifo", "second"}), ":1");
    EXPECT_EQ(second_waiter.get(),
              "*2\r\n" + Bulk("blocking-fifo") + "\r\n" + Bulk("second"));

    auto multi_waiter = std::async(std::launch::async, [port] {
      RespClient waiting(port);
      return waiting.Command({"BLMPOP", "2", "2", "blocking-multi-a",
                              "blocking-multi-b", "LEFT", "COUNT", "2"});
    });
    std::this_thread::sleep_for(20ms);
    EXPECT_EQ(
        client.Command({"RPUSH", "blocking-multi-b", "multi-1", "multi-2"}),
        ":2");
    EXPECT_EQ(multi_waiter.get(), "*2\r\n" + Bulk("blocking-multi-b") + "\r\n" +
                                      BulkArray({"multi-1", "multi-2"}));

    auto move_waiter = std::async(std::launch::async, [port] {
      RespClient waiting(port);
      return waiting.Command({"BLMOVE", "blocking-move-source",
                              "blocking-move-destination", "RIGHT", "LEFT",
                              "2"});
    });
    std::this_thread::sleep_for(20ms);
    EXPECT_EQ(client.Command({"RPUSH", "blocking-move-source", "move-wakeup"}),
              ":1");
    EXPECT_EQ(move_waiter.get(), Bulk("move-wakeup"));
    EXPECT_EQ(
        client.Command({"LRANGE", "blocking-move-destination", "0", "-1"}),
        BulkArray({"move-wakeup"}));

    auto unrelated_move_waiter = std::async(std::launch::async, [port] {
      RespClient waiting(port);
      return waiting.Command({"BLMOVE", "unrelated-move-source",
                              "shared-blocking-destination", "RIGHT", "LEFT",
                              "0.2"});
    });
    std::this_thread::sleep_for(20ms);
    auto destination_waiter = std::async(std::launch::async, [port] {
      RespClient waiting(port);
      return waiting.Command({"BLPOP", "shared-blocking-destination", "1"});
    });
    std::this_thread::sleep_for(20ms);
    EXPECT_EQ(client.Command({"RPUSH", "shared-blocking-destination",
                              "destination-value"}),
              ":1");
    EXPECT_EQ(destination_waiter.get(),
              "*2\r\n" + Bulk("shared-blocking-destination") + "\r\n" +
                  Bulk("destination-value"));
    EXPECT_EQ(unrelated_move_waiter.get(), "$-1");

    EXPECT_EQ(client.Command({"RPUSH", "blmove-source", "a", "b"}), ":2");
    EXPECT_EQ(client.Command({"BLMOVE", "blmove-source", "blmove-dest", "RIGHT",
                              "LEFT", "0.1"}),
              Bulk("b"));
    EXPECT_EQ(client.Command({"LRANGE", "blmove-dest", "0", "-1"}),
              BulkArray({"b"}));
    EXPECT_EQ(client.Command({"RPUSH", "brpoplpush-source", "moved"}), ":1");
    EXPECT_EQ(client.Command({"BRPOPLPUSH", "brpoplpush-source",
                              "brpoplpush-dest", "0.1"}),
              Bulk("moved"));
    EXPECT_EQ(client.Command({"RPUSH", "blmpop-two", "p1", "p2"}), ":2");
    EXPECT_EQ(client.Command({"BLMPOP", "0.1", "2", "blmpop-one", "blmpop-two",
                              "LEFT", "COUNT", "2"}),
              "*2\r\n" + Bulk("blmpop-two") + "\r\n" + BulkArray({"p1", "p2"}));
    EXPECT_EQ(client.Command({"BLMPOP", "0", "3", "only-one", "LEFT"}),
              "-ERR syntax error");
    EXPECT_EQ(client.Command(
                  {"BLMPOP", "0", "9223372036854775807", "only-one", "LEFT"}),
              "-ERR syntax error");
    EXPECT_EQ(client.Command({"BLMOVE", "missing-source", "missing-dest",
                              "LEFT", "RIGHT", "0.01"}),
              "$-1");

    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"RPUSH", "tx-list", "1", "2"}), "+QUEUED");
    EXPECT_EQ(client.Command({"LPUSH", "tx-list", "0"}), "+QUEUED");
    EXPECT_EQ(client.Command({"LPOP", "tx-list"}), "+QUEUED");
    EXPECT_EQ(client.Command({"LRANGE", "tx-list", "0", "-1"}), "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*4\r\n:2\r\n:3\r\n" + Bulk("0") +
                                            "\r\n" + BulkArray({"1", "2"}));

    EXPECT_EQ(client.Command({"RPUSH", "tx-mpop", "a", "b", "c"}), ":3");
    EXPECT_EQ(client.Command({"LMPOP", "1", "tx-mpop", "LEFT", "COUNT", "0"}),
              "-ERR count should be greater than 0");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"LMPOP", "2", "tx-missing", "tx-mpop", "LEFT",
                              "COUNT", "2"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*1\r\n*2\r\n" + Bulk("tx-mpop") +
                                            "\r\n" + BulkArray({"a", "b"}));
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(
        client.Command({"BLMPOP", "1", "1", "tx-mpop", "RIGHT", "COUNT", "1"}),
        "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}),
              "*1\r\n*2\r\n" + Bulk("tx-mpop") + "\r\n" + BulkArray({"c"}));
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(
        client.Command({"LMPOP", "1", "tx-mpop", "LEFT", "COUNT", "1", "junk"}),
        "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*1\r\n-ERR syntax error");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"BLPOP", "tx-mpop", "abc"}), "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}),
              "*1\r\n-ERR timeout is not a float or out of range");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"BLMPOP", "abc", "1", "tx-mpop", "LEFT"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}),
              "*1\r\n-ERR timeout is not a float or out of range");

    EXPECT_EQ(client.Command({"RPUSH", "tx-lmove-source", "a", "b"}), ":2");
    EXPECT_EQ(client.Command({"RPUSH", "tx-lmove-dest", "tail"}), ":1");
    EXPECT_EQ(client.Command({"RPUSH", "tx-rpop-source", "x", "y"}), ":2");
    EXPECT_EQ(client.Command({"RPUSH", "tx-blmove-source", "m", "n"}), ":2");
    EXPECT_EQ(client.Command({"RPUSH", "tx-brpop-source", "p", "q"}), ":2");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"LMOVE", "tx-lmove-source", "tx-lmove-dest",
                              "RIGHT", "LEFT"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"RPOPLPUSH", "tx-rpop-source", "tx-rpop-dest"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"BLMOVE", "tx-blmove-source", "tx-blmove-dest",
                              "LEFT", "RIGHT", "10"}),
              "+QUEUED");
    EXPECT_EQ(client.Command(
                  {"BRPOPLPUSH", "tx-brpop-source", "tx-brpop-dest", "10"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*4\r\n" + Bulk("b") + "\r\n" +
                                            Bulk("y") + "\r\n" + Bulk("m") +
                                            "\r\n" + Bulk("q"));
    EXPECT_EQ(client.Command({"LRANGE", "tx-lmove-dest", "0", "-1"}),
              BulkArray({"b", "tail"}));
    EXPECT_EQ(client.Command({"LRANGE", "tx-rpop-dest", "0", "-1"}),
              BulkArray({"y"}));
    EXPECT_EQ(client.Command({"LRANGE", "tx-blmove-dest", "0", "-1"}),
              BulkArray({"m"}));
    EXPECT_EQ(client.Command({"LRANGE", "tx-brpop-dest", "0", "-1"}),
              BulkArray({"q"}));

    const std::string cross_move_source =
        KeyForWorker("tx-cross-move-source", 0, 4);
    const std::string cross_move_destination =
        KeyForWorker("tx-cross-move-destination", 1, 4);
    EXPECT_EQ(client.Command({"RPUSH", cross_move_source, "cross"}), ":1");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"LMOVE", cross_move_source,
                              cross_move_destination, "RIGHT", "LEFT"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*1\r\n" + Bulk("cross"));
    EXPECT_EQ(client.Command({"LRANGE", cross_move_destination, "0", "-1"}),
              BulkArray({"cross"}));

    const std::string removable(100 * 1024, 'r');
    for (int i = 0; i < 18; ++i) {
      EXPECT_TRUE(client
                      .Command({"RPUSH", "merge-list", removable,
                                "separator-" + std::to_string(i)})
                      .starts_with(':'));
    }
    EXPECT_EQ(client.Command({"LSET", "merge-list", "1", "extreme-remove"}),
              "+OK");
    EXPECT_EQ(client.Command({"LSET", "merge-list", "35", "extreme-remove"}),
              "+OK");
    EXPECT_EQ(client.Command({"LREM", "merge-list", "-9223372036854775808",
                              "extreme-remove"}),
              ":2");
    EXPECT_EQ(client.Command({"LREM", "merge-list", "0", removable}), ":18");
    EXPECT_EQ(client.Command({"LLEN", "merge-list"}), ":16");
    EXPECT_EQ(client.Command({"LPOS", "merge-list", removable}), "$-1");

    for (std::size_t begin = 0; begin < large_values.size(); begin += 100) {
      std::vector<std::string_view> large_push{"RPUSH", "large-list"};
      const std::size_t end = std::min(large_values.size(), begin + 100);
      for (std::size_t i = begin; i < end; ++i) {
        large_push.push_back(large_values[i]);
      }
      EXPECT_EQ(client.Command(large_push), ":" + std::to_string(end));
    }
    // Search and mutation operate on the complete decoded List value.
    EXPECT_EQ(client.Command({"LINSERT", "large-list", "BEFORE",
                              large_values[777], "streamed-insert"}),
              ":1201");
    EXPECT_EQ(client.Command({"LPOS", "large-list", "streamed-insert"}),
              ":777");
    EXPECT_EQ(client.Command({"LREM", "large-list", "1", "streamed-insert"}),
              ":1");

    // Exercise the same List with committed transactional writes and ordinary
    // writes racing on disjoint elements. EXEC holds the List key for
    // both of its mutations, while each ordinary LSET must serialize either
    // before or after the complete transaction. The final values are
    // deterministic even though the physical records are interleaved.
    auto transaction_writer = std::async(std::launch::async, [port] {
      try {
        RespClient tx_client(port);
        for (int round = 0; round < 16; ++round) {
          const std::string first = "tx-a-" + std::to_string(round);
          const std::string second = "tx-b-" + std::to_string(round);
          if (tx_client.Command({"MULTI"}) != "+OK" ||
              tx_client.Command({"LSET", "large-list", "300", first}) !=
                  "+QUEUED" ||
              tx_client.Command({"LSET", "large-list", "301", second}) !=
                  "+QUEUED" ||
              tx_client.Command({"EXEC"}) != "*2\r\n+OK\r\n+OK") {
            return std::string("unexpected transactional List reply");
          }
          std::this_thread::yield();
        }
        return std::string{};
      } catch (const std::exception& error) {
        return std::string(error.what());
      }
    });
    auto ordinary_writer = std::async(std::launch::async, [port] {
      try {
        RespClient plain_client(port);
        for (int round = 0; round < 16; ++round) {
          const std::string first = "plain-a-" + std::to_string(round);
          const std::string second = "plain-b-" + std::to_string(round);
          if (plain_client.Command({"LSET", "large-list", "900", first}) !=
                  "+OK" ||
              plain_client.Command({"LSET", "large-list", "901", second}) !=
                  "+OK") {
            return std::string("unexpected ordinary List reply");
          }
          std::this_thread::yield();
        }
        return std::string{};
      } catch (const std::exception& error) {
        return std::string(error.what());
      }
    });
    EXPECT_EQ(transaction_writer.get(), "");
    EXPECT_EQ(ordinary_writer.get(), "");
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "300"}), Bulk("tx-a-15"));
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "301"}), Bulk("tx-b-15"));
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "900"}),
              Bulk("plain-a-15"));
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "901"}),
              Bulk("plain-b-15"));

    // Pin both possible winner orders independently of scheduler timing: a
    // committed transaction supersedes an ordinary root at index 302, while
    // an ordinary root supersedes a committed transaction at index 902.
    EXPECT_EQ(client.Command({"LSET", "large-list", "302", "plain-old"}),
              "+OK");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"LSET", "large-list", "302", "tx-new"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*1\r\n+OK");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"LSET", "large-list", "902", "tx-old"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*1\r\n+OK");
    EXPECT_EQ(client.Command({"LSET", "large-list", "902", "plain-new"}),
              "+OK");

    EXPECT_EQ(client.Command({"LLEN", "large-list"}), ":1200");
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "0"}),
              Bulk(large_values.front()));
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "-1"}),
              Bulk(large_values.back()));
    EXPECT_EQ(client.Command({"PEXPIRE", "large-list", "600000"}), ":1");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"LSET", "large-list", "80", "tx-segment"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"LSET", "large-list", "700", "tx-segment-2"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "80"}), "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}),
              "*3\r\n+OK\r\n+OK\r\n" + Bulk("tx-segment"));
    server.Stop();
  }

  {
    ServerProcess server(g_lavik_binary, port, data_path, log_path, 3);
    RespClient client(port);
    EXPECT_EQ(client.Command({"LLEN", "merge-list"}), ":16");
    EXPECT_EQ(
        client.Command({"LPOS", "merge-list", std::string(100 * 1024, 'r')}),
        "$-1");
    EXPECT_EQ(client.Command({"LLEN", "large-list"}), ":1200");
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "79"}),
              Bulk(large_values[79]));
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "80"}),
              Bulk("tx-segment"));
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "700"}),
              Bulk("tx-segment-2"));
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "300"}), Bulk("tx-a-15"));
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "301"}), Bulk("tx-b-15"));
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "900"}),
              Bulk("plain-a-15"));
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "901"}),
              Bulk("plain-b-15"));
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "302"}), Bulk("tx-new"));
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "902"}),
              Bulk("plain-new"));
    const std::string recovered_ttl = client.Command({"PTTL", "large-list"});
    ASSERT_TRUE(recovered_ttl.starts_with(':'));
    EXPECT_GT(std::stoll(recovered_ttl.substr(1)), 0);
    EXPECT_EQ(client.Command({"LRANGE", "tx-list", "0", "-1"}),
              BulkArray({"1", "2"}));
    EXPECT_EQ(client.Command({"LSET", "large-list", "80", "recovered"}), "+OK");
    EXPECT_EQ(client.Command({"LREM", "large-list", "1", large_values[10]}),
              ":1");
    EXPECT_EQ(client.Command({"LLEN", "large-list"}), ":1199");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"EVAL",
                              "redis.call('SET',KEYS[1],ARGV[1]); "
                              "redis.call('SET',KEYS[2],ARGV[2]); return 'OK'",
                              "2", lua_recovery_a, lua_recovery_b,
                              "recovered-a", "recovered-b"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"SET", "lua-exec-recovery-tail", "tail"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*2\r\n" + Bulk("OK") + "\r\n+OK");
    ASSERT_TRUE(WaitForDurability(client));
    server.Kill();
  }

  {
    ServerProcess server(g_lavik_binary, port, data_path, log_path, 2);
    RespClient client(port);
    EXPECT_EQ(client.Command({"LLEN", "large-list"}), ":1199");
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "79"}),
              Bulk("recovered"));
    EXPECT_EQ(client.Command({"LPOS", "large-list", large_values[10]}), "$-1");
    EXPECT_EQ(client.Command({"MGET", lua_recovery_a, lua_recovery_b,
                              "lua-exec-recovery-tail"}),
              "*3\r\n" + Bulk("recovered-a") + "\r\n" + Bulk("recovered-b") +
                  "\r\n" + Bulk("tail"));
    server.Stop();
  }

  {
    ServerProcess server(g_lavik_binary, port, data_path, log_path, 2);
    RespClient client(port);
    EXPECT_EQ(client.Command({"LTRIM", "large-list", "0", "9"}), "+OK");
    EXPECT_EQ(client.Command({"LLEN", "large-list"}), ":10");
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "0"}),
              Bulk(large_values.front()));
    server.Stop();
  }

  {
    ServerProcess server(g_lavik_binary, port, data_path, log_path, 3);
    RespClient client(port);
    EXPECT_EQ(client.Command({"LLEN", "large-list"}), ":10");
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "-1"}),
              Bulk(large_values[9]));
    EXPECT_EQ(client.Command({"DEL", "large-list"}), ":1");
    EXPECT_EQ(client.Command({"LLEN", "large-list"}), ":0");
    server.Stop();
  }

  {
    ServerProcess server(g_lavik_binary, port, data_path, log_path, 2);
    RespClient client(port);
    EXPECT_EQ(client.Command({"LLEN", "large-list"}), ":0");
    server.Stop();
  }
}

TEST(ListE2eTest, SortsCollectionsAndStoresResultsAtomically) {
  ASSERT_FALSE(g_lavik_binary.empty());
  const std::string prefix = lavik::test::TestDataPathPrefix() +
                             "lavik-sort-e2e-" + std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::posix_fallocate(fd, 0, 256ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(fd), 0);

  const std::uint16_t port = FindFreePort();
  ServerProcess server(g_lavik_binary, port, data_path, log_path, 3);
  RespClient client(port);

  EXPECT_EQ(client.Command({"RPUSH", "numbers", "3", "10", "2", "1"}), ":4");
  EXPECT_EQ(client.Command({"SORT", "numbers"}),
            BulkArray({"1", "2", "3", "10"}));
  EXPECT_EQ(
      client.Command({"SORT", "numbers", "ALPHA", "DESC", "LIMIT", "1", "2"}),
      BulkArray({"2", "10"}));
  EXPECT_EQ(client.Command({"SORT_RO", "numbers", "DESC"}),
            BulkArray({"10", "3", "2", "1"}));
  EXPECT_EQ(client.Command({"SORT_RO", "numbers", "STORE", "forbidden"}),
            "-ERR syntax error");
  EXPECT_EQ(client.Command({"SORT", "numbers", "DESC", "STORE", "numbers"}),
            ":4");
  EXPECT_EQ(client.Command({"LRANGE", "numbers", "0", "-1"}),
            BulkArray({"10", "3", "2", "1"}));

  EXPECT_EQ(client.Command({"ZADD", "ranked", "1", "a", "5", "b", "2", "c",
                            "10", "d", "3", "e"}),
            ":5");
  EXPECT_EQ(client.Command({"SORT", "ranked", "BY", "nosort", "ASC"}),
            BulkArray({"a", "c", "e", "b", "d"}));
  EXPECT_EQ(client.Command({"SORT", "ranked", "BY", "nosort", "DESC"}),
            BulkArray({"d", "b", "e", "c", "a"}));
  EXPECT_EQ(client.Command({"MULTI"}), "+OK");
  EXPECT_EQ(client.Command({"SORT", "ranked", "BY", "nosort", "ASC"}),
            "+QUEUED");
  EXPECT_EQ(client.Command({"SORT", "ranked", "BY", "nosort", "DESC"}),
            "+QUEUED");
  EXPECT_EQ(client.Command({"EXEC"}),
            "*2\r\n" + BulkArray({"a", "c", "e", "b", "d"}) + "\r\n" +
                BulkArray({"d", "b", "e", "c", "a"}));

  EXPECT_EQ(client.Command({"RPUSH", "ids", "a", "b", "c"}), ":3");
  EXPECT_EQ(client.Command({"MSET", "weight_a", "2", "weight_b", "1",
                            "weight_c", "3", "label_a", "A", "label_b", "B"}),
            "+OK");
  EXPECT_EQ(client.Command({"SORT", "ids", "BY", "weight_*", "GET", "#", "GET",
                            "label_*"}),
            "*6\r\n" + Bulk("b") + "\r\n" + Bulk("B") + "\r\n" + Bulk("a") +
                "\r\n" + Bulk("A") + "\r\n" + Bulk("c") + "\r\n$-1");
  EXPECT_EQ(
      client.Command({"HSET", "object_a", "weight", "20", "label", "hash-a"}),
      ":2");
  EXPECT_EQ(
      client.Command({"HSET", "object_b", "weight", "10", "label", "hash-b"}),
      ":2");
  EXPECT_EQ(client.Command({"SORT", "ids", "BY", "object_*->weight", "GET", "#",
                            "GET", "object_*->label"}),
            "*6\r\n" + Bulk("c") + "\r\n$-1\r\n" + Bulk("b") + "\r\n" +
                Bulk("hash-b") + "\r\n" + Bulk("a") + "\r\n" + Bulk("hash-a"));

  EXPECT_EQ(
      client.Command({"SORT", "ids", "BY", "weight_*", "STORE", "stored"}),
      ":3");
  EXPECT_EQ(client.Command({"LRANGE", "stored", "0", "-1"}),
            BulkArray({"b", "a", "c"}));
  EXPECT_EQ(client.Command({"MULTI"}), "+OK");
  EXPECT_EQ(
      client.Command({"SORT", "ids", "BY", "nosort", "STORE", "exec-stored"}),
      "+QUEUED");
  EXPECT_EQ(client.Command({"EXEC"}), "*1\r\n:3");
  EXPECT_EQ(client.Command({"LRANGE", "exec-stored", "0", "-1"}),
            BulkArray({"a", "b", "c"}));
  EXPECT_EQ(client.Command({"SORT", "missing", "STORE", "stored"}), ":0");
  EXPECT_EQ(client.Command({"EXISTS", "stored"}), ":0");

  EXPECT_EQ(client.Command({"SET", "wrong-type", "value"}), "+OK");
  EXPECT_EQ(client.Command({"SORT", "wrong-type"}),
            "-WRONGTYPE Operation against a key holding the wrong kind of "
            "value");
  EXPECT_EQ(client.Command({"RPUSH", "bad-number", "1", "not-a-double"}), ":2");
  EXPECT_EQ(client.Command({"SORT", "bad-number"}),
            "-ERR One or more scores can't be converted into double");

  auto blocked = std::async(std::launch::async, [port] {
    RespClient waiter(port);
    return waiter.Command({"BLPOP", "sort-wakeup", "5"});
  });
  std::this_thread::sleep_for(100ms);
  EXPECT_EQ(client.Command({"SORT", "numbers", "STORE", "sort-wakeup"}), ":4");
  ASSERT_EQ(blocked.wait_for(2s), std::future_status::ready);
  EXPECT_EQ(blocked.get(), "*2\r\n" + Bulk("sort-wakeup") + "\r\n" + Bulk("1"));
  EXPECT_EQ(client.Command({"LRANGE", "sort-wakeup", "0", "-1"}),
            BulkArray({"2", "3", "10"}));
  server.Stop();
}

TEST(ListE2eTest, NativeFlowCapabilityRejectsSessionHijack) {
  ASSERT_FALSE(g_lavik_binary.empty());
  const std::string prefix = lavik::test::TestDataPathPrefix() +
                             "lavik-replication-flow-capability-e2e-" +
                             std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int data_fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(data_fd, 0);
  ASSERT_EQ(::posix_fallocate(data_fd, 0, 128ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(data_fd), 0);

  const std::uint16_t port = FindFreePort();
  ServerProcess server(g_lavik_binary, port, data_path, log_path, 1);
  RespClient client(port);
  const auto ready_deadline = std::chrono::steady_clock::now() + 30s;
  while (std::chrono::steady_clock::now() < ready_deadline &&
         client.Command({"SET", "flow-capability-ready", "1"}) != "+OK") {
    std::this_thread::sleep_for(10ms);
  }
  ASSERT_EQ(client.Command({"GET", "flow-capability-ready"}), Bulk("1"));

  const int control = ConnectSocket(port);
  ASSERT_GE(control, 0);
  const std::string target_identity = "?" + std::string(40, 'a') + ":12345";
  SendAll(control,
          EncodeCommand({"LVPSYNC", "1", target_identity, "?", "?",
                         std::string(40, 'b'), std::string(40, 'c'), "?"}));
  const std::string resync = ReadRespLine(control);
  const std::string_view resync_view(resync);
  std::vector<std::string_view> words;
  for (std::size_t begin = 0; begin < resync_view.size();) {
    const std::size_t end = resync_view.find(' ', begin);
    words.push_back(resync_view.substr(begin, end == std::string::npos
                                                  ? resync_view.size() - begin
                                                  : end - begin));
    if (end == std::string::npos) break;
    begin = end + 1;
  }
  ASSERT_EQ(words.size(), 8U) << resync;
  ASSERT_EQ(words[0], "+LVFULLRESYNC");
  ASSERT_EQ(words[6], "1");
  ASSERT_EQ(words[7].size(), 40U);
  const std::string session_id(words[1]);
  const std::string capability(words[7]);
  std::string wrong_capability = capability;
  wrong_capability.front() = wrong_capability.front() == '0' ? '1' : '0';

  const int hijack = ConnectSocket(port);
  ASSERT_GE(hijack, 0);
  SendAll(hijack, EncodeCommand({"LVFLOW", "1", session_id, "0", "1", "0",
                                 wrong_capability}));
  EXPECT_THROW((void)ReadRespLine(hijack), std::runtime_error);
  ASSERT_EQ(::close(hijack), 0);

  const int authorized = ConnectSocket(port);
  ASSERT_GE(authorized, 0);
  SendAll(authorized, EncodeCommand({"LVFLOW", "1", session_id, "0", "1", "0",
                                     capability}));
  EXPECT_EQ(ReadRespLine(authorized), "+LVFLOW " + session_id + " 0 FULL");
  ASSERT_EQ(::close(authorized), 0);
  ASSERT_EQ(::close(control), 0);

  // Keep a source control handshake alive without opening its flow. Graceful
  // shutdown must cancel and join this handler rather than checkpointing while
  // it can still enable source history (or waiting for its stall timeout).
  const int shutdown_control = ConnectSocket(port);
  ASSERT_GE(shutdown_control, 0);
  SendAll(shutdown_control,
          EncodeCommand({"LVPSYNC", "1", target_identity, "?", "?",
                         std::string(40, 'd'), std::string(40, 'e'), "?"}));
  ASSERT_TRUE(ReadRespLine(shutdown_control).starts_with("+LVFULLRESYNC "));
  server.Stop();
  ASSERT_EQ(::close(shutdown_control), 0);
}

TEST(ListE2eTest, HandshakeDuringIdleHistoryRetirementKeepsMonitorAlive) {
#if !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires a fault-enabled server";
#endif
  ASSERT_FALSE(g_lavik_binary.empty());
  const std::string prefix = lavik::test::TestDataPathPrefix() +
                             "lavik-idle-history-handshake-e2e-" +
                             std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path), log_cleanup(log_path);
  const int fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::posix_fallocate(fd, 0, 256ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(fd), 0);

  const std::uint16_t port = FindFreePort();
  ServerProcess source(
      g_lavik_binary, port, data_path, log_path, 2, {}, {},
      {{"LAVIK_REPLICATION_PAUSE_IDLE_HISTORY_UNTIL_CONTROL_ONCE", "1"}});
  RespClient source_client(port);
  {
    RespClient probe(port);
    ASSERT_TRUE(probe.Command({"LVPSYNC", "1", "?", "?", "?", "?", "?", "?"})
                    .starts_with("+LVFULLRESYNC "));
  }
  // Hold the old monitor after its idle decision, where disabling the worker
  // logs may yield. The next control must wait for that reset and still leave
  // a monitor responsible for the history it subsequently enables.
  ASSERT_TRUE(WaitForLog(
      log_path, "paused idle history retirement until next native control"));
  std::string history;
  {
    RespClient probe(port);
    const std::string reply =
        probe.Command({"LVPSYNC", "1", "?", "?", "?", "?", "?", "?"});
    ASSERT_TRUE(reply.starts_with("+LVFULLRESYNC ")) << reply;
    std::istringstream fields(reply);
    for (unsigned field = 0; field <= 5; ++field) {
      ASSERT_TRUE(static_cast<bool>(fields >> history)) << reply;
    }
    ASSERT_EQ(history.size(), 40U) << reply;
  }
  const std::string retained_history = "master_replid:" + history + "\r\n";
  const auto deadline = std::chrono::steady_clock::now() + 30s;
  std::string info;
  do {
    info = source_client.Command({"INFO", "replication"});
    if (info.find(retained_history) == std::string::npos) break;
    std::this_thread::sleep_for(10ms);
  } while (std::chrono::steady_clock::now() < deadline);
  ASSERT_NE(info.find("master_replid:"), std::string::npos) << info;
  EXPECT_EQ(info.find(retained_history), std::string::npos) << info;
  source.Stop();
}

TEST(ListE2eTest, MaxClientsRejectsBeforeTlsAndUpdatesAtRuntime) {
  ASSERT_FALSE(g_lavik_binary.empty());
  const std::string prefix = lavik::test::TestDataPathPrefix() +
                             "lavik-maxclients-e2e-" +
                             std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int data_fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(data_fd, 0);
  ASSERT_EQ(::posix_fallocate(data_fd, 0, 256ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(data_fd), 0);

  const std::string tls_dir = std::string(LAVIK_SOURCE_DIR) + "/tests/tls";
  const std::uint16_t port = FindFreePort();
  std::uint16_t tls_port = FindFreePort();
  while (tls_port == port) tls_port = FindFreePort();
  std::uint16_t metrics_port = FindFreePort();
  while (metrics_port == port || metrics_port == tls_port) {
    metrics_port = FindFreePort();
  }
  ServerProcess server(
      g_lavik_binary, port, data_path, log_path, 2, {},
      {"--maxclients", "3", "--tls-port", std::to_string(tls_port),
       "--metrics-port", std::to_string(metrics_port), "--tls-cert-file",
       tls_dir + "/server.crt", "--tls-key-file", tls_dir + "/server.key"},
      {}, {}, 258);

  auto first = std::make_unique<RespClient>(port);
  EXPECT_EQ(first->Command({"CONFIG", "GET", "maxclients"}),
            BulkArray({"maxclients", "2"}));
  EXPECT_NE(first->Command({"INFO", "clients"}).find("maxclients:2\r\n"),
            std::string::npos);
  EXPECT_EQ(first->Command({"CONFIG", "SET", "maxclients", "1"}), "+OK");
  EXPECT_EQ(first->Command({"CONFIG", "GET", "maxclients"}),
            BulkArray({"maxclients", "1"}));

  EXPECT_EQ(first->Command({"CONFIG", "GET", "client-query-buffer-limit"}),
            BulkArray({"client-query-buffer-limit", "1073741824"}));
  EXPECT_EQ(
      first->Command({"CONFIG", "SET", "client-query-buffer-limit", "2gb"}),
      "+OK");
  EXPECT_EQ(first->Command({"CONFIG", "GET", "client-query-buffer-limit"}),
            BulkArray({"client-query-buffer-limit", "2147483648"}));
  EXPECT_EQ(
      first->Command({"CONFIG", "SET", "client-query-buffer-limit", "2mb"}),
      "+OK");
  EXPECT_EQ(first->Command({"CONFIG", "GET", "client-query-buffer-limit"}),
            BulkArray({"client-query-buffer-limit", "2097152"}));
  EXPECT_EQ(
      first->Command({"CONFIG", "SET", "client-query-buffer-limit", "512kb"}),
      "-ERR client-query-buffer-limit must be between 1mb and LONG_MAX "
      "bytes");
  EXPECT_EQ(first->Command({"CONFIG", "GET", "client-query-buffer-limit"}),
            BulkArray({"client-query-buffer-limit", "2097152"}));
  EXPECT_NE(first->Command({"INFO", "clients"}).find("maxclients:1\r\n"),
            std::string::npos);
  EXPECT_EQ(first->Command({"CONFIG", "SET", "maxclients", "0"}),
            "-ERR value is not a positive integer or is out of range");
  const std::string over_file_limit =
      first->Command({"CONFIG", "SET", "maxclients", "3"});
  EXPECT_NE(over_file_limit.find("cannot preserve 256 file descriptors"),
            std::string::npos);
  EXPECT_EQ(first->Command({"CONFIG", "GET", "maxclients"}),
            BulkArray({"maxclients", "1"}));

  // Metrics is a separate HTTP service and remains available while the only
  // Redis client slot is occupied.
  EXPECT_TRUE(HttpGet(metrics_port, "/metrics").starts_with("HTTP/1.1 200"));

  const int rejected_plain = ConnectSocket(port);
  ASSERT_GE(rejected_plain, 0);
  std::string plaintext_reply;
  char buffer[128];
  while (true) {
    const ssize_t received = ::recv(rejected_plain, buffer, sizeof(buffer), 0);
    if (received > 0) {
      plaintext_reply.append(buffer, static_cast<std::size_t>(received));
      continue;
    }
    if (received < 0 && errno == EINTR) continue;
    ASSERT_TRUE(received == 0 || errno == ECONNRESET) << std::strerror(errno);
    break;
  }
  EXPECT_EQ(plaintext_reply, "-ERR max number of clients reached\r\n");
  ASSERT_EQ(::close(rejected_plain), 0);

  // The TLS endpoint shares the same admission counter. It closes a rejected
  // socket without emitting plaintext or allocating handshake state.
  const int rejected_tls = ConnectSocket(tls_port);
  ASSERT_GE(rejected_tls, 0);
  const ssize_t tls_received = ::recv(rejected_tls, buffer, sizeof(buffer), 0);
  EXPECT_TRUE(tls_received == 0 || (tls_received < 0 && errno == ECONNRESET))
      << "TLS rejection returned " << tls_received << " bytes: "
      << std::string(buffer, tls_received > 0
                                 ? static_cast<std::size_t>(tls_received)
                                 : 0);
  ASSERT_EQ(::close(rejected_tls), 0);

  EXPECT_EQ(first->Command({"CONFIG", "SET", "maxclients", "2"}), "+OK");
  auto second = std::make_unique<RespClient>(port);
  EXPECT_EQ(second->Command({"PING"}), "+PONG");
  EXPECT_EQ(first->Command({"CONFIG", "SET", "maxclients", "1"}), "+OK");

  const int rejected_after_lowering = ConnectSocket(port);
  ASSERT_GE(rejected_after_lowering, 0);
  EXPECT_EQ(ReadRespLine(rejected_after_lowering),
            "-ERR max number of clients reached");
  ASSERT_EQ(::close(rejected_after_lowering), 0);

  // Lowering the limit never evicts existing clients. Once both close, the
  // one remaining slot is reusable.
  second.reset();
  first.reset();
  RespClient replacement(port);
  EXPECT_EQ(replacement.Command({"PING"}), "+PONG");
  server.Stop();
}

TEST(ListE2eTest, RedisPsyncFullSyncActivatesBeforeOnlineWrites) {
  ASSERT_FALSE(g_lavik_binary.empty());
  const std::string prefix = lavik::test::TestDataPathPrefix() +
                             "lavik-redis-fullsync-activation-e2e-" +
                             std::to_string(::getpid());
  const std::string replica_data = prefix + "-replica.data";
  const std::string replica_log = prefix + "-replica.log";
  FileCleanup replica_cleanup(replica_data);
  FileCleanup replica_log_cleanup(replica_log);
  const int fd =
      ::open(replica_data.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::posix_fallocate(fd, 0, 128ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(fd), 0);

  constexpr std::string_view baseline_library =
      "#!lua name=redis_baseline\n"
      "redis.register_function{function_name='redis_baseline_value', "
      "callback=function(keys, args) return args[1] end, "
      "flags={'no-writes'}}";
  lavik::rdb::StreamEncoder encoder(10);
  std::string rdb(encoder.Header());
  lavik::storage::RawValue baseline_value{
      .encoded_ = "snapshot-value",
      .logical_size_ = 14,
      .value_type_ = lavik::storage::ValueType::kString,
  };
  auto baseline_key_fragment =
      lavik::rdb::EncodeFileEntry(0, "redis-snapshot", baseline_value);
  ASSERT_TRUE(baseline_key_fragment.ok()) << baseline_key_fragment.status();
  encoder.Account(*baseline_key_fragment);
  rdb += *baseline_key_fragment;
  const std::string baseline_fragment =
      lavik::rdb::EncodeFunctionLibraryEntry(baseline_library);
  encoder.Account(baseline_fragment);
  rdb += baseline_fragment;
  rdb += encoder.Finish();

  constexpr std::string_view transaction_library =
      "#!lua name=redis_transaction\n"
      "redis.register_function{function_name='redis_transaction_value', "
      "callback=function(keys, args) return args[1] end, "
      "flags={'no-writes'}}";
  std::string command_stream;
  command_stream += EncodeCommand({"SET", "redis-online", "delta"});
  command_stream += EncodeCommand({"MULTI"});
  command_stream += EncodeCommand({"FUNCTION", "LOAD", transaction_library});
  command_stream +=
      EncodeCommand({"SET", "redis-transaction-key", "transaction-value"});
  command_stream += EncodeCommand({"EXEC"});
  RedisPsyncSource source(std::move(rdb), std::move(command_stream));

  std::uint16_t replica_port = FindFreePort();
  while (replica_port == source.port()) replica_port = FindFreePort();
  ServerProcess replica(
      g_lavik_binary, replica_port, replica_data, replica_log, 1, {},
      {"--redis-replicaof", "127.0.0.1", std::to_string(source.port())});
  RespClient replica_client(replica_port);

  const auto online_deadline = std::chrono::steady_clock::now() + 30s;
  std::string info;
  do {
    info = replica_client.Command({"INFO", "replication"});
    if (info.find("lavik_replication_state:online") != std::string::npos) {
      break;
    }
    std::this_thread::sleep_for(10ms);
  } while (std::chrono::steady_clock::now() < online_deadline);
  ASSERT_NE(info.find("lavik_replication_state:online"), std::string::npos);
  EXPECT_EQ(replica_client.Command({"GET", "redis-snapshot"}),
            Bulk("snapshot-value"));
  EXPECT_EQ(replica_client.Command(
                {"FCALL_RO", "redis_baseline_value", "0", "snapshot"}),
            Bulk("snapshot"));
  EXPECT_TRUE(WaitForEventualReply(replica_client, {"GET", "redis-online"},
                                   Bulk("delta")));
  EXPECT_TRUE(WaitForEventualReply(
      replica_client,
      {"FCALL_RO", "redis_transaction_value", "0", "replicated"},
      Bulk("replicated")));
  EXPECT_EQ(replica_client.Command({"GET", "redis-transaction-key"}),
            Bulk("transaction-value"));

  replica.Stop();
}

// Real storage/process coverage of large Hash extents and crash recovery.
// Group promotion is automatic; the dedicated grouped suites additionally
// exercise split/merge, root-decision and group-relocation fault boundaries.
class LargeHashDurabilityE2eTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_FALSE(g_lavik_binary.empty());
    const std::string prefix =
        lavik::test::TestDataPathPrefix() + "lavik-large-hash-durability-" +
        std::to_string(::getpid()) + "-" +
        ::testing::UnitTest::GetInstance()->current_test_info()->name();
    data_path_ = prefix + ".data";
    log_path_ = prefix + ".log";
    const int fd =
        ::open(data_path_.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    ASSERT_GE(fd, 0);
    data_cleanup_.emplace(data_path_);
    const int allocated = ::posix_fallocate(fd, 0, 128ULL * 1024 * 1024);
    ASSERT_EQ(::close(fd), 0);
    ASSERT_EQ(allocated, 0);
    const int log_fd = ::open(log_path_.c_str(),
                              O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    ASSERT_GE(log_fd, 0);
    log_cleanup_.emplace(log_path_);
    ASSERT_EQ(::close(log_fd), 0);
    port_ = FindFreePort();
    for (unsigned i = 0; i < 9; ++i)
      fields_.push_back("field-" + std::to_string(i));
  }

  void TearDown() override {
    if (HasFailure()) {
      std::ifstream log(log_path_);
      const std::string bytes((std::istreambuf_iterator<char>(log)), {});
      ADD_FAILURE() << "Server log tail:\n"
                    << bytes.substr(bytes.size() > 12000 ? bytes.size() - 12000
                                                         : 0);
    }
  }

  std::vector<std::string_view> WriteCommand(std::string_view value) const {
    std::vector<std::string_view> command{"HSET", kKey};
    for (const auto& field : fields_) {
      command.push_back(field);
      command.push_back(value);
    }
    command.push_back("version");
    command.push_back(value.substr(0, 1));
    return command;
  }

  void Verify(RespClient& client, std::string_view value) const {
    ASSERT_EQ(client.Command({"HLEN", kKey}),
              ":" + std::to_string(fields_.size() + 1));
    EXPECT_EQ(client.Command({"HGET", kKey, "version"}),
              Bulk(value.substr(0, 1)));
    for (const auto& field : fields_) {
      // Check every byte and field, not only HLEN or an aggregate checksum.
      EXPECT_EQ(client.Command({"HGET", kKey, field}), Bulk(value)) << field;
    }
  }

  static constexpr std::string_view kKey = "{large-hash-gc}:hash";
  void CheckGcCrash(std::string_view point);
  void CheckOom(bool injected_storage_failure);
  std::string data_path_;
  std::string log_path_;
  std::optional<FileCleanup> data_cleanup_;
  std::optional<FileCleanup> log_cleanup_;
  std::uint16_t port_ = 0;
  std::vector<std::string> fields_;
};

TEST_F(LargeHashDurabilityE2eTest, CrashDuringExtentWriteKeepsOldHashAndTtl) {
#if defined(NDEBUG) && !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires Debug or the explicitly test-instrumented module";
#else
  // One field must itself exceed an inline group. Nine medium fields now
  // split into independent pages and no longer exercise extent writes.
  fields_.resize(1);
  const std::string old_value(9 * 1024 * 1024, 'a');
  const std::string new_value(9 * 1024 * 1024, 'z');
  {
    ServerProcess server(g_lavik_binary, port_, data_path_, log_path_, 2);
    RespClient client(port_);
    ASSERT_EQ(client.Command(WriteCommand(old_value)), ":2");
    ASSERT_EQ(client.Command({"PEXPIRE", kKey, "3600000"}), ":1");
    ASSERT_TRUE(WaitForDurability(client));
    server.Kill();
  }
  for (const std::string_view point :
       {"extent-first-part-durable", "group-extents-durable-before-record"}) {
    SCOPED_TRACE(point);
    {
      ServerProcess server(g_lavik_binary, port_, data_path_, log_path_, 2,
                           point);
      RespClient ready(port_);
      auto command = WriteCommand(new_value);
      command.insert(command.end(), {"new-field", "must-not-appear"});
      const int fd = ConnectSocket(port_);
      ASSERT_GE(fd, 0);
      SendAll(fd, EncodeCommand(command));
      server.WaitForCrash();  // Must exit at the injection point with code 86.
      ASSERT_EQ(::close(fd), 0);
    }
    {
      // Change worker topology too; no runtime directory can survive exec.
      ServerProcess server(g_lavik_binary, port_, data_path_, log_path_, 3);
      RespClient client(port_);
      Verify(client, old_value);
      EXPECT_EQ(client.Command({"HEXISTS", kKey, "new-field"}), ":0");
      const auto ttl = client.Command({"PTTL", kKey});
      ASSERT_TRUE(ttl.starts_with(':'));
      EXPECT_GT(std::stoll(ttl.substr(1)), 0);
      ASSERT_TRUE(WaitForDurability(client));
      server.Kill();
    }
  }
  // The interrupted allocations must not accumulate into permanent disk
  // exhaustion. A new complete replacement still fits this bounded device.
  ServerProcess server(g_lavik_binary, port_, data_path_, log_path_, 2);
  RespClient client(port_);
  ASSERT_EQ(client.Command(WriteCommand(new_value)), ":0");
  ASSERT_TRUE(WaitForDurability(client));
  Verify(client, new_value);
  server.Stop();
#endif
}

TEST_F(LargeHashDurabilityE2eTest,
       RepeatedExtentReplacementReclaimsBoundedStorage) {
  std::string value(1024 * 1024, 'a');
  {
    ServerProcess server(g_lavik_binary, port_, data_path_, log_path_, 2);
    RespClient client(port_);
    // 20 complete 9 MiB rewrites exceed this 128 MiB device. Succeeding
    // requires retirement/reuse, not just retaining every old extent.
    for (unsigned round = 0; round < 20; ++round) {
      SCOPED_TRACE(round);
      value.assign(value.size(), 'a' + round);
      ASSERT_EQ(client.Command(WriteCommand(value)), round == 0 ? ":10" : ":0");
      ASSERT_TRUE(WaitForDurability(client));
      EXPECT_EQ(client.Command({"HGET", kKey, "version"}),
                Bulk(value.substr(0, 1)));
    }
    Verify(client, value);
    ASSERT_EQ(client.Command({"DEL", kKey}), ":1");
    ASSERT_TRUE(WaitForDurability(client));
    ASSERT_EQ(client.Command({"HSET", kKey, "only-new-incarnation", "fresh"}),
              ":1");
    ASSERT_TRUE(WaitForDurability(client));
    server.Kill();
  }
  ServerProcess server(g_lavik_binary, port_, data_path_, log_path_, 3);
  RespClient client(port_);
  EXPECT_EQ(client.Command({"HLEN", kKey}), ":1");
  EXPECT_EQ(client.Command({"HGET", kKey, "only-new-incarnation"}),
            Bulk("fresh"));
  EXPECT_EQ(client.Command({"HEXISTS", kKey, "field-0"}), ":0");
  server.Stop();
}

void LargeHashDurabilityE2eTest::CheckGcCrash(std::string_view point) {
#if defined(NDEBUG) && !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires Debug or the explicitly test-instrumented module";
#else
  const std::string value(1024 * 1024, 'g');
  std::vector<std::string> fillers;
  for (unsigned i = 0; i < 5; ++i)
    fillers.push_back("{large-hash-gc}:filler-" + std::to_string(i));
  {
    ServerProcess server(g_lavik_binary, port_, data_path_, log_path_, 2);
    RespClient client(port_);
    ASSERT_EQ(client.Command(WriteCommand(value)), ":10");
    // Same hash slot puts the root beside filler payloads. Group children
    // belong to transaction generations and are relocated by the tx cleaner.
    for (const auto& key : fillers)
      ASSERT_EQ(client.Command({"SET", key, value}), "+OK");
    ASSERT_TRUE(WaitForDurability(client));
    server.Kill();
  }
  {
    ServerProcess server(g_lavik_binary, port_, data_path_, log_path_, 2,
                         point);
    RespClient ready(port_);
    if (point == "hash-group-defrag-copy-staged") {
      // This case explicitly tests GC, not default pressure scheduling. Arm a
      // short cleaner cadence so an auxiliary page is actually relocated.
      ASSERT_EQ(
          ready.Command({"CONFIG", "SET", "tx-cleaner-cooldown-ms", "20"}),
          "+OK");
    }
    std::vector<std::string_view> command{"DEL"};
    for (const auto& key : fillers) command.push_back(key);
    const int fd = ConnectSocket(port_);
    ASSERT_GE(fd, 0);
    SendAll(fd, EncodeCommand(command));
    server.WaitForCrash();
    ASSERT_EQ(::close(fd), 0);
  }
  ServerProcess server(g_lavik_binary, port_, data_path_, log_path_, 3);
  RespClient client(port_);
  Verify(client, value);
  server.Stop();
#endif
}

TEST_F(LargeHashDurabilityE2eTest, GcSourceRetirementCrashPreservesExtentHash) {
  CheckGcCrash("defrag-source-retired");
}

TEST_F(LargeHashDurabilityE2eTest,
       GcStagedHashCopyCrashKeepsSourceRecoverable) {
  CheckGcCrash("hash-group-defrag-copy-staged");
}

void LargeHashDurabilityE2eTest::CheckOom(bool injected_storage_failure) {
#if defined(NDEBUG) && !LAVIK_TEST_FAULTS_AVAILABLE
  if (injected_storage_failure)
    GTEST_SKIP() << "storage admission injection requires test faults";
#endif
  const std::string value(1024 * 1024, 'o');
  {
    ServerProcess server(g_lavik_binary, port_, data_path_, log_path_, 2);
    RespClient client(port_);
    ASSERT_EQ(client.Command(WriteCommand(value)), ":10");
    ASSERT_TRUE(WaitForDurability(client));
    server.Kill();
  }
  {
    const std::vector<std::string> arguments =
        injected_storage_failure
            ? std::vector<std::string>{}
            : std::vector<std::string>{"--max-memory", "64M",
                                       "--maxmemory-clients", "64M"};
    const std::vector<std::pair<std::string, std::string>> environment =
        injected_storage_failure
            ? std::vector<std::pair<
                  std::string, std::string>>{{"LAVIK_FAIL_HASH_ADMISSION_KEY",
                                              std::string(kKey)}}
            : std::vector<std::pair<std::string, std::string>>{};
    ServerProcess server(g_lavik_binary, port_, data_path_, log_path_, 2, {},
                         arguments, environment);
    RespClient client(port_);
    // Startup needs enough memory to reconstruct the durable side index.
    // In the real-limit variant a 12 MiB request fits the separate 32 MiB
    // per-worker client budget, but its group-write scratch cannot fit the
    // worker's storage headroom. Full reads of the nine 1 MiB values also
    // exceed that headroom. The injected variant stays small.
    const std::string replacement =
        injected_storage_failure ? "wrong" : std::string(12 * 1024 * 1024, 'w');
    EXPECT_TRUE(
        client.Command({"HSET", kKey, "version", "wrong", "new", replacement})
            .starts_with("-OOM "));
    EXPECT_TRUE(client.Command({"HMSET", kKey, "version", replacement})
                    .starts_with("-OOM "));
    EXPECT_TRUE(client.Command({"HSETNX", kKey, "new", replacement})
                    .starts_with("-OOM "));
    if (!injected_storage_failure) {
      // Full reads also need admitted decoded/output ownership. Failure must
      // keep the stored Hash intact, including when successful reads transfer
      // strings instead of copying them into their reply result.
      for (const auto command : {"HGETALL", "HKEYS", "HVALS"}) {
        const auto reply = client.Command({command, kKey});
        EXPECT_TRUE(reply.starts_with("-OOM "))
            << command << ": " << reply.substr(0, 120);
      }
    }
    if (injected_storage_failure) {
      ASSERT_EQ(client.Command({"MULTI"}), "+OK");
      ASSERT_EQ(client.Command({"HSET", kKey, "version", "wrong"}), "+QUEUED");
      ASSERT_EQ(client.Command({"HGET", kKey, "version"}), "+QUEUED");
      const auto result = client.Command({"EXEC"});
      EXPECT_TRUE(result.starts_with("*2\r\n-OOM ")) << result;
      EXPECT_TRUE(result.ends_with(Bulk("o"))) << result;
    }
    EXPECT_EQ(client.Command({"HLEN", kKey}), ":10");
    if (injected_storage_failure) {
      EXPECT_EQ(client.Command({"HGET", kKey, "version"}), Bulk("o"));
      EXPECT_EQ(client.Command({"HEXISTS", kKey, "new"}), ":0");
    }
    EXPECT_EQ(client.Command({"PING"}), "+PONG");
    server.Kill();
  }
  ServerProcess server(g_lavik_binary, port_, data_path_, log_path_, 3);
  RespClient client(port_);
  Verify(client, value);
  EXPECT_EQ(client.Command({"HEXISTS", kKey, "new"}), ":0");
  server.Stop();
}

TEST_F(LargeHashDurabilityE2eTest,
       RedisOomReplyDoesNotPartiallyUpdateLargeHash) {
  CheckOom(false);
}

TEST_F(LargeHashDurabilityE2eTest, StorageOomKeepsRedisErrorClassAndOldValue) {
  CheckOom(true);
}

TEST_F(LargeHashDurabilityE2eTest,
       SingleOversizedFieldSurvivesNeighborUpdatesAndRecovery) {
  // Unlike nine medium fields exceeding one block together, this entry alone
  // cannot be made inline by splitting along field-hash boundaries.
  const std::string huge(9 * 1024 * 1024, 'v');
  {
    ServerProcess server(g_lavik_binary, port_, data_path_, log_path_, 2);
    RespClient client(port_);
    ASSERT_EQ(client.Command({"HSET", kKey, "huge", huge, "small", "before"}),
              ":2");
    ASSERT_TRUE(WaitForDurability(client));
    ASSERT_EQ(client.Command({"HSET", kKey, "small", "after"}), ":0");
    ASSERT_TRUE(WaitForDurability(client));
    ASSERT_EQ(client.Command({"HINCRBY", kKey, "counter", "5"}), ":5");
    ASSERT_TRUE(WaitForDurability(client));
    ASSERT_EQ(client.Command({"HSETNX", kKey, "huge", "wrong"}), ":0");
    ASSERT_EQ(client.Command({"HLEN", kKey}), ":3");
    server.Kill();
  }
  {
    ServerProcess server(g_lavik_binary, port_, data_path_, log_path_, 3);
    RespClient client(port_);
    ASSERT_EQ(client.Command({"HGET", kKey, "huge"}), Bulk(huge));
    ASSERT_EQ(client.Command({"HGET", kKey, "small"}), Bulk("after"));
    ASSERT_EQ(client.Command({"HGET", kKey, "counter"}), Bulk("5"));
    ASSERT_EQ(client.Command({"HDEL", kKey, "huge"}), ":1");
    ASSERT_TRUE(WaitForDurability(client));
    ASSERT_EQ(client.Command({"HLEN", kKey}), ":2");
    ASSERT_EQ(client.Command({"HGET", kKey, "small"}), Bulk("after"));
    server.Kill();
  }
  {
    ServerProcess server(g_lavik_binary, port_, data_path_, log_path_, 2);
    RespClient client(port_);
    ASSERT_EQ(client.Command({"HLEN", kKey}), ":2");
    ASSERT_EQ(client.Command({"HGET", kKey, "huge"}), "$-1");
    ASSERT_EQ(client.Command({"HGET", kKey, "counter"}), Bulk("5"));
    server.Stop();
  }
}

TEST(HashE2eTest, UpdatesTransactionsAndRecoversMonolithicValues) {
  ASSERT_FALSE(g_lavik_binary.empty());
  const std::string prefix = lavik::test::TestDataPathPrefix() +
                             "lavik-hash-e2e-" + std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::posix_fallocate(fd, 0, 256ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(fd), 0);

  constexpr std::size_t kFields = 180;
  std::vector<std::string> fields;
  std::vector<std::string> values;
  fields.reserve(kFields);
  values.reserve(kFields);
  for (std::size_t i = 0; i < kFields; ++i) {
    fields.push_back("field-" + std::to_string(i));
    values.push_back(std::string(4096, static_cast<char>('a' + i % 23)) + "-" +
                     std::to_string(i));
  }

  const std::uint16_t port = FindFreePort();
  {
    ServerProcess server(g_lavik_binary, port, data_path, log_path, 2);
    RespClient client(port);
    std::vector<std::string_view> hset{"HSET", "large-hash"};
    for (std::size_t i = 0; i < kFields; ++i) {
      hset.push_back(fields[i]);
      hset.push_back(values[i]);
    }
    EXPECT_EQ(client.Command(hset), ":180");
    EXPECT_EQ(client.Command({"HLEN", "large-hash"}), ":180");
    EXPECT_EQ(client.Command({"HGET", "large-hash", fields[0]}),
              Bulk(values[0]));
    EXPECT_EQ(client.Command({"HGET", "large-hash", fields[179]}),
              Bulk(values[179]));
    EXPECT_EQ(client.Command({"HINCRBY", "numeric-hash", "counter", "-2"}),
              ":-2");
    EXPECT_EQ(client.Command({"HINCRBY", "numeric-hash", "counter", "5"}),
              ":3");
    EXPECT_EQ(
        client.Command({"HINCRBYFLOAT", "numeric-hash", "counter", "1.5"}),
        Bulk("4.5"));
    EXPECT_EQ(client.Command({"HSET", "empty-field-hash", "", "value"}), ":1");
    auto [empty_hash_cursor, empty_hash_scan] = ParseScanReply(
        client.Command({"HSCAN", "empty-field-hash", "0", "COUNT", "100"}));
    EXPECT_EQ(empty_hash_cursor, "0");
    EXPECT_EQ(empty_hash_scan, (std::vector<std::string>{"", "value"}));
    EXPECT_EQ(client.Command({"HSET", "numeric-hash", "plus-counter", "+1.5"}),
              ":1");
    EXPECT_EQ(client.Command(
                  {"HINCRBYFLOAT", "numeric-hash", "plus-counter", "+0.5"}),
              Bulk("2"));
    // COUNT is a hint: grouped storage can return one routing page per call.
    std::string hash_cursor = "0";
    std::unordered_set<std::string> scanned_fields;
    unsigned hash_scan_calls = 0;
    do {
      auto [next, entries] = ParseScanReply(client.Command(
          {"HSCAN", "large-hash", hash_cursor, "COUNT", "1000"}));
      ASSERT_EQ(entries.size() % 2, 0u);
      for (std::size_t i = 0; i < entries.size(); i += 2) {
        const auto found = std::find(fields.begin(), fields.end(), entries[i]);
        ASSERT_NE(found, fields.end());
        EXPECT_EQ(entries[i + 1], values[found - fields.begin()]);
        scanned_fields.insert(entries[i]);
      }
      hash_cursor = std::move(next);
      ASSERT_LT(++hash_scan_calls, 2000u);
    } while (hash_cursor != "0");
    EXPECT_EQ(scanned_fields.size(), fields.size());
    const std::string random =
        client.Command({"HRANDFIELD", "large-hash", "10", "WITHVALUES"});
    EXPECT_TRUE(random.starts_with("*20\r\n"));
    const std::string repeated_random =
        client.Command({"HRANDFIELD", "large-hash", "-1000"});
    EXPECT_TRUE(repeated_random.starts_with("*1000\r\n"));
    EXPECT_EQ(
        client.Command({"HRANDFIELD", "large-hash", "-9223372036854775808"}),
        "-ERR value is out of range");
    EXPECT_EQ(client.Command({"HRANDFIELD", "large-hash",
                              "-4611686018427387904", "WITHVALUES"}),
              "-ERR value is out of range");
    EXPECT_EQ(
        client.Command({"HRANDFIELD", "missing-hash", "-9223372036854775807"}),
        "*0");
    EXPECT_TRUE(
        client.Command({"HRANDFIELD", "numeric-hash", "-2001", "WITHVALUES"})
            .starts_with("*4002\r\n"));
    CloseStreamingReplyAfterHeader(
        port, {"HRANDFIELD", "numeric-hash", "-9223372036854775807"},
        "*9223372036854775807");
    EXPECT_TRUE(WaitForReply(client, {"HLEN", "large-hash"}, ":180"));
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(
        client.Command({"HRANDFIELD", "numeric-hash", "-2001", "WITHVALUES"}),
        "+QUEUED");
    EXPECT_EQ(client.Command({"SET", "after-random-oom", "alive"}), "+QUEUED");
    const std::string hash_exec_random = client.Command({"EXEC"});
    EXPECT_TRUE(hash_exec_random.starts_with("*2\r\n*4002\r\n"));
    EXPECT_TRUE(hash_exec_random.ends_with("+OK"));
    EXPECT_EQ(client.Command({"GET", "after-random-oom"}), Bulk("alive"));
    EXPECT_EQ(
        client.Command({"HSET", "large-hash", fields[79], "ordinary-update"}),
        ":0");
    EXPECT_EQ(client.Command({"HDEL", "large-hash", fields[20], fields[21]}),
              ":2");
    EXPECT_EQ(client.Command({"HSETNX", "large-hash", fields[79], "no"}), ":0");
    EXPECT_EQ(client.Command({"HSETNX", "large-hash", "new-field", "new"}),
              ":1");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"HSET", "large-hash", fields[80], "tx-update"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"HDEL", "large-hash", fields[22]}), "+QUEUED");
    EXPECT_EQ(client.Command({"HGET", "large-hash", fields[80]}), "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}),
              "*3\r\n:0\r\n:1\r\n" + Bulk("tx-update"));
    EXPECT_EQ(
        client.Command({"HSET", "large-hash", fields[81], "ordinary-after-tx"}),
        ":0");
    EXPECT_EQ(client.Command({"PEXPIRE", "large-hash", "600000"}), ":1");
    ASSERT_TRUE(WaitForDurability(client));
    server.Kill();
  }

  {
    ServerProcess server(g_lavik_binary, port, data_path, log_path, 3);
    RespClient client(port);
    EXPECT_EQ(client.Command({"HLEN", "large-hash"}), ":178");
    EXPECT_EQ(client.Command({"HGET", "large-hash", fields[20]}), "$-1");
    EXPECT_EQ(client.Command({"HGET", "large-hash", fields[79]}),
              Bulk("ordinary-update"));
    EXPECT_EQ(client.Command({"HGET", "large-hash", fields[80]}),
              Bulk("tx-update"));
    EXPECT_EQ(client.Command({"HGET", "large-hash", fields[81]}),
              Bulk("ordinary-after-tx"));
    EXPECT_EQ(client.Command({"HGET", "large-hash", "new-field"}), Bulk("new"));
    EXPECT_EQ(client.Command({"HGET", "numeric-hash", "counter"}), Bulk("4.5"));
    const std::string ttl = client.Command({"PTTL", "large-hash"});
    ASSERT_TRUE(ttl.starts_with(':'));
    EXPECT_GT(std::stoll(ttl.substr(1)), 0);
    EXPECT_EQ(client.Command({"SET", "large-hash", "string-now"}), "+OK");
    EXPECT_EQ(client.Command({"GET", "large-hash"}), Bulk("string-now"));
    ASSERT_TRUE(WaitForDurability(client));
    server.Kill();
  }

  {
    ServerProcess server(g_lavik_binary, port, data_path, log_path, 2);
    RespClient client(port);
    EXPECT_EQ(client.Command({"GET", "large-hash"}), Bulk("string-now"));
    EXPECT_TRUE(client.Command({"HGET", "large-hash", fields[0]})
                    .starts_with("-WRONGTYPE"));
    server.Stop();
  }
}

TEST(SetE2eTest, ScanCursorDoesNotSkipAfterEarlierMembersAreDeleted) {
  ASSERT_FALSE(g_lavik_binary.empty());
  const std::string prefix = lavik::test::TestDataPathPrefix() +
                             "lavik-set-scan-stable-" +
                             std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::posix_fallocate(fd, 0, 128ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(fd), 0);

  std::vector<std::string> members;
  members.reserve(1000);
  std::vector<std::string_view> sadd{"SADD", "scan-set"};
  sadd.reserve(1002);
  for (int i = 0; i < 1000; ++i) {
    members.push_back("scan-member-" + std::to_string(i));
    sadd.push_back(members.back());
  }
  const std::uint16_t port = FindFreePort();
  ServerProcess server(g_lavik_binary, port, data_path, log_path, 2);
  RespClient client(port);
  ASSERT_EQ(client.Command(sadd), ":1000");

  std::unordered_set<std::string> seen;
  std::string cursor = "0";
  bool deleted_first_batch = false;
  std::size_t calls = 0;
  do {
    auto [next, batch] = ParseScanReply(
        client.Command({"SSCAN", "scan-set", cursor, "COUNT", "10"}));
    for (const std::string& member : batch) seen.insert(member);
    if (!deleted_first_batch && next != "0") {
      std::vector<std::string_view> srem{"SREM", "scan-set"};
      for (const std::string& member : batch) srem.push_back(member);
      ASSERT_EQ(client.Command(srem), ":" + std::to_string(batch.size()));
      deleted_first_batch = true;
    }
    cursor = std::move(next);
    ASSERT_LT(++calls, 2000u);
  } while (cursor != "0");
  EXPECT_TRUE(deleted_first_batch);
  EXPECT_EQ(seen.size(), members.size());
  for (const std::string& member : members) EXPECT_TRUE(seen.contains(member));
  server.Stop();
}

TEST(SetE2eTest, Redis72CommandsTransactionsAndRecovery) {
  ASSERT_FALSE(g_lavik_binary.empty());
  const std::string prefix = lavik::test::TestDataPathPrefix() +
                             "lavik-set-e2e-" + std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::posix_fallocate(fd, 0, 512ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(fd), 0);

  constexpr std::size_t kMembers = 180;
  std::vector<std::string> members;
  members.reserve(kMembers);
  for (std::size_t i = 0; i < kMembers; ++i) {
    members.push_back("member-" + std::to_string(i) + "-" +
                      std::string(4096, static_cast<char>('a' + i % 23)));
  }

  const std::uint16_t port = FindFreePort();
  {
    ServerProcess server(g_lavik_binary, port, data_path, log_path, 4);
    RespClient client(port);
    EXPECT_EQ(client.Command({"SADD", "basic", "a", "b", "a", "c"}), ":3");
    EXPECT_EQ(client.Command({"SCARD", "basic"}), ":3");
    EXPECT_EQ(client.Command({"SISMEMBER", "basic", "b"}), ":1");
    EXPECT_EQ(client.Command({"SMISMEMBER", "basic", "a", "z", "c"}),
              "*3\r\n:1\r\n:0\r\n:1");
    EXPECT_EQ(client.Command({"SREM", "basic", "b", "b", "missing"}), ":1");
    EXPECT_EQ(client.Command({"SPOP", "basic", "0"}), "*0");
    EXPECT_EQ(client.Command({"SADD", "empty-member-set", ""}), ":1");
    auto [empty_set_cursor, empty_set_scan] = ParseScanReply(
        client.Command({"SSCAN", "empty-member-set", "0", "COUNT", "100"}));
    EXPECT_EQ(empty_set_cursor, "0");
    EXPECT_EQ(empty_set_scan, (std::vector<std::string>{""}));
    EXPECT_EQ(client.Command({"SRANDMEMBER", "missing", "3"}), "*0");
    EXPECT_EQ(client.Command({"SRANDMEMBER", "basic", "-9223372036854775808"}),
              "-ERR value is out of range");
    EXPECT_EQ(
        client.Command({"SRANDMEMBER", "missing", "-9223372036854775807"}),
        "*0");
    EXPECT_TRUE(client.Command({"SRANDMEMBER", "basic", "-2001"})
                    .starts_with("*2001\r\n"));
    CloseStreamingReplyAfterHeader(
        port, {"SRANDMEMBER", "basic", "-9223372036854775807"},
        "*9223372036854775807");
    EXPECT_TRUE(WaitForReply(client, {"SCARD", "basic"}, ":2"));
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"SRANDMEMBER", "basic", "-1001"}), "+QUEUED");
    EXPECT_TRUE(client.Command({"EXEC"}).starts_with("*1\r\n*1001\r\n"));
    CloseExecStreamingReplyAfterHeader(
        port, {"SRANDMEMBER", "basic", "-9223372036854775807"},
        "*9223372036854775807");
    EXPECT_TRUE(WaitForReply(client, {"SCARD", "basic"}, ":2"));
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"SADD", "tx-random-view", "only"}), "+QUEUED");
    EXPECT_EQ(client.Command({"SRANDMEMBER", "tx-random-view", "-1001"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"DEL", "tx-random-view"}), "+QUEUED");
    const std::string serial_random = client.Command({"EXEC"});
    EXPECT_TRUE(serial_random.starts_with("*3\r\n:1\r\n*1001\r\n$4\r\nonly"));
    EXPECT_TRUE(serial_random.ends_with(":1"));
    EXPECT_EQ(client.Command({"SET", "not-set", "value"}), "+OK");
    EXPECT_TRUE(
        client.Command({"SMEMBERS", "not-set"}).starts_with("-WRONGTYPE"));
    EXPECT_TRUE(client.Command({"SMOVE", "basic", "not-set", "missing"})
                    .starts_with("-WRONGTYPE"));
    EXPECT_EQ(client.Command({"SMOVE", "missing-source", "not-set", "member"}),
              ":0");
    EXPECT_TRUE(client.Command({"SMOVE", "basic", "not-set", "c"})
                    .starts_with("-WRONGTYPE"));
    EXPECT_EQ(client.Command({"SMOVE", "basic", "basic", "missing"}), ":0");
    EXPECT_EQ(client.Command({"SMOVE", "basic", "basic", "a"}), ":1");
    EXPECT_TRUE(client.Command({"SINTERCARD", "0", "basic"})
                    .starts_with("-ERR numkeys"));
    EXPECT_TRUE(client.Command({"SINTERCARD", "2", "basic"})
                    .starts_with("-ERR Number of keys"));
    EXPECT_TRUE(client.Command({"SINTERCARD", "1", "basic", "LIMIT", "-1"})
                    .starts_with("-ERR LIMIT"));

    EXPECT_EQ(client.Command({"SADD", "left", "a", "b", "c", "d"}), ":4");
    EXPECT_EQ(client.Command({"SADD", "right", "c", "d", "e"}), ":3");
    EXPECT_EQ(
        client.Command({"SINTERCARD", "2", "left", "right", "LiMiT", "1"}),
        ":1");
    EXPECT_EQ(client.Command({"SINTERSTORE", "intersection", "left", "right"}),
              ":2");
    EXPECT_EQ(client.Command({"SCARD", "intersection"}), ":2");
    EXPECT_EQ(client.Command({"SMOVE", "left", "right", "a"}), ":1");
    EXPECT_EQ(client.Command({"SISMEMBER", "left", "a"}), ":0");
    EXPECT_EQ(client.Command({"SISMEMBER", "right", "a"}), ":1");

    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"SMOVE", "missing-source", "not-set", "member"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*1\r\n:0");

    std::vector<std::string_view> sadd{"SADD", "large-set"};
    for (const std::string& member : members) sadd.push_back(member);
    EXPECT_EQ(client.Command(sadd), ":180");
    EXPECT_EQ(client.Command({"SCARD", "large-set"}), ":180");
    EXPECT_EQ(client.Command({"SISMEMBER", "large-set", members[0]}), ":1");
    EXPECT_EQ(client.Command({"SISMEMBER", "large-set", members[179]}), ":1");
    std::string set_cursor = "0";
    std::unordered_set<std::string> scanned_members;
    unsigned set_scan_calls = 0;
    do {
      auto [next, entries] = ParseScanReply(
          client.Command({"SSCAN", "large-set", set_cursor, "mAtCh",
                          "member-[0-2]-*", "cOuNt", "1000"}));
      scanned_members.insert(entries.begin(), entries.end());
      set_cursor = std::move(next);
      ASSERT_LT(++set_scan_calls, 2000u);
    } while (set_cursor != "0");
    EXPECT_EQ(scanned_members, (std::unordered_set<std::string>{
                                   members[0], members[1], members[2]}));
    EXPECT_TRUE(client.Command({"SRANDMEMBER", "large-set", "10"})
                    .starts_with("*10\r\n"));

    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"SADD", "large-set", "tx-member"}), "+QUEUED");
    EXPECT_EQ(client.Command({"SREM", "large-set", members[20]}), "+QUEUED");
    EXPECT_EQ(
        client.Command({"SINTERSTORE", "tx-intersection", "left", "right"}),
        "+QUEUED");
    EXPECT_EQ(client.Command({"SMOVE", "right", "left", "e"}), "+QUEUED");
    EXPECT_EQ(client.Command({"SCARD", "large-set"}), "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*5\r\n:1\r\n:1\r\n:2\r\n:1\r\n:180");
    EXPECT_EQ(client.Command({"SADD", "large-set", "ordinary-after-tx"}), ":1");
    EXPECT_EQ(client.Command({"SADD", "watched-set", "member"}), ":1");
    EXPECT_EQ(client.Command({"WATCH", "watched-set"}), "+OK");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"SCARD", "watched-set"}), "+QUEUED");
    {
      RespClient concurrent(port);
      EXPECT_EQ(concurrent.Command({"SADD", "watched-set", "member"}), ":0");
    }
    EXPECT_EQ(client.Command({"EXEC"}), "*1\r\n:1");
    EXPECT_EQ(client.Command({"PEXPIRE", "large-set", "600000"}), ":1");
    ASSERT_TRUE(WaitForDurability(client));
    server.Kill();
  }

  {
    ServerProcess server(g_lavik_binary, port, data_path, log_path, 3);
    RespClient client(port);
    EXPECT_EQ(client.Command({"SCARD", "large-set"}), ":181");
    EXPECT_EQ(client.Command({"SISMEMBER", "large-set", members[20]}), ":0");
    EXPECT_EQ(client.Command({"SISMEMBER", "large-set", "tx-member"}), ":1");
    EXPECT_EQ(client.Command({"SISMEMBER", "large-set", "ordinary-after-tx"}),
              ":1");
    EXPECT_EQ(client.Command({"SCARD", "tx-intersection"}), ":2");
    const std::string ttl = client.Command({"PTTL", "large-set"});
    ASSERT_TRUE(ttl.starts_with(':'));
    EXPECT_GT(std::stoll(ttl.substr(1)), 0);
    EXPECT_EQ(client.Command({"SET", "large-set", "string-now"}), "+OK");
    EXPECT_EQ(client.Command({"GET", "large-set"}), Bulk("string-now"));
    ASSERT_TRUE(WaitForDurability(client));
    server.Kill();
  }

  {
    ServerProcess server(g_lavik_binary, port, data_path, log_path, 2);
    RespClient client(port);
    EXPECT_EQ(client.Command({"GET", "large-set"}), Bulk("string-now"));
    EXPECT_TRUE(
        client.Command({"SCARD", "large-set"}).starts_with("-WRONGTYPE"));
    server.Stop();
  }
}

TEST(HashE2eTest, ExpiredShieldedWinnerDoesNotResurrectOlderString) {
  ASSERT_FALSE(g_lavik_binary.empty());
  const std::string prefix = lavik::test::TestDataPathPrefix() +
                             "lavik-expired-shield-recovery-" +
                             std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::posix_fallocate(fd, 0, 256ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(fd), 0);

  const std::uint16_t port = FindFreePort();
  const std::string old_value(3900 * 1024, 'o');
  const std::string new_value(3900 * 1024, 'n');
  {
    ServerProcess server(g_lavik_binary, port, data_path, log_path, 2);
    RespClient client(port);
    ASSERT_EQ(client.Command({"SET", "shielded", old_value, "PX", "600000"}),
              "+OK");
    // This live neighbor keeps the old version's record block allocated. The
    // equally large overwrite rotates into the next records block.
    ASSERT_EQ(client.Command({"SET", "old-block-anchor", old_value}), "+OK");
    ASSERT_EQ(client.Command({"SET", "shielded", new_value, "PX", "5000"}),
              "+OK");
    ASSERT_EQ(client.Command({"SET", "unshielded", "single", "PX", "5000"}),
              "+OK");
    ASSERT_TRUE(WaitForDurability(client));
    server.Stop();
  }
  std::this_thread::sleep_for(5200ms);
  {
    ServerProcess server(g_lavik_binary, port, data_path, log_path, 3);
    RespClient client(port);
    EXPECT_EQ(client.Command({"GET", "shielded"}), "$-1");
    EXPECT_EQ(client.Command({"GET", "unshielded"}), "$-1");
    const auto expiry_deadline = std::chrono::steady_clock::now() + 10s;
    while (std::chrono::steady_clock::now() < expiry_deadline &&
           client.Command({"DBSIZE"}) != ":1") {
      std::this_thread::sleep_for(10ms);
    }
    ASSERT_EQ(client.Command({"DBSIZE"}), ":1");
    ASSERT_TRUE(WaitForDurability(client));

    const std::string filler(480 * 1024, 'f');
    for (int batch = 0; batch < 4; ++batch) {
      std::vector<std::string> keys;
      std::vector<std::string_view> mset{"MSET"};
      for (int i = 0; i < 16; ++i) {
        keys.push_back("shield-reuse-" + std::to_string(batch) + "-" +
                       std::to_string(i));
        mset.push_back(keys.back());
        mset.push_back(filler);
      }
      ASSERT_EQ(client.Command(mset), "+OK");
    }
    ASSERT_TRUE(WaitForDurability(client));
    server.Kill();
  }
  {
    ServerProcess server(g_lavik_binary, port, data_path, log_path, 2, {}, {},
                         {{"LAVIK_RECOVERY_NOW_MS", "1"}});
    RespClient client(port);
    EXPECT_EQ(client.Command({"GET", "shielded"}), "$-1");
    EXPECT_EQ(client.Command({"GET", "unshielded"}), "$-1");
    EXPECT_EQ(client.Command({"GET", "old-block-anchor"}), Bulk(old_value));
    server.Stop();
  }
}

TEST(CollectionE2eTest, MemoryLimitStillAllowsShrinkingCommands) {
  ASSERT_FALSE(g_lavik_binary.empty());
  const std::string prefix = lavik::test::TestDataPathPrefix() +
                             "lavik-memory-recovery-" +
                             std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::posix_fallocate(fd, 0, 128ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(fd), 0);

  const std::uint16_t port = FindFreePort();
  {
    ServerProcess server(g_lavik_binary, port, data_path, log_path, 2);
    RespClient client(port);
    EXPECT_EQ(client.Command({"RPUSH", "l", "a", "b"}), ":2");
    EXPECT_EQ(client.Command({"HSET", "h", "f", "v"}), ":1");
    EXPECT_EQ(client.Command({"SADD", "s", "m"}), ":1");
    EXPECT_EQ(client.Command({"ZADD", "z", "1", "m"}), ":1");
    EXPECT_EQ(client.Command({"XADD", "x", "1-0", "f", "v"}), Bulk("1-0"));
    EXPECT_EQ(client.Command({"XADD", "x", "2-0", "f", "v"}), Bulk("2-0"));
    EXPECT_EQ(client.Command({"XGROUP", "CREATE", "x", "g", "0"}), "+OK");
    EXPECT_TRUE(client
                    .Command({"XREADGROUP", "GROUP", "g", "c", "COUNT", "1",
                              "STREAMS", "x", ">"})
                    .starts_with("*1\r\n"));
    // Keep enough keys in one partition to require direct-bucket growth when
    // the index is rebuilt by the low-memory restart below.
    for (int i = 0; i < 16; ++i) {
      const std::string key = "{memory-recovery}:" + std::to_string(i);
      EXPECT_EQ(client.Command({"SET", key, "value"}), "+OK");
    }
    ASSERT_TRUE(WaitForDurability(client));
    server.Stop();
  }
  {
    ServerProcess server(g_lavik_binary, port, data_path, log_path, 2, {},
                         {"--max-memory", "1"});
    RespClient client(port);
    std::this_thread::sleep_for(200ms);
    EXPECT_TRUE(client.Command({"SET", "must-be-rejected", "value"})
                    .starts_with("-OOM command not allowed"));
    for (int i = 0; i < 16; ++i) {
      const std::string key = "{memory-recovery}:" + std::to_string(i);
      EXPECT_EQ(client.Command({"GET", key}), Bulk("value"));
    }
    {
      RespClient oversized(port);
      const std::string oversized_key(128 * 1024, 'k');
      // Large request materialization is rejected by closing only the
      // offending connection; the worker must remain available to commands
      // that release retained state.
      EXPECT_TRUE(oversized.Command({"GET", oversized_key})
                      .starts_with("-ERR client request buffers exceed the "
                                   "memory limit"));
    }
    EXPECT_EQ(client.Command({"LPOP", "l"}), Bulk("a"));
    EXPECT_EQ(client.Command({"HDEL", "h", "f"}), ":1");
    EXPECT_EQ(client.Command({"SREM", "s", "m"}), ":1");
    EXPECT_EQ(client.Command({"ZREMRANGEBYRANK", "z", "0", "-1"}), ":1");
    EXPECT_EQ(client.Command({"XACK", "x", "g", "1-0"}), ":1");
    EXPECT_EQ(client.Command({"XTRIM", "x", "MAXLEN", "0"}), ":2");
    EXPECT_EQ(client.Command({"XGROUP", "DESTROY", "x", "g"}), ":1");
    server.Stop();
  }
}

TEST(CollectionE2eTest, ExecPartialWritesRollbackDurably) {
#ifdef NDEBUG
  GTEST_SKIP() << "transaction write fault injection is debug-only";
#else
  ASSERT_FALSE(g_lavik_binary.empty());
  const std::string prefix = lavik::test::TestDataPathPrefix() +
                             "lavik-exec-command-rollback-" +
                             std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::posix_fallocate(fd, 0, 160ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(fd), 0);

  const std::uint16_t port = FindFreePort();
  {
    ServerProcess server(g_lavik_binary, port, data_path, log_path, 3, {}, {},
                         {{"LAVIK_FAIL_TX_WRITE", "exec-fail-dst"}});
    RespClient client(port);
    EXPECT_EQ(client.Command({"RPUSH", "exec-list-src", "source"}), ":1");
    EXPECT_EQ(client.Command({"RPUSH", "exec-fail-dst", "destination"}), ":1");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"SET", "exec-before-list", "kept"}), "+QUEUED");
    EXPECT_EQ(client.Command(
                  {"LMOVE", "exec-list-src", "exec-fail-dst", "LEFT", "RIGHT"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"SET", "exec-after-list", "kept"}), "+QUEUED");
    const std::string list_exec = client.Command({"EXEC"});
    EXPECT_TRUE(list_exec.starts_with("*3\r\n+OK\r\n-ERR injected"))
        << list_exec;
    EXPECT_EQ(client.Command({"LRANGE", "exec-list-src", "0", "-1"}),
              "*1\r\n" + Bulk("source"));
    EXPECT_EQ(client.Command({"LRANGE", "exec-fail-dst", "0", "-1"}),
              "*1\r\n" + Bulk("destination"));

    EXPECT_EQ(client.Command({"DEL", "exec-fail-dst"}), ":1");
    EXPECT_EQ(client.Command({"SADD", "exec-set-src", "member"}), ":1");
    EXPECT_EQ(client.Command({"SADD", "exec-fail-dst", "existing"}), ":1");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"SET", "exec-before-set", "kept"}), "+QUEUED");
    EXPECT_EQ(
        client.Command({"SMOVE", "exec-set-src", "exec-fail-dst", "member"}),
        "+QUEUED");
    const std::string set_exec = client.Command({"EXEC"});
    EXPECT_TRUE(set_exec.starts_with("*2\r\n+OK\r\n-ERR injected")) << set_exec;
    EXPECT_EQ(client.Command({"SISMEMBER", "exec-set-src", "member"}), ":1");
    EXPECT_EQ(client.Command({"SMEMBERS", "exec-fail-dst"}),
              "*1\r\n" + Bulk("existing"));
    ASSERT_TRUE(WaitForDurability(client));
    server.Kill();
  }
  {
    ServerProcess server(g_lavik_binary, port, data_path, log_path, 3);
    RespClient client(port);
    EXPECT_EQ(client.Command({"GET", "exec-before-list"}), Bulk("kept"));
    EXPECT_EQ(client.Command({"GET", "exec-after-list"}), Bulk("kept"));
    EXPECT_EQ(client.Command({"LRANGE", "exec-list-src", "0", "-1"}),
              "*1\r\n" + Bulk("source"));
    EXPECT_EQ(client.Command({"GET", "exec-before-set"}), Bulk("kept"));
    EXPECT_EQ(client.Command({"SISMEMBER", "exec-set-src", "member"}), ":1");
    EXPECT_EQ(client.Command({"SMEMBERS", "exec-fail-dst"}),
              "*1\r\n" + Bulk("existing"));
    server.Stop();
  }
#endif
}

TEST(CollectionE2eTest, ExecStoreReplacementRollbackDurably) {
#ifdef NDEBUG
  GTEST_SKIP() << "transaction write fault injection is debug-only";
#else
  ASSERT_FALSE(g_lavik_binary.empty());
  const std::string prefix = lavik::test::TestDataPathPrefix() +
                             "lavik-exec-store-rollback-" +
                             std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::posix_fallocate(fd, 0, 160ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(fd), 0);

  const std::uint16_t port = FindFreePort();
  const auto fault_environment =
      std::vector<std::pair<std::string, std::string>>{
          {"LAVIK_FAIL_TX_WRITE", "exec-store-dst"},
          {"LAVIK_FAIL_TX_WRITE_AFTER", "1"}};
  {
    ServerProcess server(g_lavik_binary, port, data_path, log_path, 3, {}, {},
                         fault_environment);
    RespClient client(port);
    EXPECT_EQ(client.Command({"SADD", "exec-store-source", "member"}), ":1");
    EXPECT_EQ(client.Command({"SET", "exec-store-dst", "old-set"}), "+OK");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"SET", "exec-before-set-store", "kept"}),
              "+QUEUED");
    EXPECT_EQ(
        client.Command({"SINTERSTORE", "exec-store-dst", "exec-store-source"}),
        "+QUEUED");
    const std::string executed = client.Command({"EXEC"});
    EXPECT_TRUE(executed.starts_with("*2\r\n+OK\r\n-ERR injected")) << executed;
    EXPECT_EQ(client.Command({"GET", "exec-store-dst"}), Bulk("old-set"));
    ASSERT_TRUE(WaitForDurability(client));
    server.Kill();
  }
  {
    ServerProcess server(g_lavik_binary, port, data_path, log_path, 3, {}, {},
                         fault_environment);
    RespClient client(port);
    EXPECT_EQ(client.Command({"GET", "exec-before-set-store"}), Bulk("kept"));
    EXPECT_EQ(client.Command({"GET", "exec-store-dst"}), Bulk("old-set"));
    EXPECT_EQ(client.Command({"ZADD", "exec-zstore-source", "1", "member"}),
              ":1");
    EXPECT_EQ(client.Command({"SET", "exec-store-dst", "old-zset"}), "+OK");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"SET", "exec-before-zset-store", "kept"}),
              "+QUEUED");
    EXPECT_EQ(client.Command(
                  {"ZUNIONSTORE", "exec-store-dst", "1", "exec-zstore-source"}),
              "+QUEUED");
    const std::string executed = client.Command({"EXEC"});
    EXPECT_TRUE(executed.starts_with("*2\r\n+OK\r\n-ERR injected")) << executed;
    EXPECT_EQ(client.Command({"GET", "exec-store-dst"}), Bulk("old-zset"));
    ASSERT_TRUE(WaitForDurability(client));
    server.Kill();
  }
  {
    ServerProcess server(g_lavik_binary, port, data_path, log_path, 3);
    RespClient client(port);
    EXPECT_EQ(client.Command({"GET", "exec-before-set-store"}), Bulk("kept"));
    EXPECT_EQ(client.Command({"GET", "exec-before-zset-store"}), Bulk("kept"));
    EXPECT_EQ(client.Command({"GET", "exec-store-dst"}), Bulk("old-zset"));
    server.Stop();
  }
#endif
}

TEST(CollectionE2eTest, SortedSetGeoAndStreamCommandsRecover) {
  ASSERT_FALSE(g_lavik_binary.empty());
  const std::string prefix = lavik::test::TestDataPathPrefix() +
                             "lavik-compact-collections-" +
                             std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::posix_fallocate(fd, 0, 256ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(fd), 0);

  const std::uint16_t port = FindFreePort();
  {
    ServerProcess server(g_lavik_binary, port, data_path, log_path, 2);
    RespClient client(port);
    EXPECT_EQ(
        client.Command({"ZADD", "z", "1", "one", "2", "two", "1.5", "mid"}),
        ":3");
    EXPECT_EQ(client.Command({"SET", "type-string", "value"}), "+OK");
    EXPECT_EQ(client.Command({"RPUSH", "type-list", "value"}), ":1");
    EXPECT_EQ(client.Command({"HSET", "type-hash", "field", "value"}), ":1");
    EXPECT_EQ(client.Command({"SADD", "type-set", "member"}), ":1");
    EXPECT_EQ(client.Command({"XADD", "type-stream", "1-0", "field", "value"}),
              Bulk("1-0"));
    EXPECT_EQ(client.Command({"TYPE", "type-string"}), "+string");
    EXPECT_EQ(client.Command({"TYPE", "type-list"}), "+list");
    EXPECT_EQ(client.Command({"TYPE", "type-hash"}), "+hash");
    EXPECT_EQ(client.Command({"TYPE", "type-set"}), "+set");
    EXPECT_EQ(client.Command({"TYPE", "z"}), "+zset");
    EXPECT_EQ(client.Command({"TYPE", "type-stream"}), "+stream");
    EXPECT_EQ(client.Command({"TYPE", "missing-type"}), "+none");

    const std::string rename_source = KeyForWorker("rename-source", 0, 2);
    const std::string rename_destination =
        KeyForWorker("rename-destination", 1, 2);
    EXPECT_EQ(
        client.Command({"SET", rename_source, "renamed-value", "PX", "60000"}),
        "+OK");
    EXPECT_EQ(client.Command({"SET", rename_destination, "overwritten"}),
              "+OK");
    EXPECT_EQ(client.Command({"RENAME", rename_source, rename_destination}),
              "+OK");
    EXPECT_EQ(client.Command({"GET", rename_source}), "$-1");
    EXPECT_EQ(client.Command({"GET", rename_destination}),
              Bulk("renamed-value"));
    const std::string renamed_ttl =
        client.Command({"PTTL", rename_destination});
    ASSERT_TRUE(renamed_ttl.starts_with(":"));
    EXPECT_GT(std::stoll(renamed_ttl.substr(1)), 0);

    EXPECT_EQ(client.Command({"HSET", "rename-hash", "field", "value"}), ":1");
    EXPECT_EQ(client.Command({"RPUSH", "rename-list-target", "old"}), ":1");
    EXPECT_EQ(client.Command({"RENAME", "rename-hash", "rename-list-target"}),
              "+OK");
    EXPECT_EQ(client.Command({"TYPE", "rename-list-target"}), "+hash");
    EXPECT_EQ(client.Command({"HGET", "rename-list-target", "field"}),
              Bulk("value"));
    EXPECT_EQ(client.Command({"SET", "rename-nx-source", "source"}), "+OK");
    EXPECT_EQ(client.Command({"SET", "rename-nx-target", "target"}), "+OK");
    EXPECT_EQ(
        client.Command({"RENAMENX", "rename-nx-source", "rename-nx-target"}),
        ":0");
    EXPECT_EQ(client.Command({"GET", "rename-nx-source"}), Bulk("source"));
    EXPECT_EQ(
        client.Command({"RENAME", "rename-nx-source", "rename-nx-source"}),
        "+OK");
    EXPECT_EQ(
        client.Command({"RENAMENX", "rename-nx-source", "rename-nx-source"}),
        ":0");
    EXPECT_EQ(client.Command({"RENAME", "missing-rename", "destination"}),
              "-ERR no such key");
    EXPECT_EQ(client.Command({"SET", "rename-exec-source", "exec-value"}),
              "+OK");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(
        client.Command({"RENAME", "rename-exec-source", "rename-persisted"}),
        "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*1\r\n+OK");
    EXPECT_EQ(client.Command({"GET", "rename-persisted"}), Bulk("exec-value"));
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"TYPE", "type-hash"}), "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*1\r\n+hash");
    auto scan_type = [&](std::string_view type) {
      std::unordered_set<std::string> found;
      std::string cursor = "0";
      std::size_t calls = 0;
      do {
        auto [next, keys] = ParseScanReply(
            client.Command({"SCAN", cursor, "TYPE", type, "COUNT", "1000"}));
        found.insert(keys.begin(), keys.end());
        cursor = std::move(next);
        EXPECT_LT(++calls, 1000u);
      } while (cursor != "0");
      return found;
    };
    EXPECT_TRUE(scan_type("string").contains("type-string"));
    EXPECT_TRUE(scan_type("list").contains("type-list"));
    EXPECT_TRUE(scan_type("hash").contains("type-hash"));
    EXPECT_TRUE(scan_type("set").contains("type-set"));
    EXPECT_TRUE(scan_type("zset").contains("z"));
    EXPECT_TRUE(scan_type("stream").contains("type-stream"));
    EXPECT_TRUE(scan_type("unknown").empty());
    EXPECT_EQ(client.Command({"ZRANGE", "z", "0", "-1", "WITHSCORES"}),
              "*6\r\n" + Bulk("one") + "\r\n" + Bulk("1") + "\r\n" +
                  Bulk("mid") + "\r\n" + Bulk("1.5") + "\r\n" + Bulk("two") +
                  "\r\n" + Bulk("2"));
    EXPECT_EQ(client.Command({"ZADD", "zmpop-second", "1", "one", "2", "two",
                              "3", "three"}),
              ":3");
    EXPECT_EQ(client.Command({"ZMPOP", "2", "zmpop-empty", "zmpop-second",
                              "MIN", "COUNT", "2"}),
              "*2\r\n" + Bulk("zmpop-second") + "\r\n*2\r\n*2\r\n" +
                  Bulk("one") + "\r\n" + Bulk("1") + "\r\n*2\r\n" +
                  Bulk("two") + "\r\n" + Bulk("2"));
    EXPECT_EQ(client.Command({"BZPOPMAX", "zmpop-second", "1"}),
              "*3\r\n" + Bulk("zmpop-second") + "\r\n" + Bulk("three") +
                  "\r\n" + Bulk("3"));
    const std::string cross_zm_empty = KeyForWorker("cross-zm-empty", 0, 2);
    const std::string cross_zm_ready = KeyForWorker("cross-zm-ready", 1, 2);
    EXPECT_EQ(client.Command({"ZADD", cross_zm_ready, "8", "eight"}), ":1");
    EXPECT_EQ(
        client.Command({"ZMPOP", "2", cross_zm_empty, cross_zm_ready, "MIN"}),
        "*2\r\n" + Bulk(cross_zm_ready) + "\r\n*1\r\n*2\r\n" + Bulk("eight") +
            "\r\n" + Bulk("8"));
    EXPECT_EQ(client.Command({"BZPOPMIN", "zmpop-timeout", "0.01"}), "*-1");
    EXPECT_EQ(client.Command({"ZMPOP", "1", "zmpop-empty", "MIN", "garbage"}),
              "-ERR syntax error");
    EXPECT_EQ(client.Command({"ZMPOP", "01", "zmpop-empty", "MIN"}),
              "-ERR numkeys should be greater than 0");
    EXPECT_EQ(
        client.Command({"ZMPOP", "1", "zmpop-empty", "MIN", "COUNT", "bad"}),
        "-ERR count should be greater than 0");
    EXPECT_EQ(
        client.Command({"ZMPOP", "1", "zmpop-empty", "MIN", "COUNT", "02"}),
        "-ERR count should be greater than 0");

    auto zset_waiting_on_list = std::async(std::launch::async, [port] {
      RespClient blocked(port);
      return blocked.Command({"BZPOPMIN", "cross-type-zset-wait", "0.15"});
    });
    std::this_thread::sleep_for(30ms);
    EXPECT_EQ(client.Command({"RPUSH", "cross-type-zset-wait", "list"}), ":1");
    EXPECT_EQ(zset_waiting_on_list.get(), "*-1");
    EXPECT_EQ(client.Command({"DEL", "cross-type-zset-wait"}), ":1");

    auto list_waiting_on_zset = std::async(std::launch::async, [port] {
      RespClient blocked(port);
      return blocked.Command({"BLPOP", "cross-type-list-wait", "0.15"});
    });
    std::this_thread::sleep_for(30ms);
    EXPECT_EQ(client.Command({"ZADD", "cross-type-list-wait", "1", "zset"}),
              ":1");
    EXPECT_EQ(list_waiting_on_zset.get(), "*-1");
    EXPECT_EQ(client.Command({"DEL", "cross-type-list-wait"}), ":1");

    auto blocked_zpop = std::async(std::launch::async, [port] {
      RespClient blocked(port);
      return blocked.Command({"BZPOPMIN", "blocking-zset", "2"});
    });
    std::this_thread::sleep_for(50ms);
    EXPECT_EQ(client.Command({"ZADD", "blocking-zset", "4", "ready"}), ":1");
    EXPECT_EQ(blocked_zpop.get(), "*3\r\n" + Bulk("blocking-zset") + "\r\n" +
                                      Bulk("ready") + "\r\n" + Bulk("4"));

    auto first_chained_zpop = std::async(std::launch::async, [port] {
      RespClient blocked(port);
      return blocked.Command({"BZPOPMIN", "blocking-zset-chain", "2"});
    });
    std::this_thread::sleep_for(30ms);
    auto second_chained_zpop = std::async(std::launch::async, [port] {
      RespClient blocked(port);
      return blocked.Command({"BZPOPMIN", "blocking-zset-chain", "2"});
    });
    std::this_thread::sleep_for(50ms);
    EXPECT_EQ(client.Command(
                  {"ZADD", "blocking-zset-chain", "1", "first", "2", "second"}),
              ":2");
    EXPECT_EQ(first_chained_zpop.get(), "*3\r\n" + Bulk("blocking-zset-chain") +
                                            "\r\n" + Bulk("first") + "\r\n" +
                                            Bulk("1"));
    EXPECT_EQ(second_chained_zpop.get(),
              "*3\r\n" + Bulk("blocking-zset-chain") + "\r\n" + Bulk("second") +
                  "\r\n" + Bulk("2"));

    auto blocked_zmpop = std::async(std::launch::async, [port] {
      RespClient blocked(port);
      return blocked.Command({"BZMPOP", "2", "2", "blocking-zm-a",
                              "blocking-zm-b", "MAX", "COUNT", "2"});
    });
    std::this_thread::sleep_for(50ms);
    EXPECT_EQ(
        client.Command({"ZADD", "blocking-zm-b", "5", "five", "6", "six"}),
        ":2");
    EXPECT_EQ(blocked_zmpop.get(), "*2\r\n" + Bulk("blocking-zm-b") +
                                       "\r\n*2\r\n*2\r\n" + Bulk("six") +
                                       "\r\n" + Bulk("6") + "\r\n*2\r\n" +
                                       Bulk("five") + "\r\n" + Bulk("5"));

    EXPECT_EQ(client.Command({"ZADD", "exec-zmpop", "7", "seven"}), ":1");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"ZMPOP", "1", "exec-zmpop", "MIN"}), "+QUEUED");
    EXPECT_EQ(client.Command({"BZPOPMIN", "missing-exec-zpop", "100"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*2\r\n*2\r\n" + Bulk("exec-zmpop") +
                                            "\r\n*1\r\n*2\r\n" + Bulk("seven") +
                                            "\r\n" + Bulk("7") + "\r\n*-1");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"ZMPOP", "0", "ignored", "MIN"}), "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}),
              "*1\r\n-ERR numkeys should be greater than 0");
    EXPECT_EQ(client.Command({"ZADD", "bad-timeout-zpop", "1", "member"}),
              ":1");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"BZPOPMIN", "bad-timeout-zpop", "bad"}),
              "+QUEUED");
    EXPECT_TRUE(client.Command({"EXEC"}).starts_with(
        "*1\r\n-ERR timeout is not a float or out of range"));
    EXPECT_EQ(client.Command({"ZCARD", "bad-timeout-zpop"}), ":1");
    EXPECT_EQ(client.Command({"BZPOPMIN", "bad-timeout-zpop", "1e100"}),
              "-ERR timeout is out of range");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"BZPOPMIN", "bad-timeout-zpop", "1e100"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*1\r\n-ERR timeout is out of range");
    EXPECT_EQ(client.Command({"ZCARD", "bad-timeout-zpop"}), ":1");
    EXPECT_EQ(client.Command({"ZADD", "z2", "4", "one", "5", "four"}), ":2");
    EXPECT_EQ(client.Command({"ZINTER", "2", "z", "z2", "WITHSCORES"}),
              "*2\r\n" + Bulk("one") + "\r\n" + Bulk("5"));
    EXPECT_EQ(client.Command({"ZUNIONSTORE", "zout", "2", "z", "z2"}), ":4");
    EXPECT_TRUE(
        client.Command({"ZINTERCARD", "2", "z", "z2", "AGGREGATE", "MAX"})
            .starts_with("-ERR syntax error"));
    EXPECT_EQ(client.Command({"ZINTERCARD", "2", "z", "z2", "LIMIT", "-1"}),
              "-ERR LIMIT can't be negative");
    EXPECT_EQ(client.Command({"ZINTERCARD", "2", "z", "z2", "LIMIT", "bad"}),
              "-ERR LIMIT can't be negative");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"ZDIFF", "2", "z", "z2"}), "+QUEUED");
    EXPECT_EQ(client.Command({"ZINTER", "2", "z", "z2"}), "+QUEUED");
    EXPECT_EQ(client.Command({"ZUNION", "2", "z", "z2"}), "+QUEUED");
    EXPECT_EQ(client.Command({"ZINTERCARD", "2", "z", "z2"}), "+QUEUED");
    EXPECT_EQ(client.Command({"ZUNIONSTORE", "exec-z", "2", "z", "z2"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}),
              "*5\r\n*2\r\n" + Bulk("mid") + "\r\n" + Bulk("two") +
                  "\r\n*1\r\n" + Bulk("one") + "\r\n*4\r\n" + Bulk("mid") +
                  "\r\n" + Bulk("two") + "\r\n" + Bulk("four") + "\r\n" +
                  Bulk("one") + "\r\n:1\r\n:4");
    EXPECT_EQ(client.Command({"ZCARD", "exec-z"}), ":4");
    EXPECT_EQ(client.Command({"SET", "exec-replace-zset", "string"}), "+OK");
    EXPECT_EQ(client.Command({"PEXPIRE", "exec-replace-zset", "60000"}), ":1");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"ZUNIONSTORE", "exec-replace-zset", "1", "z"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*1\r\n:3");
    EXPECT_EQ(client.Command({"PTTL", "exec-replace-zset"}), ":-1");
    EXPECT_TRUE(client.Command({"ZADD", "malformed-score", "+-5", "member"})
                    .starts_with("-ERR value is not a valid float"));
    EXPECT_EQ(client.Command({"ZADD", "z", "NX", "XX", "1", "member"}),
              "-ERR XX and NX options at the same time are not compatible");
    EXPECT_EQ(client.Command({"ZADD", "z", "GT", "LT", "1", "member"}),
              "-ERR GT, LT, and/or NX options at the same time are not "
              "compatible");
    EXPECT_EQ(client.Command({"ZADD", "z", "INCR", "1", "one", "2", "two"}),
              "-ERR INCR option supports a single increment-element pair");
    EXPECT_TRUE(client.Command({"ZINCRBY", "malformed-score", "+-5", "member"})
                    .starts_with("-ERR value is not a valid float"));
    EXPECT_TRUE(client.Command({"ZRANGEBYSCORE", "z", "+-5", "10"})
                    .starts_with("-ERR min or max is not a float"));
    EXPECT_EQ(client.Command({"RPUSH", "zset-syntax-wrongtype", "value"}),
              ":1");
    EXPECT_TRUE(
        client.Command({"ZRANGEBYSCORE", "zset-syntax-wrongtype", "+-5", "10"})
            .starts_with("-ERR min or max is not a float"));
    EXPECT_TRUE(
        client.Command({"ZSCAN", "zset-syntax-wrongtype", "not-a-cursor"})
            .starts_with("-ERR invalid cursor"));
    EXPECT_EQ(client.Command({"ZADD", "decimal-z", "1.1", "one", "2.3", "two"}),
              ":2");
    EXPECT_EQ(client.Command({"ZSCORE", "decimal-z", "one"}), Bulk("1.1"));
    EXPECT_EQ(client.Command({"ZSCORE", "decimal-z", "two"}), Bulk("2.3"));
    EXPECT_EQ(client.Command({"ZRANGEBYSCORE", "decimal-z", "0", "10",
                              "WITHSCORES", "LIMIT", "0", "1"}),
              "*2\r\n" + Bulk("one") + "\r\n" + Bulk("1.1"));
    EXPECT_EQ(client.Command({"ZREVRANGEBYSCORE", "decimal-z", "10", "0",
                              "WITHSCORES", "LIMIT", "0", "1"}),
              "*2\r\n" + Bulk("two") + "\r\n" + Bulk("2.3"));
    EXPECT_EQ(client.Command({"ZADD", "decimal-z", "1e10", "integer", "0.1",
                              "tenth", "4611686018427387904", "boundary",
                              "5e18", "outside", "-0", "negative-zero"}),
              ":5");
    EXPECT_EQ(client.Command({"ZSCORE", "decimal-z", "integer"}),
              Bulk("10000000000"));
    EXPECT_EQ(client.Command({"ZSCORE", "decimal-z", "tenth"}), Bulk("0.1"));
    EXPECT_EQ(client.Command({"ZSCORE", "decimal-z", "boundary"}),
              Bulk("4611686018427387904"));
    EXPECT_EQ(client.Command({"ZSCORE", "decimal-z", "outside"}),
              Bulk("5e+18"));
    EXPECT_EQ(client.Command({"ZSCORE", "decimal-z", "negative-zero"}),
              Bulk("-0"));
    EXPECT_EQ(client.Command({"ZADD", "decimal-z", "0.0001", "fixed-four",
                              "0.00003", "fixed-five", "1e-7", "small-exp"}),
              ":3");
    EXPECT_EQ(client.Command({"ZSCORE", "decimal-z", "fixed-four"}),
              Bulk("0.0001"));
    EXPECT_EQ(client.Command({"ZSCORE", "decimal-z", "fixed-five"}),
              Bulk("0.00003"));
    EXPECT_EQ(client.Command({"ZSCORE", "decimal-z", "small-exp"}),
              Bulk("1e-7"));
    EXPECT_EQ(client.Command({"ZRANK", "decimal-z", "missing", "WITHSCORE"}),
              "*-1");
    EXPECT_EQ(
        client.Command({"ZRANK", "decimal-z", "one", "WITHSCORE", "extra"}),
        "-ERR syntax error");
    EXPECT_EQ(
        client.Command({"ZREVRANK", "decimal-z", "one", "WITHSCORE", "extra"}),
        "-ERR syntax error");
    EXPECT_EQ(client.Command(
                  {"ZREVRANGE", "decimal-z", "0", "-1", "WITHSCORES", "extra"}),
              "-ERR syntax error");
    EXPECT_TRUE(client.Command({"ZRANK", "missing-z", "member", "bad"})
                    .starts_with("-ERR syntax error"));
    EXPECT_EQ(client.Command({"ZADD", "z-infinity", "inf", "last", "-inf",
                              "first", "+1", "plus"}),
              ":3");
    EXPECT_EQ(client.Command({"ZRANGE", "z-infinity", "0", "-1", "WITHSCORES"}),
              "*6\r\n" + Bulk("first") + "\r\n" + Bulk("-inf") + "\r\n" +
                  Bulk("plus") + "\r\n" + Bulk("1") + "\r\n" + Bulk("last") +
                  "\r\n" + Bulk("inf"));
    EXPECT_EQ(client.Command({"ZINCRBY", "z-infinity", "+1", "plus"}),
              Bulk("2"));
    EXPECT_EQ(client.Command({"ZINCRBY", "z-infinity", "+inf", "plus"}),
              Bulk("inf"));
    EXPECT_EQ(client.Command({"ZADD", "positive-infinity", "inf", "same"}),
              ":1");
    EXPECT_EQ(client.Command({"ZADD", "negative-infinity", "-inf", "same"}),
              ":1");
    EXPECT_EQ(client.Command({"ZUNIONSTORE", "nan-union", "2",
                              "positive-infinity", "negative-infinity"}),
              ":1");
    EXPECT_EQ(client.Command({"ZSCORE", "nan-union", "same"}), Bulk("0"));
    EXPECT_EQ(
        client.Command({"ZUNION", "2", "positive-infinity", "positive-infinity",
                        "WEIGHTS", "0.5", "0", "WITHSCORES"}),
        "*2\r\n" + Bulk("same") + "\r\n" + Bulk("inf"));
    EXPECT_EQ(client.Command({"ZUNION", "2", "positive-infinity",
                              "positive-infinity", "WEIGHTS", "0.5", "0",
                              "AGGREGATE", "MIN", "WITHSCORES"}),
              "*2\r\n" + Bulk("same") + "\r\n" + Bulk("0"));
    EXPECT_EQ(
        client.Command({"ZINTER", "2", "positive-infinity", "positive-infinity",
                        "WEIGHTS", "0.5", "0", "WITHSCORES"}),
        "*2\r\n" + Bulk("same") + "\r\n" + Bulk("0"));
    EXPECT_EQ(client.Command({"ZINTER", "2", "positive-infinity",
                              "positive-infinity", "WEIGHTS", "0.5", "0",
                              "AGGREGATE", "MIN", "WITHSCORES"}),
              "*2\r\n" + Bulk("same") + "\r\n" + Bulk("inf"));
    EXPECT_EQ(client.Command({"SADD", "aggregate-set", "set-only", "same"}),
              ":2");
    EXPECT_EQ(
        client.Command({"ZUNION", "2", "positive-infinity", "aggregate-set"}),
        "*2\r\n" + Bulk("set-only") + "\r\n" + Bulk("same"));
    EXPECT_EQ(client.Command({"ZADD", "pop-overflow", "1", "member"}), ":1");
    EXPECT_TRUE(
        client.Command({"ZPOPMIN", "pop-overflow", "9223372036854775808"})
            .starts_with("-ERR value is out of range"));
    EXPECT_EQ(client.Command({"ZCARD", "pop-overflow"}), ":1");
    EXPECT_EQ(client.Command({"ZRANGEBYLEX", "z", "-", "+", "WITHSCORES"}),
              "-ERR syntax error, WITHSCORES not supported in combination "
              "with BYLEX");
    EXPECT_EQ(client.Command({"ZRANGE", "z", "0", "-1", "LIMIT", "0", "1"}),
              "-ERR syntax error, LIMIT is only supported in combination "
              "with either BYSCORE or BYLEX");
    EXPECT_EQ(client.Command(
                  {"ZRANGE", "z", "0", "10", "BYSCORE", "LIMIT", "bad", "1"}),
              "-ERR value is not an integer or out of range");
    EXPECT_EQ(
        client.Command({"ZRANGEBYSCORE", "z", "0", "10", "LIMIT", "0", "bad"}),
        "-ERR value is not an integer or out of range");
    EXPECT_EQ(client.Command({"ZADD", "empty-member-zset", "1", ""}), ":1");
    auto [empty_zset_cursor, empty_zset_scan] = ParseScanReply(
        client.Command({"ZSCAN", "empty-member-zset", "0", "COUNT", "100"}));
    EXPECT_EQ(empty_zset_cursor, "0");
    EXPECT_EQ(empty_zset_scan, (std::vector<std::string>{"", "1"}));
    EXPECT_EQ(client.Command({"SET", "replace-zset", "string"}), "+OK");
    EXPECT_EQ(client.Command({"PEXPIRE", "replace-zset", "60000"}), ":1");
    EXPECT_EQ(client.Command({"ZUNIONSTORE", "replace-zset", "1", "z"}), ":3");
    EXPECT_EQ(client.Command({"PTTL", "replace-zset"}), ":-1");
    EXPECT_EQ(client.Command({"ZRANDMEMBER", "z", "-9223372036854775808"}),
              "-ERR value is out of range");
    EXPECT_EQ(client.Command(
                  {"ZRANDMEMBER", "z", "9223372036854775807", "WITHSCORES"}),
              "-ERR value is out of range");
    EXPECT_EQ(client.Command({"ZRANDMEMBER", "missing-zset", "invalid"}),
              "-ERR value is not an integer or out of range");
    EXPECT_EQ(
        client.Command({"ZRANDMEMBER", "missing-zset", "-9223372036854775807"}),
        "*0");
    CloseStreamingReplyAfterHeader(port,
                                   {"ZRANDMEMBER", "z", "-9223372036854775807"},
                                   "*9223372036854775807");
    CloseStreamingReplyAfterHeader(
        port, {"ZRANDMEMBER", "z", "-3000000", "WITHSCORES"}, "*6000000");
    EXPECT_TRUE(WaitForReply(client, {"ZCARD", "z"}, ":3"));
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"ZRANDMEMBER", "z", "-2001", "WITHSCORES"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"SET", "after-zrand-oom", "alive"}), "+QUEUED");
    const std::string zset_exec_random = client.Command({"EXEC"});
    EXPECT_TRUE(zset_exec_random.starts_with("*2\r\n*4002\r\n"));
    EXPECT_TRUE(zset_exec_random.ends_with("+OK"));
    EXPECT_EQ(client.Command({"GET", "after-zrand-oom"}), Bulk("alive"));
    EXPECT_EQ(client.Command({"RPUSH", "sintercard-wrongtype", "value"}), ":1");
    EXPECT_TRUE(
        client
            .Command({"SINTERCARD", "1", "sintercard-wrongtype", "LIMIT", "-1"})
            .starts_with("-ERR LIMIT can't be negative"));
    EXPECT_EQ(client.Command({"SADD", "sintercard-repeat", "a", "b", "c"}),
              ":3");
    EXPECT_EQ(client.Command({"SINTERCARD", "1", "sintercard-repeat", "LIMIT",
                              "1", "LIMIT", "2"}),
              ":2");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"SINTERCARD", "1", "sintercard-repeat", "LIMIT",
                              "1", "LIMIT", "2"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*1\r\n:2");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"BLPOP", "never-created", ""}), "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}),
              "*1\r\n-ERR timeout is not a float or out of range");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"BLPOP", "never-created", " 0"}), "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}),
              "*1\r\n-ERR timeout is not a float or out of range");

    EXPECT_EQ(client.Command({"GEOADD", "geo", "13.361389", "38.115556",
                              "Palermo", "15.087269", "37.502669", "Catania"}),
              ":2");
    EXPECT_EQ(client.Command({"GEODIST", "geo", "Palermo", "Catania", "km"}),
              Bulk("166.2742"));
    EXPECT_EQ(client.Command({"GEOPOS", "geo", "Palermo"}),
              "*1\r\n*2\r\n" + Bulk("13.36138933897018433") + "\r\n" +
                  Bulk("38.11555639549629859"));
    EXPECT_EQ(client.Command(
                  {"GEORADIUS", "geo", "15", "37", "200", "km", "COUNT", "1"}),
              "*1\r\n" + Bulk("Catania"));
    EXPECT_TRUE(client.Command({"GEORADIUS", "geo", "200", "100", "10", "km"})
                    .starts_with("-ERR invalid longitude,latitude pair"));
    EXPECT_TRUE(client
                    .Command({"GEORADIUS", "zset-syntax-wrongtype", "200",
                              "100", "10", "km"})
                    .starts_with("-ERR invalid longitude,latitude pair"));
    EXPECT_TRUE(client
                    .Command({"GEOSEARCH", "geo", "FROMLONLAT", "200", "100",
                              "BYRADIUS", "10", "km"})
                    .starts_with("-ERR invalid longitude,latitude pair"));
    EXPECT_EQ(
        client.Command({"GEORADIUSBYMEMBER", "geo", "missing", "10", "km"}),
        "-ERR could not decode requested zset member");
    EXPECT_EQ(client.Command(
                  {"GEORADIUSBYMEMBER", "missing-geo", "member", "10", "km"}),
              "*0");
    EXPECT_EQ(client.Command({"GEOPOS", "geo", "missing"}), "*1\r\n*-1");
    EXPECT_EQ(client.Command({"GEOHASH", "geo", "Palermo", "Catania"}),
              "*2\r\n" + Bulk("sqc8b49rny0") + "\r\n" + Bulk("sqdtr74hyu0"));
    EXPECT_EQ(client.Command({"ZADD", "invalid-geo-score", "-1", "member"}),
              ":1");
    EXPECT_TRUE(client.Command({"GEOPOS", "invalid-geo-score", "member"})
                    .starts_with("*1\r\n*2\r\n"));
    EXPECT_TRUE(client.Command({"GEOHASH", "invalid-geo-score", "member"})
                    .starts_with("*1\r\n$"));
    EXPECT_EQ(client.Command({"GEOPOS", "geo"}), "*0");
    EXPECT_EQ(client.Command({"GEOHASH", "geo"}), "*0");
    EXPECT_TRUE(client.Command({"GEOADD", "bad-geo", "+-100", "+-50", "m"})
                    .starts_with("-ERR invalid longitude"));
    EXPECT_EQ(client.Command({"GEOADD", "geo-empty", "13", "38", ""}), ":1");
    EXPECT_EQ(client.Command({"GEOSEARCH", "geo-empty", "FROMMEMBER", "",
                              "BYRADIUS", "1", "km"}),
              "*1\r\n" + Bulk(""));
    EXPECT_EQ(
        client
            .Command({"GEOSEARCH", "geo", "WITHCOORD", "COUNT", "1", "ANY",
                      "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km"})
            .substr(0, 8),
        "*1\r\n*2\r\n");
    EXPECT_EQ(client.Command({"GEOADD", "geo-dateline", "179.9", "0", "east",
                              "-179.9", "0", "west"}),
              ":2");
    const std::string dateline =
        client.Command({"GEOSEARCH", "geo-dateline", "FROMLONLAT", "179.95",
                        "0", "BYBOX", "40", "10", "km"});
    EXPECT_NE(dateline.find(Bulk("east")), std::string::npos) << dateline;
    EXPECT_NE(dateline.find(Bulk("west")), std::string::npos) << dateline;

    EXPECT_EQ(client.Command({"ZRANGESTORE", "range-store", "z", "1", "2"}),
              ":2");
    EXPECT_EQ(
        client.Command({"ZRANGE", "range-store", "0", "-1", "WITHSCORES"}),
        "*4\r\n" + Bulk("mid") + "\r\n" + Bulk("1.5") + "\r\n" + Bulk("two") +
            "\r\n" + Bulk("2"));
    EXPECT_TRUE(client
                    .Command({"ZRANGESTORE", "wrong-range-store",
                              "aggregate-set", "0", "-1"})
                    .starts_with("-WRONGTYPE"));
    EXPECT_EQ(
        client.Command({"GEOSEARCHSTORE", "geo-store", "geo", "WITHCOORD",
                        "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km"}),
        "-ERR syntax error");
    EXPECT_EQ(
        client.Command({"GEOSEARCHSTORE", "geo-store", "geo", "STOREDIST",
                        "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km"}),
        ":2");
    EXPECT_EQ(client.Command({"ZCARD", "geo-store"}), ":2");
    EXPECT_EQ(client.Command({"GEORADIUS", "geo", "15", "37", "200", "km",
                              "STORE", "radius-store"}),
              ":2");
    EXPECT_EQ(client.Command({"ZCARD", "radius-store"}), ":2");
    EXPECT_TRUE(client
                    .Command({"GEORADIUS_RO", "geo", "15", "37", "200", "km",
                              "STORE", "forbidden"})
                    .starts_with("-ERR syntax error"));
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(
        client.Command({"ZRANGESTORE", "exec-range-store", "z", "0", "0"}),
        "+QUEUED");
    EXPECT_EQ(
        client.Command({"GEOSEARCHSTORE", "exec-geo-store", "geo", "FROMMEMBER",
                        "Palermo", "BYRADIUS", "200", "km"}),
        "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*2\r\n:1\r\n:2");

    EXPECT_EQ(client.Command({"SET", "", "empty-key-value"}), "+OK");
    bool saw_empty_key = false;
    std::string scan_cursor = "0";
    std::size_t scan_calls = 0;
    do {
      auto [next, keys] = ParseScanReply(
          client.Command({"SCAN", scan_cursor, "MATCH", "*", "COUNT", "100"}));
      saw_empty_key = saw_empty_key ||
                      std::find(keys.begin(), keys.end(), "") != keys.end();
      scan_cursor = std::move(next);
      ASSERT_LT(++scan_calls, 1000u);
    } while (scan_cursor != "0");
    EXPECT_TRUE(saw_empty_key);

    EXPECT_EQ(client.Command({"CONFIG", "SET", "stream-node-max-entries", "1"}),
              "+OK");
    EXPECT_EQ(client.Command({"XADD", "bare-ms", "1", "f", "v"}), Bulk("1-0"));
    EXPECT_TRUE(client.Command({"XADD", "bare-ms", "1", "f", "v2"})
                    .starts_with("-ERR The ID specified in XADD"));
    EXPECT_EQ(client.Command({"XADD", "bare-ms", "2", "f", "v2"}), Bulk("2-0"));
    EXPECT_TRUE(client.Command({"XADD", "zero-ms", "0", "f", "v"})
                    .starts_with("-ERR The ID specified in XADD"));
    EXPECT_TRUE(
        client
            .Command({"XADD", "missing-nomk", "NOMKSTREAM", "bad-id", "f", "v"})
            .starts_with("-ERR Invalid stream ID"));
    EXPECT_TRUE(client.Command({"XTRIM", "bare-ms", "MAXLEN", "1", "junk"})
                    .starts_with("-ERR syntax error"));
    EXPECT_EQ(
        client.Command({"XTRIM", "bare-ms", "MAXLEN", "~", "1", "LIMIT", "1"}),
        ":1");
    EXPECT_EQ(client.Command({"XADD", "xadd-limit", "1-0", "f", "1"}),
              Bulk("1-0"));
    EXPECT_EQ(client.Command({"XADD", "xadd-limit", "2-0", "f", "2"}),
              Bulk("2-0"));
    EXPECT_EQ(client.Command({"XADD", "xadd-limit", "3-0", "f", "3"}),
              Bulk("3-0"));
    EXPECT_EQ(client.Command({"XADD", "xadd-limit", "MAXLEN", "~", "1", "LIMIT",
                              "1", "4-0", "f", "4"}),
              Bulk("4-0"));
    EXPECT_EQ(client.Command({"XLEN", "xadd-limit"}), ":3");
    EXPECT_TRUE(client
                    .Command({"XADD", "xadd-limit", "MAXLEN", "=", "1", "LIMIT",
                              "1", "5-0", "f", "5"})
                    .starts_with("-ERR syntax error"));
    EXPECT_TRUE(
        client
            .Command({"XADD", "xadd-limit", "MAXLEN", "1", "MINID", "2-0",
                      "6-0", "f", "6"})
            .starts_with("-ERR syntax error, MAXLEN and MINID options at the "
                         "same time are not compatible"));
    EXPECT_EQ(
        client.Command({"CONFIG", "SET", "stream-node-max-entries", "100"}),
        "+OK");
    EXPECT_EQ(client.Command({"XGROUP", "CREATE", "xgroup-options", "g", "0",
                              "MKSTREAM", "ENTRIESREAD", "0"}),
              "+OK");
    EXPECT_TRUE(client.Command({"XGROUP", "HELP"})
                    .starts_with("*17\r\n+XGROUP <subcommand>"));
    EXPECT_TRUE(client.Command({"XINFO", "HELP"})
                    .starts_with("*9\r\n+XINFO <subcommand>"));
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"XGROUP", "HELP"}), "+QUEUED");
    EXPECT_EQ(client.Command({"XINFO", "HELP"}), "+QUEUED");
    const std::string exec_stream_help = client.Command({"EXEC"});
    EXPECT_TRUE(
        exec_stream_help.starts_with("*2\r\n*17\r\n+XGROUP <subcommand>"))
        << exec_stream_help;
    EXPECT_NE(exec_stream_help.find("\r\n*9\r\n+XINFO <subcommand>"),
              std::string::npos)
        << exec_stream_help;
    EXPECT_EQ(client.Command({"XINFO", "FOO"}),
              "-ERR unknown subcommand or wrong number of arguments for "
              "'FOO'. Try XINFO HELP.");
    EXPECT_EQ(client.Command({"XGROUP", "HELP", "unexpected"}),
              "-ERR wrong number of arguments for 'xgroup|help' command");
    EXPECT_EQ(client.Command({"XINFO", "HELP", "unexpected"}),
              "-ERR wrong number of arguments for 'xinfo|help' command");
    EXPECT_EQ(client.Command({"PING"}), "+PONG");
    EXPECT_EQ(client.Command({"XGROUP", "DESTROY", "xgroup-options"}),
              "-ERR wrong number of arguments for 'xgroup|destroy' command");
    EXPECT_EQ(client.Command({"XGROUP", "SETID", "xgroup-options"}),
              "-ERR wrong number of arguments for 'xgroup|setid' command");
    EXPECT_EQ(client.Command({"XLEN", "xgroup-options"}), ":0");
    EXPECT_EQ(
        client.Command({"XGROUP", "DESTROY", "missing-stream", "g"}),
        "-ERR The XGROUP subcommand requires the key to exist. Note that for "
        "CREATE you may want to use the MKSTREAM option to create an empty "
        "stream automatically.");
    EXPECT_EQ(client.Command({"XPENDING", "xgroup-options", "g"}),
              "*4\r\n:0\r\n$-1\r\n$-1\r\n*-1");
    const std::string empty_group_info =
        client.Command({"XINFO", "GROUPS", "xgroup-options"});
    EXPECT_NE(empty_group_info.find(Bulk("lag") + "\r\n:0"), std::string::npos)
        << empty_group_info;
    EXPECT_EQ(client.Command({"XACK", "xgroup-options", "missing", "1-0"}),
              ":0");

    EXPECT_EQ(client.Command({"XADD", "stream", "1-0", "f", "v"}), Bulk("1-0"));
    EXPECT_EQ(client.Command({"XADD", "stream", "2-0", "f2", "v2"}),
              Bulk("2-0"));
    EXPECT_EQ(client.Command({"XREAD", "STREAMS", "stream", "$"}), "*-1");
    EXPECT_EQ(
        client.Command({"XREAD", "BLOCK", "10", "STREAMS", "stream", "$"}),
        "*-1");
    EXPECT_TRUE(
        client.Command({"XREAD", "COUNT", "-1", "STREAMS", "stream", "0-0"})
            .starts_with("*1\r\n"));
    EXPECT_EQ(
        client.Command({"XREAD", "BLOCK", "-1", "STREAMS", "stream", "$"}),
        "-ERR timeout is negative");
    EXPECT_EQ(client.Command({"XREAD", "BLOCK", "18446744073709551615",
                              "STREAMS", "stream", "$"}),
              "-ERR timeout is not an integer or out of range");
    EXPECT_TRUE(client.Command({"XSETID", "stream", "1-1"})
                    .starts_with("-ERR The ID specified in XSETID"));
    EXPECT_TRUE(client.Command({"XADD", "stream", "1-2", "bad", "order"})
                    .starts_with("-ERR The ID specified in XADD"));
    EXPECT_EQ(client.Command({"XRANGE", "stream", "-", "+"}),
              "*2\r\n*2\r\n" + Bulk("1-0") + "\r\n*2\r\n" + Bulk("f") + "\r\n" +
                  Bulk("v") + "\r\n*2\r\n" + Bulk("2-0") + "\r\n*2\r\n" +
                  Bulk("f2") + "\r\n" + Bulk("v2"));
    EXPECT_EQ(client.Command({"XRANGE", "stream", "-", "+", "COUNT", "0"}),
              "*-1");
    EXPECT_EQ(client.Command({"XRANGE", "stream", "-", "+", "COUNT", "-1"}),
              "*-1");
    EXPECT_TRUE(client
                    .Command({"XSETID", "stream", "2-0", "ENTRIESADDED",
                              "9223372036854775808"})
                    .starts_with("-ERR value is not an integer"));
    EXPECT_TRUE(
        client.Command({"XSETID", "stream", "2-0", "ENTRIESADDED", "1"})
            .starts_with("-ERR The entries_added specified in XSETID is "
                         "smaller than the target stream length"));
    EXPECT_TRUE(
        client.Command({"XSETID", "stream", "2-0", "MAXDELETEDID", "3-0"})
            .starts_with("-ERR The ID specified in XSETID is smaller than the "
                         "provided max_deleted_entry_id"));
    EXPECT_EQ(client.Command({"XADD", "xsetid-deleted", "5-0", "f", "v"}),
              Bulk("5-0"));
    EXPECT_EQ(client.Command({"XDEL", "xsetid-deleted", "5-0"}), ":1");
    EXPECT_TRUE(
        client.Command({"XSETID", "xsetid-deleted", "4-0"})
            .starts_with("-ERR The ID specified in XSETID is smaller than "
                         "current max_deleted_entry_id"));
    EXPECT_EQ(
        client.Command({"XADD", "overflow-stream",
                        "18446744073709551615-18446744073709551615", "f", "v"}),
        Bulk("18446744073709551615-18446744073709551615"));
    EXPECT_TRUE(client.Command({"XADD", "overflow-stream", "*", "f", "v"})
                    .starts_with("-ERR The stream has exhausted"));
    EXPECT_EQ(client.Command({"XGROUP", "CREATE", "stream", "g", "0-0"}),
              "+OK");
    EXPECT_TRUE(client
                    .Command({"XREADGROUP", "GROUP", "g", "consumer", "COUNT",
                              "1", "STREAMS", "stream", ">"})
                    .starts_with("*1\r\n"));
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(
        client.Command({"XREAD", "COUNT", "1", "STREAMS", "stream", "0-0"}),
        "+QUEUED");
    EXPECT_EQ(client.Command({"XREADGROUP", "GROUP", "g", "exec-consumer",
                              "COUNT", "1", "STREAMS", "stream", ">"}),
              "+QUEUED");
    const std::string stream_exec = client.Command({"EXEC"});
    EXPECT_TRUE(stream_exec.starts_with("*2\r\n*1\r\n"));
    EXPECT_EQ(stream_exec.find("command is not allowed in transactions"),
              std::string::npos);
    EXPECT_EQ(client.Command({"XADD", "stream", "3-0", "f3", "v3"}),
              Bulk("3-0"));
    EXPECT_TRUE(
        client.Command({"XREAD", "COUNT", "0", "STREAMS", "stream", "0-0"})
            .starts_with("*1\r\n"));
    EXPECT_EQ(client.Command(
                  {"XCLAIM", "stream", "g", "other", "0", "1-0", "JUSTID"}),
              "*1\r\n" + Bulk("1-0"));
    EXPECT_TRUE(
        client.Command({"XCLAIM", "stream", "g", "other", "0", "JUSTID"})
            .starts_with("-ERR syntax error"));
    EXPECT_EQ(client.Command(
                  {"XAUTOCLAIM", "stream", "g", "third", "0", "0-0", "JUSTID"}),
              "*3\r\n" + Bulk("0-0") + "\r\n*2\r\n" + Bulk("1-0") + "\r\n" +
                  Bulk("2-0") + "\r\n*0");
    EXPECT_FALSE(client
                     .Command({"XAUTOCLAIM", "stream", "g", "special-minus",
                               "0", "-", "JUSTID"})
                     .starts_with("-ERR"));
    EXPECT_FALSE(client
                     .Command({"XAUTOCLAIM", "stream", "g", "special-plus", "0",
                               "+", "JUSTID"})
                     .starts_with("-ERR"));
    EXPECT_FALSE(client
                     .Command({"XAUTOCLAIM", "stream", "g", "special-exclusive",
                               "0", "(0-0", "JUSTID"})
                     .starts_with("-ERR"));
    EXPECT_EQ(client.Command({"XREADGROUP", "GROUP", "g", "nobody", "STREAMS",
                              "stream", "0-0"}),
              "*1\r\n*2\r\n" + Bulk("stream") + "\r\n*0");
    EXPECT_EQ(client.Command({"XPENDING", "stream", "g", "-", "+", "0"}), "*0");
    EXPECT_EQ(client.Command({"XPENDING", "stream", "g", "-", "+", "-1"}),
              "*0");
    EXPECT_EQ(client.Command({"XAUTOCLAIM", "stream", "g", "nobody", "0", "0-0",
                              "COUNT", "0"}),
              "-ERR COUNT must be > 0");
    EXPECT_EQ(client.Command({"XCLAIM", "stream", "g", "forced", "0", "3-0",
                              "FORCE", "JUSTID"}),
              "*1\r\n" + Bulk("3-0"));
    EXPECT_EQ(client.Command({"XDEL", "stream", "1-0"}), ":1");
    EXPECT_EQ(client.Command({"XAUTOCLAIM", "stream", "g", "cleanup", "0",
                              "0-0", "COUNT", "10", "JUSTID"}),
              "*3\r\n" + Bulk("0-0") + "\r\n*2\r\n" + Bulk("2-0") + "\r\n" +
                  Bulk("3-0") + "\r\n*1\r\n" + Bulk("1-0"));
    EXPECT_EQ(client.Command({"XPENDING", "stream", "g"}).substr(0, 4),
              "*4\r\n");

    EXPECT_EQ(client.Command({"XADD", "force-count", "1-0", "f", "v"}),
              Bulk("1-0"));
    EXPECT_EQ(
        client.Command({"XGROUP", "CREATE", "force-count", "force-group", "$"}),
        "+OK");
    EXPECT_TRUE(client
                    .Command({"XCLAIM", "force-count", "force-group", "forced",
                              "0", "1-0", "FORCE"})
                    .starts_with("*1\r\n"));
    EXPECT_TRUE(client
                    .Command({"XPENDING", "force-count", "force-group", "1-0",
                              "1-0", "1"})
                    .ends_with("\r\n:2"));

    EXPECT_EQ(client.Command({"XADD", "autoclean", "1-0", "f", "v"}),
              Bulk("1-0"));
    EXPECT_EQ(
        client.Command({"XGROUP", "CREATE", "autoclean", "auto-group", "0"}),
        "+OK");
    EXPECT_TRUE(client
                    .Command({"XREADGROUP", "GROUP", "auto-group", "old",
                              "STREAMS", "autoclean", ">"})
                    .starts_with("*1\r\n"));
    EXPECT_EQ(client.Command({"XDEL", "autoclean", "1-0"}), ":1");
    EXPECT_EQ(client.Command({"XAUTOCLAIM", "autoclean", "auto-group", "new",
                              "999999999", "0-0", "COUNT", "10", "JUSTID"}),
              "*3\r\n" + Bulk("0-0") + "\r\n*0\r\n*1\r\n" + Bulk("1-0"));
    EXPECT_TRUE(client.Command({"XPENDING", "autoclean", "auto-group"})
                    .starts_with("*4\r\n:0"));

    EXPECT_EQ(client.Command({"XADD", "auto-cursor", "1-0", "f", "1"}),
              Bulk("1-0"));
    EXPECT_EQ(client.Command({"XADD", "auto-cursor", "2-0", "f", "2"}),
              Bulk("2-0"));
    EXPECT_EQ(client.Command(
                  {"XGROUP", "CREATE", "auto-cursor", "cursor-group", "0"}),
              "+OK");
    EXPECT_TRUE(client
                    .Command({"XREADGROUP", "GROUP", "cursor-group", "old",
                              "COUNT", "2", "STREAMS", "auto-cursor", ">"})
                    .starts_with("*1\r\n"));
    EXPECT_EQ(client.Command({"XAUTOCLAIM", "auto-cursor", "cursor-group",
                              "new", "0", "0-0", "COUNT", "1", "JUSTID"}),
              "*3\r\n" + Bulk("2-0") + "\r\n*1\r\n" + Bulk("1-0") + "\r\n*0");
    EXPECT_EQ(client.Command({"XAUTOCLAIM", "auto-cursor", "cursor-group",
                              "new", "0", "2-0", "COUNT", "1", "JUSTID"}),
              "*3\r\n" + Bulk("0-0") + "\r\n*1\r\n" + Bulk("2-0") + "\r\n*0");

    EXPECT_EQ(client.Command({"XADD", "lag-tombstone", "1-0", "f", "1"}),
              Bulk("1-0"));
    EXPECT_EQ(client.Command({"XADD", "lag-tombstone", "2-0", "f", "2"}),
              Bulk("2-0"));
    EXPECT_EQ(client.Command({"XADD", "lag-tombstone", "3-0", "f", "3"}),
              Bulk("3-0"));
    EXPECT_EQ(
        client.Command({"XGROUP", "CREATE", "lag-tombstone", "lag-group", "0"}),
        "+OK");
    EXPECT_EQ(client.Command({"XDEL", "lag-tombstone", "2-0"}), ":1");
    EXPECT_TRUE(client
                    .Command({"XREADGROUP", "GROUP", "lag-group", "reader",
                              "COUNT", "1", "STREAMS", "lag-tombstone", ">"})
                    .starts_with("*1\r\n"));
    const std::string fragmented_lag =
        client.Command({"XINFO", "GROUPS", "lag-tombstone"});
    EXPECT_NE(fragmented_lag.find(Bulk("lag") + "\r\n$-1"), std::string::npos)
        << fragmented_lag;
    EXPECT_TRUE(client
                    .Command({"XREADGROUP", "GROUP", "lag-group", "reader",
                              "COUNT", "1", "STREAMS", "lag-tombstone", ">"})
                    .starts_with("*1\r\n"));
    const std::string complete_lag =
        client.Command({"XINFO", "GROUPS", "lag-tombstone"});
    EXPECT_NE(complete_lag.find(Bulk("lag") + "\r\n:0"), std::string::npos)
        << complete_lag;

    EXPECT_EQ(client.Command({"XADD", "history-deleted", "1-0", "f", "v"}),
              Bulk("1-0"));
    EXPECT_EQ(
        client.Command({"XGROUP", "CREATE", "history-deleted", "history", "0"}),
        "+OK");
    EXPECT_TRUE(client
                    .Command({"XREADGROUP", "GROUP", "history", "reader",
                              "STREAMS", "history-deleted", ">"})
                    .starts_with("*1\r\n"));
    EXPECT_EQ(client.Command({"XDEL", "history-deleted", "1-0"}), ":1");
    const std::string deleted_history =
        "*1\r\n*2\r\n" + Bulk("history-deleted") + "\r\n*1\r\n*2\r\n" +
        Bulk("1-0") + "\r\n*-1";
    EXPECT_EQ(client.Command({"XREADGROUP", "GROUP", "history", "reader",
                              "STREAMS", "history-deleted", "0"}),
              deleted_history);
    const std::string deleted_pending = client.Command(
        {"XPENDING", "history-deleted", "history", "-", "+", "1"});
    EXPECT_TRUE(deleted_pending.ends_with("\r\n:1")) << deleted_pending;

    EXPECT_EQ(client.Command({"XADD", "history-empty", "1-0", "f", "empty"}),
              Bulk("1-0"));
    EXPECT_EQ(client.Command({"XADD", "history-data", "1-0", "f", "data"}),
              Bulk("1-0"));
    EXPECT_EQ(
        client.Command({"XGROUP", "CREATE", "history-empty", "mixed", "$"}),
        "+OK");
    EXPECT_EQ(
        client.Command({"XGROUP", "CREATE", "history-data", "mixed", "0"}),
        "+OK");
    EXPECT_TRUE(client
                    .Command({"XREADGROUP", "GROUP", "mixed", "reader",
                              "STREAMS", "history-data", ">"})
                    .starts_with("*1\r\n"));
    const std::string mixed_history =
        "*2\r\n*2\r\n" + Bulk("history-empty") + "\r\n*0\r\n*2\r\n" +
        Bulk("history-data") + "\r\n*1\r\n*2\r\n" + Bulk("1-0") + "\r\n*2\r\n" +
        Bulk("f") + "\r\n" + Bulk("data");
    EXPECT_EQ(
        client.Command({"XREADGROUP", "GROUP", "mixed", "reader", "STREAMS",
                        "history-empty", "history-data", "0", "0"}),
        mixed_history);
    const auto mixed_started = std::chrono::steady_clock::now();
    EXPECT_EQ(client.Command({"XREADGROUP", "GROUP", "mixed", "reader", "BLOCK",
                              "1000", "STREAMS", "history-empty",
                              "history-data", "0", ">"}),
              "*1\r\n*2\r\n" + Bulk("history-empty") + "\r\n*0");
    EXPECT_LT(std::chrono::steady_clock::now() - mixed_started, 500ms);

    EXPECT_EQ(client.Command({"XADD", "claim-order", "1-0", "f", "1"}),
              Bulk("1-0"));
    EXPECT_EQ(client.Command({"XADD", "claim-order", "2-0", "f", "2"}),
              Bulk("2-0"));
    EXPECT_EQ(client.Command({"XADD", "claim-order", "3-0", "f", "3"}),
              Bulk("3-0"));
    EXPECT_EQ(
        client.Command({"XGROUP", "CREATE", "claim-order", "ordered", "0"}),
        "+OK");
    EXPECT_TRUE(client
                    .Command({"XREADGROUP", "GROUP", "ordered", "first",
                              "COUNT", "3", "STREAMS", "claim-order", ">"})
                    .starts_with("*1\r\n"));
    EXPECT_EQ(client.Command({"XACK", "claim-order", "ordered", "2-0"}), ":1");
    EXPECT_EQ(client.Command({"XCLAIM", "claim-order", "ordered", "second", "0",
                              "2-0", "IDLE", "50000", "RETRYCOUNT", "7",
                              "LASTID", "9-0", "FORCE", "JUSTID"}),
              "*1\r\n" + Bulk("2-0"));
    const std::string ordered_summary =
        client.Command({"XPENDING", "claim-order", "ordered"});
    EXPECT_TRUE(ordered_summary.starts_with("*4\r\n:3\r\n" + Bulk("1-0") +
                                            "\r\n" + Bulk("3-0")))
        << ordered_summary;
    EXPECT_TRUE(
        client
            .Command({"XPENDING", "claim-order", "ordered", "(1-0", "+", "10"})
            .starts_with("*2\r\n"));
    const std::string forced_pending = client.Command(
        {"XPENDING", "claim-order", "ordered", "2-0", "2-0", "1"});
    ASSERT_TRUE(forced_pending.ends_with("\r\n:7")) << forced_pending;
    const std::size_t count_separator = forced_pending.rfind("\r\n:7");
    ASSERT_NE(count_separator, std::string::npos);
    const std::size_t idle_separator =
        forced_pending.rfind("\r\n:", count_separator - 1);
    ASSERT_NE(idle_separator, std::string::npos);
    const std::string idle = forced_pending.substr(
        idle_separator + 3, count_separator - idle_separator - 3);
    EXPECT_GE(std::stoull(idle), 49000u);
    EXPECT_TRUE(client
                    .Command({"XPENDING", "claim-order", "ordered", "IDLE",
                              "49000", "-", "+", "10"})
                    .starts_with("*1\r\n"));
    EXPECT_EQ(client.Command({"XPENDING", "claim-order", "ordered", "IDLE",
                              "100000", "-", "+", "10"}),
              "*0");
    EXPECT_EQ(
        client.Command({"XCLAIM", "claim-order", "ordered", "huge-idle", "0",
                        "1-0", "IDLE", "18446744073709551615", "JUSTID"}),
        "*1\r\n" + Bulk("1-0"));
    const std::string huge_idle_pending = client.Command(
        {"XPENDING", "claim-order", "ordered", "1-0", "1-0", "1"});
    const std::size_t huge_idle_count = huge_idle_pending.rfind("\r\n:");
    ASSERT_NE(huge_idle_count, std::string::npos);
    const std::size_t huge_idle_field =
        huge_idle_pending.rfind("\r\n:", huge_idle_count - 1);
    ASSERT_NE(huge_idle_field, std::string::npos);
    EXPECT_LT(std::stoull(huge_idle_pending.substr(
                  huge_idle_field + 3, huge_idle_count - huge_idle_field - 3)),
              1000u);
    EXPECT_TRUE(client
                    .Command({"XAUTOCLAIM", "claim-order", "ordered",
                              "exclusive", "0", "(1-0", "COUNT", "1", "JUSTID"})
                    .starts_with("*3\r\n"));
    EXPECT_TRUE(client.Command({"XINFO", "GROUPS", "missing-stream"})
                    .starts_with("-ERR no such key"));
    const std::string group_info =
        client.Command({"XINFO", "GROUPS", "claim-order"});
    EXPECT_NE(group_info.find(Bulk("last-delivered-id") + "\r\n" + Bulk("9-0")),
              std::string::npos);
    const std::string consumer_info =
        client.Command({"XINFO", "CONSUMERS", "claim-order", "ordered"});
    EXPECT_NE(consumer_info.find("*8\r\n"), std::string::npos) << consumer_info;
    const std::string full_info = client.Command(
        {"XINFO", "STREAM", "claim-order", "FULL", "COUNT", "1"});
    EXPECT_TRUE(full_info.starts_with("*18\r\n")) << full_info;
    EXPECT_NE(full_info.find(Bulk("entries")), std::string::npos);
    EXPECT_NE(full_info.find(Bulk("pending")), std::string::npos);
    for (int i = 1; i <= 12; ++i) {
      EXPECT_EQ(client.Command({"XADD", "full-count-stream",
                                std::to_string(i) + "-0", "f", "v"}),
                Bulk(std::to_string(i) + "-0"));
    }
    const std::string negative_full_count = client.Command(
        {"XINFO", "STREAM", "full-count-stream", "FULL", "COUNT", "-1"});
    EXPECT_NE(negative_full_count.find(Bulk("entries") + "\r\n*10\r\n"),
              std::string::npos)
        << negative_full_count;
    const std::string unlimited_full_count = client.Command(
        {"XINFO", "STREAM", "full-count-stream", "FULL", "COUNT", "0"});
    EXPECT_NE(unlimited_full_count.find(Bulk("entries") + "\r\n*12\r\n"),
              std::string::npos)
        << unlimited_full_count;
    EXPECT_TRUE(
        client.Command({"XINFO", "STREAM", "claim-order", "FULL", "garbage"})
            .starts_with("-ERR syntax error"));
    EXPECT_TRUE(
        client
            .Command({"XINFO", "CONSUMERS", "missing-stream", "missing-group"})
            .starts_with("-ERR no such key"));

    EXPECT_EQ(
        client.Command({"XADD", "empty-consumer-stream", "1-0", "f", "v"}),
        Bulk("1-0"));
    EXPECT_EQ(client.Command({"XGROUP", "CREATE", "empty-consumer-stream",
                              "empty-group", "$"}),
              "+OK");
    EXPECT_EQ(
        client.Command({"XREADGROUP", "GROUP", "empty-group", "read-empty",
                        "STREAMS", "empty-consumer-stream", ">"}),
        "*-1");
    EXPECT_NE(client
                  .Command({"XINFO", "CONSUMERS", "empty-consumer-stream",
                            "empty-group"})
                  .find(Bulk("read-empty")),
              std::string::npos);
    EXPECT_TRUE(client
                    .Command({"XAUTOCLAIM", "empty-consumer-stream",
                              "empty-group", "claim-empty", "0", "0-0"})
                    .starts_with("*3\r\n"));
    EXPECT_NE(client
                  .Command({"XINFO", "CONSUMERS", "empty-consumer-stream",
                            "empty-group"})
                  .find(Bulk("claim-empty")),
              std::string::npos);

    EXPECT_EQ(client.Command({"XADD", "rewind-pel", "1-0", "f", "1"}),
              Bulk("1-0"));
    EXPECT_EQ(client.Command({"XADD", "rewind-pel", "2-0", "f", "2"}),
              Bulk("2-0"));
    EXPECT_EQ(client.Command({"XGROUP", "CREATE", "rewind-pel", "g", "0"}),
              "+OK");
    EXPECT_TRUE(client
                    .Command({"XREADGROUP", "GROUP", "g", "first", "STREAMS",
                              "rewind-pel", ">"})
                    .starts_with("*1\r\n"));
    EXPECT_EQ(client.Command({"XGROUP", "SETID", "rewind-pel", "g", "0"}),
              "+OK");
    EXPECT_TRUE(client
                    .Command({"XREADGROUP", "GROUP", "g", "second", "STREAMS",
                              "rewind-pel", ">"})
                    .starts_with("*1\r\n"));
    const std::string rewind_pending =
        client.Command({"XPENDING", "rewind-pel", "g", "1-0", "1-0", "1"});
    EXPECT_TRUE(rewind_pending.ends_with("\r\n:1")) << rewind_pending;
    EXPECT_TRUE(client.Command({"XPENDING", "rewind-pel", "g"})
                    .starts_with("*4\r\n:2\r\n"));
    EXPECT_EQ(client.Command({"XACK", "rewind-pel", "g", "1-0"}), ":1");
    EXPECT_TRUE(client.Command({"XPENDING", "rewind-pel", "g"})
                    .starts_with("*4\r\n:1\r\n"));

    EXPECT_EQ(client.Command({"HSET", "watched-same-hash", "field", "value"}),
              ":1");
    EXPECT_EQ(client.Command({"WATCH", "watched-same-hash"}), "+OK");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"HGET", "watched-same-hash", "field"}),
              "+QUEUED");
    {
      RespClient concurrent(port);
      EXPECT_EQ(
          concurrent.Command({"HSET", "watched-same-hash", "field", "value"}),
          ":0");
    }
    EXPECT_EQ(client.Command({"EXEC"}), "*-1");

    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"SINTERCARD", "5", "z", "z2"}), "+QUEUED");
    const std::string invalid_exec = client.Command({"EXEC"});
    EXPECT_TRUE(invalid_exec.starts_with(
        "*1\r\n-ERR Number of keys can't be greater than number of args"))
        << invalid_exec;
    ASSERT_TRUE(WaitForDurability(client));
    server.Stop();
  }
  {
    ServerProcess server(g_lavik_binary, port, data_path, log_path, 3);
    RespClient client(port);
    EXPECT_EQ(client.Command({"ZCARD", "zout"}), ":4");
    EXPECT_EQ(client.Command({"GET", "rename-persisted"}), Bulk("exec-value"));
    EXPECT_EQ(client.Command({"XLEN", "stream"}), ":2");
    EXPECT_TRUE(
        client.Command({"XINFO", "GROUPS", "stream"}).starts_with("*1\r\n"));
    server.Stop();
  }
}

TEST(CollectionE2eTest, StringCommandsRecover) {
  ASSERT_FALSE(g_lavik_binary.empty());
  const std::string prefix = lavik::test::TestDataPathPrefix() +
                             "lavik-string-commands-" +
                             std::to_string(::getpid());
  const std::string data_path = prefix + ".data";
  const std::string log_path = prefix + ".log";
  FileCleanup data_cleanup(data_path);
  FileCleanup log_cleanup(log_path);
  const int fd =
      ::open(data_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::posix_fallocate(fd, 0, 160ULL * 1024 * 1024), 0);
  ASSERT_EQ(::close(fd), 0);

  const std::uint16_t port = FindFreePort();
  const std::string cross_a = KeyForWorker("string-cross-a", 0, 2);
  const std::string cross_b = KeyForWorker("string-cross-b", 1, 2);
  const std::string bitmap_a = KeyForWorker("bitmap-cross-a", 0, 2);
  const std::string bitmap_b = KeyForWorker("bitmap-cross-b", 1, 2);
  const std::string bitmap_destination =
      KeyForWorker("bitmap-cross-destination", 1, 2);
  {
    ServerProcess server(g_lavik_binary, port, data_path, log_path, 2);
    RespClient client(port);

    EXPECT_EQ(client.Command({"APPEND", "append", "abc"}), ":3");
    EXPECT_EQ(client.Command({"PEXPIRE", "append", "60000"}), ":1");
    EXPECT_EQ(client.Command({"APPEND", "append", "def"}), ":6");
    EXPECT_EQ(client.Command({"GET", "append"}), Bulk("abcdef"));
    EXPECT_GT(std::stoll(client.Command({"PTTL", "append"}).substr(1)), 0);
    EXPECT_EQ(client.Command({"GETRANGE", "append", "1", "-2"}), Bulk("bcde"));
    EXPECT_EQ(client.Command({"SUBSTR", "append", "-3", "-1"}), Bulk("def"));
    EXPECT_EQ(client.Command({"GETRANGE", "missing", "0", "-1"}), Bulk(""));

    EXPECT_EQ(client.Command({"SETRANGE", "range", "3", "x"}), ":4");
    const std::string zero_padded("\0\0\0x", 4);
    EXPECT_EQ(client.Command({"GET", "range"}), Bulk(zero_padded));
    EXPECT_EQ(client.Command({"SETRANGE", "range", "1", "yz"}), ":4");
    const std::string overwritten("\0yzx", 4);
    EXPECT_EQ(client.Command({"GET", "range"}), Bulk(overwritten));
    EXPECT_EQ(client.Command({"SETRANGE", "range", "2", ""}), ":4");
    EXPECT_EQ(client.Command({"SETRANGE", "range", "-1", "x"}),
              "-ERR offset is out of range");

    EXPECT_EQ(client.Command({"SETNX", "nx", "first"}), ":1");
    EXPECT_EQ(client.Command({"SETNX", "nx", "second"}), ":0");
    EXPECT_EQ(client.Command({"GET", "nx"}), Bulk("first"));
    EXPECT_EQ(client.Command({"SETEX", "seconds", "30", "value"}), "+OK");
    EXPECT_EQ(client.Command({"PSETEX", "millis", "30000", "value"}), "+OK");
    EXPECT_GT(std::stoll(client.Command({"PTTL", "seconds"}).substr(1)), 0);
    EXPECT_GT(std::stoll(client.Command({"PTTL", "millis"}).substr(1)), 0);
    EXPECT_EQ(client.Command({"SETEX", "bad-expire", "0", "value"}),
              "-ERR invalid expire time in 'setex' command");

    EXPECT_EQ(client.Command({"SET", "swap", "old", "PX", "60000"}), "+OK");
    EXPECT_EQ(client.Command({"GETSET", "swap", "new"}), Bulk("old"));
    EXPECT_EQ(client.Command({"GET", "swap"}), Bulk("new"));
    EXPECT_EQ(client.Command({"PTTL", "swap"}), ":-1");
    EXPECT_EQ(client.Command({"GETEX", "swap", "EX", "30"}), Bulk("new"));
    EXPECT_GT(std::stoll(client.Command({"PTTL", "swap"}).substr(1)), 0);
    EXPECT_EQ(client.Command({"GETEX", "swap", "PERSIST"}), Bulk("new"));
    EXPECT_EQ(client.Command({"PTTL", "swap"}), ":-1");
    EXPECT_EQ(client.Command({"GETDEL", "swap"}), Bulk("new"));
    EXPECT_EQ(client.Command({"GETDEL", "swap"}), "$-1");
    EXPECT_EQ(client.Command({"GETEX", "missing", "PX", "not-an-integer"}),
              "$-1");
    EXPECT_EQ(client.Command({"GETEX", "missing", "UNKNOWN"}),
              "-ERR syntax error");
    EXPECT_EQ(client.Command({"GETEX", "missing", "EX", "1", "PERSIST"}),
              "-ERR syntax error");
    EXPECT_EQ(client.Command({"SET", "past-expiry", "old"}), "+OK");
    EXPECT_EQ(client.Command({"GETEX", "past-expiry", "PXAT", "1"}),
              Bulk("old"));
    EXPECT_EQ(client.Command({"GET", "past-expiry"}), "$-1");
    EXPECT_EQ(client.Command({"RPUSH", "getset-list", "x"}), ":1");
    EXPECT_EQ(
        client.Command({"GETSET", "getset-list", "replacement"}),
        "-WRONGTYPE Operation against a key holding the wrong kind of value");
    EXPECT_EQ(client.Command({"SETNX", "getset-list", "replacement"}), ":0");

    EXPECT_EQ(client.Command({"DECR", "integer"}), ":-1");
    EXPECT_EQ(client.Command({"INCRBY", "integer", "11"}), ":10");
    EXPECT_EQ(client.Command({"DECRBY", "integer", "3"}), ":7");
    EXPECT_EQ(client.Command({"INCRBY", "integer", "+5"}),
              "-ERR value is not an integer or out of range");
    EXPECT_EQ(client.Command({"INCRBY", "integer", "01"}),
              "-ERR value is not an integer or out of range");
    EXPECT_EQ(client.Command({"INCRBY", "integer", "-0"}),
              "-ERR value is not an integer or out of range");
    EXPECT_EQ(client.Command({"SET", "noncanonical-integer", "01"}), "+OK");
    EXPECT_EQ(client.Command({"DECR", "noncanonical-integer"}),
              "-ERR value is not an integer or out of range");
    EXPECT_EQ(client.Command({"INCR", "noncanonical-integer"}),
              "-ERR value is not an integer or out of range");
    EXPECT_EQ(client.Command({"INCRBYFLOAT", "floating", "0.1"}), Bulk("0.1"));
    EXPECT_EQ(client.Command({"INCRBYFLOAT", "floating", "1.25"}),
              Bulk("1.35"));
    EXPECT_EQ(client.Command({"SET", "overflow", "9223372036854775807"}),
              "+OK");
    EXPECT_EQ(client.Command({"INCRBY", "overflow", "1"}),
              "-ERR increment or decrement would overflow");
    EXPECT_EQ(client.Command({"INCRBYFLOAT", "floating", "inf"}),
              "-ERR increment would produce NaN or Infinity");

    EXPECT_EQ(client.Command({"MSETNX", cross_a, "one", cross_b, "two"}), ":1");
    EXPECT_EQ(
        client.Command({"MSETNX", cross_a, "changed", "new-msetnx", "new"}),
        ":0");
    EXPECT_EQ(client.Command({"GET", cross_a}), Bulk("one"));
    EXPECT_EQ(client.Command({"GET", "new-msetnx"}), "$-1");
    EXPECT_EQ(client.Command({"MSETNX", "duplicate-msetnx", "first",
                              "duplicate-msetnx", "last"}),
              ":1");
    EXPECT_EQ(client.Command({"GET", "duplicate-msetnx"}), Bulk("last"));

    EXPECT_EQ(client.Command({"SET", cross_a, "abcXYZ"}), "+OK");
    EXPECT_EQ(client.Command({"SET", cross_b, "123XYZ"}), "+OK");
    EXPECT_EQ(client.Command({"LCS", cross_a, cross_b}), Bulk("XYZ"));
    EXPECT_EQ(client.Command({"LCS", cross_a, cross_b, "LEN"}), ":3");
    EXPECT_EQ(client.Command({"LCS", cross_a, cross_b, "IDX", "MINMATCHLEN",
                              "2", "WITHMATCHLEN"}),
              "*4\r\n" + Bulk("matches") +
                  "\r\n*1\r\n*3\r\n*2\r\n:3\r\n:5\r\n*2\r\n:3\r\n:5\r\n:3\r\n" +
                  Bulk("len") + "\r\n:3");
    EXPECT_EQ(client.Command({"LCS", cross_a, cross_a}), Bulk("abcXYZ"));
    EXPECT_EQ(client.Command({"LCS", "missing-a", "missing-b"}), Bulk(""));
    EXPECT_EQ(client.Command({"RPUSH", "not-string", "x"}), ":1");
    EXPECT_EQ(client.Command({"LCS", "not-string", cross_b}),
              "-ERR The specified keys must contain string values");

    EXPECT_EQ(client.Command({"GETBIT", "missing-bitmap", "123"}), ":0");
    EXPECT_EQ(client.Command({"SETBIT", "bitmap", "0", "1"}), ":0");
    EXPECT_EQ(client.Command({"SETBIT", "bitmap", "9", "1"}), ":0");
    EXPECT_EQ(client.Command({"GETBIT", "bitmap", "0"}), ":1");
    EXPECT_EQ(client.Command({"GETBIT", "bitmap", "1"}), ":0");
    EXPECT_EQ(client.Command({"GET", "bitmap"}),
              Bulk(std::string("\x80\x40", 2)));
    EXPECT_EQ(client.Command({"PEXPIRE", "bitmap", "60000"}), ":1");
    EXPECT_EQ(client.Command({"SETBIT", "bitmap", "1", "1"}), ":0");
    EXPECT_GT(std::stoll(client.Command({"PTTL", "bitmap"}).substr(1)), 0);
    EXPECT_EQ(client.Command({"BITCOUNT", "bitmap"}), ":3");
    EXPECT_EQ(client.Command({"BITCOUNT", "bitmap", "-1", "-1"}), ":1");
    EXPECT_EQ(client.Command({"BITCOUNT", "bitmap", "0", "8", "BIT"}), ":2");
    EXPECT_EQ(client.Command({"BITCOUNT", "bitmap", "0", "1", "BYTE", "extra"}),
              "-ERR syntax error");
    EXPECT_EQ(client.Command({"BITPOS", "bitmap", "1"}), ":0");
    EXPECT_EQ(client.Command({"BITPOS", "bitmap", "0"}), ":2");
    EXPECT_EQ(client.Command({"BITPOS", "bitmap", "1", "1", "1"}), ":9");
    EXPECT_EQ(client.Command({"BITPOS", "bitmap", "1", "2", "7", "BIT"}),
              ":-1");
    EXPECT_EQ(
        client.Command({"BITPOS", "bitmap", "1", "0", "1", "BIT", "extra"}),
        "-ERR syntax error");
    EXPECT_EQ(client.Command({"BITPOS", "missing-bitmap", "0", "bad"}), ":0");
    EXPECT_EQ(client.Command({"SET", "empty-bitmap", ""}), "+OK");
    EXPECT_EQ(client.Command({"BITPOS", "empty-bitmap", "0"}), ":-1");
    EXPECT_EQ(client.Command({"GETBIT", "bitmap", "-1"}),
              "-ERR bit offset is not an integer or out of range");
    EXPECT_EQ(client.Command({"SETBIT", "bitmap", "2", "2"}),
              "-ERR bit is not an integer or out of range");

    EXPECT_EQ(
        client.Command({"BITFIELD", "field", "SET", "u4", "0", "15", "OVERFLOW",
                        "FAIL", "INCRBY", "u4", "0", "1", "GET", "u4", "0"}),
        "*3\r\n:0\r\n$-1\r\n:15");
    EXPECT_EQ(client.Command({"BITFIELD", "field", "OVERFLOW", "SAT", "INCRBY",
                              "u4", "0", "1"}),
              "*1\r\n:15");
    EXPECT_EQ(client.Command({"BITFIELD", "field", "SET", "i12", "4", "-2",
                              "GET", "i12", "4"}),
              "*2\r\n:0\r\n:-2");
    EXPECT_EQ(client.Command({"BITFIELD", "field", "SET", "u8", "#2", "42",
                              "GET", "u8", "16"}),
              "*2\r\n:0\r\n:42");
    EXPECT_EQ(client.Command({"BITFIELD_RO", "field", "GET", "u8", "#2"}),
              "*1\r\n:42");
    EXPECT_EQ(client.Command({"BITFIELD_RO", "field", "SET", "u8", "0", "1"}),
              "-ERR BITFIELD_RO only supports the GET subcommand");
    EXPECT_EQ(client.Command({"BITFIELD", "field", "GET", "u64", "0"}),
              "-ERR Invalid bitfield type. Use something like i16 u8. Note "
              "that u64 is not supported but i64 is.");
    EXPECT_EQ(
        client.Command({"GETBIT", "not-string", "0"}),
        "-WRONGTYPE Operation against a key holding the wrong kind of value");

    EXPECT_EQ(client.Command({"SET", bitmap_a, std::string("\x0f\xf0", 2)}),
              "+OK");
    EXPECT_EQ(client.Command({"SET", bitmap_b, std::string("\x33\x55", 2)}),
              "+OK");
    EXPECT_EQ(client.Command(
                  {"BITOP", "XOR", bitmap_destination, bitmap_a, bitmap_b}),
              ":2");
    EXPECT_EQ(client.Command({"GET", bitmap_destination}),
              Bulk(std::string("\x3c\xa5", 2)));
    EXPECT_EQ(client.Command({"BITOP", "AND", "bitmap-and-missing", bitmap_a,
                              "bitmap-missing-source"}),
              ":2");
    EXPECT_EQ(client.Command({"GET", "bitmap-and-missing"}),
              Bulk(std::string("\0\0", 2)));
    EXPECT_EQ(client.Command({"PEXPIRE", bitmap_destination, "60000"}), ":1");
    EXPECT_EQ(client.Command({"BITOP", "OR", bitmap_destination, bitmap_a}),
              ":2");
    EXPECT_EQ(client.Command({"PTTL", bitmap_destination}), ":-1");
    EXPECT_EQ(
        client.Command({"SET", "bitmap-in-place", std::string("\x0f", 1)}),
        "+OK");
    EXPECT_EQ(
        client.Command({"BITOP", "NOT", "bitmap-in-place", "bitmap-in-place"}),
        ":1");
    EXPECT_EQ(client.Command({"GET", "bitmap-in-place"}),
              Bulk(std::string("\xf0", 1)));
    EXPECT_EQ(client.Command({"SET", "bitmap-empty-destination", "old"}),
              "+OK");
    EXPECT_EQ(client.Command({"BITOP", "OR", "bitmap-empty-destination",
                              "bitmap-missing-source"}),
              ":0");
    EXPECT_EQ(client.Command({"GET", "bitmap-empty-destination"}), "$-1");
    EXPECT_EQ(client.Command({"RPUSH", "bitmap-list-destination", "old"}),
              ":1");
    EXPECT_EQ(
        client.Command({"BITOP", "OR", "bitmap-list-destination", bitmap_a}),
        ":2");
    EXPECT_EQ(client.Command({"GET", "bitmap-list-destination"}),
              Bulk(std::string("\x0f\xf0", 2)));
    EXPECT_EQ(
        client.Command({"BITOP", "NOT", "bitmap-bad", bitmap_a, bitmap_b}),
        "-ERR BITOP NOT must be called with a single source key.");
    EXPECT_EQ(
        client.Command({"BITOP", "OR", "bitmap-bad", bitmap_a, "not-string"}),
        "-WRONGTYPE Operation against a key holding the wrong kind of value");

    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"APPEND", "exec-string", "a"}), "+QUEUED");
    EXPECT_EQ(client.Command({"INCRBY", "exec-number", "4"}), "+QUEUED");
    EXPECT_EQ(client.Command({"MSETNX", "exec-nx-a", "a", "exec-nx-b", "b"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"LCS", cross_a, cross_b, "LEN"}), "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*4\r\n:1\r\n:4\r\n:1\r\n:3");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"MSETNX", "exec-duplicate", "first",
                              "exec-duplicate", "last"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"LCS", cross_a, cross_a, "LEN"}), "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*2\r\n:1\r\n:6");
    EXPECT_EQ(client.Command({"GET", "exec-duplicate"}), Bulk("last"));
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"SETBIT", "exec-bitmap", "0", "1"}), "+QUEUED");
    EXPECT_EQ(client.Command({"BITFIELD", "exec-bitmap", "INCRBY", "u4", "0",
                              "1", "GET", "u4", "0"}),
              "+QUEUED");
    EXPECT_EQ(
        client.Command({"BITOP", "XOR", "exec-bitop", bitmap_a, bitmap_b}),
        "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}), "*3\r\n:0\r\n*2\r\n:9\r\n:9\r\n:2");
    EXPECT_EQ(client.Command({"GET", "exec-bitop"}),
              Bulk(std::string("\x3c\xa5", 2)));
    EXPECT_EQ(client.Command({"SET", "exec-strict-expire", "safe"}), "+OK");
    EXPECT_EQ(client.Command({"MULTI"}), "+OK");
    EXPECT_EQ(client.Command({"EXPIRE", "exec-strict-expire", "00"}),
              "+QUEUED");
    EXPECT_EQ(client.Command({"GET", "exec-strict-expire"}), "+QUEUED");
    EXPECT_EQ(client.Command({"EXEC"}),
              "*2\r\n-ERR value is not an integer or out of range\r\n" +
                  Bulk("safe"));
    ASSERT_TRUE(WaitForDurability(client));
    server.Stop();
  }
  {
    ServerProcess server(g_lavik_binary, port, data_path, log_path, 2);
    RespClient client(port);
    EXPECT_EQ(client.Command({"GET", cross_a}), Bulk("abcXYZ"));
    EXPECT_EQ(client.Command({"GET", cross_b}), Bulk("123XYZ"));
    EXPECT_EQ(client.Command({"GET", "duplicate-msetnx"}), Bulk("last"));
    EXPECT_EQ(client.Command({"GET", "exec-string"}), Bulk("a"));
    EXPECT_EQ(client.Command({"GET", "exec-nx-b"}), Bulk("b"));
    EXPECT_EQ(client.Command({"GET", "bitmap"}),
              Bulk(std::string("\xc0\x40", 2)));
    EXPECT_EQ(client.Command({"BITFIELD_RO", "field", "GET", "u8", "#2"}),
              "*1\r\n:42");
    EXPECT_EQ(client.Command({"GET", bitmap_destination}),
              Bulk(std::string("\x0f\xf0", 2)));
    EXPECT_EQ(client.Command({"GET", "exec-bitop"}),
              Bulk(std::string("\x3c\xa5", 2)));
    EXPECT_GT(std::stoll(client.Command({"PTTL", "append"}).substr(1)), 0);
    server.Stop();
  }
}

}  // namespace

int main(int argc, char** argv) {
  if (argc >= 2) {
    g_lavik_binary = argv[1];
    for (int index = 1; index + 1 < argc; ++index) {
      argv[index] = argv[index + 1];
    }
    --argc;
  }
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
