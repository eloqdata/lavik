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

#pragma once

// Exercise the real RESP -> promotion -> incremental writer -> cold recovery
// path. The authoritative-image recovery suite deliberately remains separate.
#include <arpa/inet.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <exception>
#include <filesystem>
#include <fstream>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "lavik/storage/format.h"
#include "support/test_data_path.h"

namespace grouped_e2e {
using namespace lavik::storage;
using namespace std::chrono_literals;
inline std::string server_binary;

inline void Check(bool condition, std::string_view message) {
  if (!condition) throw std::runtime_error(std::string(message));
}

class PrivateDisk {
 public:
  explicit PrivateDisk(
      std::uint64_t bytes = 64 * kStorageBlockBytes,
      std::filesystem::path directory = lavik::test::TestDataDirectory()) {
    path_ = (directory / "lavik-grouped-write-XXXXXX").string();
    const int fd = ::mkstemp(path_.data());
    Check(fd >= 0, "mkstemp failed");
    Check(bytes <= INT64_MAX, "private disk size exceeds off_t");
    const int result = ::ftruncate(fd, static_cast<off_t>(bytes));
    ::close(fd);
    Check(result == 0, "sizing private disk failed");
  }
  ~PrivateDisk() {
    if (!preserve_on_failure_ ||
        (!::testing::Test::HasFailure() && std::uncaught_exceptions() == 0))
      ::unlink(path_.c_str());
  }
  const std::string& path() const { return path_; }
  void PreserveOnFailure() { preserve_on_failure_ = true; }

  // Only called with no live writer. Count identities rather than physical
  // copies, because shutdown may relocate committed transaction records.
  std::map<std::uint64_t, std::set<std::pair<std::uint64_t, unsigned>>>
  Auxiliaries(std::string_view wanted) const {
    std::ifstream input(path_, std::ios::binary);
    std::vector<std::byte> bytes(kStorageBlockBytes);
    std::map<std::uint64_t, std::set<std::pair<std::uint64_t, unsigned>>>
        result;
    while (input.read(reinterpret_cast<char*>(bytes.data()), bytes.size())) {
      BlockHeader block;
      if (!DecodeBlockHeaderPages(std::span<const std::byte, kBlockHeaderBytes>(
                                      bytes.data(), kBlockHeaderBytes),
                                  &block) ||
          (block.kind_ != BlockKind::kRecords &&
           block.kind_ != BlockKind::kTransaction)) {
        continue;
      }
      for (std::size_t offset = kBlockHeaderBytes;
           offset < block.committed_bytes_;) {
        RecordHeader record;
        std::string_view key;
        if (!DecodeRecordHeader(std::span(bytes).subspan(
                                    offset, block.committed_bytes_ - offset),
                                &record, &key)) {
          offset = (offset / kDirectIoAlignment + 1) * kDirectIoAlignment;
          continue;
        }
        if (record.auxiliary_group_ && key == wanted) {
          result[record.mutation_sequence_].emplace(record.group_prefix_,
                                                    record.group_prefix_bits_);
        }
        Check(record.total_disk_bytes_ != 0, "zero record size");
        offset += record.total_disk_bytes_;
      }
    }
    return result;
  }

 private:
  std::string path_;
  bool preserve_on_failure_ = false;
};

struct Reply {
  char kind_ = 0;
  std::string text_;
  std::vector<Reply> items_;
};

class Client {
 public:
  explicit Client(std::uint16_t port) {
    const auto until = std::chrono::steady_clock::now() + 20s;
    while (std::chrono::steady_clock::now() < until) {
      fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
      Check(fd_ >= 0, "socket failed");
      sockaddr_in address{.sin_family = AF_INET,
                          .sin_port = htons(port),
                          .sin_addr = {.s_addr = htonl(INADDR_LOOPBACK)}};
      if (::connect(fd_, reinterpret_cast<sockaddr*>(&address),
                    sizeof(address)) == 0) {
        timeval timeout{.tv_sec = 15};
        ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        return;
      }
      ::close(fd_);
      fd_ = -1;
      std::this_thread::sleep_for(10ms);
    }
    throw std::runtime_error("server startup timed out");
  }
  ~Client() {
    if (fd_ >= 0) ::close(fd_);
  }
  Reply Command(const std::vector<std::string>& args) {
    std::string wire = "*" + std::to_string(args.size()) + "\r\n";
    for (const auto& arg : args) {
      wire += "$" + std::to_string(arg.size()) + "\r\n";
      wire.append(arg).append("\r\n");
    }
    std::string_view remaining(wire);
    while (!remaining.empty()) {
      const auto n =
          ::send(fd_, remaining.data(), remaining.size(), MSG_NOSIGNAL);
      if (n < 0 && errno == EINTR) continue;
      Check(n > 0, "send failed");
      remaining.remove_prefix(n);
    }
    return Read();
  }
  void Durable() {
    const auto until = std::chrono::steady_clock::now() + 20s;
    while (std::chrono::steady_clock::now() < until) {
      if (Command({"INFO", "STATS"})
              .text_.find("storage_durability_pending:0\r\n") !=
          std::string::npos)
        return;
      std::this_thread::sleep_for(10ms);
    }
    throw std::runtime_error("durability timed out");
  }

 private:
  std::string Bytes(std::size_t size) {
    std::string bytes(size, '\0');
    for (std::size_t offset = 0; offset < size;) {
      const auto n = ::recv(fd_, bytes.data() + offset, size - offset, 0);
      if (n < 0 && errno == EINTR) continue;
      Check(n > 0, "response ended early");
      offset += n;
    }
    return bytes;
  }
  Reply Read() {
    std::string line;
    while (!line.ends_with("\r\n")) line += Bytes(1);
    Reply reply{.kind_ = line.front(),
                .text_ = line.substr(1, line.size() - 3)};
    if (reply.kind_ == '$' && reply.text_ != "-1") {
      reply.text_ = Bytes(std::stoull(reply.text_));
      Check(Bytes(2) == "\r\n", "malformed bulk reply");
    } else if (reply.kind_ == '*' && reply.text_ != "-1") {
      const auto count = std::stoull(reply.text_);
      for (std::size_t i = 0; i < count; ++i) reply.items_.push_back(Read());
    }
    return reply;
  }
  int fd_ = -1;
};

class Server {
 public:
  Server(const PrivateDisk& disk, unsigned workers = 2,
         std::string_view crash = {}, std::string_view fail_aux = {},
         bool pause_handoff = false, unsigned fail_aux_nth = 2,
         std::string_view max_memory = "1G", std::string_view load_rdb = {},
         std::string_view max_memory_clients = {}) {
    const int socket = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    Check(socket >= 0, "port socket failed");
    sockaddr_in address{.sin_family = AF_INET,
                        .sin_addr = {.s_addr = htonl(INADDR_LOOPBACK)}};
    Check(::bind(socket, reinterpret_cast<sockaddr*>(&address),
                 sizeof(address)) == 0,
          "port bind failed");
    socklen_t size = sizeof(address);
    Check(::getsockname(socket, reinterpret_cast<sockaddr*>(&address), &size) ==
              0,
          "port lookup failed");
    port_ = ntohs(address.sin_port);
    ::close(socket);
    log_ = disk.path() + ".log";
    dump_ = disk.path() + ".rdb";
    pid_ = ::fork();
    Check(pid_ >= 0, "fork failed");
    if (pid_ == 0) {
      ::unsetenv("LAVIK_CRASH_POINT");
      ::unsetenv("LAVIK_FAIL_GROUP_AUX_KEY");
      ::unsetenv("LAVIK_FAIL_GROUP_BATCH_KEY");
      if (pause_handoff) {
        ::setenv("LAVIK_REPLICATION_PAUSE_FULLSYNC_AFTER_HANDOFF_MS", "6000",
                 1);
      }
      if (!crash.empty())
        ::setenv("LAVIK_CRASH_POINT", std::string(crash).c_str(), 1);
      if (!fail_aux.empty()) {
        ::setenv("LAVIK_FAIL_GROUP_AUX_KEY", std::string(fail_aux).c_str(), 1);
        ::setenv("LAVIK_FAIL_GROUP_AUX_NTH",
                 std::to_string(fail_aux_nth).c_str(), 1);
      }
      const int log = ::open(log_.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0600);
      if (log < 0) ::_exit(127);
      ::dup2(log, STDOUT_FILENO);
      ::dup2(log, STDERR_FILENO);
      ::close(log);
      // Worker counts exercise handoffs and may exceed the CI host CPU count.
      std::vector<std::string> args{server_binary,
                                    "--bind",
                                    "127.0.0.1",
                                    "--port",
                                    std::to_string(port_),
                                    "--threads",
                                    std::to_string(workers),
                                    "--no-pin-workers",
                                    "--recv-buffers-per-worker",
                                    "8",
                                    "--max-memory",
                                    std::string(max_memory),
                                    "--flush-max-ms",
                                    "20",
                                    "--logtostderr",
                                    "--data-file",
                                    disk.path(),
                                    "--defrag-paused",
                                    "--rdb-dir",
                                    dump_.substr(0, dump_.rfind('/')),
                                    "--dbfilename",
                                    dump_.substr(dump_.rfind('/') + 1)};
      if (!load_rdb.empty()) {
        args.emplace_back("--load-rdb");
        args.emplace_back(load_rdb);
      }
      if (!max_memory_clients.empty()) {
        args.emplace_back("--maxmemory-clients");
        args.emplace_back(max_memory_clients);
      }
      std::vector<char*> argv;
      for (auto& arg : args) argv.push_back(arg.data());
      argv.push_back(nullptr);
      ::execv(argv.front(), argv.data());
      ::_exit(127);
    }
  }
  ~Server() {
    if (pid_ > 0) {
      ::kill(pid_, SIGKILL);
      ::waitpid(pid_, nullptr, 0);
    }
    if (!preserve_on_failure_ ||
        (!::testing::Test::HasFailure() && std::uncaught_exceptions() == 0)) {
      ::unlink(log_.c_str());
      ::unlink(dump_.c_str());
    }
  }
  std::uint16_t port() const { return port_; }
  std::string Log() const {
    std::ifstream input(log_);
    return {std::istreambuf_iterator<char>(input), {}};
  }
  void PreserveOnFailure() { preserve_on_failure_ = true; }
  // Observe termination without reaping: Wait() must still report the actual
  // exit status, and the fixture remains responsible for child cleanup.
  bool Running() const {
    if (pid_ <= 0) return false;
    siginfo_t info{};
    int result;
    do {
      result = ::waitid(P_PID, static_cast<id_t>(pid_), &info,
                        WEXITED | WNOHANG | WNOWAIT);
    } while (result < 0 && errno == EINTR);
    Check(result == 0, "checking server exit failed");
    return info.si_pid == 0;
  }
  void RecordDiagnostics(std::string_view reason, std::string_view info = {}) {
    std::ofstream output(log_, std::ios::app);
    output << "\nTEST DIAGNOSTICS " << reason << " pid=" << pid_ << '\n'
           << info << '\n';
    std::error_code error;
    const auto directory = "/proc/" + std::to_string(pid_) + "/task";
    for (const auto& task :
         std::filesystem::directory_iterator(directory, error)) {
      for (const char* name : {"wchan", "stack", "status"}) {
        const auto path = task.path() / name;
        std::ifstream input(path);
        output << path << ":\n";
        if (input)
          output << input.rdbuf();
        else
          output << "unavailable\n";
        output.clear();
        output << '\n';
      }
    }
  }
  int Wait(bool stop = false, std::chrono::seconds timeout = 25s) {
    if (stop) Check(::kill(pid_, SIGINT) == 0, "stop failed");
    const auto started = std::chrono::steady_clock::now();
    const auto until = started + timeout;
    bool diagnosed = false;
    while (std::chrono::steady_clock::now() < until) {
      int status = 0;
      if (::waitpid(pid_, &status, WNOHANG) == pid_) {
        pid_ = -1;
        Check(WIFEXITED(status), "server terminated by signal: " + Log());
        return WEXITSTATUS(status);
      }
      if (!diagnosed && std::chrono::steady_clock::now() - started >= 25s) {
        diagnosed = true;
        RecordDiagnostics("shutdown still pending after 25 seconds");
      }
      std::this_thread::sleep_for(10ms);
    }
    throw std::runtime_error("server exit timed out; evidence at " + log_ +
                             ": " + Log());
  }

 private:
  pid_t pid_ = -1;
  std::uint16_t port_ = 0;
  std::string log_;
  std::string dump_;
  bool preserve_on_failure_ = false;
};

}  // namespace grouped_e2e
