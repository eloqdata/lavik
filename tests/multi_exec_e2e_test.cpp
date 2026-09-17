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
  RespClient(RespClient&& other) noexcept
      : fd_(other.fd_),
        command_index_(other.command_index_),
        last_command_(std::move(other.last_command_)) {
    other.fd_ = -1;
  }
  RespClient& operator=(RespClient&&) = delete;
  ~RespClient() {
    if (fd_ >= 0) ::close(fd_);
  }

  void SendCommand(const std::vector<std::string_view>& args) {
    last_command_ = std::to_string(++command_index_);
    for (std::string_view arg : args) {
      last_command_.push_back(' ');
      last_command_.append(arg.substr(0, 80));
    }
    std::string request = "*" + std::to_string(args.size()) + "\r\n";
    for (std::string_view arg : args) {
      request += "$" + std::to_string(arg.size()) + "\r\n";
      request.append(arg);
      request += "\r\n";
    }
    SendAll(request);
  }

  std::string Command(const std::vector<std::string_view>& args) {
    SendCommand(args);
    return ReadReply();
  }

  std::string ReadPush() { return ReadReply(); }

 private:
  std::string ReadReply() {
    const std::string line = ReadLine();
    switch (line.empty() ? '\0' : line.front()) {
      case '+':
      case '-':
      case ':':
      case ',':
      case '(':
      case '#':
      case '_':
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
      case '=':
      case '!': {
        const std::size_t size = ParseLength(line);
        std::string payload(size + 2, '\0');
        ReadExact(payload.data(), payload.size());
        if (!payload.ends_with("\r\n")) {
          Fail("malformed RESP3 bulk terminator");
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
      case '%': {
        const std::size_t count = ParseLength(line);
        std::string reply = line;
        for (std::size_t i = 0; i < count * 2; ++i) {
          reply += "\r\n" + ReadReply();
        }
        return reply;
      }
      case '~':
      case '>': {
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
        Fail("recv failed while reading " + last_command_ + ": " +
             std::string(std::strerror(errno)));
      }
      if (received == 0) {
        Fail("server closed the connection while reading " + last_command_);
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
      if (response.size() > 4096) Fail("unexpectedly long RESP line");
    }
    response.resize(response.size() - 2);
    return response;
  }

  int fd_ = -1;
  std::uint64_t command_index_ = 0;
  std::string last_command_;
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
          "20",
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

void Expect(std::string_view actual, std::string_view expected,
            std::string_view operation) {
  if (actual != expected) {
    Fail(std::string(operation) + " returned '" + std::string(actual) +
         "', expected '" + std::string(expected) + "'");
  }
}

void ExpectContains(std::string_view actual, std::string_view expected,
                    std::string_view operation) {
  if (actual.find(expected) == std::string_view::npos) {
    Fail(std::string(operation) + " returned '" + std::string(actual) +
         "', expected it to contain '" + std::string(expected) + "'");
  }
}

void WaitForBusyScript(RespClient& client, std::string_view operation) {
  constexpr std::string_view kBusy = "-BUSY Redis is busy running a script";
  const auto deadline = std::chrono::steady_clock::now() + 10s;
  std::string reply;
  while (std::chrono::steady_clock::now() < deadline) {
    reply = client.Command({"PING"});
    if (reply.find(kBusy) != std::string::npos) return;
    if (reply != "+PONG") {
      Fail(std::string(operation) + " returned unexpected readiness reply '" +
           reply + "'");
    }
    std::this_thread::sleep_for(10ms);
  }
  Fail(std::string(operation) + " timed out waiting for BUSY; last reply '" +
       reply + "'");
}

std::string WaitForCommandContains(RespClient& client,
                                   const std::vector<std::string_view>& command,
                                   std::string_view expected,
                                   std::string_view operation) {
  const auto deadline = std::chrono::steady_clock::now() + 10s;
  std::string reply;
  while (std::chrono::steady_clock::now() < deadline) {
    reply = client.Command(command);
    if (reply.find(expected) != std::string::npos) return reply;
    std::this_thread::sleep_for(10ms);
  }
  Fail(std::string(operation) + " timed out; last reply '" + reply + "'");
}

std::string Bulk(std::string_view value) {
  return "$" + std::to_string(value.size()) + "\r\n" + std::string(value);
}

std::string BulkPayload(std::string_view reply) {
  const std::size_t separator = reply.find("\r\n");
  if (reply.empty() || reply.front() != '$' ||
      separator == std::string_view::npos) {
    Fail("expected bulk reply");
  }
  return std::string(reply.substr(separator + 2));
}

std::string ReadFile(const std::string& path) {
  std::ifstream input(path);
  return std::string(std::istreambuf_iterator<char>(input),
                     std::istreambuf_iterator<char>());
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "usage: multi_exec_e2e_test /path/to/lavik\n";
    return 1;
  }
  const std::string suffix = std::to_string(::getpid());
  const std::string data_path =
      lavik::test::TestDataPath("lavik-multiexec-" + suffix + ".data");
  const std::string log_path =
      lavik::test::TestDataPath("lavik-multiexec-" + suffix + ".log");
  (void)::unlink(data_path.c_str());
  (void)::unlink(log_path.c_str());

  int exit_code = 0;
  try {
    const std::uint16_t port = FindFreePort();
    CreateDataFile(data_path, 256ULL * 1024 * 1024);
    ServerProcess server(argv[1], port, data_path, log_path);
    RespClient client = Connect(port);
    Expect(client.Command({"PING"}), "+PONG", "PING");

    constexpr std::string_view invalid_function_library =
        "#!lua name=invalid_named_arg\n"
        "redis.register_function{function_name='invalid_named_arg', "
        "callback=function(keys, args) return 1 end, surprise=true}";
    ExpectContains(
        client.Command({"FUNCTION", "LOAD", invalid_function_library}),
        "unknown argument given to redis.register_function",
        "FUNCTION LOAD rejects unknown named argument");
    constexpr std::string_view case_insensitive_function_library =
        "#!lua name=named_arg_case\n"
        "redis.register_function{FUNCTION_NAME='named_arg_case', "
        "CALLBACK=function(keys, args) return args[1] end, "
        "FLAGS={'NO-WRITES'}}";
    Expect(
        client.Command({"FUNCTION", "LOAD", case_insensitive_function_library}),
        Bulk("named_arg_case"), "FUNCTION LOAD named argument case");
    Expect(client.Command({"FCALL_RO", "named_arg_case", "0", "works"}),
           Bulk("works"), "FCALL case-insensitive named arguments");
    Expect(client.Command({"FUNCTION", "DELETE", "named_arg_case"}), "+OK",
           "FUNCTION DELETE named argument fixture");

    constexpr std::string_view function_library =
        "#!lua name=lavik_test\n"
        "local prefix = 'fn:'\n"
        "redis.register_function('lavik_set', function(keys, args) "
        "return redis.call('SET', keys[1], prefix .. args[1]) end)\n"
        "redis.register_function{function_name='lavik_get', "
        "callback=function(keys, args) return redis.call('GET', keys[1]) "
        "end, description='read a value', flags={'no-writes'}}\n"
        "redis.register_function{function_name='lavik_wait', "
        "callback=function(keys, args) return redis.call('WAIT', args[1], "
        "args[2]) end, flags={'no-writes'}}\n"
        "redis.register_function{function_name='lavik_no_writes_set', "
        "callback=function(keys, args) return redis.call('SET', keys[1], "
        "args[1]) end, flags={'no-writes'}}\n"
        "redis.register_function{function_name='lavik_loop', "
        "callback=function(keys, args) while true do "
        "redis.call('GET', keys[1]) end end, flags={'no-writes'}}\n"
        "redis.register_function{function_name='lavik_globals', "
        "callback=function(keys, args) return type(KEYS)..':'..type(ARGV) "
        "end, flags={'no-writes'}}\n"
        "redis.register_function{function_name='lavik_json', "
        "callback=function(keys, args) local value=cjson.decode(args[1]); "
        "return cjson.encode({answer=value.number + 1}) end, "
        "flags={'no-writes'}}";
    Expect(client.Command({"FUNCTION", "LOAD", function_library}),
           Bulk("lavik_test"), "FUNCTION LOAD");
    Expect(client.Command({"FCALL", "lavik_set", "1", "function:key", "value"}),
           "+OK", "FCALL write function");
    Expect(client.Command({"FCALL", "lavik_get", "1", "function:key"}),
           Bulk("fn:value"), "FCALL read function");
    Expect(client.Command({"FCALL_RO", "lavik_get", "1", "function:key"}),
           Bulk("fn:value"), "FCALL_RO no-writes function");
    Expect(client.Command({"FCALL_RO", "lavik_wait", "0", "1", "0"}), ":0",
           "FCALL_RO WAIT returns immediately");
    Expect(client.Command({"FCALL_RO", "lavik_no_writes_set", "1",
                           "function:key", "blocked"}),
           "-ERR Write commands are not allowed from read-only scripts.",
           "FCALL_RO returns the direct read-only write error");
    ExpectContains(client.Command({"FCALL_RO", "lavik_globals", "0"}),
                   "Script attempted to access nonexistent global variable "
                   "'KEYS'",
                   "FCALL does not inherit EVAL globals");
    Expect(client.Command({"FCALL_RO", "lavik_json", "0", "{\"number\":41}"}),
           Bulk("{\"answer\":42}"), "FCALL cjson encode and decode");

    ExpectContains(
        client.Command(
            {"FCALL_RO", "lavik_set", "1", "function:key", "blocked"}),
        "Can not execute a script with write flag using *_ro command",
        "FCALL_RO rejects write function");
    const std::string function_list =
        client.Command({"FUNCTION", "LIST", "WITHCODE"});
    ExpectContains(function_list, "lavik_test", "FUNCTION LIST library");
    ExpectContains(function_list, "lavik_get", "FUNCTION LIST function");
    ExpectContains(function_list, "read a value", "FUNCTION LIST description");
    ExpectContains(function_list, "no-writes", "FUNCTION LIST flags");
    ExpectContains(function_list, "#!lua name=lavik_test",
                   "FUNCTION LIST WITHCODE");
    const std::string function_dump =
        BulkPayload(client.Command({"FUNCTION", "DUMP"}));
    Expect(client.Command({"FUNCTION", "DELETE", "lavik_test"}), "+OK",
           "FUNCTION DELETE");
    ExpectContains(client.Command({"FCALL", "lavik_get", "1", "function:key"}),
                   "Function not found", "FCALL after FUNCTION DELETE");
    Expect(client.Command({"FUNCTION", "RESTORE", function_dump}), "+OK",
           "FUNCTION RESTORE");
    Expect(client.Command({"FCALL", "lavik_get", "1", "function:key"}),
           Bulk("fn:value"), "FCALL after FUNCTION RESTORE");
    Expect(client.Command({"FUNCTION", "FLUSH", "ASYNC"}), "+OK",
           "FUNCTION FLUSH ASYNC");
    ExpectContains(client.Command({"FCALL", "lavik_get", "1", "function:key"}),
                   "Function not found", "FCALL after FUNCTION FLUSH");
    Expect(client.Command({"FUNCTION", "RESTORE", function_dump, "FLUSH"}),
           "+OK", "FUNCTION RESTORE FLUSH");
    ExpectContains(client.Command({"FUNCTION", "RESTORE", "broken"}),
                   "DUMP payload version or checksum are wrong",
                   "FUNCTION RESTORE invalid payload");

    constexpr std::string_view transaction_library =
        "#!lua name=transaction_library\n"
        "redis.register_function('transaction_set', function(keys, args) "
        "return redis.call('SET', keys[1], args[1]) end)";
    Expect(client.Command({"MULTI"}), "+OK", "MULTI FUNCTION LOAD");
    Expect(client.Command({"FUNCTION", "LOAD", transaction_library}), "+QUEUED",
           "queue FUNCTION LOAD");
    Expect(client.Command({"FCALL", "transaction_set", "1",
                           "function:transaction", "loaded-in-exec"}),
           "+QUEUED", "queue FCALL after FUNCTION LOAD");
    Expect(client.Command({"EXEC"}),
           "*2\r\n" + Bulk("transaction_library") + "\r\n+OK",
           "FUNCTION LOAD then FCALL inside EXEC");
    Expect(client.Command({"GET", "function:transaction"}),
           Bulk("loaded-in-exec"), "FCALL effect inside EXEC");

    Expect(client.Command({"WATCH", "function:watched"}), "+OK",
           "WATCH before FCALL");
    RespClient function_writer = Connect(port);
    Expect(function_writer.Command({"SET", "function:watched", "changed"}),
           "+OK", "invalidate FCALL watch");
    Expect(client.Command({"MULTI"}), "+OK", "MULTI watched FCALL");
    Expect(client.Command({"FCALL", "transaction_set", "1", "function:watched",
                           "must-not-run"}),
           "+QUEUED", "queue watched FCALL");
    Expect(client.Command({"EXEC"}), "*-1", "WATCH aborts FCALL");
    Expect(client.Command({"GET", "function:watched"}), Bulk("changed"),
           "aborted FCALL made no write");

    const std::string script_help = client.Command({"SCRIPT", "HELP"});
    ExpectContains(script_help, "*17\r\n+SCRIPT <subcommand>", "SCRIPT HELP");
    ExpectContains(script_help, "+LOAD <script>", "SCRIPT HELP LOAD");
    Expect(client.Command({"SCRIPT", "KILL"}),
           "-NOTBUSY No scripts in execution right now.",
           "SCRIPT KILL without active script");
    ExpectContains(client.Command({"CONFIG", "GET", "lua-time-limit"}),
                   "lua-time-limit", "CONFIG GET lua-time-limit");
    Expect(client.Command({"CONFIG", "SET", "busy-reply-threshold", "10"}),
           "+OK", "CONFIG SET busy-reply-threshold alias");

    // redis.call yields to the worker scheduler, allowing another connection
    // to request termination. The instruction hook then stops the script.
    RespClient looping_script = Connect(port);
    looping_script.SendCommand({"EVAL",
                                "while true do redis.call('GET',KEYS[1]) end",
                                "1", "lua:kill-loop"});
    WaitForBusyScript(client, "busy script command gate");
    // Connect only after the looping script has executed. SO_REUSEPORT can
    // otherwise accept these two new sockets out of connect order and place
    // the persistent killer on the same worker as later looping functions.
    RespClient script_killer = Connect(port);
    Expect(script_killer.Command({"SCRIPT", "KILL"}), "+OK", "SCRIPT KILL");
    ExpectContains(looping_script.ReadPush(),
                   "Script killed by user with SCRIPT KILL",
                   "killed script reply");
    Expect(script_killer.Command({"SCRIPT", "KILL"}),
           "-NOTBUSY No scripts in execution right now.",
           "SCRIPT KILL after termination");
    Expect(client.Command({"CONFIG", "SET", "lua-time-limit", "5000"}), "+OK",
           "CONFIG SET lua-time-limit");
    ExpectContains(ReadFile(log_path), "Slow script detected",
                   "busy script warning log");

    RespClient looping_function = Connect(port);
    looping_function.SendCommand(
        {"FCALL", "lavik_loop", "1", "lua:function-kill-loop"});
    const std::string function_stats =
        WaitForCommandContains(script_killer, {"FUNCTION", "STATS"},
                               "lavik_loop", "FUNCTION STATS name");
    ExpectContains(function_stats, "duration_ms", "FUNCTION STATS duration");
    ExpectContains(script_killer.Command({"SCRIPT", "KILL"}),
                   "You can only call FUNCTION KILL",
                   "SCRIPT KILL cannot kill function");
    Expect(script_killer.Command({"FUNCTION", "KILL"}), "+OK", "FUNCTION KILL");
    ExpectContains(looping_function.ReadPush(),
                   "Script killed by user with FUNCTION KILL",
                   "killed function reply");

    // SCRIPT/FUNCTION KILL must remain effective when Lua pcall catches the
    // first hook error and the script immediately retries from an outer loop.
    // Redis switches the kill hook to line granularity after the first catch.
    Expect(client.Command({"CONFIG", "SET", "lua-time-limit", "10"}), "+OK",
           "short Lua time limit for pcall kill");
    RespClient pcall_script = Connect(port);
    pcall_script.SendCommand(
        {"EVAL",
         "local f=function() while true do redis.call('PING') end end "
         "while true do pcall(f) end",
         "0"});
    WaitForBusyScript(client, "pcall script command gate");
    Expect(script_killer.Command({"SCRIPT", "KILL"}), "+OK",
           "SCRIPT KILL escapes pcall");
    ExpectContains(pcall_script.ReadPush(),
                   "Script killed by user with SCRIPT KILL",
                   "pcall cannot swallow SCRIPT KILL");

    const std::string pcall_function_library =
        "#!lua name=pcall_kill_library\n"
        "redis.register_function('pcall_kill', function() "
        "local f=function() while true do redis.call('PING') end end "
        "while true do pcall(f) end end)";
    Expect(client.Command({"FUNCTION", "LOAD", pcall_function_library}),
           Bulk("pcall_kill_library"), "load pcall kill function");
    RespClient pcall_function = Connect(port);
    pcall_function.SendCommand({"FCALL", "pcall_kill", "0"});
    WaitForBusyScript(client, "pcall function command gate");
    Expect(script_killer.Command({"FUNCTION", "KILL"}), "+OK",
           "FUNCTION KILL escapes pcall");
    ExpectContains(pcall_function.ReadPush(),
                   "Script killed by user with FUNCTION KILL",
                   "pcall cannot swallow FUNCTION KILL");
    Expect(client.Command({"CONFIG", "SET", "lua-time-limit", "5000"}), "+OK",
           "restore Lua time limit after pcall kill");

    RespClient dirty_script = Connect(port);
    dirty_script.SendCommand(
        {"EVAL",
         "redis.call('SET',KEYS[1],'written'); "
         "for i=1,20000 do redis.call('GET',KEYS[1]) end; return 'done'",
         "1", "lua:unkillable"});
    std::this_thread::sleep_for(50ms);
    ExpectContains(script_killer.Command({"SCRIPT", "KILL"}),
                   "-UNKILLABLE Sorry the script already executed write "
                   "commands against the dataset.",
                   "SCRIPT KILL refuses dirty script");
    Expect(dirty_script.ReadPush(), Bulk("done"), "dirty script completion");
    Expect(client.Command({"GET", "lua:unkillable"}), Bulk("written"),
           "SCRIPT KILL preserves dirty script write");

    // Lua scripts use a node-local SHA cache and execute their declared key
    // set under one transaction. EVALSHA reuses the cache populated by EVAL.
    constexpr std::string_view argv_script = "return ARGV[1]";
    Expect(client.Command({"EVAL", argv_script, "0", "hello"}), Bulk("hello"),
           "EVAL ARGV");
    Expect(client.Command({"EVAL", argv_script, "0", "eval-cache"}),
           Bulk("eval-cache"), "EVAL reuses worker registry closure");
    Expect(client.Command({"EVAL", "return redis.REDIS_VERSION", "0"}),
           Bulk("7.2.4"), "Lua Redis compatibility version");
    Expect(client.Command({"EVAL", "return redis.REDIS_VERSION_NUM", "0"}),
           ":459268", "Lua numeric Redis compatibility version");
    Expect(client.Command({"EVAL", "return redis.replicate_commands()", "0"}),
           ":1", "redis.replicate_commands compatibility no-op");
    ExpectContains(client.Command({"EVAL", "return redis.call()", "0"}),
                   "at least one argument",
                   "redis.call requires a command argument");
    ExpectContains(
        client.Command({"EVAL", "return redis.call('PING',{})", "0"}),
        "Lua redis lib command arguments must be strings or integers",
        "redis.call argument type validation");
    ExpectContains(client.Command({"EVAL", "return redis.sha1hex()", "0"}),
                   "wrong number of arguments", "redis.sha1hex arity");
    Expect(client.Command({"EVAL", "return redis.error_reply('')", "0"}),
           "-ERR", "redis.error_reply empty normalization");
    constexpr std::string_view seeded_random_script =
        "math.randomseed(ARGV[1]); return "
        "tostring(math.random())..':'..math.random(100)..':'..math.random(-5,"
        "5)";
    const std::string seeded_random =
        client.Command({"EVAL", seeded_random_script, "0", "10"});
    Expect(client.Command({"EVAL", seeded_random_script, "0", "10"}),
           seeded_random, "deterministic math.random seed");
    if (client.Command({"EVAL", seeded_random_script, "0", "20"}) ==
        seeded_random) {
      Fail("different math.random seeds returned the same sequence");
    }
    constexpr std::string_view lua_log_marker = "lavik-lua-log-e2e-marker";
    Expect(client.Command(
               {"EVAL", "redis.log(redis.LOG_WARNING,ARGV[1],42); return true",
                "0", lua_log_marker}),
           ":1", "redis.log warning");
    ExpectContains(ReadFile(log_path), std::string(lua_log_marker) + " 42",
                   "redis.log output");
    ExpectContains(client.Command({"EVAL", "redis.log(4,'bad')", "0"}),
                   "Invalid debug level", "redis.log level validation");
    ExpectContains(client.Command({"EVAL", "redis.log(redis.LOG_NOTICE)", "0"}),
                   "requires two arguments or more",
                   "redis.log arity validation");
    ExpectContains(client.Command({"EVAL", "redis.setresp(4)", "0"}),
                   "RESP version must be 2 or 3", "redis.setresp validation");
    Expect(client.Command({"HSET", "lua:resp3:hash", "field", "value"}), ":1",
           "RESP3 Lua hash seed");
    Expect(client.Command({"EVAL",
                           "redis.setresp(3); return "
                           "redis.call('HGETALL',KEYS[1]).map.field",
                           "1", "lua:resp3:hash"}),
           Bulk("value"), "RESP3 map to Lua");
    Expect(client.Command({"SADD", "lua:resp3:set", "member"}), ":1",
           "RESP3 Lua set seed");
    Expect(client.Command({"EVAL",
                           "redis.setresp(3); return "
                           "redis.call('SMEMBERS',KEYS[1]).set.member",
                           "1", "lua:resp3:set"}),
           ":1", "RESP3 set to Lua");
    Expect(client.Command({"ZADD", "lua:resp3:zset", "1.5", "member"}), ":1",
           "RESP3 Lua double seed");
    Expect(
        client.Command({"EVAL",
                        "redis.setresp(3); return "
                        "redis.call('ZSCORE',KEYS[1],'member').double ~= nil",
                        "1", "lua:resp3:zset"}),
        ":1", "RESP3 double to Lua");
    Expect(client.Command(
               {"EVAL",
                "redis.setresp(3); return redis.call('GET',KEYS[1]) == nil",
                "1", "lua:resp3:missing"}),
           ":1", "RESP3 null to Lua");
    Expect(client.Command({"EVAL", "return redis.call('GET',KEYS[1]) == false",
                           "1", "lua:resp2:missing"}),
           ":1", "RESP2 null remains false in Lua");

    RespClient resp3_client = Connect(port);
    ExpectContains(resp3_client.Command({"HELLO", "3"}), "%", "HELLO RESP3");
    Expect(resp3_client.Command({"EVAL", "return true", "0"}), ":1",
           "RESP2 script boolean to RESP3 client");
    Expect(resp3_client.Command({"EVAL", "redis.setresp(3); return true", "0"}),
           "#t", "RESP3 script boolean");
    Expect(resp3_client.Command({"EVAL", "return false", "0"}), "_",
           "RESP2 script false to RESP3 null");
    Expect(resp3_client.Command({"EVAL", "return {map={one='two'}}", "0"}),
           "%1\r\n" + Bulk("one") + "\r\n" + Bulk("two"), "Lua map to RESP3");
    Expect(resp3_client.Command({"EVAL", "return {set={one=true}}", "0"}),
           "~1\r\n" + Bulk("one"), "Lua set to RESP3");
    Expect(resp3_client.Command({"EVAL", "return {double=1.5}", "0"}), ",1.5",
           "Lua double to RESP3");
    Expect(resp3_client.Command(
               {"EVAL", "return {big_number='12345678901234567890'}", "0"}),
           "(12345678901234567890", "Lua big number to RESP3");
    Expect(resp3_client.Command(
               {"EVAL", "return {verbatim_string={format='txt',string='hey'}}",
                "0"}),
           "=7\r\ntxt:hey", "Lua verbatim string to RESP3");
    Expect(
        client.Command({"EVALSHA", "098e0f0d1448c0a81dafe820f66d460eb09263da",
                        "0", "cached"}),
        Bulk("cached"), "EVALSHA cached");
    // EVAL populates every worker-local cache before replying. New connections
    // may be accepted by any worker and must all observe the same node cache.
    for (unsigned i = 0; i < 16; ++i) {
      RespClient cache_client = Connect(port);
      Expect(cache_client.Command({"EVALSHA",
                                   "098e0f0d1448c0a81dafe820f66d460eb09263da",
                                   "0", "worker-cache"}),
             Bulk("worker-cache"), "EVALSHA worker-local cache");
    }
    Expect(client.Command(
               {"EVALSHA", "0000000000000000000000000000000000000000", "0"}),
           "-NOSCRIPT No matching script. Please use EVAL.", "EVALSHA missing");

    // SCRIPT LOAD shares EVAL's compiler/cache path but does not execute the
    // chunk. EXISTS preserves argument order, and FLUSH clears every worker's
    // local compiled-chunk index before releasing the backing storage.
    constexpr std::string_view loaded_script = "return 'loaded'";
    constexpr std::string_view loaded_sha =
        "b534286061d4b9e4026607613b95c06c06015ae8";
    Expect(client.Command({"SCRIPT", "LOAD", loaded_script}), Bulk(loaded_sha),
           "SCRIPT LOAD");
    Expect(client.Command({"SCRIPT", "LOAD", loaded_script}), Bulk(loaded_sha),
           "SCRIPT LOAD reuses worker registry closure");
    Expect(client.Command({"SCRIPT", "EXISTS", loaded_sha,
                           "0000000000000000000000000000000000000000"}),
           "*2\r\n:1\r\n:0", "SCRIPT EXISTS");
    Expect(client.Command({"EVALSHA", loaded_sha, "0"}), Bulk("loaded"),
           "EVALSHA after SCRIPT LOAD");
    constexpr std::string_view uppercase_loaded_sha =
        "B534286061D4B9E4026607613B95C06C06015AE8";
    Expect(client.Command({"EVALSHA", uppercase_loaded_sha, "0"}),
           Bulk("loaded"), "EVALSHA accepts uppercase SHA");
    Expect(client.Command(
               {"EVALSHA_RO", "b534286061D4B9E4026607613b95C06C06015aE8", "0"}),
           Bulk("loaded"), "EVALSHA_RO accepts mixed-case SHA");
    Expect(client.Command({"SCRIPT", "EXISTS", uppercase_loaded_sha}),
           "*1\r\n:0", "SCRIPT EXISTS preserves exact SHA matching");
    for (unsigned i = 0; i < 16; ++i) {
      RespClient cache_client = Connect(port);
      Expect(cache_client.Command({"EVALSHA", loaded_sha, "0"}), Bulk("loaded"),
             "SCRIPT LOAD worker-local compiled cache");
    }
    Expect(client.Command({"SCRIPT", "FLUSH", "ASYNC"}), "+OK",
           "SCRIPT FLUSH ASYNC");
    Expect(client.Command({"SCRIPT", "EXISTS", loaded_sha}), "*1\r\n:0",
           "SCRIPT EXISTS after FLUSH");
    ExpectContains(client.Command({"INFO", "MEMORY"}),
                   "number_of_cached_scripts:0",
                   "INFO MEMORY script count after flush");
    for (unsigned i = 0; i < 16; ++i) {
      RespClient cache_client = Connect(port);
      Expect(cache_client.Command({"EVALSHA", loaded_sha, "0"}),
             "-NOSCRIPT No matching script. Please use EVAL.",
             "SCRIPT FLUSH worker barrier");
    }
    ExpectContains(client.Command({"SCRIPT", "LOAD", "return +"}),
                   "Error compiling script", "SCRIPT LOAD compile error");
    ExpectContains(client.Command({"SCRIPT", "FLUSH", "INVALID"}),
                   "SCRIPT FLUSH only support SYNC|ASYNC option",
                   "SCRIPT FLUSH option validation");

    // EVAL also stores the compiled chunk after FLUSH.
    Expect(client.Command({"EVAL", argv_script, "0", "recompiled"}),
           Bulk("recompiled"), "EVAL compiled cache after FLUSH");
    ExpectContains(client.Command({"INFO", "MEMORY"}),
                   "number_of_cached_scripts:1",
                   "INFO MEMORY script count after EVAL");
    Expect(
        client.Command({"EVALSHA", "098e0f0d1448c0a81dafe820f66d460eb09263da",
                        "0", "compiled-cache"}),
        Bulk("compiled-cache"), "EVALSHA loads compiled chunk");

    // The connection remains on one worker: repeated EVALSHA calls exercise
    // the same registry closure while each coroutine receives fresh ARGV.
    for (unsigned i = 0; i < 128; ++i) {
      const std::string value = "registry-call-" + std::to_string(i);
      Expect(
          client.Command({"EVALSHA", "098e0f0d1448c0a81dafe820f66d460eb09263da",
                          "0", value}),
          Bulk(value), "EVALSHA reuses worker registry closure");
    }

    // Persistent workers must not let one invocation alter shared libraries
    // or globals for later scripts.
    ExpectContains(
        client.Command(
            {"EVAL", "math.abs=function() return 9 end; return 1", "0"}),
        "Attempt to modify a readonly table", "Lua shared library is readonly");
    Expect(client.Command({"EVAL", "return math.abs(-3)", "0"}), ":3",
           "Lua shared library remains intact");
    Expect(client.Command({"EVAL",
                           "local value=cjson.decode(ARGV[1]); "
                           "return {value.number,value.text}",
                           "0", "{\"number\":42,\"text\":\"hello\"}"}),
           "*2\r\n:42\r\n" + Bulk("hello"), "EVAL cjson decode");
    ExpectContains(
        client.Command(
            {"EVAL", "cjson.encode=function() return 'broken' end", "0"}),
        "Attempt to modify a readonly table", "Lua cjson table is readonly");
    Expect(client.Command({"EVAL", "return cjson.encode({answer=42})", "0"}),
           Bulk("{\"answer\":42}"), "Lua cjson table remains intact");
    ExpectContains(client.Command({"EVAL", "return missing_global", "0"}),
                   "Script attempted to access nonexistent global variable "
                   "'missing_global'",
                   "Lua global reads are protected");
    ExpectContains(client.Command({"EVAL", "return loadfile()", "0"}),
                   "Script attempted to access nonexistent global variable "
                   "'loadfile'",
                   "Lua unsafe globals are protected");
    ExpectContains(
        client.Command({"EVAL", "local g=getmetatable(_G); g.__index={}", "0"}),
        "Attempt to modify a readonly table",
        "Lua global protection metatable is readonly");
    ExpectContains(
        client.Command({"EVAL", "lavik_persistent_global=1; return 1", "0"}),
        "Attempt to modify a readonly table", "Lua global table is readonly");

    Expect(client.Command({"EVAL",
                           "redis.call('SET',KEYS[1],9007199254740991); "
                           "return redis.call('GET',KEYS[1])",
                           "1", "lua:number-precision"}),
           Bulk("9007199254740991"), "Lua numeric command argument precision");

    Expect(client.Command({"SET", "lua:ro", "seed"}), "+OK", "EVAL_RO seed");
    Expect(client.Command({"EVAL_RO", "return redis.call('WAIT',1,0)", "0"}),
           ":0", "EVAL_RO WAIT returns immediately");
    Expect(client.Command({"SADD", "lua:sort", "a", "b", "aa"}), ":3",
           "Lua deterministic SORT fixture");
    Expect(client.Command({"EVAL", "return redis.call('SORT',KEYS[1],'BY','_')",
                           "1", "lua:sort"}),
           "*3\r\n" + Bulk("a") + "\r\n" + Bulk("aa") + "\r\n" + Bulk("b"),
           "Lua SORT constant BY orders Set deterministically");
    ExpectContains(
        client.Command(
            {"EVAL",
             "return redis.call('SORT',KEYS[1],'BY','_','GET','#','GET','_:*')",
             "1", "lua:sort"}),
        "SORT BY/GET pattern keys are not supported inside MULTI",
        "Lua SORT rejects dynamically derived pattern keys");
    Expect(client.Command(
               {"EVAL_RO", "return redis.call('GET',KEYS[1])", "1", "lua:ro"}),
           Bulk("seed"), "EVAL_RO read");
    Expect(client.Command({"EVALSHA_RO",
                           "098e0f0d1448c0a81dafe820f66d460eb09263da", "0",
                           "readonly-cache"}),
           Bulk("readonly-cache"), "EVALSHA_RO shared compiled cache");
    Expect(
        client.Command({"EVAL_RO", "return redis.call('SET',KEYS[1],ARGV[1])",
                        "1", "lua:ro", "changed"}),
        "-ERR Write commands are not allowed from read-only scripts.",
        "EVAL_RO returns the direct read-only write error");
    Expect(client.Command({"GET", "lua:ro"}), Bulk("seed"),
           "EVAL_RO write made no change");
    Expect(client.Command(
               {"EVAL_RO",
                "if false then redis.call('SET',KEYS[1],'changed') end; "
                "return redis.call('GET',KEYS[1])",
                "1", "lua:ro"}),
           Bulk("seed"), "EVAL_RO checks executed commands, not source text");
    Expect(client.Command(
               {"EVALSHA_RO", "0000000000000000000000000000000000000000", "0"}),
           "-NOSCRIPT No matching script. Please use EVAL.",
           "EVALSHA_RO missing");

    constexpr std::string_view cross_script =
        "redis.call('MSET',KEYS[1],ARGV[1],KEYS[2],ARGV[2]); "
        "return redis.call('MGET',KEYS[1],KEYS[2])";
    Expect(client.Command({"EVAL", cross_script, "2", "{lua:a}:cross",
                           "{lua:b}:cross", "left", "right"}),
           "*2\r\n" + Bulk("left") + "\r\n" + Bulk("right"),
           "cross-shard EVAL");

    // Like Valkey and MULTI/EXEC, a later runtime error does not roll back
    // commands that the script already completed.
    constexpr std::string_view error_script =
        "redis.call('SET',KEYS[1],ARGV[1]); "
        "return redis.call('NOPE',KEYS[1])";
    ExpectContains(
        client.Command({"EVAL", error_script, "1", "lua:error", "retained"}),
        "Unknown Redis command called from script: NOPE", "EVAL runtime error");
    Expect(client.Command({"GET", "lua:error"}), Bulk("retained"),
           "EVAL keeps writes before runtime error");
    Expect(client.Command({"EVAL",
                           "local e=redis.pcall('NOPE',KEYS[1]); return e.err",
                           "1", "lua:error"}),
           Bulk("ERR Unknown Redis command called from script: NOPE"),
           "redis.pcall");
    ExpectContains(client.Command({"EVAL", "return redis.call('GET',ARGV[1])",
                                   "0", "lua:error"}),
                   "Script attempted to access an undeclared key",
                   "EVAL undeclared key");

    // Scripts retain their declared-key transaction while redis.call runs.
    // Blocking here could never be woken without breaking script atomicity,
    // so Valkey gives list and sorted-set blocking pops one immediate attempt.
    Expect(client.Command({"EVAL", "return redis.call('BLPOP',KEYS[1],0)", "1",
                           "lua:blocking:list"}),
           "$-1", "Lua BLPOP empty is immediate");
    Expect(client.Command({"EVAL", "return redis.call('BRPOP',KEYS[1],0)", "1",
                           "lua:blocking:list"}),
           "$-1", "Lua BRPOP empty is immediate");
    Expect(client.Command(
               {"EVAL", "return redis.call('BRPOPLPUSH',KEYS[1],KEYS[2],0)",
                "2", "{lua:blocking}:source", "{lua:blocking}:destination"}),
           "$-1", "Lua BRPOPLPUSH empty is immediate");
    Expect(client.Command({"EVAL",
                           "return redis.call('BLMOVE',KEYS[1],KEYS[2],"
                           "'LEFT','RIGHT',0)",
                           "2", "{lua:blocking}:source",
                           "{lua:blocking}:destination"}),
           "$-1", "Lua BLMOVE empty is immediate");
    Expect(client.Command({"EVAL", "return redis.call('BZPOPMIN',KEYS[1],0)",
                           "1", "lua:blocking:zset"}),
           "$-1", "Lua BZPOPMIN empty is immediate");
    Expect(client.Command({"EVAL", "return redis.call('BZPOPMAX',KEYS[1],0)",
                           "1", "lua:blocking:zset"}),
           "$-1", "Lua BZPOPMAX empty is immediate");

    Expect(client.Command({"RPUSH", "lua:blocking:list", "ready"}), ":1",
           "Lua blocking list seed");
    Expect(client.Command({"EVAL", "return redis.call('BLPOP',KEYS[1],0)", "1",
                           "lua:blocking:list"}),
           "*2\r\n" + Bulk("lua:blocking:list") + "\r\n" + Bulk("ready"),
           "Lua BLPOP consumes available value");
    Expect(client.Command({"ZADD", "lua:blocking:zset", "1", "ready"}), ":1",
           "Lua blocking zset seed");
    Expect(client.Command({"EVAL",
                           "local r=redis.call('BZPOPMIN',KEYS[1],0); "
                           "return {r[1],r[2],r[3]}",
                           "1", "lua:blocking:zset"}),
           "*3\r\n" + Bulk("lua:blocking:zset") + "\r\n" + Bulk("ready") +
               "\r\n" + Bulk("1"),
           "Lua BZPOPMIN consumes available member");

    // XREAD and XREADGROUP are already non-blocking when BLOCK is absent.
    // Unlike the pop commands, Valkey rejects an explicit BLOCK option rather
    // than silently treating it as an immediate timeout.
    Expect(client.Command({"EVAL",
                           "return redis.call('XREAD','STREAMS',KEYS[1],'$')",
                           "1", "lua:blocking:stream"}),
           "$-1", "Lua XREAD empty is immediate");
    ExpectContains(
        client.Command({"XADD", "lua:blocking:stream", "*", "field", "ready"}),
        "-", "Lua XREAD fixture");
    Expect(
        client.Command({"EVAL_RO",
                        "local r=redis.call('XREAD','STREAMS',KEYS[1],'0-0'); "
                        "return r[1][2][1][2][2]",
                        "1", "lua:blocking:stream"}),
        Bulk("ready"), "Lua XREAD returns available entry");
    ExpectContains(
        client.Command(
            {"EVAL",
             "return redis.call('XREAD','BLOCK',0,'STREAMS',KEYS[1],'$')", "1",
             "lua:blocking:stream"}),
        "xread command is not allowed with BLOCK option from scripts",
        "Lua XREAD rejects BLOCK");
    Expect(client.Command({"XGROUP", "CREATE", "lua:blocking:group-stream",
                           "lua-blocking-group", "0", "MKSTREAM"}),
           "+OK", "Lua XREADGROUP fixture");
    Expect(client.Command(
               {"EVAL",
                "return redis.call('XREADGROUP','GROUP','lua-blocking-group',"
                "'consumer','STREAMS',KEYS[1],'>')",
                "1", "lua:blocking:group-stream"}),
           "$-1", "Lua XREADGROUP empty is immediate");
    ExpectContains(client.Command({"XADD", "lua:blocking:group-stream", "*",
                                   "field", "ready"}),
                   "-", "Lua XREADGROUP data fixture");
    Expect(client.Command(
               {"EVAL",
                "local r=redis.call('XREADGROUP','GROUP',"
                "'lua-blocking-group','consumer','STREAMS',KEYS[1],'>'); "
                "return r[1][2][1][2][2]",
                "1", "lua:blocking:group-stream"}),
           Bulk("ready"), "Lua XREADGROUP returns available entry");
    ExpectContains(
        client.Command(
            {"EVAL",
             "return redis.call('XREADGROUP','GROUP','lua-blocking-group',"
             "'consumer','BLOCK',0,'STREAMS',KEYS[1],'>')",
             "1", "lua:blocking:group-stream"}),
        "xreadgroup command is not allowed with BLOCK option from scripts",
        "Lua XREADGROUP rejects BLOCK");

    // Lua is an ordering barrier inside EXEC and borrows the outer
    // transaction's locks, write receipts, txid, and commit record.
    constexpr std::string_view exec_order_script =
        "local v=redis.call('GET',KEYS[1]); "
        "redis.call('SET',KEYS[2],v..ARGV[1]); return v";
    Expect(client.Command({"MULTI"}), "+OK", "MULTI Lua ordering");
    Expect(client.Command({"SET", "{exec-lua:a}:source", "before"}), "+QUEUED",
           "queue before Lua");
    Expect(
        client.Command({"EVAL", exec_order_script, "2", "{exec-lua:a}:source",
                        "{exec-lua:b}:destination", "-lua"}),
        "+QUEUED", "queue Lua ordering barrier");
    Expect(client.Command({"GET", "{exec-lua:b}:destination"}), "+QUEUED",
           "queue after Lua");
    Expect(client.Command({"EXEC"}),
           "*3\r\n+OK\r\n" + Bulk("before") + "\r\n" + Bulk("before-lua"),
           "EXEC Lua ordering");
    Expect(client.Command(
               {"EVALSHA", "66e95443c434dfcc86d257faaa686b00a87a4afd", "2",
                "{exec-lua:a}:source", "{exec-lua:b}:destination", "-cached"}),
           Bulk("before"), "successful EXEC EVAL populated cache");
    Expect(client.Command({"GET", "{exec-lua:b}:destination"}),
           Bulk("before-cached"), "cached EXEC script ran");

    // A keyless EVALSHA is still executed at its exact queue position.
    Expect(client.Command({"MULTI"}), "+OK", "MULTI EVALSHA");
    Expect(
        client.Command({"EVALSHA", "098e0f0d1448c0a81dafe820f66d460eb09263da",
                        "0", "inside-exec"}),
        "+QUEUED", "queue EVALSHA");
    Expect(client.Command({"PING"}), "+QUEUED", "queue after EVALSHA");
    Expect(client.Command({"EXEC"}),
           "*2\r\n" + Bulk("inside-exec") + "\r\n+PONG", "EXEC EVALSHA");

    Expect(client.Command({"MULTI"}), "+OK", "MULTI SCRIPT LOAD");
    Expect(client.Command({"SCRIPT", "LOAD", loaded_script}), "+QUEUED",
           "queue SCRIPT LOAD");
    Expect(client.Command({"EVALSHA", loaded_sha, "0"}), "+QUEUED",
           "queue EVALSHA after SCRIPT LOAD");
    Expect(client.Command({"EXEC"}),
           "*2\r\n" + Bulk(loaded_sha) + "\r\n" + Bulk("loaded"),
           "EXEC SCRIPT LOAD before EVALSHA");

    Expect(client.Command({"MULTI"}), "+OK", "MULTI EVAL_RO write guard");
    Expect(
        client.Command({"EVAL_RO", "return redis.call('SET',KEYS[1],ARGV[1])",
                        "1", "lua:ro", "changed-in-exec"}),
        "+QUEUED", "queue EVAL_RO write");
    Expect(client.Command({"SET", "lua:after-ro-error", "continued"}),
           "+QUEUED", "queue after EVAL_RO write");
    const std::string exec_ro_error = client.Command({"EXEC"});
    ExpectContains(exec_ro_error,
                   "Write commands are not allowed from read-only scripts",
                   "EXEC EVAL_RO rejects write");
    ExpectContains(exec_ro_error, "+OK", "EXEC continues after EVAL_RO error");
    Expect(client.Command({"GET", "lua:ro"}), Bulk("seed"),
           "EXEC EVAL_RO write made no change");
    Expect(client.Command({"GET", "lua:after-ro-error"}), Bulk("continued"),
           "EXEC continued after EVAL_RO error");

    // Runtime errors retain earlier script effects and do not prevent later
    // queued commands from running.
    Expect(client.Command({"MULTI"}), "+OK", "MULTI Lua runtime error");
    Expect(client.Command({"EVAL", error_script, "1", "lua:exec-error",
                           "retained-in-exec"}),
           "+QUEUED", "queue failing Lua");
    Expect(client.Command({"SET", "lua:after-error", "continued"}), "+QUEUED",
           "queue after failing Lua");
    const std::string exec_lua_error = client.Command({"EXEC"});
    ExpectContains(exec_lua_error,
                   "Unknown Redis command called from script: NOPE",
                   "EXEC Lua runtime error");
    ExpectContains(exec_lua_error, "+OK", "EXEC continues after Lua error");
    Expect(client.Command({"GET", "lua:exec-error"}), Bulk("retained-in-exec"),
           "EXEC retains Lua write before error");
    Expect(client.Command({"GET", "lua:after-error"}), Bulk("continued"),
           "EXEC runs command after Lua error");

    // NOSCRIPT and movable-key argument errors are runtime errors in the EXEC
    // array, not queue-time errors that doom the transaction.
    Expect(client.Command({"MULTI"}), "+OK", "MULTI Lua runtime validation");
    Expect(client.Command(
               {"EVALSHA", "0000000000000000000000000000000000000000", "0"}),
           "+QUEUED", "queue missing EVALSHA");
    Expect(client.Command({"EVAL", "return 1", "not-a-number"}), "+QUEUED",
           "queue invalid EVAL numkeys");
    Expect(client.Command({"EVAL", "return +", "0"}), "+QUEUED",
           "queue Lua compile error");
    Expect(client.Command({"SET", "lua:after-validation", "ran"}), "+QUEUED",
           "queue after invalid Lua commands");
    const std::string exec_lua_validation = client.Command({"EXEC"});
    ExpectContains(exec_lua_validation,
                   "NOSCRIPT No matching script. Please use EVAL.",
                   "EXEC EVALSHA NOSCRIPT");
    ExpectContains(exec_lua_validation,
                   "value is not an integer or out of range",
                   "EXEC EVAL numkeys error");
    ExpectContains(exec_lua_validation, "Error compiling script",
                   "EXEC EVAL compile error");
    Expect(client.Command({"GET", "lua:after-validation"}), Bulk("ran"),
           "EXEC continues after Lua validation errors");

    // Basic transaction: replies in queue order, later commands see earlier
    // effects.
    Expect(client.Command({"MULTI"}), "+OK", "MULTI");
    Expect(client.Command({"SET", "k", "1"}), "+QUEUED", "queue SET");
    Expect(client.Command({"INCR", "k"}), "+QUEUED", "queue INCR");
    Expect(client.Command({"GET", "k"}), "+QUEUED", "queue GET");
    Expect(client.Command({"PING"}), "+QUEUED", "queue PING");
    Expect(client.Command({"EXEC"}),
           "*4\r\n+OK\r\n:2\r\n" + Bulk("2") + "\r\n+PONG", "EXEC basic");
    Expect(client.Command({"GET", "k"}), Bulk("2"), "state after EXEC");

    // Cross-shard transaction with multi-key commands inside.
    Expect(client.Command({"MULTI"}), "+OK", "MULTI cross");
    Expect(client.Command({"SET", "xa", "av"}), "+QUEUED", "queue xa");
    Expect(client.Command({"SET", "xb", "bv"}), "+QUEUED", "queue xb");
    Expect(client.Command({"SET", "xc", "cv"}), "+QUEUED", "queue xc");
    Expect(client.Command({"MGET", "xc", "xa", "nope", "xb"}), "+QUEUED",
           "queue MGET");
    Expect(client.Command({"EXISTS", "xa", "xa", "nope"}), "+QUEUED",
           "queue EXISTS");
    Expect(client.Command({"DEL", "xa", "xb", "nope"}), "+QUEUED", "queue DEL");
    Expect(client.Command({"EXEC"}),
           "*6\r\n+OK\r\n+OK\r\n+OK\r\n*4\r\n" + Bulk("cv") + "\r\n" +
               Bulk("av") + "\r\n$-1\r\n" + Bulk("bv") + "\r\n:2\r\n:2",
           "EXEC cross-shard");
    Expect(client.Command({"MGET", "xa", "xb", "xc"}),
           "*3\r\n$-1\r\n$-1\r\n" + Bulk("cv"), "state after cross EXEC");

    Expect(client.Command({"MSET", "ua", "1", "ub", "2"}), "+OK",
           "UNLINK transaction seed");
    Expect(client.Command({"MULTI"}), "+OK", "MULTI unlink");
    Expect(client.Command({"UNLINK", "ua", "ub", "missing"}), "+QUEUED",
           "queue UNLINK");
    Expect(client.Command({"EXEC"}), "*1\r\n:2", "EXEC unlink");
    Expect(client.Command({"MGET", "ua", "ub"}), "*2\r\n$-1\r\n$-1",
           "state after EXEC UNLINK");

    Expect(client.Command({"SELECT", "14"}), "+OK", "RANDOMKEY transaction db");
    Expect(client.Command({"SET", "transaction-random", "v"}), "+OK",
           "RANDOMKEY transaction seed");
    Expect(client.Command({"MULTI"}), "+OK", "MULTI RANDOMKEY");
    Expect(client.Command({"RANDOMKEY"}), "+QUEUED", "queue RANDOMKEY");
    Expect(client.Command({"TOUCH", "transaction-random", "transaction-random",
                           "missing"}),
           "+QUEUED", "queue TOUCH");
    Expect(client.Command({"EXEC"}),
           "*2\r\n" + Bulk("transaction-random") + "\r\n:2",
           "EXEC RANDOMKEY and TOUCH");
    Expect(client.Command({"SELECT", "0"}), "+OK",
           "RANDOMKEY transaction return db0");

    Expect(client.Command({"SET", "transaction-copy-source", "value"}), "+OK",
           "COPY transaction seed");
    Expect(client.Command({"MULTI"}), "+OK", "MULTI COPY");
    Expect(client.Command({"COPY", "transaction-copy-source",
                           "transaction-copy-destination", "DB", "3"}),
           "+QUEUED", "queue cross-db COPY");
    Expect(client.Command(
               {"PEXPIREAT", "transaction-copy-source", "4102444800000"}),
           "+QUEUED", "queue PEXPIREAT");
    Expect(client.Command({"PEXPIRETIME", "transaction-copy-source"}),
           "+QUEUED", "queue PEXPIRETIME");
    Expect(client.Command({"EXEC"}), "*3\r\n:1\r\n:1\r\n:4102444800000",
           "EXEC COPY and absolute expiration");
    Expect(client.Command({"SELECT", "3"}), "+OK",
           "COPY transaction destination DB");
    Expect(client.Command({"GET", "transaction-copy-destination"}),
           Bulk("value"), "COPY transaction destination value");
    Expect(client.Command({"SELECT", "0"}), "+OK",
           "COPY transaction return db0");

    Expect(client.Command({"MULTI"}), "+OK", "MULTI invalid COPY");
    Expect(client.Command(
               {"COPY", "transaction-copy-source", "bad-copy", "DB", "bad"}),
           "+QUEUED", "queue invalid COPY");
    Expect(client.Command({"SET", "after-invalid-copy", "ok"}), "+QUEUED",
           "queue after invalid COPY");
    Expect(client.Command({"EXEC"}),
           "*2\r\n-ERR value is not an integer or out of range\r\n+OK",
           "EXEC invalid COPY remains runtime error");
    Expect(client.Command({"GET", "after-invalid-copy"}), Bulk("ok"),
           "command after invalid COPY ran");

    // Nested MULTI errors without dooming the transaction.
    Expect(client.Command({"MULTI"}), "+OK", "MULTI nested");
    Expect(client.Command({"MULTI"}), "-ERR MULTI calls can not be nested",
           "nested MULTI");
    Expect(client.Command({"SET", "n", "v"}), "+QUEUED", "queue after nested");
    Expect(client.Command({"EXEC"}), "*1\r\n+OK", "EXEC after nested");

    // Queue-time errors doom EXEC.
    Expect(client.Command({"MULTI"}), "+OK", "MULTI unknown");
    Expect(client.Command({"BOGUS", "x"}), "-ERR unknown command 'BOGUS'",
           "unknown queued");
    Expect(client.Command({"SET", "u", "v"}), "+QUEUED", "queue after bogus");
    Expect(client.Command({"EXEC"}),
           "-EXECABORT Transaction discarded because of previous errors.",
           "EXECABORT unknown");
    Expect(client.Command({"GET", "u"}), "$-1", "doomed EXEC ran nothing");

    Expect(client.Command({"MULTI"}), "+OK", "MULTI arity");
    Expect(client.Command({"GET"}),
           "-ERR wrong number of arguments for 'get' command", "arity queued");
    Expect(client.Command({"EXEC"}),
           "-EXECABORT Transaction discarded because of previous errors.",
           "EXECABORT arity");

    Expect(client.Command({"MULTI"}), "+OK", "MULTI flushdb");
    Expect(client.Command({"FLUSHDB"}),
           "-ERR flushdb is not allowed in transactions", "queue FLUSHDB");
    Expect(client.Command({"EXEC"}),
           "-EXECABORT Transaction discarded because of previous errors.",
           "EXECABORT flushdb");

    Expect(client.Command({"MULTI"}), "+OK", "MULTI flushall");
    Expect(client.Command({"FLUSHALL"}),
           "-ERR flushall is not allowed in transactions", "queue FLUSHALL");
    Expect(client.Command({"EXEC"}),
           "-EXECABORT Transaction discarded because of previous errors.",
           "EXECABORT flushall");

    Expect(client.Command({"MULTI"}), "+OK", "MULTI Sentinel isolation");
    Expect(client.Command({"CONFIG", "REWRITE"}), "+QUEUED",
           "queue Sentinel CONFIG REWRITE");
    Expect(client.Command({"PING"}),
           "-ERR Sentinel management commands must be queued alone",
           "reject ordinary command after Sentinel command");
    Expect(client.Command({"EXEC"}),
           "-EXECABORT Transaction discarded because of previous errors.",
           "EXECABORT mixed Sentinel transaction");

    // DISCARD clears everything.
    Expect(client.Command({"MULTI"}), "+OK", "MULTI discard");
    Expect(client.Command({"SET", "d", "1"}), "+QUEUED", "queue discard SET");
    Expect(client.Command({"DISCARD"}), "+OK", "DISCARD");
    Expect(client.Command({"EXEC"}), "-ERR EXEC without MULTI",
           "EXEC after DISCARD");
    Expect(client.Command({"GET", "d"}), "$-1", "discarded write");
    Expect(client.Command({"DISCARD"}), "-ERR DISCARD without MULTI",
           "stray DISCARD");

    // Runtime errors reply inline; later commands still run.
    Expect(client.Command({"MULTI"}), "+OK", "MULTI runtime");
    Expect(client.Command({"SET", "rx", "abc"}), "+QUEUED", "queue bad SET");
    Expect(client.Command({"INCR", "rx"}), "+QUEUED", "queue bad INCR");
    Expect(client.Command({"SET", "ry", "1"}), "+QUEUED", "queue good SET");
    Expect(client.Command({"EXEC"}),
           "*3\r\n+OK\r\n-ERR value is not an integer or out of range\r\n+OK",
           "EXEC runtime error inline");
    Expect(client.Command({"GET", "ry"}), Bulk("1"), "post-error write ran");

    // Empty transaction.
    Expect(client.Command({"MULTI"}), "+OK", "MULTI empty");
    Expect(client.Command({"EXEC"}), "*0", "empty EXEC");

    // SELECT inside MULTI moves later queued commands to the new database
    // and sticks after EXEC.
    Expect(client.Command({"MULTI"}), "+OK", "MULTI select");
    Expect(client.Command({"SELECT", "1"}), "+QUEUED", "queue SELECT");
    Expect(client.Command({"SET", "sk", "sv"}), "+QUEUED", "queue db1 SET");
    Expect(client.Command({"EXEC"}), "*2\r\n+OK\r\n+OK", "EXEC select");
    Expect(client.Command({"GET", "sk"}), Bulk("sv"), "db1 read after EXEC");
    Expect(client.Command({"SELECT", "0"}), "+OK", "back to db0");
    Expect(client.Command({"GET", "sk"}), "$-1", "db0 unaffected");

    // Transactions spanning databases: SELECT inside MULTI moves later
    // commands to another database and everything commits atomically.
    Expect(client.Command({"MULTI"}), "+OK", "MULTI cross-db");
    Expect(client.Command({"SET", "c0", "zero"}), "+QUEUED", "queue db0 SET");
    Expect(client.Command({"SELECT", "1"}), "+QUEUED", "queue SELECT 1");
    Expect(client.Command({"SET", "c1", "one"}), "+QUEUED", "queue db1 SET");
    Expect(client.Command({"GET", "c1"}), "+QUEUED", "queue db1 GET");
    Expect(client.Command({"EXEC"}),
           "*4\r\n+OK\r\n+OK\r\n+OK\r\n" + Bulk("one"), "cross-db EXEC");
    Expect(client.Command({"GET", "c1"}), Bulk("one"), "db1 sticky read");
    Expect(client.Command({"GET", "c0"}), "$-1", "c0 not in db1");
    Expect(client.Command({"SELECT", "0"}), "+OK", "back to db0");
    Expect(client.Command({"GET", "c0"}), Bulk("zero"), "db0 read");
    Expect(client.Command({"GET", "c1"}), "$-1", "c1 not in db0");

    // FLUSHALL clears every logical database without changing the connection's
    // selected database.
    Expect(client.Command({"SET", "flushall-db0", "zero"}), "+OK",
           "FLUSHALL seed db0");
    Expect(client.Command({"SELECT", "1"}), "+OK", "FLUSHALL select db1");
    Expect(client.Command({"SET", "flushall-db1", "one"}), "+OK",
           "FLUSHALL seed db1");
    Expect(client.Command({"SELECT", "15"}), "+OK", "FLUSHALL select db15");
    Expect(client.Command({"SET", "flushall-db15", "fifteen"}), "+OK",
           "FLUSHALL seed db15");
    Expect(client.Command({"FLUSHALL"}), "+OK", "FLUSHALL");
    Expect(client.Command({"DBSIZE"}), ":0", "FLUSHALL db15 empty");
    Expect(client.Command({"SELECT", "1"}), "+OK",
           "FLUSHALL verify select db1");
    Expect(client.Command({"DBSIZE"}), ":0", "FLUSHALL db1 empty");
    Expect(client.Command({"SELECT", "0"}), "+OK",
           "FLUSHALL verify select db0");
    Expect(client.Command({"DBSIZE"}), ":0", "FLUSHALL db0 empty");
    Expect(client.Command({"FLUSHALL", "INVALID"}), "-ERR syntax error",
           "FLUSHALL invalid mode");
    Expect(client.Command({"FLUSHALL", "SYNC", "EXTRA"}),
           "-ERR wrong number of arguments for 'flushall' command",
           "FLUSHALL wrong arity");
    Expect(client.Command({"SET", "flushall-async-db0", "zero"}), "+OK",
           "FLUSHALL ASYNC seed db0");
    Expect(client.Command({"SELECT", "1"}), "+OK", "FLUSHALL ASYNC select db1");
    Expect(client.Command({"SET", "flushall-async-db1", "one"}), "+OK",
           "FLUSHALL ASYNC seed db1");
    Expect(client.Command({"FLUSHALL", "ASYNC"}), "+OK", "FLUSHALL ASYNC");
    Expect(client.Command({"DBSIZE"}), ":0", "FLUSHALL ASYNC db1 empty");
    Expect(client.Command({"SELECT", "0"}), "+OK",
           "FLUSHALL ASYNC verify select db0");
    Expect(client.Command({"DBSIZE"}), ":0", "FLUSHALL ASYNC db0 empty");
    Expect(client.Command({"SELECT", "1"}), "+OK",
           "seed INFO keyspace select db1");
    Expect(client.Command({"SET", "info-db1", "value"}), "+OK",
           "seed INFO keyspace db1");
    Expect(client.Command({"SELECT", "0"}), "+OK",
           "return to db0 after INFO seed");

    // ---- WATCH / UNWATCH ----
    RespClient other = Connect(port);

    // WATCH aborts before the Lua body starts. In particular, an aborted EVAL
    // must not populate the node-local script cache.
    constexpr std::string_view watched_script =
        "return 'watch-should-not-cache'";
    Expect(client.Command({"SET", "watch-lua", "base"}), "+OK",
           "WATCH Lua seed");
    Expect(client.Command({"WATCH", "watch-lua"}), "+OK", "WATCH Lua");
    Expect(client.Command({"MULTI"}), "+OK", "MULTI watched Lua");
    Expect(client.Command({"EVAL", watched_script, "0"}), "+QUEUED",
           "queue watched Lua");
    Expect(other.Command({"SET", "watch-lua", "changed"}), "+OK",
           "invalidate watched Lua");
    Expect(client.Command({"EXEC"}), "*-1", "WATCH aborts Lua EXEC");
    Expect(client.Command(
               {"EVALSHA", "30f2362e36ab75397d4f6d756a73a1254c57464a", "0"}),
           "-NOSCRIPT No matching script. Please use EVAL.",
           "aborted EVAL did not cache script");

    // A watched key may also be one of the Lua KEYS. The EXEC exclusive hold
    // covers the watch fence and the script writes it only after validation.
    Expect(client.Command({"SET", "watch-lua-key", "base"}), "+OK",
           "WATCH declared Lua key seed");
    Expect(client.Command({"WATCH", "watch-lua-key"}), "+OK",
           "WATCH declared Lua key");
    Expect(client.Command({"MULTI"}), "+OK", "MULTI declared watched Lua key");
    Expect(client.Command({"EVAL",
                           "redis.call('SET',KEYS[1],ARGV[1]); return ARGV[1]",
                           "1", "watch-lua-key", "updated"}),
           "+QUEUED", "queue declared watched Lua key");
    Expect(client.Command({"EXEC"}), "*1\r\n" + Bulk("updated"),
           "EXEC declared watched Lua key");
    Expect(client.Command({"GET", "watch-lua-key"}), Bulk("updated"),
           "declared watched Lua key updated");

    // Unmodified watch: EXEC proceeds.
    Expect(client.Command({"SET", "w1", "base"}), "+OK", "watch seed");
    Expect(client.Command({"WATCH", "w1"}), "+OK", "WATCH clean");
    Expect(client.Command({"MULTI"}), "+OK", "MULTI watch clean");
    Expect(client.Command({"SET", "w1", "mine"}), "+QUEUED", "queue clean");
    Expect(client.Command({"EXEC"}), "*1\r\n+OK", "EXEC clean watch");
    Expect(client.Command({"GET", "w1"}), Bulk("mine"), "clean watch wrote");

    // Another client's write aborts the transaction; nothing runs.
    Expect(client.Command({"WATCH", "w1"}), "+OK", "WATCH conflicted");
    Expect(other.Command({"SET", "w1", "theirs"}), "+OK", "outside write");
    Expect(client.Command({"MULTI"}), "+OK", "MULTI conflicted");
    Expect(client.Command({"SET", "w1", "mine2"}), "+QUEUED",
           "queue conflicted");
    Expect(client.Command({"EXEC"}), "*-1", "EXEC aborted by write");
    Expect(client.Command({"GET", "w1"}), Bulk("theirs"),
           "aborted EXEC ran nothing");

    // Watches are consumed by EXEC: the next transaction is unaffected.
    Expect(client.Command({"MULTI"}), "+OK", "MULTI after abort");
    Expect(client.Command({"SET", "w1", "fresh"}), "+QUEUED",
           "queue after abort");
    Expect(client.Command({"EXEC"}), "*1\r\n+OK", "watches consumed");

    // Setting the value back does not un-mark: version semantics.
    Expect(client.Command({"WATCH", "w1"}), "+OK", "WATCH set-back");
    Expect(other.Command({"SET", "w1", "detour"}), "+OK", "detour write");
    Expect(other.Command({"SET", "w1", "fresh"}), "+OK", "restore write");
    Expect(client.Command({"MULTI"}), "+OK", "MULTI set-back");
    Expect(client.Command({"SET", "w1", "x"}), "+QUEUED", "queue set-back");
    Expect(client.Command({"EXEC"}), "*-1", "set-back still aborts");

    // Deleting a watched key aborts; creating a watched-missing key aborts.
    Expect(client.Command({"WATCH", "w1"}), "+OK", "WATCH for DEL");
    Expect(other.Command({"DEL", "w1"}), ":1", "outside DEL");
    Expect(client.Command({"MULTI"}), "+OK", "MULTI del");
    Expect(client.Command({"PING"}), "+QUEUED", "queue del ping");
    Expect(client.Command({"EXEC"}), "*-1", "DEL aborts watcher");

    Expect(client.Command({"WATCH", "wmissing"}), "+OK", "WATCH missing");
    Expect(other.Command({"SET", "wmissing", "born"}), "+OK",
           "create watched-missing");
    Expect(client.Command({"MULTI"}), "+OK", "MULTI created");
    Expect(client.Command({"PING"}), "+QUEUED", "queue created ping");
    Expect(client.Command({"EXEC"}), "*-1", "creation aborts watcher");

    // A write by this connection before MULTI also aborts.
    Expect(client.Command({"WATCH", "wmissing"}), "+OK", "WATCH self");
    Expect(client.Command({"SET", "wmissing", "self"}), "+OK", "self write");
    Expect(client.Command({"MULTI"}), "+OK", "MULTI self");
    Expect(client.Command({"PING"}), "+QUEUED", "queue self ping");
    Expect(client.Command({"EXEC"}), "*-1", "self write aborts");

    // UNWATCH forgives earlier modifications.
    Expect(client.Command({"WATCH", "w1"}), "+OK", "WATCH unwatch");
    Expect(other.Command({"SET", "w1", "poke"}), "+OK", "poke");
    Expect(client.Command({"UNWATCH"}), "+OK", "UNWATCH");
    Expect(client.Command({"MULTI"}), "+OK", "MULTI unwatched");
    Expect(client.Command({"SET", "w1", "won"}), "+QUEUED", "queue unwatched");
    Expect(client.Command({"EXEC"}), "*1\r\n+OK", "UNWATCH cleared");

    // WATCH inside MULTI is refused without dooming the transaction.
    Expect(client.Command({"MULTI"}), "+OK", "MULTI watch-inside");
    Expect(client.Command({"WATCH", "w1"}),
           "-ERR WATCH inside MULTI is not allowed", "WATCH inside MULTI");
    Expect(client.Command({"PING"}), "+QUEUED", "queue after watch error");
    Expect(client.Command({"EXEC"}), "*1\r\n+PONG", "EXEC after watch error");

    // Passive expiration invalidates like a write.
    Expect(client.Command({"SET", "wexp", "v", "PX", "80"}), "+OK",
           "expiring seed");
    Expect(client.Command({"WATCH", "wexp"}), "+OK", "WATCH expiring");
    std::this_thread::sleep_for(300ms);
    Expect(client.Command({"MULTI"}), "+OK", "MULTI expired");
    Expect(client.Command({"PING"}), "+QUEUED", "queue expired ping");
    Expect(client.Command({"EXEC"}), "*-1", "expiration aborts watcher");

    // FLUSHDB invalidates every watcher, existing keys or not.
    Expect(client.Command({"WATCH", "never-existed"}), "+OK", "WATCH flush");
    Expect(other.Command({"FLUSHDB"}), "+OK", "outside FLUSHDB");
    Expect(client.Command({"MULTI"}), "+OK", "MULTI flushed");
    Expect(client.Command({"PING"}), "+QUEUED", "queue flushed ping");
    Expect(client.Command({"EXEC"}), "*-1", "FLUSHDB aborts watcher");

    // ---- INFO ----
    const std::string info = client.Command({"INFO"});
    auto contains = [&](const std::string& haystack, std::string_view needle,
                        const char* what) {
      if (haystack.find(needle) == std::string::npos) {
        Fail(std::string(what) +
             " missing from INFO reply: " + haystack.substr(0, 300));
      }
    };
    contains(info, "# Server", "server section");
    contains(info, "lavik_version:", "version field");
    contains(info, "worker_threads:4", "threads field");
    contains(info, "# Clients", "clients section");
    contains(info, "connected_clients:", "clients field");
    contains(info, "blocked_clients:0", "blocked clients field");
    contains(info, "# Transactions", "transactions section");
    contains(info, "tx_fastpath_runs:", "fastpath counter");
    contains(info, "tx_queued_runs:", "queued counter");
    contains(info, "tx_schedule_retries:", "retry counter");
    contains(info, "# Keyspace", "keyspace section");
    contains(info, "db1:keys=", "db1 keyspace line");

    const std::string tx_only = client.Command({"INFO", "transactions"});
    contains(tx_only, "# Transactions", "filtered section");
    if (tx_only.find("# Server") != std::string::npos) {
      Fail("INFO section filter returned other sections");
    }

    Expect(client.Command({"MULTI"}), "+OK", "MULTI info");
    Expect(client.Command({"INFO", "server"}), "+QUEUED", "queue INFO");
    const std::string exec_info = client.Command({"EXEC"});
    contains(exec_info, "# Server", "INFO inside EXEC");

    // ---- MONITOR ----
    // Connections are assigned round-robin to four workers, so this monitor
    // and the original client exercise cross-worker one-way delivery.
    {
      RespClient monitor = Connect(port);
      Expect(monitor.Command({"MONITOR"}), "+OK", "MONITOR");

      Expect(client.Command({"SET", "monitor-key", "value"}), "+OK",
             "monitored SET");
      const std::string initial_message = monitor.ReadPush();
      ExpectContains(initial_message, "[0 127.0.0.1:", "MONITOR endpoint");
      ExpectContains(initial_message, "\"SET\" \"monitor-key\" \"value\"",
                     "MONITOR SET");

      const std::string escaped = std::string("line\n\"\\") + '\x01';
      Expect(client.Command({"ECHO", escaped}), Bulk(escaped),
             "monitored escaped ECHO");
      const std::string echo_message = monitor.ReadPush();
      ExpectContains(echo_message, "\"ECHO\"", "MONITOR command name");
      ExpectContains(echo_message, "\"line\\n\\\"\\\\\\x01\"",
                     "MONITOR argument escaping");

      // AUTH is visible like Valkey, but every credential argument is
      // redacted before it enters the shared monitor message.
      ExpectContains(client.Command({"AUTH", "monitor-secret"}),
                     "AUTH called without any password", "AUTH without config");
      const std::string auth_message = monitor.ReadPush();
      ExpectContains(auth_message, "\"AUTH\" \"(redacted)\"",
                     "MONITOR AUTH redaction");
      if (auth_message.find("monitor-secret") != std::string::npos) {
        Fail("MONITOR exposed an AUTH credential");
      }

      // ADMIN commands are omitted. The next visible message must be PING,
      // not CONFIG.
      (void)client.Command({"CONFIG", "GET", "maxmemory"});
      Expect(client.Command({"PING"}), "+PONG", "monitor marker PING");
      const std::string after_admin = monitor.ReadPush();
      ExpectContains(after_admin, "\"PING\"", "MONITOR skips ADMIN");
      if (after_admin.find("CONFIG") != std::string::npos) {
        Fail("MONITOR published an ADMIN command");
      }

      Expect(client.Command({"MULTI"}), "+OK", "monitored MULTI");
      Expect(client.Command({"SET", "monitor-tx", "1"}), "+QUEUED",
             "monitored queued SET");
      Expect(client.Command({"GET", "monitor-tx"}), "+QUEUED",
             "monitored queued GET");
      Expect(client.Command({"EXEC"}), "*2\r\n+OK\r\n" + Bulk("1"),
             "monitored EXEC");
      const std::string multi_message = monitor.ReadPush();
      const std::string set_message = monitor.ReadPush();
      const std::string get_message = monitor.ReadPush();
      const std::string exec_message = monitor.ReadPush();
      ExpectContains(multi_message, "\"MULTI\"", "MONITOR MULTI order");
      ExpectContains(set_message, "\"SET\" \"monitor-tx\" \"1\"",
                     "MONITOR SET order");
      ExpectContains(get_message, "\"GET\" \"monitor-tx\"",
                     "MONITOR GET order");
      ExpectContains(exec_message, "\"EXEC\"", "MONITOR EXEC order");

      // Like Valkey's DENY BLOCKING EXEC state, MONITOR cannot turn a
      // transaction connection into a streaming connection. The ADMIN child
      // is omitted, while MULTI and EXEC remain visible.
      Expect(client.Command({"MULTI"}), "+OK", "MONITOR inside MULTI setup");
      Expect(client.Command({"MONITOR"}), "+QUEUED",
             "MONITOR queued inside MULTI");
      Expect(client.Command({"EXEC"}),
             "*1\r\n-ERR MONITOR isn't allowed for DENY BLOCKING client",
             "MONITOR rejected inside EXEC");
      ExpectContains(monitor.ReadPush(), "\"MULTI\"",
                     "MONITOR nested MULTI visibility");
      ExpectContains(monitor.ReadPush(), "\"EXEC\"",
                     "MONITOR nested EXEC visibility");
      Expect(client.Command({"PING"}), "+PONG",
             "connection remains non-monitor after EXEC");
      ExpectContains(monitor.ReadPush(), "\"PING\"",
                     "MONITOR post-EXEC connection state");
    }
    // Let the worker-local peer watcher observe the closed monitor socket so
    // graceful shutdown also exercises unregister cleanup.
    std::this_thread::sleep_for(300ms);

    server.Stop();

    // Function mutations acknowledge only after their complete catalog dump
    // is durable. Exercise startup recovery for an ordinary LOAD/RESTORE and
    // for the two emptying mutations, including ASYNC (which changes only
    // runtime reclamation, never the durability boundary).
    {
      ServerProcess restarted(argv[1], port, data_path, log_path);
      RespClient recovered = ConnectReady(port);
      ExpectContains(
          recovered.Command({"FCALL_RO", "lavik_globals", "0"}),
          "Script attempted to access nonexistent global variable 'KEYS'",
          "Function catalog preserves protected globals after restart");
      Expect(recovered.Command({"FCALL", "transaction_set", "1",
                                "function:restart", "durable"}),
             "+OK", "transaction-loaded Function survives restart");
      Expect(recovered.Command({"FUNCTION", "DELETE", "lavik_test"}), "+OK",
             "durable FUNCTION DELETE");
      restarted.Stop();
    }
    {
      ServerProcess restarted(argv[1], port, data_path, log_path);
      RespClient recovered = ConnectReady(port);
      ExpectContains(
          recovered.Command({"FCALL", "lavik_get", "1", "function:key"}),
          "Function not found", "FUNCTION DELETE survives restart");
      Expect(recovered.Command({"GET", "function:restart"}), Bulk("durable"),
             "Function write survives restart");
      Expect(recovered.Command({"FUNCTION", "FLUSH", "ASYNC"}), "+OK",
             "durable FUNCTION FLUSH ASYNC");
      restarted.Stop();
    }
    {
      ServerProcess restarted(argv[1], port, data_path, log_path);
      RespClient recovered = ConnectReady(port);
      ExpectContains(recovered.Command({"FCALL", "transaction_set", "1",
                                        "function:restart", "x"}),
                     "Function not found", "FUNCTION FLUSH survives restart");
      restarted.Stop();
    }
  } catch (const std::exception& error) {
    std::cerr << error.what() << "\n--- Lavik log ---\n"
              << ReadFile(log_path) << std::flush;
    exit_code = 1;
  }

  (void)::unlink(data_path.c_str());
  (void)::unlink(log_path.c_str());
  std::cout << (exit_code == 0 ? "multi/exec e2e passed\n" : "") << std::flush;
  return exit_code;
}
