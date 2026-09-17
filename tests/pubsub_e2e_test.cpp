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
#include <cstdlib>
#include <cstring>
#include <iostream>
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
  ~RespClient() {
    if (fd_ >= 0) ::close(fd_);
  }

  std::string Command(const std::vector<std::string_view>& args) {
    SendPipeline({args});
    return ReadReply();
  }
  void SendPipeline(
      const std::vector<std::vector<std::string_view>>& commands) {
    std::string wire;
    for (const auto& args : commands) {
      wire += "*" + std::to_string(args.size()) + "\r\n";
      for (std::string_view arg : args) {
        wire += "$" + std::to_string(arg.size()) + "\r\n";
        wire.append(arg);
        wire += "\r\n";
      }
    }
    SendAll(wire);
  }
  std::string ReadPush() { return ReadReply(); }

 private:
  static std::size_t Length(std::string_view line) {
    std::size_t value = 0;
    const auto parsed =
        std::from_chars(line.data() + 1, line.data() + line.size(), value);
    if (parsed.ec != std::errc{} || parsed.ptr != line.data() + line.size()) {
      Fail("invalid RESP length");
    }
    return value;
  }
  void SendAll(std::string_view bytes) {
    while (!bytes.empty()) {
      const ssize_t sent =
          ::send(fd_, bytes.data(), bytes.size(), MSG_NOSIGNAL);
      if (sent < 0 && errno == EINTR) continue;
      if (sent <= 0) Fail("send failed");
      bytes.remove_prefix(static_cast<std::size_t>(sent));
    }
  }
  void ReadExact(char* output, std::size_t bytes) {
    while (bytes != 0) {
      const ssize_t read = ::recv(fd_, output, bytes, 0);
      if (read < 0 && errno == EINTR) continue;
      if (read <= 0) Fail("connection closed while reading RESP");
      output += read;
      bytes -= static_cast<std::size_t>(read);
    }
  }
  std::string ReadLine() {
    std::string line;
    while (!line.ends_with("\r\n")) {
      char byte = 0;
      ReadExact(&byte, 1);
      line.push_back(byte);
      if (line.size() > 4096) Fail("RESP line too long");
    }
    line.resize(line.size() - 2);
    return line;
  }
  std::string ReadReply() {
    const std::string line = ReadLine();
    if (line.empty()) Fail("empty RESP reply");
    if (line[0] == '+' || line[0] == '-' || line[0] == ':' || line[0] == ',' ||
        line[0] == '#') {
      return line;
    }
    if (line == "_") return line;
    if (line[0] == '$' || line[0] == '=') {
      if (line == "$-1") return line;
      std::string body(Length(line) + 2, '\0');
      ReadExact(body.data(), body.size());
      body.resize(body.size() - 2);
      return line + "\r\n" + body;
    }
    if (line[0] == '*' || line[0] == '~' || line[0] == '>' || line[0] == '%') {
      if (line == "*-1") return line;
      std::string result = line;
      const std::size_t elements = Length(line) * (line[0] == '%' ? 2 : 1);
      for (std::size_t i = 0; i < elements; ++i) {
        result += "\r\n" + ReadReply();
      }
      return result;
    }
    Fail("unknown RESP reply type");
  }
  int fd_ = -1;
};

std::uint16_t FreePort() {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (fd < 0) Fail("socket failed");
  sockaddr_in address{.sin_family = AF_INET,
                      .sin_port = 0,
                      .sin_addr = {.s_addr = htonl(INADDR_LOOPBACK)}};
  if (::bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0)
    Fail("bind failed");
  socklen_t size = sizeof(address);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &size) != 0)
    Fail("getsockname failed");
  ::close(fd);
  return ntohs(address.sin_port);
}

RespClient Connect(std::uint16_t port) {
  const auto deadline = std::chrono::steady_clock::now() + 20s;
  while (std::chrono::steady_clock::now() < deadline) {
    const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    timeval timeout{.tv_sec = 20, .tv_usec = 0};
    (void)::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    sockaddr_in address{.sin_family = AF_INET,
                        .sin_port = htons(port),
                        .sin_addr = {.s_addr = htonl(INADDR_LOOPBACK)}};
    if (::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) ==
        0) {
      return RespClient(fd);
    }
    ::close(fd);
    std::this_thread::sleep_for(10ms);
  }
  Fail("timed out connecting to Lavik");
}

void CreateDataFile(const std::string& path) {
  const int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_EXCL, 0600);
  if (fd < 0) Fail("failed to open data file");
  const int allocated = ::posix_fallocate(fd, 0, 160ULL * 1024 * 1024);
  const int closed = ::close(fd);
  if (allocated != 0 || closed != 0) Fail("failed to create data file");
}

class Server {
 public:
  Server(const std::string& binary, std::uint16_t port, const std::string& data,
         const std::string& log, std::string requirepass = {},
         std::vector<std::pair<std::string, std::string>> environment = {}) {
    pid_ = ::fork();
    if (pid_ < 0) Fail("fork failed");
    if (pid_ != 0) return;
    const int log_fd = ::open(log.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0600);
    if (log_fd >= 0) {
      (void)::dup2(log_fd, STDOUT_FILENO);
      (void)::dup2(log_fd, STDERR_FILENO);
      ::close(log_fd);
    }
    std::vector<std::string> args{binary,
                                  "--port",
                                  std::to_string(port),
                                  "--threads",
                                  "2",
                                  "--no-pin-workers",
                                  "--logtostderr",
                                  "--recv-buffers-per-worker",
                                  "0",
                                  "--data-file",
                                  data};
    if (!requirepass.empty()) {
      args.push_back("--requirepass");
      args.push_back(std::move(requirepass));
    }
    for (const auto& [name, value] : environment) {
      if (::setenv(name.c_str(), value.c_str(), 1) != 0) _exit(127);
    }
    std::vector<char*> argv;
    for (std::string& arg : args) argv.push_back(arg.data());
    argv.push_back(nullptr);
    ::execv(binary.c_str(), argv.data());
    _exit(127);
  }
  ~Server() { Stop(SIGKILL); }
  void Stop(int signal = SIGINT) {
    if (pid_ <= 0) return;
    (void)::kill(pid_, signal);
    (void)::waitpid(pid_, nullptr, 0);
    pid_ = -1;
  }

 private:
  pid_t pid_ = -1;
};

void Expect(std::string_view actual, std::string_view expected,
            std::string_view label) {
  if (actual != expected) {
    Fail(std::string(label) + ": expected [" + std::string(expected) +
         "] got [" + std::string(actual) + "]");
  }
}

void ExpectContains(std::string_view actual, std::string_view expected,
                    std::string_view label) {
  if (actual.find(expected) == std::string_view::npos) {
    Fail(std::string(label) + ": expected to find [" + std::string(expected) +
         "] in [" + std::string(actual) + "]");
  }
}

std::string Subscription(std::string_view kind, std::string_view channel,
                         unsigned count) {
  return "*3\r\n$" + std::to_string(kind.size()) + "\r\n" + std::string(kind) +
         "\r\n$" + std::to_string(channel.size()) + "\r\n" +
         std::string(channel) + "\r\n:" + std::to_string(count);
}

std::string Message(std::string_view channel, std::string_view payload) {
  return "*3\r\n$7\r\nmessage\r\n$" + std::to_string(channel.size()) + "\r\n" +
         std::string(channel) + "\r\n$" + std::to_string(payload.size()) +
         "\r\n" + std::string(payload);
}

std::string Resp3Subscription(std::string_view kind, std::string_view channel,
                              unsigned count) {
  return ">3\r\n$" + std::to_string(kind.size()) + "\r\n" + std::string(kind) +
         "\r\n$" + std::to_string(channel.size()) + "\r\n" +
         std::string(channel) + "\r\n:" + std::to_string(count);
}

std::string Resp3Message(std::string_view channel, std::string_view payload) {
  return ">3\r\n$7\r\nmessage\r\n$" + std::to_string(channel.size()) + "\r\n" +
         std::string(channel) + "\r\n$" + std::to_string(payload.size()) +
         "\r\n" + std::string(payload);
}

std::string PatternMessage(std::string_view pattern, std::string_view channel,
                           std::string_view payload) {
  return "*4\r\n$8\r\npmessage\r\n$" + std::to_string(pattern.size()) + "\r\n" +
         std::string(pattern) + "\r\n$" + std::to_string(channel.size()) +
         "\r\n" + std::string(channel) + "\r\n$" +
         std::to_string(payload.size()) + "\r\n" + std::string(payload);
}

std::string EmptySubscription(std::string_view kind, unsigned count) {
  return "*3\r\n$" + std::to_string(kind.size()) + "\r\n" + std::string(kind) +
         "\r\n$-1\r\n:" + std::to_string(count);
}

void WaitForReplica(RespClient* replica) {
  const auto deadline = std::chrono::steady_clock::now() + 30s;
  while (std::chrono::steady_clock::now() < deadline) {
    if (replica->Command({"INFO", "replication"})
            .find("master_link_status:up") != std::string::npos) {
      return;
    }
    std::this_thread::sleep_for(20ms);
  }
  Fail("replica did not become online");
}

void ExpectExecPublishUsesCommandTimeSubscriptions(RespClient* publisher,
                                                   std::uint16_t source_port,
                                                   std::string_view channel,
                                                   std::string_view label) {
  RespClient subscriber = Connect(source_port);
  Expect(subscriber.Command({"MULTI"}), "+OK", std::string(label) + " multi");
  Expect(subscriber.Command({"PUBLISH", channel, "before-subscribe"}),
         "+QUEUED", std::string(label) + " queue publish");
  Expect(subscriber.Command({"SUBSCRIBE", channel}), "+QUEUED",
         std::string(label) + " queue subscribe");
  Expect(subscriber.Command({"EXEC"}),
         "*2\r\n:0\r\n" + Subscription("subscribe", channel, 1),
         std::string(label) + " exec");

  // The second publication is both an observable ordering barrier and proof
  // that the subscription established later in EXEC is live. If the deferred
  // first PUBLISH is incorrectly resolved against the final subscription
  // table, ReadPush observes before-subscribe instead and fails.
  Expect(publisher->Command({"PUBLISH", channel, "after-subscribe"}), ":1",
         std::string(label) + " publish barrier");
  Expect(subscriber.ReadPush(), Message(channel, "after-subscribe"),
         std::string(label) + " excludes later subscriber");
  Expect(subscriber.Command({"UNSUBSCRIBE", channel}),
         Subscription("unsubscribe", channel, 0),
         std::string(label) + " unsubscribe");
}

void ExpectExecPublishPrecedesLaterUnsubscribe(RespClient* subscriber,
                                               std::string_view channel) {
  ExpectContains(subscriber->Command({"HELLO", "3"}), "$5\r\nproto\r\n:3",
                 "unsubscribe-order HELLO 3");
  Expect(subscriber->Command({"SUBSCRIBE", channel}),
         Resp3Subscription("subscribe", channel, 1),
         "unsubscribe-order subscribe");
  Expect(subscriber->Command({"MULTI"}), "+OK", "unsubscribe-order multi");
  Expect(subscriber->Command({"PUBLISH", channel, "before-unsubscribe"}),
         "+QUEUED", "unsubscribe-order queue publish");
  Expect(subscriber->Command({"UNSUBSCRIBE", channel}), "+QUEUED",
         "unsubscribe-order queue unsubscribe");
  subscriber->SendPipeline({{"EXEC"}});
  Expect(subscriber->ReadPush(), Resp3Message(channel, "before-unsubscribe"),
         "unsubscribe-order captured message");
  Expect(subscriber->ReadPush(),
         "*2\r\n:1\r\n" + Resp3Subscription("unsubscribe", channel, 0),
         "unsubscribe-order exec count");
  Expect(subscriber->Command({"PING"}), "+PONG",
         "unsubscribe-order exits subscribed mode");
}

}  // namespace

int main(int argc, char** argv) {
  try {
    if (argc != 2) Fail("usage: pubsub_e2e_test LAVIK_BINARY");
    std::string directory =
        lavik::test::TestDataPath("lavik-pubsub-e2e-XXXXXX");
    if (::mkdtemp(directory.data()) == nullptr) Fail("mkdtemp failed");
    const std::string root(directory);
    const std::string source_data = root + "/source.data";
    const std::string replica_data = root + "/replica.data";
    const std::string auth_data = root + "/auth.data";
    CreateDataFile(source_data);
    CreateDataFile(replica_data);
    CreateDataFile(auth_data);
    const std::uint16_t source_port = FreePort();
    const std::uint16_t replica_port = FreePort();
    std::vector<std::pair<std::string, std::string>> source_environment;
#if !defined(NDEBUG)
    source_environment.emplace_back(
        "LAVIK_EXEC_REJECT_EPHEMERAL_FINAL_RECHECK_ONCE", "1");
#endif
    Server source(argv[1], source_port, source_data, root + "/source.log", {},
                  std::move(source_environment));
    Server replica(argv[1], replica_port, replica_data, root + "/replica.log");

    RespClient source_client = Connect(source_port);
    RespClient replica_client = Connect(replica_port);
    Expect(source_client.Command({"CLIENT", "GETNAME"}), "$-1",
           "initial client name");
    Expect(source_client.Command({"CLIENT", "SETNAME", "sentinel-probe"}),
           "+OK", "set client name");
    Expect(source_client.Command({"CLIENT", "GETNAME"}),
           "$14\r\nsentinel-probe", "get client name");
    ExpectContains(source_client.Command({"CLIENT", "LIST"}),
                   "name=sentinel-probe", "client list name");

    ExpectExecPublishUsesCommandTimeSubscriptions(
        &source_client, source_port, "tx-order-standalone", "standalone");

    RespClient resp3_client = Connect(source_port);
    const std::string hello3 =
        resp3_client.Command({"HELLO", "3", "SETNAME", "resp3-client"});
    ExpectContains(hello3, "%7\r\n$6\r\nserver\r\n$5\r\nlavik", "HELLO 3 map");
    ExpectContains(hello3, "$5\r\nproto\r\n:3", "HELLO 3 protocol");
    ExpectContains(resp3_client.Command({"CLIENT", "LIST"}),
                   "name=resp3-client", "RESP3 client name");
    ExpectContains(resp3_client.Command({"CLIENT", "LIST"}), "resp=3",
                   "RESP3 client metadata");
    Expect(
        resp3_client.Command({"CLIENT", "SETINFO", "lib-name", "lavik-test"}),
        "+OK", "RESP3 CLIENT SETINFO");
    const std::string resp3_client_info =
        resp3_client.Command({"CLIENT", "INFO"});
    ExpectContains(resp3_client_info, "=", "RESP3 CLIENT INFO verbatim");
    ExpectContains(resp3_client_info, "txt:id=", "RESP3 CLIENT INFO format");
    ExpectContains(resp3_client_info, "lib-name=lavik-test",
                   "RESP3 CLIENT INFO metadata");
    Expect(resp3_client.Command({"GET", "resp3-missing"}), "_", "RESP3 null");
    Expect(resp3_client.Command({"HSET", "resp3-hash", "field", "value"}), ":1",
           "RESP3 HSET");
    Expect(resp3_client.Command({"HGETALL", "resp3-hash"}),
           "%1\r\n$5\r\nfield\r\n$5\r\nvalue", "RESP3 HGETALL map");
    Expect(resp3_client.Command({"SADD", "resp3-set", "member"}), ":1",
           "RESP3 SADD");
    Expect(resp3_client.Command({"SMEMBERS", "resp3-set"}),
           "~1\r\n$6\r\nmember", "RESP3 SMEMBERS set");
    Expect(resp3_client.Command({"SDIFF", "resp3-set", "missing-set"}),
           "~1\r\n$6\r\nmember", "RESP3 SDIFF set");
    Expect(resp3_client.Command({"MGET", "resp3-missing"}), "*1\r\n_",
           "RESP3 MGET null");
    Expect(
        resp3_client.Command({"HINCRBYFLOAT", "resp3-hash", "number", "1.5"}),
        ",1.5", "RESP3 hash double");
    Expect(
        resp3_client.Command({"HSET", "resp3-random-hash", "field", "value"}),
        ":1", "seed RESP3 random hash");
    Expect(resp3_client.Command(
               {"HRANDFIELD", "resp3-random-hash", "1", "WITHVALUES"}),
           "*1\r\n*2\r\n$5\r\nfield\r\n$5\r\nvalue", "RESP3 random hash pairs");
    Expect(resp3_client.Command({"ZADD", "resp3-zset", "INCR", "1.5", "m"}),
           ",1.5", "RESP3 ZADD INCR double");
    Expect(resp3_client.Command({"ZSCORE", "resp3-zset", "m"}), ",1.5",
           "RESP3 ZSCORE double");
    Expect(resp3_client.Command({"ZMSCORE", "resp3-zset", "m", "missing"}),
           "*2\r\n,1.5\r\n_", "RESP3 ZMSCORE values");
    Expect(
        resp3_client.Command({"ZRANGE", "resp3-zset", "0", "-1", "WITHSCORES"}),
        "*1\r\n*2\r\n$1\r\nm\r\n,1.5", "RESP3 sorted-set scored pairs");
    ExpectContains(resp3_client.Command({"CONFIG", "GET", "repl-backlog-size"}),
                   "%1\r\n$17\r\nrepl-backlog-size", "RESP3 CONFIG map");
    Expect(resp3_client.Command({"SET", "lcs-a", "abc"}), "+OK", "seed LCS a");
    Expect(resp3_client.Command({"SET", "lcs-b", "abc"}), "+OK", "seed LCS b");
    ExpectContains(resp3_client.Command({"LCS", "lcs-a", "lcs-b", "IDX"}),
                   "%2\r\n$7\r\nmatches", "RESP3 LCS map");
    ExpectContains(
        resp3_client.Command({"XADD", "resp3-stream", "*", "f", "v"}), "-",
        "RESP3 XADD id");
    ExpectContains(resp3_client.Command({"XINFO", "STREAM", "resp3-stream"}),
                   "%10\r\n$6\r\nlength", "RESP3 XINFO map");
    ExpectContains(
        resp3_client.Command({"XREAD", "STREAMS", "resp3-stream", "0-0"}),
        "%1\r\n$12\r\nresp3-stream", "RESP3 XREAD map");

    RespClient resp3_subscriber = Connect(source_port);
    ExpectContains(resp3_subscriber.Command({"HELLO", "3"}),
                   "$5\r\nproto\r\n:3", "subscriber HELLO 3");
    Expect(resp3_subscriber.Command({"SUBSCRIBE", "mixed"}),
           Resp3Subscription("subscribe", "mixed", 1), "RESP3 subscribe push");
    RespClient resp2_subscriber = Connect(source_port);
    Expect(resp2_subscriber.Command({"SUBSCRIBE", "mixed"}),
           Subscription("subscribe", "mixed", 1), "RESP2 mixed subscribe");
    Expect(source_client.Command({"PUBLISH", "mixed", "payload"}), ":2",
           "mixed protocol publish count");
    Expect(resp3_subscriber.ReadPush(), Resp3Message("mixed", "payload"),
           "RESP3 message push");
    Expect(resp2_subscriber.ReadPush(), Message("mixed", "payload"),
           "RESP2 mixed message");
    Expect(source_client.Command({"SET", "subscribed-read", "visible"}), "+OK",
           "seed subscribed RESP3 read");
    Expect(resp3_subscriber.Command({"GET", "subscribed-read"}),
           "$7\r\nvisible", "RESP3 subscribed client ordinary command");
    Expect(resp3_subscriber.Command({"PING", "token"}), "$5\r\ntoken",
           "RESP3 subscribed ping is an ordinary reply");
    Expect(resp3_subscriber.Command({"UNSUBSCRIBE", "mixed"}),
           Resp3Subscription("unsubscribe", "mixed", 0),
           "RESP3 unsubscribe push");
    Expect(resp2_subscriber.Command({"UNSUBSCRIBE", "mixed"}),
           Subscription("unsubscribe", "mixed", 0), "RESP2 mixed unsubscribe");
    Expect(resp3_client.Command({"MULTI"}), "+OK", "RESP3 MULTI");
    Expect(resp3_client.Command({"HELLO", "2"}), "+QUEUED",
           "HELLO queued in MULTI");
    ExpectContains(resp3_client.Command({"EXEC"}), "*1\r\n*14\r\n$6\r\nserver",
                   "EXEC keeps its starting protocol and HELLO switches");
    Expect(resp3_client.Command({"GET", "resp3-missing"}), "$-1",
           "RESP2 null after HELLO 2");
    Expect(resp3_client.Command({"MULTI"}), "+OK", "RESP2 MULTI");
    Expect(resp3_client.Command({"HELLO", "3"}), "+QUEUED",
           "HELLO 3 queued in RESP2 MULTI");
    ExpectContains(resp3_client.Command({"EXEC"}), "*1\r\n%7\r\n$6\r\nserver",
                   "queued HELLO emits RESP3 map");
    Expect(resp3_client.Command({"GET", "resp3-missing"}), "_",
           "queued HELLO preserves new protocol");
    Expect(resp3_client.Command({"MULTI"}), "+OK", "mixed protocol MULTI");
    Expect(resp3_client.Command({"HELLO", "2"}), "+QUEUED",
           "queue protocol downgrade");
    Expect(resp3_client.Command({"HGETALL", "resp3-hash"}), "+QUEUED",
           "queue keyed command after HELLO");
    ExpectContains(resp3_client.Command({"EXEC"}), "*2\r\n*14\r\n$6\r\nserver",
                   "mixed EXEC keeps original outer protocol");
    Expect(resp3_client.Command({"HGETALL", "resp3-random-hash"}),
           "*2\r\n$5\r\nfield\r\n$5\r\nvalue",
           "keyed command follows transaction protocol switch");
    ExpectContains(resp3_client.Command({"HELLO", "3"}), "%7\r\n$6\r\nserver",
                   "restore RESP3 after mixed EXEC");

    RespClient local_subscriber = Connect(source_port);
    Expect(local_subscriber.Command({"SUBSCRIBE", "alpha"}),
           Subscription("subscribe", "alpha", 1), "subscribe");
    Expect(local_subscriber.Command({"SUBSCRIBE", "alpha"}),
           Subscription("subscribe", "alpha", 1), "duplicate subscribe");
    Expect(source_client.Command({"PUBLISH", "alpha", "hello"}), ":1",
           "local publish count");
    Expect(local_subscriber.ReadPush(), Message("alpha", "hello"),
           "local message");
    Expect(local_subscriber.Command({"PING", "token"}),
           "*2\r\n$4\r\npong\r\n$5\r\ntoken", "subscribed ping");
    Expect(local_subscriber.Command({"UNSUBSCRIBE", "alpha"}),
           Subscription("unsubscribe", "alpha", 0), "unsubscribe");
    Expect(local_subscriber.Command({"PING"}), "+PONG", "normal mode ping");

    RespClient pattern_subscriber = Connect(source_port);
    Expect(pattern_subscriber.Command({"PSUBSCRIBE", "news.*"}),
           Subscription("psubscribe", "news.*", 1), "pattern subscribe");
    Expect(pattern_subscriber.Command({"PSUBSCRIBE", "news.*"}),
           Subscription("psubscribe", "news.*", 1),
           "duplicate pattern subscribe");
    Expect(pattern_subscriber.Command({"SUBSCRIBE", "news.one"}),
           Subscription("subscribe", "news.one", 2),
           "combined exact subscribe");
    const std::string pubsub_clients =
        source_client.Command({"CLIENT", "LIST", "TYPE", "pubsub"});
    ExpectContains(pubsub_clients, "flags=P", "pubsub client flag");
    ExpectContains(pubsub_clients, "sub=1 psub=1", "pubsub client counts");
    Expect(source_client.Command({"PUBLISH", "news.one", "overlap"}), ":2",
           "exact and pattern publish count");
    Expect(pattern_subscriber.ReadPush(), Message("news.one", "overlap"),
           "overlapping exact message");
    Expect(pattern_subscriber.ReadPush(),
           PatternMessage("news.*", "news.one", "overlap"),
           "overlapping pattern message");
    Expect(source_client.Command({"PUBSUB", "CHANNELS", "news.*"}),
           "*1\r\n$8\r\nnews.one", "pubsub channels pattern");
    Expect(source_client.Command({"PUBSUB", "CHANNELS", "other*"}), "*0",
           "pubsub channels empty");
    Expect(source_client.Command({"PUBSUB", "NUMSUB", "news.one", "missing"}),
           "*4\r\n$8\r\nnews.one\r\n:1\r\n$7\r\nmissing\r\n:0",
           "pubsub numsub");
    Expect(source_client.Command({"PUBSUB", "NUMPAT"}), ":1", "pubsub numpat");

    RespClient duplicate_pattern_subscriber = Connect(source_port);
    Expect(duplicate_pattern_subscriber.Command({"PSUBSCRIBE", "news.*"}),
           Subscription("psubscribe", "news.*", 1),
           "second client pattern subscribe");
    Expect(source_client.Command({"PUBSUB", "NUMPAT"}), ":1",
           "pubsub numpat counts unique patterns");
    Expect(duplicate_pattern_subscriber.Command({"PUNSUBSCRIBE"}),
           Subscription("punsubscribe", "news.*", 0),
           "second client pattern unsubscribe all");

    Expect(pattern_subscriber.Command({"PUNSUBSCRIBE", "news.*"}),
           Subscription("punsubscribe", "news.*", 1),
           "pattern unsubscribe keeps exact subscription");
    Expect(pattern_subscriber.Command({"PUNSUBSCRIBE"}),
           EmptySubscription("punsubscribe", 1),
           "empty pattern unsubscribe keeps exact count");
    Expect(source_client.Command({"PUBLISH", "news.one", "exact-only"}), ":1",
           "exact-only publish count");
    Expect(pattern_subscriber.ReadPush(), Message("news.one", "exact-only"),
           "exact-only message");
    Expect(pattern_subscriber.Command({"UNSUBSCRIBE"}),
           Subscription("unsubscribe", "news.one", 0), "exact unsubscribe all");
    Expect(source_client.Command({"PUBSUB", "NUMPAT"}), ":0",
           "pubsub numpat after unsubscribe");

    // Exercise the transition into and out of subscribed mode when commands
    // following SUBSCRIBE/UNSUBSCRIBE are already in the parser's batch.
    RespClient pipelined_subscriber = Connect(source_port);
    pipelined_subscriber.SendPipeline(
        {{"SUBSCRIBE", "pipeline"}, {"PING", "inside"}});
    Expect(pipelined_subscriber.ReadPush(),
           Subscription("subscribe", "pipeline", 1), "pipelined subscribe");
    Expect(pipelined_subscriber.ReadPush(), "*2\r\n$4\r\npong\r\n$6\r\ninside",
           "pipelined subscribed ping");
    pipelined_subscriber.SendPipeline({{"UNSUBSCRIBE"}, {"PING"}});
    Expect(pipelined_subscriber.ReadPush(),
           Subscription("unsubscribe", "pipeline", 0), "pipelined unsubscribe");
    Expect(pipelined_subscriber.ReadPush(), "+PONG",
           "pipelined command after subscription mode");

    RespClient reset_subscriber = Connect(source_port);
    Expect(reset_subscriber.Command({"PSUBSCRIBE", "reset.*"}),
           Subscription("psubscribe", "reset.*", 1), "reset subscribe");
    Expect(reset_subscriber.Command({"RESET", "extra"}),
           "-ERR wrong number of arguments for 'reset' command",
           "invalid reset while subscribed");
    Expect(reset_subscriber.Command({"RESET"}), "+RESET",
           "reset subscribed client");
    Expect(reset_subscriber.Command({"PING"}), "+PONG",
           "ping after subscribed reset");
    Expect(source_client.Command({"PUBSUB", "NUMPAT"}), ":0",
           "reset removes pattern subscription");

    RespClient quit_subscriber = Connect(source_port);
    Expect(quit_subscriber.Command({"SUBSCRIBE", "quit"}),
           Subscription("subscribe", "quit", 1), "quit subscribe");
    Expect(quit_subscriber.Command({"QUIT", "extra"}),
           "-ERR wrong number of arguments for 'quit' command",
           "invalid quit while subscribed");
    Expect(quit_subscriber.Command({"QUIT"}), "+OK", "quit subscribed client");
    Expect(source_client.Command({"PUBSUB", "NUMSUB", "quit"}),
           "*2\r\n$4\r\nquit\r\n:0", "quit removes subscription");

    RespClient killed_subscriber = Connect(source_port);
    Expect(killed_subscriber.Command({"CLIENT", "SETNAME", "kill-pubsub"}),
           "+OK", "name client before pubsub kill");
    Expect(killed_subscriber.Command({"SUBSCRIBE", "kill-me"}),
           Subscription("subscribe", "kill-me", 1), "subscribe before kill");
    Expect(source_client.Command({"CLIENT", "KILL", "TYPE", "pubsub"}), ":1",
           "kill pubsub clients");
    const auto kill_deadline = std::chrono::steady_clock::now() + 5s;
    while (source_client.Command({"PUBSUB", "NUMSUB", "kill-me"}) !=
           "*2\r\n$7\r\nkill-me\r\n:0") {
      if (std::chrono::steady_clock::now() >= kill_deadline) {
        Fail("CLIENT KILL TYPE pubsub did not remove subscription");
      }
      std::this_thread::sleep_for(10ms);
    }

    Expect(replica_client.Command(
               {"REPLICAOF", "127.0.0.1", std::to_string(source_port)}),
           "+OK", "replicaof");
    WaitForReplica(&replica_client);
    Expect(replica_client.Command({"READONLY"}), "+OK", "replica readonly");
    RespClient replica_subscriber = Connect(replica_port);
    Expect(replica_subscriber.Command({"SUBSCRIBE", "replicated"}),
           Subscription("subscribe", "replicated", 1), "replica subscribe");
    RespClient replica_pattern_subscriber = Connect(replica_port);
    Expect(replica_pattern_subscriber.Command({"PSUBSCRIBE", "rep*"}),
           Subscription("psubscribe", "rep*", 1), "replica pattern subscribe");
    Expect(source_client.Command({"publish", "replicated", "downstream"}), ":0",
           "source count excludes replica");
    Expect(replica_subscriber.ReadPush(), Message("replicated", "downstream"),
           "replicated message");
    Expect(replica_pattern_subscriber.ReadPush(),
           PatternMessage("rep*", "replicated", "downstream"),
           "replicated pattern message");
    Expect(replica_pattern_subscriber.Command({"PUNSUBSCRIBE"}),
           Subscription("punsubscribe", "rep*", 0),
           "replica pattern unsubscribe");
    RespClient source_replication_subscriber = Connect(source_port);
    Expect(source_replication_subscriber.Command({"SUBSCRIBE", "replicated"}),
           Subscription("subscribe", "replicated", 1),
           "source replicated subscribe");
    Expect(replica_client.Command({"PUBLISH", "replicated", "local-only"}),
           ":1", "replica local publish");
    Expect(replica_subscriber.ReadPush(), Message("replicated", "local-only"),
           "replica local message");

#if !defined(NDEBUG)
    // This deterministic final-check fault is compiled into Debug only. A
    // failed replication check must not leak the captured PUBLISH to either the
    // source's local subscribers or the replica backlog. The next direct
    // publication is an ordering barrier on both paths: it must be the first
    // message either subscriber observes.
    RespClient rejected_exec_client = Connect(source_port);
    Expect(rejected_exec_client.Command({"MULTI"}), "+OK",
           "rejected publish-only multi");
    Expect(
        rejected_exec_client.Command({"PUBLISH", "replicated", "rejected-tx"}),
        "+QUEUED", "queue rejected publish-only transaction");
    Expect(rejected_exec_client.Command({"EXEC"}),
           "-ERR EXEC replication failed: cluster authority changed",
           "reject publish-only exec at final check");
    Expect(source_client.Command({"PUBLISH", "replicated", "after-reject"}),
           ":1", "publish barrier after rejected exec");
    Expect(source_replication_subscriber.ReadPush(),
           Message("replicated", "after-reject"),
           "rejected exec did not publish locally");
    Expect(replica_subscriber.ReadPush(), Message("replicated", "after-reject"),
           "rejected exec did not enter the replica backlog");
#endif

    ExpectExecPublishUsesCommandTimeSubscriptions(
        &source_client, source_port, "tx-order-replicated", "replicated");
    RespClient unsubscribe_order_subscriber = Connect(source_port);
    ExpectExecPublishPrecedesLaterUnsubscribe(&unsubscribe_order_subscriber,
                                              "tx-order-unsubscribe");

    // A PUBLISH-only EXEC uses the channel-sharded ephemeral source flow.
    Expect(source_client.Command({"MULTI"}), "+OK", "publish-only multi");
    Expect(source_client.Command({"PUBLISH", "replicated", "tx-only"}),
           "+QUEUED", "queue publish-only transaction");
    Expect(source_client.Command({"EXEC"}), "*1\r\n:1", "publish-only exec");
    Expect(source_replication_subscriber.ReadPush(),
           Message("replicated", "tx-only"), "publish-only exec local message");
    Expect(replica_subscriber.ReadPush(), Message("replicated", "tx-only"),
           "replicated publish-only exec message");

    // A mixed transaction keeps PUBLISH in the durable transaction envelope
    // so the replica observes the write and message at the same apply point.
    Expect(source_client.Command({"MULTI"}), "+OK", "mixed multi");
    Expect(source_client.Command({"SET", "tx-key", "value"}), "+QUEUED",
           "queue mixed write");
    Expect(source_client.Command({"PUBLISH", "replicated", "tx-mixed"}),
           "+QUEUED", "queue mixed publish");
    Expect(source_client.Command({"EXEC"}), "*2\r\n+OK\r\n:1", "mixed exec");
    Expect(source_replication_subscriber.ReadPush(),
           Message("replicated", "tx-mixed"), "mixed exec local message");
    Expect(replica_subscriber.ReadPush(), Message("replicated", "tx-mixed"),
           "replicated mixed exec message");
    const auto read_deadline = std::chrono::steady_clock::now() + 10s;
    std::string replicated_value;
    while ((replicated_value = replica_client.Command({"GET", "tx-key"})) !=
           "$5\r\nvalue") {
      if (std::chrono::steady_clock::now() >= read_deadline) {
        Fail("replicated mixed EXEC write did not become visible: " +
             replicated_value);
      }
      std::this_thread::sleep_for(10ms);
    }

    replica.Stop();
    source.Stop();

    const std::uint16_t auth_port = FreePort();
    Server auth_server(argv[1], auth_port, auth_data, root + "/auth.log",
                       "secret");
    RespClient auth_client = Connect(auth_port);
    ExpectContains(auth_client.Command({"HELLO", "3"}), "-NOAUTH ",
                   "HELLO without authentication");
    ExpectContains(
        auth_client.Command({"HELLO", "3", "AUTH", "default", "secret",
                             "SETNAME", "authenticated-resp3"}),
        "$5\r\nproto\r\n:3", "HELLO AUTH and SETNAME");
    Expect(auth_client.Command({"GET", "missing"}), "_",
           "authenticated RESP3 null");
    ExpectContains(
        auth_client.Command({"HELLO", "2", "AUTH", "default", "wrong"}),
        "-WRONGPASS ", "failed HELLO AUTH");
    Expect(auth_client.Command({"GET", "missing"}), "_",
           "failed HELLO preserves protocol");
    ExpectContains(
        auth_client.Command({"HELLO", "2", "AUTH", "default", "secret"}),
        "*14\r\n$6\r\nserver", "authenticated HELLO 2");
    Expect(auth_client.Command({"GET", "missing"}), "$-1",
           "authenticated RESP2 null");
    auth_server.Stop();
    std::cout << "pubsub e2e passed\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "pubsub e2e failed: " << error.what() << '\n';
    return 1;
  }
}
