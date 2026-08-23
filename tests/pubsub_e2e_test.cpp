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
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

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
    if (line[0] == '+' || line[0] == '-' || line[0] == ':') return line;
    if (line[0] == '$') {
      if (line == "$-1") return line;
      std::string body(Length(line) + 2, '\0');
      ReadExact(body.data(), body.size());
      body.resize(body.size() - 2);
      return line + "\r\n" + body;
    }
    if (line[0] == '*') {
      if (line == "*-1") return line;
      std::string result = line;
      for (std::size_t i = 0; i < Length(line); ++i) {
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
  Fail("timed out connecting to Keylane");
}

void CreateDataFile(const std::string& path) {
  const int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_EXCL, 0600);
  if (fd < 0) Fail("failed to open data file");
  const int allocated = ::posix_fallocate(fd, 0, 128ULL * 1024 * 1024);
  const int closed = ::close(fd);
  if (allocated != 0 || closed != 0) Fail("failed to create data file");
}

class Server {
 public:
  Server(const std::string& binary, std::uint16_t port, const std::string& data,
         const std::string& log) {
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
                                  "--recv-buffers-per-worker",
                                  "0",
                                  "--data-file",
                                  data};
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

}  // namespace

int main(int argc, char** argv) {
  try {
    if (argc != 2) Fail("usage: pubsub_e2e_test KEYLANE_BINARY");
    char directory[] = "/tmp/keylane-pubsub-e2e-XXXXXX";
    if (::mkdtemp(directory) == nullptr) Fail("mkdtemp failed");
    const std::string root(directory);
    const std::string source_data = root + "/source.data";
    const std::string replica_data = root + "/replica.data";
    CreateDataFile(source_data);
    CreateDataFile(replica_data);
    const std::uint16_t source_port = FreePort();
    const std::uint16_t replica_port = FreePort();
    Server source(argv[1], source_port, source_data, root + "/source.log");
    Server replica(argv[1], replica_port, replica_data, root + "/replica.log");

    RespClient source_client = Connect(source_port);
    RespClient replica_client = Connect(replica_port);

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
    Expect(replica_client.Command({"PUBLISH", "replicated", "local-only"}),
           ":1", "replica local publish");
    Expect(replica_subscriber.ReadPush(), Message("replicated", "local-only"),
           "replica local message");

    // A PUBLISH-only EXEC uses the channel-sharded ephemeral source flow.
    Expect(source_client.Command({"MULTI"}), "+OK", "publish-only multi");
    Expect(source_client.Command({"PUBLISH", "replicated", "tx-only"}),
           "+QUEUED", "queue publish-only transaction");
    Expect(source_client.Command({"EXEC"}), "*1\r\n:0", "publish-only exec");
    Expect(replica_subscriber.ReadPush(), Message("replicated", "tx-only"),
           "replicated publish-only exec message");

    // A mixed transaction keeps PUBLISH in the durable transaction envelope
    // so the replica observes the write and message at the same apply point.
    Expect(source_client.Command({"MULTI"}), "+OK", "mixed multi");
    Expect(source_client.Command({"SET", "tx-key", "value"}), "+QUEUED",
           "queue mixed write");
    Expect(source_client.Command({"PUBLISH", "replicated", "tx-mixed"}),
           "+QUEUED", "queue mixed publish");
    Expect(source_client.Command({"EXEC"}), "*2\r\n+OK\r\n:0", "mixed exec");
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
    std::cout << "pubsub e2e passed\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "pubsub e2e failed: " << error.what() << '\n';
    return 1;
  }
}
