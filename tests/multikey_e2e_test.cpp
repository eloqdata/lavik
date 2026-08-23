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
#include <future>
#include <iostream>
#include <iterator>
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

// RESP client returning the raw wire text of one complete reply, including
// nested array elements, so expectations compare exact protocol output.
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
    switch (line.empty() ? '\0' : line.front()) {
      case '+':
      case '-':
      case ':':
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
      return RespClient(fd);
    }
    ::close(fd);
    std::this_thread::sleep_for(10ms);
  }
  Fail("timed out connecting to Keylane");
}

class ServerProcess {
 public:
  ServerProcess(const std::string& binary, std::uint16_t port,
                const std::string& data_path, const std::string& log_path,
                std::string_view fail_tx_write = {},
                std::string_view tx_active_pause_ms = {},
                bool fail_tx_cleaner_once = false, unsigned threads = 4) {
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
      if (!fail_tx_write.empty()) {
        (void)::setenv("KEYLANE_FAIL_TX_WRITE",
                       std::string(fail_tx_write).c_str(), 1);
      }
      if (!tx_active_pause_ms.empty()) {
        (void)::setenv("KEYLANE_TX_ACTIVE_BLOCK_PAUSE_MS",
                       std::string(tx_active_pause_ms).c_str(), 1);
      }
      if (fail_tx_cleaner_once) {
        (void)::setenv("KEYLANE_FAIL_TX_CLEANER_ONCE", "1", 1);
      }
      std::vector<std::string> arguments{
          binary,
          "--logtostderr",
          "--port",
          std::to_string(port),
          "--threads",
          std::to_string(threads),
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
          Fail("Keylane exited unsuccessfully");
        }
        return;
      }
      if (result < 0) Fail("waitpid failed");
      std::this_thread::sleep_for(10ms);
    }
    Fail("Keylane did not stop");
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

std::string Bulk(std::string_view value) {
  return "$" + std::to_string(value.size()) + "\r\n" + std::string(value);
}

std::string ReadFile(const std::string& path) {
  std::ifstream input(path);
  return std::string(std::istreambuf_iterator<char>(input),
                     std::istreambuf_iterator<char>());
}

std::uint64_t TxCleanerStat(RespClient& client, std::string_view marker) {
  const std::string info = client.Command({"INFO", "STATS"});
  const std::size_t begin = info.find(marker);
  if (begin == std::string::npos) Fail("tx cleaner INFO field is missing");
  const std::size_t value_begin = begin + marker.size();
  const std::size_t value_end = info.find("\r\n", value_begin);
  if (value_end == std::string::npos) Fail("malformed tx cleaner INFO field");
  std::uint64_t retired = 0;
  const char* first = info.data() + value_begin;
  const char* last = info.data() + value_end;
  const auto [parsed, error] = std::from_chars(first, last, retired);
  if (error != std::errc{} || parsed != last) {
    Fail("invalid tx cleaner INFO counter");
  }
  return retired;
}

std::uint64_t TxCleanerRetiredGenerations(RespClient& client) {
  return TxCleanerStat(client, "tx_cleaner_retired_generations:");
}

bool WaitForCleanerStat(RespClient& client, std::string_view marker,
                        std::uint64_t baseline,
                        std::chrono::seconds timeout = 30s) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (TxCleanerStat(client, marker) > baseline) return true;
    std::this_thread::sleep_for(20ms);
  }
  return false;
}

bool WaitForCleanerRetirement(RespClient& client, std::uint64_t baseline) {
  return WaitForCleanerStat(client, "tx_cleaner_retired_generations:",
                            baseline);
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "usage: multikey_e2e_test /path/to/keylane\n";
    return 1;
  }
  const std::string suffix = std::to_string(::getpid());
  const std::string data_path = "/tmp/keylane-multikey-" + suffix + ".data";
  const std::string log_path = "/tmp/keylane-multikey-" + suffix + ".log";
  const std::string source_data =
      "/tmp/keylane-multikey-repl-source-" + suffix + ".data";
  const std::string replica_data =
      "/tmp/keylane-multikey-repl-replica-" + suffix + ".data";
  (void)::unlink(data_path.c_str());
  (void)::unlink(log_path.c_str());
  (void)::unlink(source_data.c_str());
  (void)::unlink(replica_data.c_str());

  int exit_code = 0;
  try {
    const std::uint16_t port = FindFreePort();
    CreateDataFile(data_path, 512ULL * 1024 * 1024);
    ServerProcess server(argv[1], port, data_path, log_path);
    RespClient client = Connect(port);
    Expect(client.Command({"PING"}), "+PONG", "PING");

    Expect(client.Command({"RANDOMKEY"}), "$-1", "RANDOMKEY empty database");
    Expect(client.Command({"SELECT", "15"}), "+OK", "RANDOMKEY select db15");
    Expect(client.Command({"SET", "only-random-key", "v"}), "+OK",
           "RANDOMKEY seed");
    Expect(client.Command({"RANDOMKEY"}), Bulk("only-random-key"),
           "RANDOMKEY single key");
    Expect(client.Command({"PEXPIRE", "only-random-key", "0"}), ":1",
           "RANDOMKEY expire seed");
    Expect(client.Command({"RANDOMKEY"}), "$-1",
           "RANDOMKEY ignores expired key");
    Expect(client.Command({"SELECT", "0"}), "+OK", "RANDOMKEY back to db0");

    Expect(client.Command({"MSET", "touch-a", "1", "touch-b", "2"}), "+OK",
           "TOUCH seed");
    Expect(
        client.Command({"TOUCH", "touch-a", "missing", "touch-a", "touch-b"}),
        ":3", "TOUCH counts duplicate live keys");

    Expect(client.Command({"SET", "copy-source", "source", "EX", "60"}), "+OK",
           "COPY string seed");
    Expect(client.Command({"COPY", "copy-source", "copy-destination"}), ":1",
           "COPY string");
    Expect(client.Command({"GET", "copy-destination"}), Bulk("source"),
           "COPY string value");
    Expect(client.Command({"PERSIST", "copy-destination"}), ":1",
           "COPY preserves TTL");
    Expect(client.Command({"SET", "copy-destination", "old"}), "+OK",
           "COPY existing destination");
    Expect(client.Command({"COPY", "copy-source", "copy-destination"}), ":0",
           "COPY without REPLACE");
    Expect(client.Command({"GET", "copy-destination"}), Bulk("old"),
           "COPY leaves destination without REPLACE");
    Expect(
        client.Command({"COPY", "copy-source", "copy-destination", "REPLACE"}),
        ":1", "COPY REPLACE");
    Expect(client.Command({"GET", "copy-destination"}), Bulk("source"),
           "COPY REPLACE value");
    Expect(
        client.Command({"COPY", "missing-copy", "copy-destination", "REPLACE"}),
        ":0", "COPY missing source");
    Expect(client.Command({"COPY", "copy-source", "copy-source"}),
           "-ERR source and destination objects are the same",
           "COPY same object");

    Expect(client.Command({"LPUSH", "copy-list", "a", "b"}), ":2",
           "COPY list seed");
    Expect(client.Command({"COPY", "copy-list", "copy-list-destination"}), ":1",
           "COPY list");
    Expect(client.Command({"LLEN", "copy-list-destination"}), ":2",
           "COPY preserves collection type");

    Expect(client.Command({"COPY", "copy-source", "copy-db", "DB", "2"}), ":1",
           "COPY cross database");
    Expect(client.Command({"SELECT", "2"}), "+OK", "COPY select destination");
    Expect(client.Command({"GET", "copy-db"}), Bulk("source"),
           "COPY cross database value");
    Expect(client.Command({"PERSIST", "copy-db"}), ":1",
           "COPY cross database TTL");
    Expect(client.Command({"SELECT", "0"}), "+OK", "COPY return to db0");
    Expect(client.Command({"COPY", "copy-source", "x", "DB", "16"}),
           "-ERR DB index is out of range", "COPY invalid DB");
    Expect(client.Command({"COPY", "copy-source", "x", "DB", "01"}),
           "-ERR value is not an integer or out of range",
           "COPY rejects a non-canonical DB index");
    Expect(client.Command({"COPY", "copy-source", "x", "DB", "+1"}),
           "-ERR value is not an integer or out of range",
           "COPY rejects a signed positive DB index");
    Expect(client.Command({"COPY", "copy-source", "x", "UNKNOWN"}),
           "-ERR syntax error", "COPY invalid option");

    // Cross-shard MSET/MGET: values come back in request order regardless of
    // which worker owns each key.
    Expect(client.Command({"MSET", "mk0", "v0", "mk1", "v1", "mk2", "v2", "mk3",
                           "v3", "mk4", "v4", "mk5", "v5", "mk6", "v6", "mk7",
                           "v7"}),
           "+OK", "cross-shard MSET");
    Expect(client.Command({"MGET", "mk5", "mk0", "missing", "mk7", "mk2"}),
           "*5\r\n" + Bulk("v5") + "\r\n" + Bulk("v0") + "\r\n$-1\r\n" +
               Bulk("v7") + "\r\n" + Bulk("v2"),
           "shuffled MGET");
    Expect(client.Command({"GET", "mk3"}), Bulk("v3"), "single GET after MSET");

    // Arity and pairing errors.
    Expect(client.Command({"MSET", "solo"}),
           "-ERR wrong number of arguments for 'mset' command",
           "MSET missing value");
    Expect(client.Command({"MSET", "a", "1", "b"}),
           "-ERR wrong number of arguments for 'mset' command",
           "MSET odd pair");
    Expect(client.Command({"MGET"}),
           "-ERR wrong number of arguments for 'mget' command", "empty MGET");

    // Duplicate keys: MSET applies in argument order (last wins), EXISTS
    // counts every occurrence, DEL deletes once.
    Expect(client.Command({"MSET", "dup", "first", "dup", "second"}), "+OK",
           "duplicate MSET");
    Expect(client.Command({"GET", "dup"}), Bulk("second"),
           "duplicate MSET last wins");
    Expect(client.Command({"EXISTS", "dup", "dup", "missing", "mk0"}), ":3",
           "EXISTS with duplicates");
    Expect(client.Command({"DEL", "dup", "dup"}), ":1", "duplicate DEL");
    Expect(client.Command({"EXISTS", "dup"}), ":0", "deleted dup");

    // Cross-shard DEL counts exactly the live keys it removed.
    Expect(client.Command({"DEL", "mk0", "missing", "mk5", "mk7", "mk7"}), ":3",
           "cross-shard DEL");
    Expect(client.Command({"MGET", "mk0", "mk5", "mk7", "mk1"}),
           "*4\r\n$-1\r\n$-1\r\n$-1\r\n" + Bulk("v1"), "MGET after DEL");

    // UNLINK shares DEL's deferred tombstone retirement but remains a
    // distinct command at dispatch and metrics boundaries.
    Expect(client.Command({"MSET", "unlink-a", "1", "unlink-b", "2"}), "+OK",
           "UNLINK seed");
    Expect(client.Command(
               {"UNLINK", "unlink-a", "missing", "unlink-b", "unlink-b"}),
           ":2", "cross-shard UNLINK");
    Expect(client.Command({"MGET", "unlink-a", "unlink-b"}), "*2\r\n$-1\r\n$-1",
           "MGET after UNLINK");

    // Hashtag keys share one slot: the whole command stays on a single shard
    // (fast path) and must behave identically.
    Expect(
        client.Command({"MSET", "{tag}a", "1", "{tag}b", "2", "{tag}c", "3"}),
        "+OK", "hashtag MSET");
    Expect(client.Command({"MGET", "{tag}c", "{tag}a", "{tag}b"}),
           "*3\r\n" + Bulk("3") + "\r\n" + Bulk("1") + "\r\n" + Bulk("2"),
           "hashtag MGET");
    Expect(client.Command({"DEL", "{tag}a", "{tag}b", "{tag}c", "{tag}d"}),
           ":3", "hashtag DEL");

    // Binary safety: RESP is length-prefixed, so keys and values may carry
    // CRLF, NUL, and arbitrary bytes with no escaping anywhere in the chain.
    {
      const std::string bin_key("k\r\n\x00\xff\x01", 6);
      const std::string bin_value("v\x00\r\n\xfe\\x41", 8);
      Expect(client.Command({"SET", bin_key, bin_value}), "+OK", "binary SET");
      Expect(client.Command({"GET", bin_key}), Bulk(bin_value), "binary GET");
      Expect(client.Command({"MGET", bin_key, "missing"}),
             "*2\r\n" + Bulk(bin_value) + "\r\n$-1", "binary MGET");
      Expect(client.Command({"EXISTS", bin_key}), ":1", "binary EXISTS");
      Expect(client.Command({"DEL", bin_key}), ":1", "binary DEL");
    }

    // Mixed sizes across shards, including a value above the inline limit.
    const std::string large(9ULL * 1024 * 1024, 'L');
    Expect(client.Command({"MSET", "small", "s", "large", large}), "+OK",
           "MSET with large value");
    Expect(client.Command({"MGET", "large", "small"}),
           "*2\r\n" + Bulk(large) + "\r\n" + Bulk("s"), "MGET large");
    Expect(client.Command({"DEL", "large", "small"}), ":2", "DEL large");

    // ---- KEYS / SCAN TYPE ----
    Expect(client.Command(
               {"MSET", "kx:1", "a", "kx:2", "b", "kx:3", "c", "other", "1"}),
           "+OK", "KEYS seed");
    auto expect_members = [&](const std::string& reply, std::size_t count,
                              const std::vector<std::string>& members,
                              const char* what) {
      const std::string header = "*" + std::to_string(count) + "\r\n";
      if (reply.compare(0, header.size(), header) != 0) {
        Fail(std::string(what) + " count mismatch: " + reply.substr(0, 120));
      }
      for (const std::string& member : members) {
        const std::string element =
            "$" + std::to_string(member.size()) + "\r\n" + member;
        if (reply.find(element) == std::string::npos) {
          Fail(std::string(what) + " missing member '" + member + "'");
        }
      }
    };
    expect_members(client.Command({"KEYS", "kx:*"}), 3,
                   {"kx:1", "kx:2", "kx:3"}, "KEYS glob");
    expect_members(client.Command({"KEYS", "kx:?"}), 3,
                   {"kx:1", "kx:2", "kx:3"}, "KEYS question mark");
    Expect(client.Command({"KEYS", "nomatch:*"}), "*0", "KEYS no match");
    expect_members(client.Command({"KEYS", "other"}), 1, {"other"},
                   "KEYS exact");

    // Full Redis glob: character classes, ranges, negation, and escapes.
    expect_members(client.Command({"KEYS", "kx:[12]"}), 2, {"kx:1", "kx:2"},
                   "KEYS char class");
    expect_members(client.Command({"KEYS", "kx:[1-2]"}), 2, {"kx:1", "kx:2"},
                   "KEYS class range");
    expect_members(client.Command({"KEYS", "kx:[^1]"}), 2, {"kx:2", "kx:3"},
                   "KEYS negated class");
    Expect(client.Command({"SET", "lit*eral", "x"}), "+OK", "escape seed");
    expect_members(client.Command({"KEYS", "lit\\*eral"}), 1, {"lit*eral"},
                   "KEYS escaped star");
    Expect(client.Command({"KEYS", "lit\\?eral"}), "*0",
           "KEYS escaped question mark");

    // Large key names: the total far exceeds one 64 KiB stream chunk, so
    // the reply must arrive complete across several bounded chunks.
    {
      std::vector<std::string> long_names;
      for (int i = 0; i < 48; ++i) {
        std::string name = "longname:" + std::to_string(i) + ":";
        name.append(3500, 'x');
        Expect(client.Command({"SET", name, "v"}), "+OK", "long name SET");
        long_names.push_back(std::move(name));
      }
      expect_members(client.Command({"KEYS", "longname:*"}), long_names.size(),
                     long_names, "KEYS long names");
      for (const std::string& name : long_names) {
        Expect(client.Command({"DEL", name}), ":1", "long name DEL");
      }
    }
    Expect(client.Command({"DEL", "lit*eral"}), ":1", "escape cleanup");

    // Streaming stays bounded: several hundred keys still arrive with an
    // exact element count.
    std::vector<std::string> volume_storage;
    for (unsigned batch = 0; batch < 6; ++batch) {
      std::vector<std::string_view> mset_args;
      volume_storage.clear();
      mset_args.push_back("MSET");
      for (unsigned i = 0; i < 50; ++i) {
        volume_storage.push_back("vol:" + std::to_string(batch * 50 + i));
        volume_storage.push_back("v");
      }
      for (const std::string& arg : volume_storage) {
        mset_args.push_back(arg);
      }
      Expect(client.Command(mset_args), "+OK", "volume MSET");
    }
    {
      const std::string reply = client.Command({"KEYS", "vol:*"});
      if (reply.compare(0, 6, "*300\r\n") != 0) {
        Fail("KEYS volume count mismatch: " + reply.substr(0, 60));
      }
      if (reply.find("$5\r\nvol:0\r\n") == std::string::npos ||
          reply.find("$7\r\nvol:299") == std::string::npos) {
        Fail("KEYS volume members missing");
      }
    }

    // SCAN TYPE: strings match, other types match nothing. Follow the
    // cursor to completion as any SCAN client must.
    auto scan_all = [&](std::string_view type) {
      std::string collected;
      std::string cursor = "0";
      do {
        const std::string reply = client.Command(
            {"SCAN", cursor, "MATCH", "kx:*", "COUNT", "1000", "TYPE", type});
        const std::size_t cursor_start = reply.find("\r\n") + 2;
        const std::size_t digits = reply.find("\r\n", cursor_start) + 2;
        const std::size_t digits_end = reply.find("\r\n", digits);
        cursor = reply.substr(digits, digits_end - digits);
        collected += reply.substr(digits_end);
      } while (cursor != "0");
      return collected;
    };
    if (scan_all("string").find("kx:1") == std::string::npos) {
      Fail("SCAN TYPE string missing keys");
    }
    if (scan_all("hash").find("kx:") != std::string::npos) {
      Fail("SCAN TYPE hash returned string keys");
    }

    // SCAN MATCH speaks the same glob dialect.
    {
      std::string collected;
      std::string cursor = "0";
      do {
        const std::string reply = client.Command(
            {"SCAN", cursor, "MATCH", "kx:[13]", "COUNT", "100000"});
        const std::size_t cursor_start = reply.find("\r\n") + 2;
        const std::size_t digits = reply.find("\r\n", cursor_start) + 2;
        const std::size_t digits_end = reply.find("\r\n", digits);
        cursor = reply.substr(digits, digits_end - digits);
        collected += reply.substr(digits_end);
      } while (cursor != "0");
      if (collected.find("kx:1") == std::string::npos ||
          collected.find("kx:3") == std::string::npos ||
          collected.find("kx:2") != std::string::npos) {
        Fail("SCAN MATCH character class mismatch: " + collected);
      }
    }

    // Transaction generations are rotated and cleaned by whichever periodic
    // worker wins the process-wide guard. Wait for an observed retirement so
    // this verifies the cleaner itself rather than merely sleeping.
    Expect(client.Command({"CONFIG", "SET", "tx-cleaner-cooldown-ms", "20"}),
           "+OK", "enable tx cleaner");
    Expect(client.Command({"CONFIG", "GET", "tx-cleaner-cooldown-ms"}),
           "*2\r\n" + Bulk("tx-cleaner-cooldown-ms") + "\r\n" + Bulk("20"),
           "read tx cleaner cooldown");
    const std::uint64_t cleaner_baseline =
        TxCleanerRetiredGenerations(client);
    Expect(client.Command({"MSET", "cleaner-a", "after-a", "cleaner-b",
                           "after-b", "cleaner-c", "after-c", "cleaner-d",
                           "after-d"}),
           "+OK", "tx cleaner seed");
    if (!WaitForCleanerRetirement(client, cleaner_baseline)) {
      Fail("transaction cleaner did not retire a generation");
    }
    Expect(client.Command({"MGET", "cleaner-d", "cleaner-a", "cleaner-c",
                           "cleaner-b"}),
           "*4\r\n" + Bulk("after-d") + "\r\n" + Bulk("after-a") +
               "\r\n" + Bulk("after-c") + "\r\n" + Bulk("after-b"),
           "values after tx cleaner retirement");

    server.Stop();
    ServerProcess recovered_server(argv[1], port, data_path, log_path);
    RespClient recovered = Connect(port);
    Expect(recovered.Command({"MGET", "cleaner-a", "cleaner-b", "cleaner-c",
                              "cleaner-d"}),
           "*4\r\n" + Bulk("after-a") + "\r\n" + Bulk("after-b") +
               "\r\n" + Bulk("after-c") + "\r\n" + Bulk("after-d"),
           "promoted values after restart");
    Expect(recovered.Command(
               {"CONFIG", "SET", "tx-cleaner-cooldown-ms", "0"}),
           "+OK", "disable tx cleaner before recovery fixture");
    Expect(recovered.Command({"MSET", "cleaner-recovery-a", "disk-a",
                              "cleaner-recovery-b", "disk-b"}),
           "+OK", "persist a closed generation for recovery");
    recovered_server.Stop();

    ServerProcess generation_recovery_server(argv[1], port, data_path,
                                             log_path);
    RespClient generation_recovery = Connect(port);
    const std::uint64_t recovered_cleaner_baseline =
        TxCleanerRetiredGenerations(generation_recovery);
    Expect(generation_recovery.Command(
               {"CONFIG", "SET", "tx-cleaner-cooldown-ms", "20"}),
           "+OK", "enable tx cleaner after generation recovery");
    if (!WaitForCleanerRetirement(generation_recovery,
                                  recovered_cleaner_baseline)) {
      Fail("recovered transaction generation was not retired");
    }
    Expect(generation_recovery.Command(
               {"MGET", "cleaner-recovery-a", "cleaner-recovery-b"}),
           "*2\r\n" + Bulk("disk-a") + "\r\n" + Bulk("disk-b"),
           "recovered generation values after retirement");

    // FLUSHDB invalidates tagged winners by advancing the database epoch.
    // The cleaner must not promote them into the new epoch; detached-index
    // reclaim instead drops their tagged-byte accounting so the complete
    // transaction generation can still be retired.
    Expect(generation_recovery.Command(
               {"CONFIG", "SET", "tx-cleaner-cooldown-ms", "0"}),
           "+OK", "disable tx cleaner before FLUSHDB fixture");
    Expect(generation_recovery.Command(
               {"MSET", "cleaner-flush-a", "old-a", "cleaner-flush-b",
                "old-b"}),
           "+OK", "persist tagged values before FLUSHDB");
    Expect(generation_recovery.Command({"FLUSHDB", "SYNC"}), "+OK",
           "flush tagged transaction generation");
    const std::uint64_t flushed_cleaner_baseline =
        TxCleanerRetiredGenerations(generation_recovery);
    Expect(generation_recovery.Command(
               {"CONFIG", "SET", "tx-cleaner-cooldown-ms", "20"}),
           "+OK", "enable tx cleaner after FLUSHDB");
    if (!WaitForCleanerRetirement(generation_recovery,
                                  flushed_cleaner_baseline)) {
      Fail("FLUSHDB-invalidated transaction generation was not retired");
    }
    Expect(generation_recovery.Command(
               {"EXISTS", "cleaner-flush-a", "cleaner-flush-b"}),
           ":0", "FLUSHDB values after transaction generation retirement");
    generation_recovery_server.Stop();

#ifndef NDEBUG
    // A failed transaction keeps its generation lease through rollback. Once
    // UNDO has restored every old value, dependency pins drop and the same
    // cleaner can retire the aborted tagged records safely.
    ServerProcess rollback_server(argv[1], port, data_path, log_path,
                                  "cleaner-undo-d");
    RespClient rollback = Connect(port);
    Expect(rollback.Command(
               {"CONFIG", "SET", "tx-cleaner-cooldown-ms", "20"}),
           "+OK", "enable tx cleaner during rollback");
    for (std::string_view key : {"cleaner-undo-a", "cleaner-undo-b",
                                 "cleaner-undo-c", "cleaner-undo-d"}) {
      Expect(rollback.Command({"SET", key, "old"}), "+OK",
             "tx cleaner rollback seed");
    }
    const std::uint64_t rollback_cleaner_baseline =
        TxCleanerRetiredGenerations(rollback);
    const std::string failed = rollback.Command(
        {"MSET", "cleaner-undo-a", "new-a", "cleaner-undo-b", "new-b",
         "cleaner-undo-c", "new-c", "cleaner-undo-d", "new-d"});
    if (!failed.starts_with("-ERR injected transaction write fault")) {
      Fail("fault-injected MSET unexpectedly returned: " + failed);
    }
    Expect(rollback.Command({"MGET", "cleaner-undo-a", "cleaner-undo-b",
                             "cleaner-undo-c", "cleaner-undo-d"}),
           "*4\r\n" + Bulk("old") + "\r\n" + Bulk("old") + "\r\n" +
               Bulk("old") + "\r\n" + Bulk("old"),
           "UNDO values while tx cleaner is enabled");
    if (!WaitForCleanerRetirement(rollback, rollback_cleaner_baseline)) {
      Fail("transaction cleaner did not retire the rolled-back generation");
    }
    rollback_server.Stop();

    ServerProcess rollback_recovered_server(argv[1], port, data_path,
                                            log_path);
    RespClient rollback_recovered = Connect(port);
    Expect(rollback_recovered.Command(
               {"MGET", "cleaner-undo-a", "cleaner-undo-b",
                "cleaner-undo-c", "cleaner-undo-d"}),
           "*4\r\n" + Bulk("old") + "\r\n" + Bulk("old") + "\r\n" +
               Bulk("old") + "\r\n" + Bulk("old"),
           "UNDO values after cleaner restart");
    rollback_recovered_server.Stop();

    // The first transaction suspends after selecting the current generation's
    // append stream and releasing store_state_mutex for block allocation. A
    // completed peer write lets the cleaner rotate, then the next generation
    // inserts into the same flat_hash_map while the first writer is suspended.
    // The resumed writer must re-find the old generation rather than
    // dereference storage invalidated by that insertion's rehash.
    ServerProcess rehash_server(argv[1], port, data_path, log_path, {}, "3000");
    RespClient rehash_control = Connect(port);
    Expect(rehash_control.Command(
               {"CONFIG", "SET", "tx-cleaner-cooldown-ms", "20"}),
           "+OK", "enable cleaner for active transaction map rehash");
    std::this_thread::sleep_for(100ms);
    const std::uint64_t rehash_round_baseline =
        TxCleanerStat(rehash_control, "tx_cleaner_rounds:");
    auto paused_write = std::async(std::launch::async, [port] {
      RespClient client = Connect(port);
      return client.Command({"MSET", "rehash-paused-a{tx-map}", "paused-a",
                             "rehash-paused-b{tx-map}", "paused-b"});
    });
    std::this_thread::sleep_for(100ms);
    Expect(rehash_control.Command(
               {"MSET", "rehash-seed-a{tx-map}", "seed-a",
                "rehash-seed-b{tx-map}", "seed-b"}),
           "+OK", "seed generation while allocation is paused");
    if (!WaitForCleanerStat(rehash_control, "tx_cleaner_rounds:",
                            rehash_round_baseline)) {
      Fail("cleaner did not rotate the paused transaction generation");
    }
    Expect(rehash_control.Command(
               {"MSET", "rehash-trigger-a{tx-map}", "trigger-a",
                "rehash-trigger-b{tx-map}", "trigger-b"}),
           "+OK", "insert a new active transaction generation");
    if (paused_write.wait_for(5s) != std::future_status::ready) {
      Fail("paused transaction did not resume after active map rehash");
    }
    Expect(paused_write.get(), "+OK", "paused transaction after map rehash");
    Expect(rehash_control.Command(
               {"MGET", "rehash-paused-a{tx-map}",
                "rehash-paused-b{tx-map}", "rehash-seed-a{tx-map}",
                "rehash-seed-b{tx-map}", "rehash-trigger-a{tx-map}",
                "rehash-trigger-b{tx-map}"}),
           "*6\r\n" + Bulk("paused-a") + "\r\n" + Bulk("paused-b") +
               "\r\n" + Bulk("seed-a") + "\r\n" + Bulk("seed-b") +
               "\r\n" + Bulk("trigger-a") + "\r\n" + Bulk("trigger-b"),
           "values after active transaction map rehash");
    rehash_server.Stop();

    // A retryable cleaner failure is observable but must not terminate the
    // periodic flush coroutine or report a shutdown drain as complete. The
    // same process must run a later round and retire the generation.
    ServerProcess retry_server(argv[1], port, data_path, log_path, {}, {},
                               true);
    RespClient retry = Connect(port);
    const std::uint64_t failure_baseline =
        TxCleanerStat(retry, "tx_cleaner_failures:");
    const std::uint64_t retry_retired_baseline =
        TxCleanerRetiredGenerations(retry);
    Expect(retry.Command({"MSET", "cleaner-retry-a{tx}", "durable-a",
                          "cleaner-retry-b{tx}", "durable-b"}), "+OK",
           "seed retryable cleaner failure");
    Expect(retry.Command(
               {"CONFIG", "SET", "tx-cleaner-cooldown-ms", "20"}),
           "+OK", "enable retryable cleaner fixture");
    if (!WaitForCleanerStat(retry, "tx_cleaner_failures:",
                            failure_baseline, 10s)) {
      Fail("injected cleaner failure was not recorded");
    }
    if (!WaitForCleanerRetirement(retry, retry_retired_baseline)) {
      Fail("periodic flush stopped after a retryable cleaner failure");
    }
    Expect(retry.Command({"MGET", "cleaner-retry-a{tx}",
                          "cleaner-retry-b{tx}"}),
           "*2\r\n" + Bulk("durable-a") + "\r\n" + Bulk("durable-b"),
           "value after cleaner retry");
    retry_server.Stop();

    ServerProcess retry_recovered_server(argv[1], port, data_path, log_path);
    RespClient retry_recovered = Connect(port);
    Expect(retry_recovered.Command({"MGET", "cleaner-retry-a{tx}",
                                    "cleaner-retry-b{tx}"}),
           "*2\r\n" + Bulk("durable-a") + "\r\n" + Bulk("durable-b"),
           "cleaner retry value after graceful shutdown");
    retry_recovered_server.Stop();
#endif

    // A reservation that is never given back stands against its worker's
    // publish-queue waterline for the life of the process, so a long run of
    // wide writes against a deliberately small waterline wedges if any
    // participant is ever missed. The replica must also converge on exactly
    // the effects the source applied, in the order it applied them.
    {
      // The dataset here is a few megabytes; size the pair for that rather
      // than for the whole-suite fixture above.
      CreateDataFile(source_data, 256ULL * 1024 * 1024);
      CreateDataFile(replica_data, 256ULL * 1024 * 1024);
      const std::uint16_t source_port = FindFreePort();
      std::uint16_t replica_port = FindFreePort();
      while (replica_port == source_port) replica_port = FindFreePort();
      // Both log to log_path: the likeliest failures here are replica-side,
      // and that is the log the failure handler prints.
      ServerProcess replication_source(argv[1], source_port, source_data,
                                       log_path);
      ServerProcess replication_replica(argv[1], replica_port, replica_data,
                                        log_path, {}, {}, false, 2);
      RespClient source_client = Connect(source_port);
      RespClient replica_client = Connect(replica_port);
      Expect(source_client.Command({"CONFIG", "SET",
                                    "replication-publish-queue-mb-per-worker",
                                    "1"}),
             "+OK", "shrink the publisher waterline");
      Expect(replica_client.Command(
                 {"REPLICAOF", "127.0.0.1", std::to_string(source_port)}),
             "+OK", "attach replica for wide multi-key writes");
      const auto online_deadline = std::chrono::steady_clock::now() + 120s;
      bool online = false;
      while (!online && std::chrono::steady_clock::now() < online_deadline) {
        online =
            replica_client.Command({"INFO", "replication"})
                .find("keylane_replication_state:online") != std::string::npos;
        if (!online) std::this_thread::sleep_for(20ms);
      }
      if (!online) Fail("replica did not come online for wide writes");
      // A replica redirects keyed reads to its upstream unless the connection
      // opts into serving them locally.
      Expect(replica_client.Command({"READONLY"}), "+OK",
             "serve reads from the replica");

      constexpr int kWideKeys = 24;
      constexpr int kWideRounds = 120;
      std::vector<std::string> wide_keys;
      wide_keys.reserve(kWideKeys);
      for (int key = 0; key < kWideKeys; ++key) {
        wide_keys.push_back("wide-multikey:" + std::to_string(key));
      }
      const std::string payload(1024, 'w');
      std::vector<std::string> values(kWideKeys);
      for (int round = 0; round < kWideRounds; ++round) {
        std::vector<std::string_view> command{"MSET"};
        command.reserve(1 + 2 * kWideKeys);
        for (int key = 0; key < kWideKeys; ++key) {
          values[key] =
              std::to_string(round) + ":" + std::to_string(key) + ":" + payload;
          command.push_back(wide_keys[key]);
          command.push_back(values[key]);
        }
        Expect(source_client.Command(command), "+OK",
               "wide MSET round " + std::to_string(round));
      }
      // A multi-key DEL settles the same per-worker journals. Deleting only
      // half the keys makes the replica's final state prove ordering: had the
      // DEL been applied before the last MSET round, those keys would still
      // hold values.
      std::vector<std::string_view> wide_delete{"DEL"};
      for (int key = 0; key < kWideKeys; key += 2) {
        wide_delete.push_back(wide_keys[key]);
      }
      Expect(source_client.Command(wide_delete),
             ":" + std::to_string(kWideKeys / 2), "wide multi-key DEL");

      std::vector<std::string_view> wide_read{"MGET"};
      std::string expected = "*" + std::to_string(kWideKeys) + "\r\n";
      for (int key = 0; key < kWideKeys; ++key) {
        wide_read.push_back(wide_keys[key]);
        if (key != 0) expected += "\r\n";
        expected += key % 2 == 0 ? std::string("$-1") : Bulk(values[key]);
      }
      Expect(source_client.Command(wide_read), expected,
             "source state after wide multi-key writes");
      const auto converge_deadline = std::chrono::steady_clock::now() + 120s;
      std::string replicated;
      while (std::chrono::steady_clock::now() < converge_deadline) {
        replicated = replica_client.Command(wide_read);
        if (replicated == expected) break;
        std::this_thread::sleep_for(20ms);
      }
      Expect(replicated, expected, "replicated wide multi-key effects");
      // Whatever the run reserved has to have been given back: the source
      // must still admit a further write rather than sit permanently wedged
      // against its own waterline. A wedge surfaces as the client's socket
      // read timing out, not as an error reply, since a write that cannot be
      // admitted simply never answers.
      Expect(source_client.Command({"SET", "wide-multikey:after", "ok"}), "+OK",
             "source write after the wide multi-key burst");
    }
  } catch (const std::exception& error) {
    std::cerr << error.what() << "\n--- Keylane log ---\n"
              << ReadFile(log_path) << std::flush;
    exit_code = 1;
  }

  (void)::unlink(data_path.c_str());
  (void)::unlink(log_path.c_str());
  (void)::unlink(source_data.c_str());
  (void)::unlink(replica_data.c_str());
  std::cout << (exit_code == 0 ? "multikey e2e passed\n" : "") << std::flush;
  return exit_code;
}
