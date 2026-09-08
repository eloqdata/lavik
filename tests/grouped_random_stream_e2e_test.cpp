#include "grouped_write_e2e_support.h"

namespace {
using namespace grouped_e2e;

struct ScopedEnvironment {
  ScopedEnvironment(const char* name, const char* value) : name_(name) {
    if (const char* old = std::getenv(name)) old_ = old;
    ::setenv(name, value, 1);
  }
  ~ScopedEnvironment() {
    if (old_)
      ::setenv(name_, old_->c_str(), 1);
    else
      ::unsetenv(name_);
  }
  const char* name_;
  std::optional<std::string> old_;
};

std::string Bulk(std::string_view value) {
  return "$" + std::to_string(value.size()) + "\r\n" + std::string(value) +
         "\r\n";
}

class WireClient {
 public:
  explicit WireClient(std::uint16_t port) {
    fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    Check(fd_ >= 0, "socket failed");
    sockaddr_in address{.sin_family = AF_INET,
                        .sin_port = htons(port),
                        .sin_addr = {.s_addr = htonl(INADDR_LOOPBACK)}};
    Check(::connect(fd_, reinterpret_cast<sockaddr*>(&address),
                    sizeof(address)) == 0,
          "connect failed");
    timeval timeout{.tv_sec = 15};
    ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
  }
  ~WireClient() { ::close(fd_); }
  void Send(const std::vector<std::string>& args) {
    std::string wire = "*" + std::to_string(args.size()) + "\r\n";
    for (const auto& arg : args) wire += Bulk(arg);
    std::string_view rest(wire);
    while (!rest.empty()) {
      const auto n = ::send(fd_, rest.data(), rest.size(), MSG_NOSIGNAL);
      if (n < 0 && errno == EINTR) continue;
      Check(n > 0, "send failed");
      rest.remove_prefix(n);
    }
  }
  std::string Read(std::size_t bytes) {
    std::string result(bytes, '\0');
    for (std::size_t offset = 0; offset != bytes;) {
      const auto n = ::recv(fd_, result.data() + offset, bytes - offset, 0);
      if (n < 0 && errno == EINTR) continue;
      Check(n > 0, "stream ended early");
      offset += n;
    }
    return result;
  }
  void Expect(const std::vector<std::string>& args, std::string_view expected) {
    Send(args);
    EXPECT_EQ(Read(expected.size()), expected);
  }

 private:
  int fd_ = -1;
};

std::vector<std::string> Seed(std::string_view type, const std::string& key,
                              const std::string& item) {
  if (type == "hash") return {"HSET", key, item, item};
  if (type == "set") return {"SADD", key, item};
  return {"ZADD", key, "7", item};
}
std::vector<std::string> Sample(std::string_view type, const std::string& key) {
  if (type == "hash") return {"HRANDFIELD", key, "-1001", "WITHVALUES"};
  if (type == "set") return {"SRANDMEMBER", key, "-1001"};
  return {"ZRANDMEMBER", key, "-1001", "WITHSCORES"};
}

TEST(GroupedRandomStreamE2e, FiniteRepliesAreByteExactAcrossBulkBoundaries) {
  PrivateDisk disk;
  Server server(disk);
  Client client(server.port());
  // A singleton makes sampling deterministic; RESP punctuation and NUL bytes
  // catch accidental framing or text conversion at chunk boundaries.
  std::string item(2048, 'r');
  item.replace(19, 5, "\r\n$*\0", 5);
  for (const auto* type : {"hash", "set", "zset"}) {
    SCOPED_TRACE(type);
    const std::string key = std::string(type) + "{stream}";
    ASSERT_EQ(client.Command(Seed(type, key, item)).text_, "1");
    const std::string tuple =
        Bulk(item) + (std::string_view(type) == "hash"   ? Bulk(item)
                      : std::string_view(type) == "zset" ? Bulk("7")
                                                         : "");
    std::string expected =
        std::string_view(type) == "set" ? "*1001\r\n" : "*2002\r\n";
    for (unsigned i = 0; i < 1001; ++i) expected += tuple;
    WireClient wire(server.port());
    wire.Expect(Sample(type, key), expected);
    wire.Expect({"PING"}, "+PONG\r\n");
    wire.Expect({"MULTI"}, "+OK\r\n");
    wire.Expect(Sample(type, key), "+QUEUED\r\n");
    wire.Expect({"DEL", key}, "+QUEUED\r\n");
    // DEL executes before draining: sampling must preserve the earlier view.
    wire.Expect({"EXEC"}, "*2\r\n" + expected + ":1\r\n");
    wire.Expect({"PING"}, "+PONG\r\n");
    EXPECT_EQ(client.Command({"EXISTS", key}).text_, "0");
  }
  ASSERT_EQ(server.Wait(true), 0) << server.Log();
}

TEST(GroupedRandomStreamE2e, DisconnectDuringLargeBulkReleasesStreamState) {
  PrivateDisk disk;
  Server server(disk, 2, {}, {}, false, 2, "128M", {}, "32M");
  Client client(server.port());
  const std::string item(2 * 1024 * 1024, 'b');
  for (const auto* type : {"hash", "set", "zset"}) {
    for (const bool transaction : {false, true}) {
      SCOPED_TRACE(std::string(type) + (transaction ? " EXEC" : " ordinary"));
      const std::string key = std::string(type) + "{cancel}";
      ASSERT_EQ(client.Command(Seed(type, key, item)).text_, "1");
      {
        WireClient wire(server.port());
        if (transaction) {
          wire.Expect({"MULTI"}, "+OK\r\n");
          wire.Expect(Sample(type, key), "+QUEUED\r\n");
          wire.Expect({"DEL", key}, "+QUEUED\r\n");
          wire.Send({"EXEC"});
        } else {
          wire.Send(Sample(type, key));
        }
        std::string prefix = transaction ? "*2\r\n" : "";
        prefix += std::string_view(type) == "set" ? "*1001\r\n" : "*2002\r\n";
        prefix += "$2097152\r\n";
        prefix.append(65536, 'b');
        ASSERT_EQ(wire.Read(prefix.size()), prefix);
        // Cancel within one bulk: never collect the finite declared response.
      }
      EXPECT_EQ(client.Command({"PING"}).text_, "PONG");
      EXPECT_EQ(client.Command({"DEL", key}).text_, transaction ? "0" : "1");
    }
  }
  ASSERT_EQ(client.Command({"SET", "after-cancel", "ok"}).text_, "OK");
  ASSERT_EQ(client.Command({"GET", "after-cancel"}).text_, "ok");
  ASSERT_EQ(server.Wait(true), 0) << server.Log();
}

TEST(GroupedRandomStreamE2e,
     AllocationFailureReturnsOneErrorWithoutArrayPrefix) {
#if !KEYLANE_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires Debug/fault random stream construction hook";
#endif
  const std::string key = "random-build-failure";
  for (const auto* stage : {"before-source", "after-header"}) {
    SCOPED_TRACE(stage);
    ScopedEnvironment fail_key("KEYLANE_FAIL_RANDOM_STREAM_KEY", key.c_str());
    ScopedEnvironment fail_stage("KEYLANE_FAIL_RANDOM_STREAM_STAGE", stage);
    PrivateDisk disk;
    Server server(disk);
    Client client(server.port());
    WireClient wire(server.port());
    for (const auto* type : {"hash", "set", "zset"}) {
      SCOPED_TRACE(type);
      ASSERT_EQ(client.Command(Seed(type, key, std::string(2048, 'f'))).text_,
                "1");
      wire.Expect(
          Sample(type, key),
          "-OOM command not allowed when used memory > 'maxmemory'.\r\n");
      wire.Expect({"PING"}, "+PONG\r\n");
      wire.Expect({"MULTI"}, "+OK\r\n");
      wire.Expect(Sample(type, key), "+QUEUED\r\n");
      wire.Expect({"PING"}, "+QUEUED\r\n");
      wire.Expect({"EXEC"},
                  "*2\r\n-ERR OOM transactional random stream\r\n+PONG\r\n");
      wire.Expect({"PING"}, "+PONG\r\n");
      EXPECT_EQ(client.Command({"DEL", key}).text_, "1");
    }
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
}
}  // namespace
