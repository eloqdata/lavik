#include <optional>

#include "grouped_write_e2e_support.h"

namespace {
using namespace grouped_e2e;

enum class Collection { kSet, kList, kSortedSet };
constexpr std::uint64_t kPrivateDiskBytes = 128ULL * 1024 * 1024;

class ScopedFault {
 public:
  ScopedFault(const char* variable, const char* value) : variable_(variable) {
    if (const char* previous = std::getenv(variable)) previous_ = previous;
    Check(::setenv(variable, value, 1) == 0, "setting test fault failed");
  }
  ~ScopedFault() {
    if (previous_)
      ::setenv(variable_, previous_->c_str(), 1);
    else
      ::unsetenv(variable_);
  }

 private:
  const char* variable_;
  std::optional<std::string> previous_;
};

// Keep send and read separate: the marker identifies the unlocked interval,
// and another connection can prove progress without a guessed start delay.
class PendingWrite {
 public:
  explicit PendingWrite(std::uint16_t port) {
    fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    Check(fd_ >= 0, "pending socket failed");
    sockaddr_in address{.sin_family = AF_INET,
                        .sin_port = htons(port),
                        .sin_addr = {.s_addr = htonl(INADDR_LOOPBACK)}};
    if (::connect(fd_, reinterpret_cast<sockaddr*>(&address),
                  sizeof(address)) != 0) {
      ::close(fd_);
      fd_ = -1;
      throw std::runtime_error("pending connection failed");
    }
    const timeval timeout{.tv_sec = 15, .tv_usec = 0};
    ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
  }
  ~PendingWrite() {
    if (fd_ >= 0) ::close(fd_);
  }
  void Send(const std::vector<std::string>& args) {
    std::string wire = "*" + std::to_string(args.size()) + "\r\n";
    for (const auto& arg : args)
      wire += "$" + std::to_string(arg.size()) + "\r\n" + arg + "\r\n";
    std::string_view remaining(wire);
    while (!remaining.empty()) {
      const auto sent =
          ::send(fd_, remaining.data(), remaining.size(), MSG_NOSIGNAL);
      if (sent < 0 && errno == EINTR) continue;
      Check(sent > 0, "pending send failed");
      remaining.remove_prefix(sent);
    }
  }
  bool HasReply() {
    char byte;
    ssize_t received;
    do {
      received = ::recv(fd_, &byte, 1, MSG_PEEK | MSG_DONTWAIT);
    } while (received < 0 && errno == EINTR);
    if (received < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return false;
    Check(received > 0, "pending connection closed");
    return true;
  }
  Reply Read() {
    std::string line;
    while (!line.ends_with("\r\n")) {
      line += Bytes(1);
      Check(line.size() < 4096, "unexpected pending reply header");
    }
    Reply result{.kind_ = line.front(),
                 .text_ = line.substr(1, line.size() - 3)};
    if (result.kind_ == '$' && result.text_ != "-1") {
      result.text_ = Bytes(std::stoull(result.text_));
      Check(Bytes(2) == "\r\n", "invalid pending bulk reply");
    }
    return result;
  }

 private:
  std::string Bytes(std::size_t size) {
    std::string result(size, '\0');
    for (std::size_t offset = 0; offset < size;) {
      const auto count = ::recv(fd_, result.data() + offset, size - offset, 0);
      if (count < 0 && errno == EINTR) continue;
      Check(count > 0, "pending reply ended early");
      offset += count;
    }
    return result;
  }
  int fd_ = -1;
};

bool AwaitMarker(const Server& server, const std::string& marker) {
  const auto deadline = std::chrono::steady_clock::now() + 20s;
  do {
    if (server.Log().find(marker) != std::string::npos) return true;
    std::this_thread::sleep_for(10ms);
  } while (std::chrono::steady_clock::now() < deadline);
  return false;
}

std::size_t MarkerCount(std::string_view text, std::string_view marker) {
  std::size_t count = 0;
  for (std::size_t at = 0; (at = text.find(marker, at)) != text.npos;
       at += marker.size())
    ++count;
  return count;
}

std::vector<std::string> Seed(Collection type, const std::string& key,
                              bool single = false) {
  if (type == Collection::kSet)
    return single ? std::vector<std::string>{"SADD", key, "a"}
                  : std::vector<std::string>{"SADD", key, "a", "b"};
  if (type == Collection::kList)
    return single ? std::vector<std::string>{"RPUSH", key, "a"}
                  : std::vector<std::string>{"RPUSH", key, "a", "b"};
  return single ? std::vector<std::string>{"ZADD", key, "1", "a"}
                : std::vector<std::string>{"ZADD", key, "1", "a", "2", "b"};
}

std::string LengthCommand(Collection type) {
  return type == Collection::kSet    ? "SCARD"
         : type == Collection::kList ? "LLEN"
                                     : "ZCARD";
}

std::vector<std::string> FirstWrite(Collection type, const std::string& key) {
  if (type == Collection::kSet) return {"SADD", key, "c"};
  if (type == Collection::kList) return {"LPUSH", key, "first"};
  return {"ZINCRBY", key, "4", "a"};
}

std::vector<std::string> SecondWrite(Collection type, const std::string& key) {
  if (type == Collection::kSet) return {"SREM", key, "a"};
  if (type == Collection::kList) return {"RPUSH", key, "second"};
  return {"ZADD", key, "6", "c"};
}

void CheckPausedResult(Client& client, Collection type,
                       const std::string& key) {
  if (type == Collection::kSet) {
    EXPECT_EQ(client.Command({"SCARD", key}).text_, "2");
    EXPECT_EQ(client.Command({"SISMEMBER", key, "a"}).text_, "0");
    for (const char* member : {"b", "c"})
      EXPECT_EQ(client.Command({"SISMEMBER", key, member}).text_, "1");
  } else if (type == Collection::kList) {
    const auto reply = client.Command({"LRANGE", key, "0", "-1"});
    ASSERT_EQ(reply.items_.size(), 4);
    const std::vector<std::string> expected{"first", "a", "b", "second"};
    for (std::size_t i = 0; i < expected.size(); ++i)
      EXPECT_EQ(reply.items_[i].text_, expected[i]);
  } else {
    EXPECT_EQ(client.Command({"ZCARD", key}).text_, "3");
    EXPECT_EQ(client.Command({"ZSCORE", key, "a"}).text_, "5");
    EXPECT_EQ(client.Command({"ZSCORE", key, "b"}).text_, "2");
    EXPECT_EQ(client.Command({"ZSCORE", key, "c"}).text_, "6");
  }
}

void ExpectWatchOutcome(Client& watcher, Collection type,
                        const std::string& key, bool aborted) {
  ASSERT_EQ(watcher.Command({"MULTI"}).text_, "OK");
  ASSERT_EQ(watcher.Command({LengthCommand(type), key}).text_, "QUEUED");
  const auto result = watcher.Command({"EXEC"});
  EXPECT_EQ(result.kind_, '*');
  if (aborted)
    EXPECT_EQ(result.text_, "-1");
  else
    EXPECT_EQ(result.items_.size(), 1);
}

class CompactCollectionWriteE2e : public testing::TestWithParam<Collection> {};

TEST_P(CompactCollectionWriteE2e, ColdPauseReleasesStateButSerializesSameKey) {
#if !KEYLANE_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires Debug/fault compact collection pause hook";
#endif
  const auto type = GetParam();
  const std::string key = "compact-target";
  PrivateDisk disk(kPrivateDiskBytes);
  disk.PreserveOnFailure();
  {
    Server server(disk, 1);
    server.PreserveOnFailure();
    Client client(server.port());
    ASSERT_EQ(client.Command(Seed(type, key)).text_, "2");
    ASSERT_EQ(client.Command(Seed(type, "other-collection")).text_, "2");
    ASSERT_EQ(client.Command({"SET", "other-string", "before"}).text_, "OK");
    ASSERT_EQ(client.Command({"HSET", "other-hash", "seed", "keep"}).text_,
              "1");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  ASSERT_TRUE(disk.Auxiliaries(key).empty());
  {
    ScopedFault paused_key("KEYLANE_COMPACT_WRITE_PAUSE_KEY", key.c_str());
    ScopedFault pause_ms("KEYLANE_COMPACT_WRITE_PAUSE_MS", "3000");
    Server server(disk, 1);
    server.PreserveOnFailure();
    Client client(server.port());
    PendingWrite first(server.port()), second(server.port());
    // Do not read the target before this command: its old bytes must be on
    // disk after restart, not warmed by a fixture read or a previous write.
    first.Send(FirstWrite(type, key));
    const std::string armed = "compact collection write pause armed key=" + key;
    const std::string complete =
        "compact collection write pause complete key=" + key;
    ASSERT_TRUE(AwaitMarker(server, armed)) << server.Log();
    second.Send(SecondWrite(type, key));
    EXPECT_EQ(client.Command({"SET", "other-string", "after"}).text_, "OK");
    EXPECT_EQ(
        client.Command({"HSET", "other-hash", "new", "independent"}).text_,
        "1");
    EXPECT_EQ(client.Command({"GET", "other-string"}).text_, "after");
    EXPECT_EQ(client.Command({"HGET", "other-hash", "new"}).text_,
              "independent");
    EXPECT_NE(client.Command(FirstWrite(type, "other-collection")).kind_, '-');
    EXPECT_EQ(server.Log().find(complete), std::string::npos) << server.Log();
    EXPECT_FALSE(first.HasReply());
    EXPECT_FALSE(second.HasReply());
    const auto a = first.Read();
    EXPECT_EQ(a.kind_, type == Collection::kSortedSet ? '$' : ':');
    EXPECT_EQ(a.text_, type == Collection::kSet    ? "1"
                       : type == Collection::kList ? "3"
                                                   : "5");
    const auto b = second.Read();
    EXPECT_EQ(b.kind_, ':');
    EXPECT_EQ(b.text_, type == Collection::kList ? "4" : "1");
    CheckPausedResult(client, type, key);

    // Borrowed EXEC writes retain their old path, even with the pause selector
    // armed for this same compact key. Repeating existing bytes preserves the
    // final contents while detecting accidental expansion of the new scope.
    const auto pauses_before = MarkerCount(server.Log(), armed);
    ASSERT_EQ(client.Command({"MULTI"}).text_, "OK");
    const auto unchanged =
        type == Collection::kSet ? std::vector<std::string>{"SADD", key, "b"}
        : type == Collection::kList
            ? std::vector<std::string>{"LSET", key, "0", "first"}
            : std::vector<std::string>{"ZADD", key, "XX", "5", "a"};
    ASSERT_EQ(client.Command(unchanged).text_, "QUEUED");
    ASSERT_EQ(client.Command({"EXEC"}).items_.size(), 1);
    EXPECT_EQ(MarkerCount(server.Log(), armed), pauses_before) << server.Log();
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server recovered(disk, 2);
  recovered.PreserveOnFailure();
  Client client(recovered.port());
  CheckPausedResult(client, type, key);
  EXPECT_EQ(client.Command({"GET", "other-string"}).text_, "after");
  EXPECT_EQ(client.Command({"HGET", "other-hash", "seed"}).text_, "keep");
  ASSERT_EQ(recovered.Wait(true), 0) << recovered.Log();
}

TEST_P(CompactCollectionWriteE2e, ConditionsErrorsTtlWatchAndDeleteRecover) {
  const auto type = GetParam();
  const std::string key = "compact-semantics";
  PrivateDisk disk(kPrivateDiskBytes);
  disk.PreserveOnFailure();
  {
    Server server(disk, 1);
    server.PreserveOnFailure();
    Client client(server.port());
    ASSERT_EQ(client.Command(Seed(type, key)).text_, "2");
    ASSERT_EQ(client.Command(Seed(type, "last", true)).text_, "1");
    ASSERT_EQ(client.Command({"EXPIRE", key, "600"}).text_, "1");
    ASSERT_EQ(client.Command({"EXPIRE", "last", "600"}).text_, "1");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  ASSERT_TRUE(disk.Auxiliaries(key).empty());
  {
    Server server(disk, 1);
    server.PreserveOnFailure();
    Client client(server.port()), watcher(server.port());
    const auto ttl = std::stoll(client.Command({"PTTL", key}).text_);
    ASSERT_GT(ttl, 0);
    ASSERT_EQ(watcher.Command({"WATCH", key}).text_, "OK");
    if (type == Collection::kSet) {
      EXPECT_EQ(client.Command({"SADD", key, "a", "a"}).text_, "0");
      ExpectWatchOutcome(watcher, type, key, false);
      ASSERT_EQ(watcher.Command({"WATCH", key}).text_, "OK");
      EXPECT_EQ(client.Command({"SADD", key, "c", "c"}).text_, "1");
      ExpectWatchOutcome(watcher, type, key, true);
      EXPECT_EQ(client.Command({"SREM", key, "a", "a", "missing"}).text_, "1");
      EXPECT_EQ(client.Command({"SREM", "last", "a", "a"}).text_, "1");
    } else if (type == Collection::kList) {
      EXPECT_EQ(client.Command({"LSET", key, "99", "invalid"}).kind_, '-');
      ExpectWatchOutcome(watcher, type, key, false);
      ASSERT_EQ(watcher.Command({"WATCH", key}).text_, "OK");
      EXPECT_EQ(client.Command({"LSET", key, "0", "a"}).text_, "OK");
      ExpectWatchOutcome(watcher, type, key, true);
      EXPECT_EQ(client.Command({"LPUSH", key, "left"}).text_, "3");
      EXPECT_EQ(client.Command({"RPUSH", key, "right"}).text_, "4");
      EXPECT_EQ(client.Command({"LPOP", key}).text_, "left");
      EXPECT_EQ(client.Command({"RPOP", key}).text_, "right");
      EXPECT_EQ(client.Command({"LSET", key, "-1", "B"}).text_, "OK");
      EXPECT_EQ(client.Command({"LPOP", "last"}).text_, "a");
      EXPECT_EQ(client.Command({"LSET", "last", "0", "missing"}).kind_, '-');
    } else {
      EXPECT_EQ(client.Command({"ZADD", key, "NX", "9", "a"}).text_, "0");
      EXPECT_EQ(client.Command({"ZADD", key, "XX", "9", "missing"}).text_, "0");
      ExpectWatchOutcome(watcher, type, key, false);
      ASSERT_EQ(watcher.Command({"WATCH", key}).text_, "OK");
      EXPECT_EQ(client.Command({"ZADD", key, "XX", "CH", "4", "a"}).text_, "1");
      ExpectWatchOutcome(watcher, type, key, true);
      EXPECT_EQ(client.Command({"ZINCRBY", key, "2", "a"}).text_, "6");
      EXPECT_EQ(client.Command({"ZADD", key, "CH", "7", "c", "8", "c"}).text_,
                "2");
      EXPECT_EQ(
          client.Command({"ZADD", key, "9", "not-added", "nan", "bad"}).kind_,
          '-');
      EXPECT_EQ(client.Command({"ZSCORE", key, "not-added"}).text_, "-1");
      EXPECT_EQ(client.Command({"ZADD", key, "inf", "infinity"}).text_, "1");
      EXPECT_EQ(client.Command({"ZINCRBY", key, "-inf", "infinity"}).kind_,
                '-');
      EXPECT_EQ(client.Command({"ZSCORE", key, "infinity"}).text_, "inf");
      EXPECT_EQ(
          client.Command({"ZREM", key, "b", "b", "missing", "infinity"}).text_,
          "2");
      EXPECT_EQ(client.Command({"ZREM", "last", "a", "a"}).text_, "1");
    }
    EXPECT_EQ(client.Command({"EXISTS", "last"}).text_, "0");
    ASSERT_EQ(client.Command(Seed(type, "last", true)).text_, "1");
    EXPECT_EQ(client.Command({"PTTL", "last"}).text_, "-1");
    const auto after_ttl = std::stoll(client.Command({"PTTL", key}).text_);
    EXPECT_GT(after_ttl, 0);
    EXPECT_LE(after_ttl, ttl);
    EXPECT_EQ(client.Command({LengthCommand(type), key}).text_, "2");
    EXPECT_EQ(client.Command({"SET", "wrongtype", "kept"}).text_, "OK");
    EXPECT_TRUE(client.Command(FirstWrite(type, "wrongtype"))
                    .text_.starts_with("WRONGTYPE"));
    EXPECT_EQ(client.Command({"GET", "wrongtype"}).text_, "kept");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  ASSERT_TRUE(disk.Auxiliaries(key).empty());
  Server recovered(disk, 2);
  recovered.PreserveOnFailure();
  Client client(recovered.port());
  EXPECT_EQ(client.Command({LengthCommand(type), key}).text_, "2");
  EXPECT_GT(std::stoll(client.Command({"PTTL", key}).text_), 0);
  EXPECT_EQ(client.Command({LengthCommand(type), "last"}).text_, "1");
  EXPECT_EQ(client.Command({"PTTL", "last"}).text_, "-1");
  if (type == Collection::kSet) {
    EXPECT_EQ(client.Command({"SISMEMBER", key, "b"}).text_, "1");
    EXPECT_EQ(client.Command({"SISMEMBER", key, "c"}).text_, "1");
  } else if (type == Collection::kList) {
    EXPECT_EQ(client.Command({"LINDEX", key, "0"}).text_, "a");
    EXPECT_EQ(client.Command({"LINDEX", key, "1"}).text_, "B");
  } else {
    EXPECT_EQ(client.Command({"ZSCORE", key, "a"}).text_, "6");
    EXPECT_EQ(client.Command({"ZSCORE", key, "c"}).text_, "8");
  }
  ASSERT_EQ(recovered.Wait(true), 0) << recovered.Log();
}

TEST_P(CompactCollectionWriteE2e, ColdCompactPromotionPreservesContentsAndTtl) {
  const auto type = GetParam();
  const std::string key = "compact-promotion";
  PrivateDisk disk(kPrivateDiskBytes);
  disk.PreserveOnFailure();
  {
    Server server(disk, 1);
    server.PreserveOnFailure();
    Client client(server.port());
    ASSERT_EQ(client.Command(Seed(type, key)).text_, "2");
    ASSERT_EQ(client.Command({"EXPIRE", key, "600"}).text_, "1");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  ASSERT_TRUE(disk.Auxiliaries(key).empty());
  std::vector<std::string> command{type == Collection::kSet    ? "SADD"
                                   : type == Collection::kList ? "RPUSH"
                                                               : "ZADD",
                                   key};
  std::string last;
  for (unsigned i = 0; i < 256; ++i) {
    last = "item" + std::to_string(i) + std::string(128, 'v');
    if (type == Collection::kSortedSet) command.push_back(std::to_string(i));
    command.push_back(last);
  }
  {
    Server server(disk, 1);
    server.PreserveOnFailure();
    Client client(server.port());
    ASSERT_EQ(client.Command(command).text_,
              type == Collection::kList ? "258" : "256");
    EXPECT_EQ(client.Command({LengthCommand(type), key}).text_, "258");
    EXPECT_GT(std::stoll(client.Command({"PTTL", key}).text_), 0);
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  ASSERT_FALSE(disk.Auxiliaries(key).empty());
  Server recovered(disk, 2);
  recovered.PreserveOnFailure();
  Client client(recovered.port());
  EXPECT_EQ(client.Command({LengthCommand(type), key}).text_, "258");
  EXPECT_GT(std::stoll(client.Command({"PTTL", key}).text_), 0);
  if (type == Collection::kSet) {
    EXPECT_EQ(client.Command({"SISMEMBER", key, "a"}).text_, "1");
    EXPECT_EQ(client.Command({"SISMEMBER", key, last}).text_, "1");
  } else if (type == Collection::kList) {
    EXPECT_EQ(client.Command({"LINDEX", key, "0"}).text_, "a");
    EXPECT_EQ(client.Command({"LINDEX", key, "-1"}).text_, last);
  } else {
    EXPECT_EQ(client.Command({"ZSCORE", key, "a"}).text_, "1");
    EXPECT_EQ(client.Command({"ZSCORE", key, last}).text_, "255");
  }
  ASSERT_EQ(recovered.Wait(true), 0) << recovered.Log();
}

INSTANTIATE_TEST_SUITE_P(
    SmallTypes, CompactCollectionWriteE2e,
    testing::Values(Collection::kSet, Collection::kList,
                    Collection::kSortedSet),
    [](const testing::TestParamInfo<Collection>& parameter) {
      return parameter.param == Collection::kSet    ? "Set"
             : parameter.param == Collection::kList ? "List"
                                                    : "SortedSet";
    });
}  // namespace

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  if (argc != 2) return 2;
  grouped_e2e::server_binary = argv[1];
  return RUN_ALL_TESTS();
}
