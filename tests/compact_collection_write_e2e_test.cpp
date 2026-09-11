#include <optional>
#include <tuple>

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

enum class GroupedKind { kHash, kSet, kList, kSortedSet };
using GroupedCase = std::tuple<GroupedKind, const char*>;

struct GroupedCommands {
  GroupedKind kind_;
  std::string a_, b_, c_;
  explicit GroupedCommands(GroupedKind kind, bool extent = false)
      : kind_(kind),
        a_(std::string("a") +
           std::string(extent ? 9 * 1024 * 1024 : 20000, 'x')),
        b_("b" + std::string(20000, 'y')),
        c_(std::string("c") +
           std::string(extent ? 9 * 1024 * 1024 : 20000, 'z')) {}
  std::vector<std::string> Seed(const std::string& key) const {
    switch (kind_) {
      case GroupedKind::kHash:
        return {"HSET", key, "a", a_, "b", b_};
      case GroupedKind::kSet:
        return {"SADD", key, a_, b_};
      case GroupedKind::kList:
        return {"RPUSH", key, a_, b_};
      case GroupedKind::kSortedSet:
        return {"ZADD", key, "1", a_, "2", b_};
    }
    return {};
  }
  std::vector<std::string> First(const std::string& key) const {
    switch (kind_) {
      case GroupedKind::kHash:
        return {"HSET", key, "a", c_};
      case GroupedKind::kSet:
        return {"SADD", key, c_};
      case GroupedKind::kList:
        return {"LSET", key, "0", c_};
      case GroupedKind::kSortedSet:
        return {"ZADD", key, "3", a_};
    }
    return {};
  }
  std::vector<std::string> Second(const std::string& key) const {
    switch (kind_) {
      case GroupedKind::kHash:
        return {"HSET", key, "b", "after"};
      case GroupedKind::kSet:
        return {"SREM", key, a_};
      case GroupedKind::kList:
        return {"RPUSH", key, "after"};
      case GroupedKind::kSortedSet:
        return {"ZINCRBY", key, "1", a_};
    }
    return {};
  }
  void CheckResult(Client& client, const std::string& key) const {
    switch (kind_) {
      case GroupedKind::kHash:
        EXPECT_EQ(client.Command({"HLEN", key}).text_, "2");
        EXPECT_EQ(client.Command({"HGET", key, "a"}).text_, c_);
        EXPECT_EQ(client.Command({"HGET", key, "b"}).text_, "after");
        break;
      case GroupedKind::kSet:
        EXPECT_EQ(client.Command({"SCARD", key}).text_, "2");
        EXPECT_EQ(client.Command({"SISMEMBER", key, a_}).text_, "0");
        EXPECT_EQ(client.Command({"SISMEMBER", key, b_}).text_, "1");
        EXPECT_EQ(client.Command({"SISMEMBER", key, c_}).text_, "1");
        break;
      case GroupedKind::kList:
        EXPECT_EQ(client.Command({"LLEN", key}).text_, "3");
        EXPECT_EQ(client.Command({"LINDEX", key, "0"}).text_, c_);
        EXPECT_EQ(client.Command({"LINDEX", key, "1"}).text_, b_);
        EXPECT_EQ(client.Command({"LINDEX", key, "2"}).text_, "after");
        break;
      case GroupedKind::kSortedSet:
        EXPECT_EQ(client.Command({"ZCARD", key}).text_, "2");
        EXPECT_EQ(client.Command({"ZSCORE", key, a_}).text_, "4");
        EXPECT_EQ(client.Command({"ZSCORE", key, b_}).text_, "2");
        EXPECT_EQ(client.Command({"ZRANGE", key, "0", "0"}).items_.at(0).text_,
                  b_);
        break;
    }
  }
};

class GroupedWriteConcurrencyE2e : public testing::TestWithParam<GroupedCase> {
};

TEST_P(GroupedWriteConcurrencyE2e, ColdWriteYieldsWorkerButRetainsKey) {
#if !KEYLANE_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires grouped preparation/extent pause hooks";
#endif
  const auto [kind, phase] = GetParam();
  const std::string key = "grouped-target";
  const GroupedCommands commands(kind, std::string_view(phase) == "extent");
  PrivateDisk disk;
  disk.PreserveOnFailure();
  {
    Server server(disk, 1);
    Client client(server.port());
    ASSERT_EQ(client.Command(commands.Seed(key)).text_, "2");
    ASSERT_EQ(client.Command({"EXPIRE", key, "3600"}).text_, "1");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  ASSERT_FALSE(disk.Auxiliaries(key).empty());
  {
    ScopedFault pause_key("KEYLANE_GROUPED_WRITE_PAUSE_KEY", key.c_str());
    ScopedFault pause_phase("KEYLANE_GROUPED_WRITE_PAUSE_PHASE", phase);
    Server server(disk, 1);
    server.PreserveOnFailure();
    Client client(server.port());
    PendingWrite first(server.port()), second(server.port());
    first.Send(commands.First(key));
    const std::string armed =
        "grouped write pause armed key=" + key + " phase=" + phase;
    const std::string complete =
        "grouped write pause complete key=" + key + " phase=" + phase;
    ASSERT_TRUE(AwaitMarker(server, armed)) << server.Log();
    second.Send(commands.Second(key));
    // SET/HSET need store state; PING or GET alone would not prove the writer
    // actually released it. Both must finish before the selected pause ends.
    EXPECT_EQ(client.Command({"SET", "other-string", "progress"}).text_, "OK");
    EXPECT_EQ(
        client.Command({"HSET", "other-hash", "f", std::string(20000, 'p')})
            .text_,
        "1");
    EXPECT_EQ(server.Log().find(complete), std::string::npos) << server.Log();
    EXPECT_FALSE(first.HasReply());
    EXPECT_FALSE(second.HasReply());
    const auto a = first.Read();
    ASSERT_NE(a.kind_, '-') << a.text_ << server.Log();
    const auto b = second.Read();
    ASSERT_NE(b.kind_, '-') << b.text_ << server.Log();
    commands.CheckResult(client, key);
    EXPECT_GT(std::stoll(client.Command({"PTTL", key}).text_), 0);
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  // The directory belongs to the key owner, while old blocks can be assigned
  // to another worker. Validate both graphs after that ownership change too.
  Server recovered(disk, 3);
  Client client(recovered.port());
  commands.CheckResult(client, key);
  EXPECT_EQ(client.Command({"GET", "other-string"}).text_, "progress");
  ASSERT_EQ(recovered.Wait(true), 0) << recovered.Log();
}

TEST(GroupedWriteSemanticsE2e, NoOpDeletionExecAndLuaRetainLogicalIdentity) {
  for (const auto kind : {GroupedKind::kHash, GroupedKind::kSet,
                          GroupedKind::kList, GroupedKind::kSortedSet}) {
    SCOPED_TRACE(static_cast<int>(kind));
    const GroupedCommands commands(kind);
    const std::string key = "grouped-semantics";
    PrivateDisk disk;
    disk.PreserveOnFailure();
    {
      Server server(disk, 1);
      server.PreserveOnFailure();
      Client client(server.port());
      ASSERT_EQ(client.Command(commands.Seed(key)).text_, "2");
      ASSERT_EQ(client.Command({"EXPIRE", key, "3600"}).text_, "1");
      const auto expiry = client.Command({"PEXPIRETIME", key}).text_;
      std::vector<std::string> noop, remove;
      switch (kind) {
        case GroupedKind::kHash:
          noop = {"HSETNX", key, "a", "unchanged"};
          remove = {"HDEL", key, "a", "b"};
          // Error after unlocking must not unlock another coroutine's guard.
          EXPECT_EQ(client.Command({"HINCRBY", key, "a", "1"}).kind_, '-');
          break;
        case GroupedKind::kSet:
          noop = {"SADD", key, commands.a_};
          remove = {"SREM", key, commands.a_, commands.b_};
          break;
        case GroupedKind::kList:
          noop = {"LINSERT", key, "BEFORE", "missing", "unchanged"};
          remove = {"LTRIM", key, "1", "0"};
          EXPECT_EQ(client.Command({"LSET", key, "999", "invalid"}).kind_, '-');
          break;
        case GroupedKind::kSortedSet:
          noop = {"ZADD", key, "NX", "99", commands.a_};
          remove = {"ZREM", key, commands.a_, commands.b_};
          break;
      }
      EXPECT_NE(client.Command(noop).kind_, '-');
      EXPECT_EQ(client.Command({"PEXPIRETIME", key}).text_, expiry);
      ASSERT_EQ(client.Command({"MULTI"}).text_, "OK");
      ASSERT_EQ(client.Command(noop).text_, "QUEUED");
      ASSERT_EQ(client.Command(remove).text_, "QUEUED");
      const auto deleted = client.Command({"EXEC"});
      ASSERT_EQ(deleted.items_.size(), 2);
      for (const auto& reply : deleted.items_) EXPECT_NE(reply.kind_, '-');
      EXPECT_EQ(client.Command({"EXISTS", key}).text_, "0");
      // Recreating this key must use a new incarnation, then survive unlocked
      // preparation inside both Lua and an outer EXEC decision.
      ASSERT_EQ(client.Command(commands.Seed(key)).text_, "2");
      auto first = commands.First(key);
      std::vector<std::string> script{"EVAL", "return redis.call(unpack(ARGV))",
                                      "1", key};
      script.insert(script.end(), first.begin(), first.end());
      ASSERT_NE(client.Command(script).kind_, '-');
      ASSERT_EQ(client.Command({"MULTI"}).text_, "OK");
      ASSERT_EQ(client.Command(commands.Second(key)).text_, "QUEUED");
      const auto updated = client.Command({"EXEC"});
      ASSERT_EQ(updated.items_.size(), 1);
      ASSERT_NE(updated.items_.front().kind_, '-');
      commands.CheckResult(client, key);
      client.Durable();
      ASSERT_EQ(server.Wait(true), 0) << server.Log();
    }
    Server recovered(disk, 3);
    Client client(recovered.port());
    commands.CheckResult(client, key);
    ASSERT_EQ(recovered.Wait(true), 0) << recovered.Log();
  }
}

TEST(GroupedWriteSemanticsE2e,
     MemberPreparationAllocationFailureLeavesWorkerUsable) {
#if !KEYLANE_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires member preparation allocation-failure hook";
#endif
  const std::string key = "member-prepare-failure";
  const GroupedCommands commands(GroupedKind::kSortedSet);
  PrivateDisk disk;
  disk.PreserveOnFailure();
  {
    Server server(disk, 1);
    Client client(server.port());
    ASSERT_EQ(client.Command(commands.Seed(key)).text_, "2");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  {
    ScopedFault fault("KEYLANE_FAIL_GROUP_MEMBER_PREPARE_KEY", key.c_str());
    Server server(disk, 1);
    server.PreserveOnFailure();
    Client client(server.port());
    const auto failed = client.Command(commands.First(key));
    EXPECT_EQ(failed.kind_, '-');
    EXPECT_TRUE(failed.text_.starts_with("OOM")) << failed.text_;
    EXPECT_EQ(client.Command({"ZSCORE", key, commands.a_}).text_, "1");
    EXPECT_EQ(client.Command({"ZADD", key, "NX", "99", commands.a_}).text_,
              "0");
    ASSERT_EQ(client.Command({"MULTI"}).text_, "OK");
    ASSERT_EQ(client.Command(commands.First(key)).text_, "QUEUED");
    ASSERT_EQ(
        client.Command({"HSET", "independent", "f", std::string(20000, 'p')})
            .text_,
        "QUEUED");
    const auto executed = client.Command({"EXEC"});
    ASSERT_EQ(executed.items_.size(), 2);
    EXPECT_EQ(executed.items_[0].kind_, '-');
    EXPECT_TRUE(executed.items_[0].text_.starts_with("OOM"));
    EXPECT_EQ(executed.items_[1].text_, "1");
    EXPECT_EQ(client.Command({"ZCARD", key}).text_, "2");
    EXPECT_EQ(client.Command({"ZSCORE", key, commands.a_}).text_, "1");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server recovered(disk, 3);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"ZSCORE", key, commands.a_}).text_, "1");
  EXPECT_EQ(client.Command({"HGET", "independent", "f"}).text_,
            std::string(20000, 'p'));
  ASSERT_EQ(recovered.Wait(true), 0) << recovered.Log();
}

TEST(GroupedWriteSemanticsE2e,
     ShutdownDrainsUnlockedExtentBeforeDestroyingStore) {
#if !KEYLANE_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires grouped extent pause hook";
#endif
  const std::string key = "extent-shutdown";
  const GroupedCommands commands(GroupedKind::kHash, true);
  PrivateDisk disk;
  disk.PreserveOnFailure();
  {
    Server server(disk, 1);
    Client client(server.port());
    ASSERT_EQ(client.Command(commands.Seed(key)).text_, "2");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  {
    ScopedFault pause_key("KEYLANE_GROUPED_WRITE_PAUSE_KEY", key.c_str());
    ScopedFault pause_phase("KEYLANE_GROUPED_WRITE_PAUSE_PHASE", "extent");
    Server server(disk, 1);
    server.PreserveOnFailure();
    Client ready(server.port());
    PendingWrite writer(server.port());
    writer.Send(commands.First(key));
    ASSERT_TRUE(AwaitMarker(
        server, "grouped write pause armed key=" + key + " phase=extent"));
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server recovered(disk, 3);
  Client client(recovered.port());
  // Shutdown may cancel an unpublished command or drain it. Never admit a
  // torn graph, freed IO buffer, missing predecessor or partial new value.
  EXPECT_EQ(client.Command({"HLEN", key}).text_, "2");
  const auto value = client.Command({"HGET", key, "a"});
  EXPECT_TRUE(value.text_ == commands.a_ || value.text_ == commands.c_);
  EXPECT_EQ(client.Command({"HGET", key, "b"}).text_, commands.b_);
  ASSERT_EQ(recovered.Wait(true), 0) << recovered.Log();
}

using CreationCase = std::tuple<GroupedKind, const char*, const char*>;
class CollectionCreationE2e : public testing::TestWithParam<CreationCase> {};

TEST_P(CollectionCreationE2e,
       PrivatePreparationYieldsButKeepsMissingKeyLocked) {
#if !KEYLANE_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires collection creation pause hook";
#endif
  const auto [kind, layout, phase] = GetParam();
  const bool compact = std::string_view(layout) == "compact";
  GroupedCommands commands(kind, std::string_view(layout) == "extent");
  if (compact) {
    commands.a_ = "a";
    commands.b_ = "b";
    commands.c_ = "c";
  }
  const std::string key = "new-target";
  PrivateDisk disk;
  disk.PreserveOnFailure();
  {
    ScopedFault pause_key("KEYLANE_GROUPED_WRITE_PAUSE_KEY", key.c_str());
    ScopedFault pause_phase("KEYLANE_GROUPED_WRITE_PAUSE_PHASE", phase);
    Server server(disk, 1);
    server.PreserveOnFailure();
    Client client(server.port()), watcher(server.port());
    ASSERT_EQ(watcher.Command({"WATCH", key}).text_, "OK");
    PendingWrite first(server.port()), second(server.port());
    first.Send(commands.Seed(key));
    const std::string armed =
        "grouped write pause armed key=" + key + " phase=" + phase;
    const std::string complete =
        "grouped write pause complete key=" + key + " phase=" + phase;
    ASSERT_TRUE(AwaitMarker(server, armed)) << server.Log();
    second.Send(commands.First(key));
    EXPECT_EQ(client.Command({"SET", "independent", "progress"}).text_, "OK");
    EXPECT_EQ(
        client
            .Command({"HSET", "independent-hash", "f", std::string(20000, 'p')})
            .text_,
        "1");
    EXPECT_EQ(server.Log().find(complete), std::string::npos) << server.Log();
    EXPECT_FALSE(first.HasReply());
    EXPECT_FALSE(second.HasReply());
    EXPECT_EQ(first.Read().text_, "2");
    const auto changed = second.Read();
    ASSERT_NE(changed.kind_, '-') << changed.text_ << server.Log();
    ASSERT_NE(client.Command(commands.Second(key)).kind_, '-');
    commands.CheckResult(client, key);
    EXPECT_EQ(client.Command({"PTTL", key}).text_, "-1");
    ASSERT_EQ(watcher.Command({"MULTI"}).text_, "OK");
    ASSERT_EQ(watcher.Command({"EXISTS", key}).text_, "QUEUED");
    EXPECT_EQ(watcher.Command({"EXEC"}).text_, "-1");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  EXPECT_EQ(disk.Auxiliaries(key).empty(), compact);
  Server recovered(disk, 3);
  Client client(recovered.port());
  commands.CheckResult(client, key);
  EXPECT_EQ(client.Command({"PTTL", key}).text_, "-1");
  EXPECT_EQ(client.Command({"GET", "independent"}).text_, "progress");
  ASSERT_EQ(recovered.Wait(true), 0) << recovered.Log();
}

TEST(CollectionCreationSemanticsE2e, TombstoneAndExpiredOtherTypeStartFresh) {
  const std::vector<std::string> keys{"deleted", "expired",
                                      "deleted-" + std::string(32000, 'k'),
                                      "expired-" + std::string(32000, 'k')};
  for (const auto kind : {GroupedKind::kHash, GroupedKind::kSet,
                          GroupedKind::kList, GroupedKind::kSortedSet}) {
    const GroupedCommands commands(kind);
    PrivateDisk disk;
    disk.PreserveOnFailure();
    {
      Server server(disk, 1);
      server.PreserveOnFailure();
      Client client(server.port());
      for (const auto& key : keys) {
        // Use a different, grouped predecessor type. Neither its fields nor
        // its incarnation/TTL may become part of the fresh collection.
        if (kind == GroupedKind::kHash)
          ASSERT_EQ(
              client.Command({"RPUSH", key, std::string(20000, 'q')}).text_,
              "1");
        else
          ASSERT_EQ(
              client.Command({"HSET", key, "old", std::string(20000, 'q')})
                  .text_,
              "1");
        if (key.starts_with("deleted")) {
          ASSERT_EQ(client.Command({"DEL", key}).text_, "1");
        } else {
          ASSERT_EQ(client.Command({"PEXPIRE", key, "50"}).text_, "1");
          std::this_thread::sleep_for(80ms);
        }
        ASSERT_EQ(client.Command(commands.Seed(key)).text_, "2");
        ASSERT_NE(client.Command(commands.First(key)).kind_, '-');
        ASSERT_NE(client.Command(commands.Second(key)).kind_, '-');
        commands.CheckResult(client, key);
        EXPECT_EQ(client.Command({"PTTL", key}).text_, "-1");
      }
      EXPECT_EQ(client.Command({"HSETNX", "nx", "f", "1"}).text_, "1");
      EXPECT_EQ(client.Command({"HINCRBY", "integer", "f", "2"}).text_, "2");
      EXPECT_EQ(client.Command({"HINCRBYFLOAT", "float", "f", "1.5"}).text_,
                "1.5");
      EXPECT_EQ(client.Command({"ZADD", "noop", "XX", "1", "a"}).text_, "0");
      EXPECT_EQ(client.Command({"EXISTS", "noop"}).text_, "0");
      EXPECT_EQ(client.Command({"HSET", "duplicate", "f", "first", "f", "last"})
                    .text_,
                "1");
      EXPECT_EQ(client.Command({"HGET", "duplicate", "f"}).text_, "last");
      EXPECT_EQ(client.Command({"SADD", "duplicate-set", "a", "a"}).text_, "1");
      EXPECT_EQ(client.Command({"LPUSH", "left", "a", "b", "c"}).text_, "3");
      EXPECT_EQ(client.Command({"LINDEX", "left", "0"}).text_, "c");
      client.Durable();
      ASSERT_EQ(server.Wait(true), 0) << server.Log();
    }
    Server recovered(disk, 3);
    Client client(recovered.port());
    for (const auto& key : keys) {
      commands.CheckResult(client, key);
      EXPECT_EQ(client.Command({"PTTL", key}).text_, "-1");
    }
    EXPECT_EQ(client.Command({"EXISTS", "noop"}).text_, "0");
    ASSERT_EQ(recovered.Wait(true), 0) << recovered.Log();
  }
}

TEST(CollectionCreationSemanticsE2e,
     AllocationFailureLeavesNoKeyOrWatchEffect) {
#if !KEYLANE_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires creation allocation fault hook";
#endif
  const std::string key = "failed-create";
  for (const auto kind : {GroupedKind::kHash, GroupedKind::kSet,
                          GroupedKind::kList, GroupedKind::kSortedSet}) {
    const GroupedCommands commands(kind);
    PrivateDisk disk;
    disk.PreserveOnFailure();
    {
      ScopedFault fault("KEYLANE_FAIL_COLLECTION_CREATE_PREPARE_KEY",
                        key.c_str());
      Server server(disk, 1);
      server.PreserveOnFailure();
      Client client(server.port()), watcher(server.port());
      ASSERT_EQ(watcher.Command({"WATCH", key}).text_, "OK");
      const auto failed = client.Command(commands.Seed(key));
      EXPECT_EQ(failed.kind_, '-');
      EXPECT_TRUE(failed.text_.starts_with("OOM")) << failed.text_;
      EXPECT_EQ(client.Command({"EXISTS", key}).text_, "0");
      ASSERT_EQ(watcher.Command({"MULTI"}).text_, "OK");
      ASSERT_EQ(watcher.Command({"EXISTS", key}).text_, "QUEUED");
      const auto watched = watcher.Command({"EXEC"});
      ASSERT_EQ(watched.items_.size(), 1);
      EXPECT_EQ(watched.items_[0].text_, "0");
      // Both the missing-key intent and worker state must have unwound.
      EXPECT_EQ(client.Command({"SET", key, "reused"}).text_, "OK");
      EXPECT_EQ(client.Command(commands.Seed("independent")).text_, "2");
      client.Durable();
      ASSERT_EQ(server.Wait(true), 0) << server.Log();
    }
    EXPECT_TRUE(disk.Auxiliaries(key).empty());
    Server recovered(disk, 3);
    Client client(recovered.port());
    EXPECT_EQ(client.Command({"GET", key}).text_, "reused");
    ASSERT_EQ(recovered.Wait(true), 0) << recovered.Log();
  }
}

TEST(CollectionCreationSemanticsE2e,
     MemberPreparationFailurePublishesNeitherGraph) {
#if !KEYLANE_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires member preparation allocation fault hook";
#endif
  const std::string key = "failed-members";
  const GroupedCommands commands(GroupedKind::kSortedSet);
  PrivateDisk disk;
  disk.PreserveOnFailure();
  {
    ScopedFault fault("KEYLANE_FAIL_GROUP_MEMBER_PREPARE_KEY", key.c_str());
    Server server(disk, 1);
    server.PreserveOnFailure();
    Client client(server.port());
    const auto failed = client.Command(commands.Seed(key));
    EXPECT_EQ(failed.kind_, '-');
    EXPECT_TRUE(failed.text_.starts_with("OOM")) << failed.text_;
    EXPECT_EQ(client.Command({"EXISTS", key}).text_, "0");
    EXPECT_EQ(client.Command({"ZADD", key, "7", "retry"}).text_, "1");
    EXPECT_EQ(client.Command({"ZSCORE", key, "retry"}).text_, "7");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  EXPECT_TRUE(disk.Auxiliaries(key).empty());
  Server recovered(disk, 3);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"ZCARD", key}).text_, "1");
  EXPECT_EQ(client.Command({"ZSCORE", key, "retry"}).text_, "7");
  ASSERT_EQ(recovered.Wait(true), 0) << recovered.Log();
}

INSTANTIATE_TEST_SUITE_P(
    NewTypes, CollectionCreationE2e,
    testing::Values(CreationCase{GroupedKind::kHash, "compact", "create"},
                    CreationCase{GroupedKind::kSet, "compact", "create"},
                    CreationCase{GroupedKind::kList, "compact", "create"},
                    CreationCase{GroupedKind::kSortedSet, "compact", "create"},
                    CreationCase{GroupedKind::kHash, "grouped", "create"},
                    CreationCase{GroupedKind::kSet, "grouped", "create"},
                    CreationCase{GroupedKind::kList, "grouped", "create"},
                    CreationCase{GroupedKind::kSortedSet, "grouped", "create"},
                    CreationCase{GroupedKind::kSortedSet, "members",
                                 "create-members"},
                    CreationCase{GroupedKind::kHash, "extent", "extent"},
                    CreationCase{GroupedKind::kSet, "extent", "extent"},
                    CreationCase{GroupedKind::kList, "extent", "extent"},
                    CreationCase{GroupedKind::kSortedSet, "extent", "extent"}),
    [](const testing::TestParamInfo<CreationCase>& parameter) {
      const auto kind = std::get<0>(parameter.param);
      const std::string name = kind == GroupedKind::kHash   ? "Hash"
                               : kind == GroupedKind::kSet  ? "Set"
                               : kind == GroupedKind::kList ? "List"
                                                            : "SortedSet";
      return name + "_" + std::get<1>(parameter.param);
    });

INSTANTIATE_TEST_SUITE_P(
    LargeTypes, GroupedWriteConcurrencyE2e,
    testing::Values(GroupedCase{GroupedKind::kHash, "prepare"},
                    GroupedCase{GroupedKind::kSet, "prepare"},
                    GroupedCase{GroupedKind::kList, "prepare"},
                    GroupedCase{GroupedKind::kSortedSet, "prepare"},
                    GroupedCase{GroupedKind::kSortedSet, "members"},
                    GroupedCase{GroupedKind::kHash, "extent"},
                    GroupedCase{GroupedKind::kSet, "extent"},
                    GroupedCase{GroupedKind::kList, "extent"},
                    GroupedCase{GroupedKind::kSortedSet, "extent"}),
    [](const testing::TestParamInfo<GroupedCase>& parameter) {
      const auto kind = std::get<0>(parameter.param);
      const auto phase = std::get<1>(parameter.param);
      const std::string name = kind == GroupedKind::kHash   ? "Hash"
                               : kind == GroupedKind::kSet  ? "Set"
                               : kind == GroupedKind::kList ? "List"
                                                            : "SortedSet";
      return name + "_" + phase;
    });

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
