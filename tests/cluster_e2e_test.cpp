// Redis Cluster data plane over a static topology.
//
// Starts real keylane processes with --cluster-enabled and a shared
// nodes.conf file, then verifies the wire contract end to end: discovery in
// RESP2/RESP3, MOVED/CROSSSLOT/CLUSTERDOWN routing errors, the SELECT/COPY/
// REPLICAOF cluster-mode policies, READONLY replica admission, and
// SIGHUP-driven owner switches under concurrent traffic (including that an
// invalid reload keeps the old state and that SIGHUP never kills the server).
//
// argv[1] = keylane binary; argv[2] = redis-cli path or "" (the real-client
// MOVED-following test skips when empty).

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "absl/strings/str_cat.h"
#include "gtest/gtest.h"
#include "keylane/storage/format.h"

namespace {

using namespace std::chrono_literals;

std::string g_keylane_binary;
std::string g_redis_cli;

// Fixed 40-hex node ids, as Redis writes them in nodes.conf.
constexpr std::string_view kNodeA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
constexpr std::string_view kNodeB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
constexpr std::string_view kNodeC = "cccccccccccccccccccccccccccccccccccccccc";
constexpr std::string_view kNodeR = "dddddddddddddddddddddddddddddddddddddddd";
constexpr std::string_view kNodeD = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
constexpr std::string_view kNodeE = "ffffffffffffffffffffffffffffffffffffffff";

// Slot ranges of the main topology: A owns 0-5460, B owns 5461-10922, C owns
// the rest. R replicates A. D is used standalone with a coverage gap.
constexpr std::uint16_t kSlotsAFirst = 0;
constexpr std::uint16_t kSlotsALast = 5460;
constexpr std::uint16_t kSlotsBFirst = 5461;
constexpr std::uint16_t kSlotsBLast = 10922;

std::uint16_t SlotOf(std::string_view key) {
  return keylane::storage::RedisSlot(key);
}

// Brute-forces a key whose hash slot lands inside [first, last].
std::string KeyInSlotRange(std::uint16_t first, std::uint16_t last,
                           std::string_view prefix = "k") {
  for (std::uint64_t i = 0;; ++i) {
    std::string key = absl::StrCat(prefix, i);
    const std::uint16_t slot = SlotOf(key);
    if (slot >= first && slot <= last) return key;
  }
}

// Finds a hash tag whose slot lands inside [first, last], so "{tag}a" and
// "{tag}b" are same-slot keys owned by that range's group.
std::string TagInRange(std::uint16_t first, std::uint16_t last) {
  for (std::uint64_t i = 0;; ++i) {
    std::string tag = absl::StrCat("{tag", i, "}");
    const std::uint16_t slot = SlotOf(tag + "probe");
    if (slot >= first && slot <= last) return tag;
  }
}

class TempDir {
 public:
  TempDir() {
    path_ = std::filesystem::temp_directory_path() /
            absl::StrCat("keylane_cluster_e2e_", ::getpid(), "_", counter_++);
    std::filesystem::create_directories(path_);
  }
  ~TempDir() { std::filesystem::remove_all(path_); }
  std::string Path(const std::string& name) const {
    return (path_ / name).string();
  }

 private:
  static inline std::atomic<unsigned> counter_ = 0;
  std::filesystem::path path_;
};

void WriteFile(const std::string& path, std::string_view content) {
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  out << content;
  out.flush();
  if (!out) throw std::runtime_error("failed to write " + path);
}

// Storage requires the data file to exist with its full capacity up front
// (the same 128 MiB the other e2e suites pre-allocate).
void CreateDataFile(const std::string& path) {
  const int fd =
      ::open(path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  if (fd < 0) throw std::runtime_error("failed to create " + path);
  if (::posix_fallocate(fd, 0, 128ULL * 1024 * 1024) != 0) {
    ::close(fd);
    throw std::runtime_error("failed to allocate " + path);
  }
  if (::close(fd) != 0) throw std::runtime_error("failed to close " + path);
}

// Rewrites a live topology file atomically so the server never observes a
// partial file on SIGHUP reload.
void ReplaceFile(const std::string& path, std::string_view content) {
  const std::string staging = path + ".staging";
  WriteFile(staging, content);
  std::filesystem::rename(staging, path);
}

class ServerProcess {
 public:
  ServerProcess(std::uint16_t port, std::string_view data_path,
                std::string_view log_path, std::string_view nodes_file) {
    pid_ = ::fork();
    if (pid_ < 0) throw std::runtime_error("fork failed");
    if (pid_ == 0) {
      const int log_fd =
          ::open(std::string(log_path).c_str(),
                 O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0600);
      if (log_fd >= 0) {
        (void)::dup2(log_fd, STDOUT_FILENO);
        (void)::dup2(log_fd, STDERR_FILENO);
        ::close(log_fd);
      }
      const std::string port_arg = std::to_string(port);
      std::vector<char*> argv;
      std::vector<std::string> arguments{g_keylane_binary,
                                         "--logtostderr",
                                         "--port",
                                         port_arg,
                                         "--threads",
                                         "1",
                                         "--recv-buffers-per-worker",
                                         "0",
                                         "--max-memory",
                                         "8589934592",
                                         "--flush-max-ms",
                                         "20",
                                         "--data-file",
                                         std::string(data_path),
                                         "--cluster-enabled",
                                         "--cluster-static-nodes-file",
                                         std::string(nodes_file)};
      for (std::string& argument : arguments) argv.push_back(argument.data());
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

  bool Alive() const { return pid_ > 0 && ::kill(pid_, 0) == 0; }

  void Sighup() {
    ASSERT_GT(pid_, 0);
    ASSERT_EQ(::kill(pid_, SIGHUP), 0);
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
    FAIL() << "Keylane did not stop";
  }

 private:
  pid_t pid_ = -1;
};

void SendAll(int fd, std::string_view bytes) {
  while (!bytes.empty()) {
    const ssize_t sent = ::send(fd, bytes.data(), bytes.size(), MSG_NOSIGNAL);
    if (sent < 0) {
      if (errno == EINTR) continue;
      throw std::runtime_error("send failed: " +
                               std::string(std::strerror(errno)));
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

std::uint16_t FindFreePort() {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (fd < 0) throw std::runtime_error("socket failed while selecting a port");
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
  if (fd < 0) return -1;
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

// Minimal RESP2/RESP3 client. Replies are returned with their type prefix:
// "+PONG", "-MOVED 1 host:6379", ":42", "$3\r\nfoo", nested arrays as one
// flattened string. RESP3 maps ('%') are read like arrays of pairs.
class RespClient {
 public:
  explicit RespClient(std::uint16_t port) {
    const auto deadline = std::chrono::steady_clock::now() + 30s;
    while (std::chrono::steady_clock::now() < deadline) {
      fd_ = ConnectSocket(port);
      if (fd_ >= 0) return;
      std::this_thread::sleep_for(10ms);
    }
    throw std::runtime_error("timed out connecting to Redis port");
  }

  ~RespClient() {
    if (fd_ >= 0) ::close(fd_);
  }

  std::string Command(const std::vector<std::string_view>& args) {
    SendAll(fd_, EncodeCommand(args));
    return ReadReply();
  }

 private:
  std::string ReadLine() {
    std::string line;
    while (!line.ends_with("\r\n")) {
      char byte = 0;
      const ssize_t received = ::recv(fd_, &byte, 1, 0);
      if (received < 0 && errno == EINTR) continue;
      if (received <= 0) throw std::runtime_error("failed to read RESP line");
      line.push_back(byte);
    }
    line.resize(line.size() - 2);
    return line;
  }

  std::size_t ParseLength(const std::string& header) {
    std::size_t size = 0;
    const auto parsed =
        std::from_chars(header.data() + 1, header.data() + header.size(), size);
    if (parsed.ec != std::errc{} || parsed.ptr != header.data() + header.size())
      throw std::runtime_error("invalid RESP length: " + header);
    return size;
  }

  void ReadExact(char* out, std::size_t bytes) {
    std::size_t received_total = 0;
    while (received_total < bytes) {
      const ssize_t received =
          ::recv(fd_, out + received_total, bytes - received_total, 0);
      if (received < 0 && errno == EINTR) continue;
      if (received <= 0) throw std::runtime_error("failed to read RESP body");
      received_total += static_cast<std::size_t>(received);
    }
  }

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
        if (!payload.ends_with("\r\n"))
          throw std::runtime_error("malformed bulk terminator");
        payload.resize(size);
        return line + "\r\n" + payload;
      }
      case '*': {
        if (line == "*-1") return line;
        const std::size_t count = ParseLength(line);
        std::string reply = line;
        for (std::size_t i = 0; i < count; ++i) reply += "\r\n" + ReadReply();
        return reply;
      }
      case '%': {
        // RESP3 map: N pairs, read as 2N replies.
        const std::size_t count = ParseLength(line);
        std::string reply = line;
        for (std::size_t i = 0; i < count * 2; ++i)
          reply += "\r\n" + ReadReply();
        return reply;
      }
      case '_':  // RESP3 null
      case ',':  // RESP3 double (single line)
      case '#':  // RESP3 boolean
        return line;
      default:
        throw std::runtime_error("unexpected RESP type: " + line);
    }
  }

  int fd_ = -1;
};

// Builds the shared nodes.conf content for ports [a, b, c, r]. R's line only
// appears when with_replica is set.
std::string MainTopology(std::uint16_t port_a, std::uint16_t port_b,
                         std::uint16_t port_c, std::uint16_t port_r,
                         bool with_replica = true) {
  std::string content = absl::StrCat(
      kNodeA, " 127.0.0.1:", port_a, "@0 master - 0 0 1 connected ",
      kSlotsAFirst, "-", kSlotsALast, "\n", kNodeB, " 127.0.0.1:", port_b,
      "@0 master - 0 0 2 connected ", kSlotsBFirst, "-", kSlotsBLast, "\n",
      kNodeC, " 127.0.0.1:", port_c, "@0 master - 0 0 3 connected ",
      kSlotsBLast + 1, "-16383\n");
  if (with_replica) {
    absl::StrAppend(&content, kNodeR, " 127.0.0.1:", port_r, "@0 slave ",
                    kNodeA, " 0 0 1 connected\n");
  }
  absl::StrAppend(&content, "vars currentEpoch 3 lastVoteEpoch 0\n");
  return content;
}

// Waits until the node's storage is ready. PING is whitelisted during
// LOADING, so readiness must be probed with a local write on primaries and
// with a READONLY read on the replica (which never serves writes).
void WaitReadyPrimary(RespClient& client, std::uint16_t first,
                      std::uint16_t last) {
  const std::string probe = KeyInSlotRange(first, last, "probe");
  const auto deadline = std::chrono::steady_clock::now() + 30s;
  while (std::chrono::steady_clock::now() < deadline) {
    if (client.Command({"SET", probe, "1"}) == "+OK") return;
    std::this_thread::sleep_for(20ms);
  }
  FAIL() << "primary never became ready";
}

void WaitReadyReplica(RespClient& client, std::uint16_t first,
                      std::uint16_t last) {
  const std::string probe = KeyInSlotRange(first, last, "probe");
  const auto deadline = std::chrono::steady_clock::now() + 30s;
  while (std::chrono::steady_clock::now() < deadline) {
    const std::string reply = client.Command({"GET", probe});
    if (!reply.starts_with("-LOADING")) return;
    std::this_thread::sleep_for(20ms);
  }
  FAIL() << "replica never became ready";
}

// The main 4-process topology: primaries A/B/C with full coverage and replica
// R of A. Server processes register before clients so every declaration is
// destroyed in reverse order.
struct MainCluster {
  TempDir dir;
  std::uint16_t port_a = 0;
  std::uint16_t port_b = 0;
  std::uint16_t port_c = 0;
  std::uint16_t port_r = 0;
  std::string nodes_file = dir.Path("nodes.conf");
  std::optional<ServerProcess> a, b, c, r;

  MainCluster() {
    // FindFreePort releases the probe socket, so consecutive calls can return
    // the same port; keep drawing until all four differ.
    std::set<std::uint16_t> ports;
    for (std::uint16_t* out : {&port_a, &port_b, &port_c, &port_r}) {
      do {
        *out = FindFreePort();
      } while (!ports.insert(*out).second);
    }
  }

  void Start() {
    WriteFile(nodes_file, MainTopology(port_a, port_b, port_c, port_r));
    CreateDataFile(dir.Path("a.data"));
    CreateDataFile(dir.Path("b.data"));
    CreateDataFile(dir.Path("c.data"));
    CreateDataFile(dir.Path("r.data"));
    a.emplace(port_a, dir.Path("a.data"), dir.Path("a.log"), nodes_file);
    b.emplace(port_b, dir.Path("b.data"), dir.Path("b.log"), nodes_file);
    c.emplace(port_c, dir.Path("c.data"), dir.Path("c.log"), nodes_file);
    r.emplace(port_r, dir.Path("r.data"), dir.Path("r.log"), nodes_file);
    RespClient ca(port_a), cb(port_b), cc(port_c), cr(port_r);
    WaitReadyPrimary(ca, kSlotsAFirst, kSlotsALast);
    WaitReadyPrimary(cb, kSlotsBFirst, kSlotsBLast);
    WaitReadyPrimary(cc, kSlotsBLast + 1, 16383);
    WaitReadyReplica(cr, kSlotsAFirst, kSlotsALast);
  }

  void StopAll() {
    // Clients are already gone; stop servers gracefully so the processes
    // exit 0 and the SIGHUP cleanup paths run.
    if (r) r->Stop();
    if (c) c->Stop();
    if (b) b->Stop();
    if (a) a->Stop();
  }
};

TEST(ClusterE2eTest, DiscoveryResp2AndResp3) {
  MainCluster cluster;
  cluster.Start();

  RespClient client(cluster.port_a);
  // KEYSLOT is a pure function and agrees with the storage slot.
  EXPECT_EQ(client.Command({"CLUSTER", "KEYSLOT", "foo"}),
            ":" + std::to_string(SlotOf("foo")));
  // The {hashtag} portion decides the slot, so these must agree.
  EXPECT_EQ(client.Command({"cluster", "keyslot", "{t}x"}),
            ":" + std::to_string(SlotOf("{t}y")));
  // MYID is the file-matched self id.
  EXPECT_EQ(client.Command({"CLUSTER", "MYID"}),
            "$40\r\n" + std::string(kNodeA));

  // INFO returns this exact field set when every slot has coverage.
  const std::string expected_info = absl::StrCat(
      "cluster_state:ok\r\ncluster_slots_assigned:16384\r\n"
      "cluster_slots_ok:16384\r\ncluster_slots_pfail:0\r\n"
      "cluster_slots_fail:0\r\ncluster_known_nodes:4\r\ncluster_size:3\r\n"
      "cluster_current_epoch:3\r\ncluster_my_epoch:1\r\n"
      "cluster_stats_messages_sent:0\r\ncluster_stats_messages_received:0\r\n");
  const std::string info = client.Command({"CLUSTER", "INFO"});
  EXPECT_EQ(info,
            absl::StrCat("$", expected_info.size(), "\r\n", expected_info))
      << info;

  // SLOTS: one range per group; A's range lists its replica.
  const std::string slots = client.Command({"CLUSTER", "SLOTS"});
  EXPECT_TRUE(slots.starts_with("*3\r\n")) << slots;
  EXPECT_NE(slots.find(absl::StrCat("\r\n:", kSlotsAFirst, "\r\n:", kSlotsALast,
                                    "\r\n")),
            std::string::npos)
      << slots;
  EXPECT_NE(slots.find(absl::StrCat("$9\r\n127.0.0.1\r\n:", cluster.port_a,
                                    "\r\n$40\r\n", kNodeA, "\r\n")),
            std::string::npos)
      << slots;
  // A's range carries the replica entry; B's does not.
  EXPECT_NE(slots.find(absl::StrCat("$9\r\n127.0.0.1\r\n:", cluster.port_r,
                                    "\r\n$40\r\n", kNodeR, "\r\n")),
            std::string::npos)
      << slots;

  // NODES: myself flag on A, replica wired to A, slots only on primaries.
  const std::string nodes = client.Command({"CLUSTER", "NODES"});
  EXPECT_NE(
      nodes.find(absl::StrCat(kNodeA, " 127.0.0.1:", cluster.port_a,
                              "@0 myself,master - 0 0 1 connected 0-5460")),
      std::string::npos)
      << nodes;
  EXPECT_NE(nodes.find(absl::StrCat(kNodeR, " 127.0.0.1:", cluster.port_r,
                                    "@0 slave ", kNodeA, " 0 0 1 connected")),
            std::string::npos)
      << nodes;

  // Unknown subcommands get Redis's exact text, RESP2 and RESP3 alike.
  EXPECT_EQ(client.Command({"CLUSTER", "MEET", "1.2.3.4", "7000"}),
            "-ERR Unknown CLUSTER subcommand or wrong number of arguments for "
            "'MEET'");
  EXPECT_EQ(client.Command({"CLUSTER", "KEYSLOT"}),
            "-ERR Unknown CLUSTER subcommand or wrong number of arguments for "
            "'KEYSLOT'");

  // HELLO 3 negotiates RESP3: the reply is a map and reports mode cluster.
  RespClient client3(cluster.port_b);
  const std::string hello = client3.Command({"HELLO", "3"});
  EXPECT_TRUE(hello.starts_with("%")) << hello;
  EXPECT_NE(hello.find("$4\r\nmode\r\n$7\r\ncluster"), std::string::npos)
      << hello;
  // Discovery keeps the same nested-array shape under RESP3.
  const std::string slots3 = client3.Command({"CLUSTER", "SLOTS"});
  EXPECT_TRUE(slots3.starts_with("*3\r\n")) << slots3;
  // MOVED stays a plain error line under RESP3.
  const std::string foreign = KeyInSlotRange(kSlotsAFirst, kSlotsALast);
  EXPECT_EQ(
      client3.Command({"SET", foreign, "1"}),
      absl::StrCat("-MOVED ", SlotOf(foreign), " 127.0.0.1:", cluster.port_a));

  cluster.StopAll();
}

TEST(ClusterE2eTest, RoutingAndClusterModePolicies) {
  MainCluster cluster;
  cluster.Start();

  RespClient ca(cluster.port_a);
  RespClient cb(cluster.port_b);

  // Local write/read on the owning node.
  const std::string key_a = KeyInSlotRange(kSlotsAFirst, kSlotsALast);
  EXPECT_EQ(ca.Command({"SET", key_a, "v"}), "+OK");
  EXPECT_EQ(ca.Command({"GET", key_a}), "$1\r\nv");

  // A key owned by B sent to A: MOVED with the computed slot and B's
  // concrete endpoint; B serves the same key directly.
  const std::string key_b = KeyInSlotRange(kSlotsBFirst, kSlotsBLast);
  EXPECT_EQ(
      ca.Command({"SET", key_b, "1"}),
      absl::StrCat("-MOVED ", SlotOf(key_b), " 127.0.0.1:", cluster.port_b));
  EXPECT_EQ(cb.Command({"SET", key_b, "1"}), "+OK");

  // Two different slots are CROSSSLOT even when the same node owns both
  // (Redis checks slot equality, not shared ownership).
  const std::string slot5 = KeyInSlotRange(5, 5);
  const std::string slot7 = KeyInSlotRange(7, 7);
  EXPECT_EQ(ca.Command({"MGET", slot5, slot7}),
            "-CROSSSLOT Keys in request don't hash to the same slot");
  // Hash tags make multi-key commands work. The tag must live in A's range.
  const std::string tag = TagInRange(kSlotsAFirst, kSlotsALast);
  const std::string tag1 = tag + "1";
  const std::string tag2 = tag + "2";
  EXPECT_EQ(ca.Command({"MSET", tag1, "a", tag2, "b"}), "+OK");
  EXPECT_NE(ca.Command({"MGET", tag1, tag2}).find("a"), std::string::npos);

  // EXEC over same-slot keys works; cross-slot EXEC aborts with CROSSSLOT.
  EXPECT_EQ(ca.Command({"MULTI"}), "+OK");
  EXPECT_EQ(ca.Command({"SET", tag + "x", "1"}), "+QUEUED");
  EXPECT_EQ(ca.Command({"EXEC"}), "*1\r\n+OK");
  EXPECT_EQ(ca.Command({"MULTI"}), "+OK");
  EXPECT_EQ(ca.Command({"SET", slot5, "1"}), "+QUEUED");
  EXPECT_EQ(ca.Command({"SET", slot7, "1"}), "+QUEUED");
  EXPECT_EQ(ca.Command({"EXEC"}),
            "-CROSSSLOT Keys in request don't hash to the same slot");

  // Cluster-mode policies and stable wire texts.
  EXPECT_EQ(ca.Command({"SELECT", "0"}), "+OK");
  EXPECT_EQ(ca.Command({"SELECT", "1"}),
            "-ERR SELECT is not allowed in cluster mode");
  EXPECT_EQ(ca.Command({"COPY", tag1, tag + "3", "DB", "1"}),
            "-ERR Copying to another database is not allowed in cluster mode");
  EXPECT_EQ(ca.Command({"REPLICAOF", "127.0.0.1", "1"}),
            "-ERR REPLICAOF not allowed in cluster mode.");
  // Static compatibility mode has permanent authority. Its sole local
  // slot-owning primary retains the existing process-wide mutation surface;
  // the replica case below remains fail-closed.
  EXPECT_EQ(ca.Command({"FLUSHDB"}), "+OK");
  EXPECT_EQ(ca.Command({"FLUSHALL"}), "+OK");
  constexpr std::string_view static_library =
      "#!lua name=static_cluster\n"
      "redis.register_function('static_value', function(keys, args) "
      "return 1 end)";
  EXPECT_EQ(ca.Command({"FUNCTION", "LOAD", std::string(static_library)}),
            "$14\r\nstatic_cluster");
  EXPECT_NE(ca.Command({"FUNCTION", "LIST"}).find("static_cluster"),
            std::string::npos);
  EXPECT_EQ(ca.Command({"FUNCTION", "DELETE", "static_cluster"}), "+OK");
  constexpr std::string_view static_exec_library =
      "#!lua name=static_exec\n"
      "redis.register_function('static_exec_value', function(keys, args) "
      "return 1 end)";
  EXPECT_EQ(ca.Command({"MULTI"}), "+OK");
  EXPECT_EQ(
      ca.Command({"FUNCTION", "LOAD", std::string(static_exec_library)}),
      "+QUEUED");
  EXPECT_EQ(ca.Command({"EXEC"}), "*1\r\n$11\r\nstatic_exec");
  EXPECT_NE(ca.Command({"FUNCTION", "LIST"}).find("static_exec"),
            std::string::npos);
  EXPECT_EQ(ca.Command({"FUNCTION", "DELETE", "static_exec"}), "+OK");
  EXPECT_TRUE(
      ca.Command({"SCRIPT", "LOAD", "return 1"}).starts_with("$40\r\n"));
  RespClient native_export(cluster.port_a);
  EXPECT_EQ(
      native_export.Command({"KLPSYNC", "1", "?", "?", "?", "?", "?", "?"}),
      "-ERR native replication export is unavailable in static cluster mode");
  RespClient redis_export(cluster.port_a);
  EXPECT_EQ(redis_export.Command({"REPLCONF", "capa", "eof"}), "+OK");
  EXPECT_EQ(redis_export.Command({"PSYNC", "?", "-1"}),
            "-ERR Redis replication export is unavailable in cluster mode");

  // INFO advertises cluster mode in both sections.
  const std::string info_all = ca.Command({"INFO"});
  EXPECT_NE(info_all.find("redis_mode:cluster"), std::string::npos) << info_all;
  EXPECT_NE(info_all.find("# Cluster\r\ncluster_enabled:1"), std::string::npos)
      << info_all;

  cluster.StopAll();
}

TEST(ClusterE2eTest, ReplicaReadonlyAdmission) {
  MainCluster cluster;
  cluster.Start();

  RespClient replica(cluster.port_r);
  // Static topology has no replication lifecycle capable of transferring
  // durable expiration authority. tomb_raider_enabled is fixed from that same
  // startup authority bit, so this catches accidental coupling to Meta-only
  // population management.
  const std::string replica_stats = replica.Command({"INFO", "STATS"});
  EXPECT_NE(replica_stats.find("tomb_raider_enabled:0"), std::string::npos)
      << replica_stats;
  EXPECT_EQ(replica.Command({"FLUSHDB"}),
            "-ERR FLUSHDB is not allowed in cluster mode");
  const std::string key_a = KeyInSlotRange(kSlotsAFirst, kSlotsALast);

  // Without READONLY every keyed request redirects to the primary.
  EXPECT_EQ(
      replica.Command({"GET", key_a}),
      absl::StrCat("-MOVED ", SlotOf(key_a), " 127.0.0.1:", cluster.port_a));
  EXPECT_EQ(
      replica.Command({"SET", key_a, "1"}),
      absl::StrCat("-MOVED ", SlotOf(key_a), " 127.0.0.1:", cluster.port_a));
  // READONLY admits reads (v1 wires no replication: the reply is a nil read,
  // i.e. admission without data); writes still redirect; READWRITE restores.
  EXPECT_EQ(replica.Command({"READONLY"}), "+OK");
  EXPECT_EQ(replica.Command({"GET", key_a}), "$-1");
  EXPECT_EQ(
      replica.Command({"SET", key_a, "1"}),
      absl::StrCat("-MOVED ", SlotOf(key_a), " 127.0.0.1:", cluster.port_a));
  EXPECT_EQ(replica.Command({"READWRITE"}), "+OK");
  EXPECT_EQ(
      replica.Command({"GET", key_a}),
      absl::StrCat("-MOVED ", SlotOf(key_a), " 127.0.0.1:", cluster.port_a));

  cluster.StopAll();
}

TEST(ClusterE2eTest, SlotlessStaticPrimaryRejectsGlobalMutationsAndExec) {
  TempDir dir;
  const std::uint16_t port = FindFreePort();
  std::uint16_t remote_port = 0;
  do {
    remote_port = FindFreePort();
  } while (remote_port == port);
  const std::string nodes = dir.Path("nodes.conf");
  const std::string data = dir.Path("data");
  const std::string log = dir.Path("server.log");
  WriteFile(nodes, absl::StrCat(
                       kNodeD, " 127.0.0.1:", port,
                       "@0 master - 0 0 1 connected\n", kNodeE,
                       " 127.0.0.1:", remote_port,
                       "@0 master - 0 0 1 connected 0-16383\n"
                       "vars currentEpoch 1 lastVoteEpoch 0\n"));
  CreateDataFile(data);
  ServerProcess server(port, data, log, nodes);
  RespClient client(port);

  const auto ready_deadline = std::chrono::steady_clock::now() + 30s;
  while (std::chrono::steady_clock::now() < ready_deadline &&
         !client.Command({"DBSIZE"}).starts_with(":")) {
    std::this_thread::sleep_for(20ms);
  }
  ASSERT_TRUE(client.Command({"DBSIZE"}).starts_with(":"));
  EXPECT_EQ(client.Command({"FLUSHDB"}),
            "-ERR FLUSHDB is not allowed in cluster mode");
  EXPECT_EQ(client.Command({"FLUSHALL"}),
            "-ERR FLUSHALL is not allowed in cluster mode");
  constexpr std::string_view library =
      "#!lua name=slotless_rejected\n"
      "redis.register_function('slotless_value', function(keys, args) "
      "return 1 end)";
  EXPECT_EQ(client.Command({"FUNCTION", "LOAD", std::string(library)}),
            "-ERR FUNCTION LOAD is not allowed in cluster mode");
  EXPECT_EQ(client.Command({"FUNCTION", "DELETE", "slotless_rejected"}),
            "-ERR FUNCTION DELETE is not allowed in cluster mode");
  EXPECT_EQ(client.Command({"FUNCTION", "FLUSH"}),
            "-ERR FUNCTION FLUSH is not allowed in cluster mode");
  EXPECT_EQ(client.Command({"FUNCTION", "RESTORE", "payload"}),
            "-ERR FUNCTION RESTORE is not allowed in cluster mode");
  EXPECT_EQ(client.Command({"FUNCTION", "LIST"}), "*0");

  // FUNCTION mutations are otherwise legal transaction children. Rejecting
  // this one at queue time is important: a slotless EXEC has no union slot on
  // which to perform its final authority recheck.
  EXPECT_EQ(client.Command({"MULTI"}), "+OK");
  EXPECT_EQ(client.Command({"FUNCTION", "LOAD", std::string(library)}),
            "-ERR FUNCTION LOAD is not allowed in cluster mode");
  EXPECT_EQ(client.Command({"EXEC"}),
            "-EXECABORT Transaction discarded because of previous errors.");
  EXPECT_EQ(client.Command({"FUNCTION", "LIST"}), "*0");

  server.Stop();
}

TEST(ClusterE2eTest, CoverageGapReportsClusterDown) {
  TempDir dir;
  const std::uint16_t port = FindFreePort();
  const std::string nodes = dir.Path("nodes.conf");
  // Only slots 0-5460 are owned; the rest of the ring is unbound.
  WriteFile(nodes, absl::StrCat(kNodeD, " 127.0.0.1:", port,
                                "@0 master - 0 0 1 connected 0-5460\n"
                                "vars currentEpoch 1 lastVoteEpoch 0\n"));
  CreateDataFile(dir.Path("d.data"));
  ServerProcess server(port, dir.Path("d.data"), dir.Path("d.log"), nodes);
  RespClient client(port);
  WaitReadyPrimary(client, 0, 5460);

  const std::string covered = KeyInSlotRange(0, 5460);
  EXPECT_EQ(client.Command({"SET", covered, "1"}), "+OK");
  const std::string uncovered = KeyInSlotRange(5461, 16383);
  EXPECT_EQ(client.Command({"SET", uncovered, "1"}),
            "-CLUSTERDOWN Hash slot not served");
  // Redis order: a first key on an unbound slot yields CLUSTERDOWN even when
  // another key sits on a different (covered) slot — not CROSSSLOT.
  EXPECT_EQ(client.Command({"MGET", uncovered, covered}),
            "-CLUSTERDOWN Hash slot not served");
  // Discovery reports the gap.
  EXPECT_TRUE(client.Command({"CLUSTER", "INFO"})
                  .find("cluster_state:fail\r\n"
                        "cluster_slots_assigned:5461\r\n") !=
              std::string::npos);

  server.Stop();
}

TEST(ClusterE2eTest, SighupOwnerSwitchUnderTraffic) {
  MainCluster cluster;
  cluster.Start();

  RespClient ca(cluster.port_a);
  RespClient cb(cluster.port_b);
  const std::string moved_key = KeyInSlotRange(kSlotsAFirst, kSlotsALast);
  const std::string stable_key = KeyInSlotRange(kSlotsBFirst, kSlotsBLast);
  ASSERT_EQ(ca.Command({"SET", moved_key, "before"}), "+OK");

  std::atomic<bool> stop{false};
  std::atomic<bool> saw_nonstandard_error{false};
  std::atomic<std::uint64_t> operations{0};
  std::thread traffic([&] {
    // Reconnect after each failure: a switch may legitimately drop or refuse
    // a connection, and a dead fd must not poison the rest of the run.
    std::unique_ptr<RespClient> ta, tb;
    std::uint64_t i = 0;
    while (!stop.load(std::memory_order_relaxed)) {
      for (auto* client : {&ta, &tb}) {
        const std::uint16_t port =
            client == &ta ? cluster.port_a : cluster.port_b;
        try {
          if (!*client) *client = std::make_unique<RespClient>(port);
          const std::string reply = (*client)->Command(
              {"SET", i % 2 == 0 ? moved_key : stable_key, "x"});
          if (!reply.empty() && reply.front() == '-') {
            static const std::set<std::string> standard{
                "-ERR",         "-MOVED",   "-CROSSSLOT",
                "-CLUSTERDOWN", "-LOADING", "-TRYAGAIN"};
            const std::string prefix = reply.substr(0, reply.find(' '));
            if (!standard.contains(prefix)) saw_nonstandard_error = true;
          }
        } catch (const std::exception&) {
          client->reset();  // reconnect next iteration
        }
        ++operations;
      }
      ++i;
    }
  });

  // Move A's range to B and bump epochs; R now follows a slotless A.
  ReplaceFile(cluster.nodes_file,
              absl::StrCat(kNodeA, " 127.0.0.1:", cluster.port_a,
                           "@0 master - 0 0 4 connected\n", kNodeB,
                           " 127.0.0.1:", cluster.port_b,
                           "@0 master - 0 0 5 connected 0-10922\n", kNodeC,
                           " 127.0.0.1:", cluster.port_c,
                           "@0 master - 0 0 6 connected 10923-16383\n", kNodeR,
                           " 127.0.0.1:", cluster.port_r, "@0 slave ", kNodeA,
                           " 0 0 4 connected\n"
                           "vars currentEpoch 6 lastVoteEpoch 0\n"));
  cluster.a->Sighup();
  cluster.b->Sighup();

  // Poll until the new state is visible on both nodes (reload is async).
  const auto deadline = std::chrono::steady_clock::now() + 30s;
  while (std::chrono::steady_clock::now() < deadline) {
    if (cb.Command({"SET", moved_key, "after"}) == "+OK") break;
    std::this_thread::sleep_for(20ms);
  }
  EXPECT_EQ(cb.Command({"SET", moved_key, "after"}), "+OK")
      << "new owner never took over";
  while (std::chrono::steady_clock::now() < deadline) {
    if (ca.Command({"SET", moved_key, "x"}).starts_with("-MOVED ")) break;
    std::this_thread::sleep_for(20ms);
  }
  EXPECT_EQ(
      ca.Command({"SET", moved_key, "x"}),
      absl::StrCat("-MOVED ", SlotOf(moved_key), " 127.0.0.1:", cluster.port_b))
      << "old owner never fenced";
  // The switch never touches C's slots or B's original range.
  EXPECT_EQ(cb.Command({"SET", stable_key, "1"}), "+OK");

  stop.store(true, std::memory_order_relaxed);
  traffic.join();
  EXPECT_GT(operations.load(), 100U);
  EXPECT_FALSE(saw_nonstandard_error.load());
  EXPECT_TRUE(cluster.a->Alive());
  EXPECT_TRUE(cluster.b->Alive());
  EXPECT_TRUE(cluster.c->Alive());
  EXPECT_TRUE(cluster.r->Alive());

  cluster.StopAll();
}

TEST(ClusterE2eTest, SighupInvalidReloadKeepsOldState) {
  TempDir dir;
  const std::uint16_t port = FindFreePort();
  const std::string nodes = dir.Path("nodes.conf");
  WriteFile(nodes, absl::StrCat(kNodeD, " 127.0.0.1:", port,
                                "@0 master - 0 0 1 connected 0-5460\n"
                                "vars currentEpoch 1 lastVoteEpoch 0\n"));
  CreateDataFile(dir.Path("d.data"));
  ServerProcess server(port, dir.Path("d.data"), dir.Path("d.log"), nodes);
  RespClient client(port);
  WaitReadyPrimary(client, 0, 5460);
  const std::string covered = KeyInSlotRange(0, 5460);
  ASSERT_EQ(client.Command({"SET", covered, "1"}), "+OK");

  // A corrupt reload (slot overlap with a newcomer) is rejected: the last
  // published state stays in effect and the server keeps serving.
  ReplaceFile(nodes, absl::StrCat(kNodeD, " 127.0.0.1:", port,
                                  "@0 master - 0 0 2 connected 0-5460\n",
                                  kNodeE, " 127.0.0.1:", port + 1,
                                  "@0 master - 0 0 2 connected 100-200\n"
                                  "vars currentEpoch 2 lastVoteEpoch 0\n"));
  server.Sighup();
  std::this_thread::sleep_for(500ms);  // give the reload time to (not) apply
  EXPECT_TRUE(server.Alive());
  EXPECT_EQ(client.Command({"SET", covered, "2"}), "+OK");
  // A key in the overlapping range still routes to the old owner (self).
  const std::string overlap = KeyInSlotRange(100, 200);
  EXPECT_EQ(client.Command({"SET", overlap, "3"}), "+OK");

  server.Stop();
}

TEST(ClusterE2eTest, RedisCliFollowsMoved) {
  if (g_redis_cli.empty()) {
    GTEST_SKIP() << "redis-cli not available in this environment";
  }
  MainCluster cluster;
  cluster.Start();

  // A cluster-mode client must discover, follow MOVED, and land the write on
  // the owner without ever seeing a Keylane-private response.
  const std::string key_b = KeyInSlotRange(kSlotsBFirst, kSlotsBLast);
  const std::string command = absl::StrCat(
      g_redis_cli, " -c -p ", cluster.port_a, " SET ", key_b, " via-cli");
  const int rc = std::system(command.c_str());
  EXPECT_EQ(rc, 0);
  RespClient cb(cluster.port_b);
  EXPECT_EQ(cb.Command({"GET", key_b}), "$7\r\nvia-cli");

  cluster.StopAll();
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 2) {
    std::fprintf(stderr, "usage: %s <keylane-binary> [redis-cli]\n", argv[0]);
    return 1;
  }
  g_keylane_binary = argv[1];
  if (argc > 2) g_redis_cli = argv[2];
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
