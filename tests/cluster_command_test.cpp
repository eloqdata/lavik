#include "../src/redis/cluster_command.h"

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "../src/redis/blocking_wait.h"
#include "../src/redis/cluster_gate.h"
#include "absl/strings/str_cat.h"
#include "cluster/test_topology_installer.h"
#include "keylane/cluster/runtime.h"
#include "keylane/cluster/topology.h"
#include "keylane/resp.h"
#include "keylane/resp_version.h"
#include "keylane/session.h"
#include "keylane/storage/format.h"

namespace {

namespace cluster = keylane::cluster;

// Node ids are 40 lowercase hex chars, as the topology builder validates.
constexpr std::string_view kNodeA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
constexpr std::string_view kNodeB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
constexpr std::string_view kNodeR = "cccccccccccccccccccccccccccccccccccccccc";
constexpr cluster::NodeIndex kNodeAIndex = 0;
constexpr cluster::NodeIndex kNodeBIndex = 1;
constexpr cluster::NodeIndex kNodeRIndex = 2;

cluster::NodeId ParseNodeId(std::string_view id) {
  const std::optional<cluster::NodeId> parsed = cluster::NodeId::Parse(id);
  EXPECT_TRUE(parsed.has_value());
  return parsed.value_or(cluster::NodeId{});
}

cluster::NodeDescriptor MakeNode(std::string_view id, std::string_view host,
                                 std::uint16_t port, std::uint16_t tls_port,
                                 cluster::NodeIndex primary_node_index,
                                 std::uint64_t config_epoch,
                                 bool link_connected = true) {
  cluster::NodeDescriptor node;
  node.node_id_ = ParseNodeId(id);
  node.SetHost(host);
  node.port_ = port;
  node.tls_port_ = tls_port;
  node.primary_node_index_ = primary_node_index;
  node.config_epoch_ = config_epoch;
  node.link_connected_ = link_connected;
  return node;
}

cluster::GroupView MakeGroup(std::string_view group_id,
                             cluster::NodeIndex primary_node_index,
                             std::vector<cluster::NodeIndex> replica_indices,
                             std::vector<cluster::SlotRange> ranges) {
  cluster::GroupView group;
  group.group_id_ = std::string(group_id);
  group.primary_node_index_ = primary_node_index;
  group.replica_node_indices_ = std::move(replica_indices);
  group.slot_ranges_ = std::move(ranges);
  return group;
}

// Primary A serving [0,100] with replica R, and primary B serving the tail
// range. full_coverage extends B's range to 16383 so the state has complete
// slot coverage; otherwise only [0,200] is assigned. Self defaults to A.
std::shared_ptr<const cluster::ServingState> BuildThreeNodeState(
    bool full_coverage, std::string_view self_id = kNodeA,
    bool pause_group_a = false) {
  cluster::ServingStateBuilder builder;
  builder.SetTopologyEpoch(1);
  if (self_id == kNodeA) {
    builder.SetSelfNodeIndex(kNodeAIndex);
  } else if (self_id == kNodeB) {
    builder.SetSelfNodeIndex(kNodeBIndex);
  } else if (self_id == kNodeR) {
    builder.SetSelfNodeIndex(kNodeRIndex);
  }
  builder.AddNode(
      MakeNode(kNodeA, "127.0.0.1", 7000, 17001, cluster::kNoNodeIndex, 1));
  builder.AddNode(
      MakeNode(kNodeB, "127.0.0.2", 7001, 17002, cluster::kNoNodeIndex, 2));
  builder.AddNode(MakeNode(kNodeR, "127.0.0.3", 7002, 17003, kNodeAIndex, 1));
  cluster::GroupView group_a = MakeGroup("group-a", kNodeAIndex, {kNodeRIndex},
                                         {cluster::SlotRange{0, 100}});
  group_a.mutations_paused_ = pause_group_a;
  builder.AddGroup(std::move(group_a));
  const std::uint16_t last =
      full_coverage ? static_cast<std::uint16_t>(cluster::kSlotCount - 1)
                    : static_cast<std::uint16_t>(200);
  builder.AddGroup(
      MakeGroup("group-b", kNodeBIndex, {}, {cluster::SlotRange{101, last}}));
  auto state = builder.Build();
  EXPECT_TRUE(state.ok()) << state.status();
  return state.ok() ? std::move(*state) : nullptr;
}

// Self A with unsorted, mergeable ranges and a disconnected peer B; exercises
// the slot-range compaction and the link-state flag in NODES.
std::shared_ptr<const cluster::ServingState> BuildCompactionState() {
  cluster::ServingStateBuilder builder;
  builder.SetTopologyEpoch(1);
  builder.SetSelfNodeIndex(kNodeAIndex);
  builder.AddNode(
      MakeNode(kNodeA, "127.0.0.1", 7000, 17001, cluster::kNoNodeIndex, 1));
  builder.AddNode(MakeNode(kNodeB, "127.0.0.2", 7001, 17002,
                           cluster::kNoNodeIndex, 2, false));
  builder.AddGroup(
      MakeGroup("group-a", kNodeAIndex, {},
                {cluster::SlotRange{10, 10}, cluster::SlotRange{5, 5},
                 cluster::SlotRange{7, 9}}));
  builder.AddGroup(MakeGroup("group-b", kNodeBIndex, {},
                             {cluster::SlotRange{16383, 16383}}));
  auto state = builder.Build();
  EXPECT_TRUE(state.ok()) << state.status();
  return state.ok() ? std::move(*state) : nullptr;
}

std::unique_ptr<cluster::ClusterRuntime> MakeRuntime(
    const std::shared_ptr<const cluster::ServingState>& state,
    std::string announce_ip = "") {
  auto runtime = std::make_unique<cluster::ClusterRuntime>();
  runtime->announce_ip_ = std::move(announce_ip);
  runtime->announce_port_ = 7000;
  runtime->announce_tls_port_ = 17001;
  if (state != nullptr) runtime->topology_cache_.Publish(state);
  return runtime;
}

// Installs the process-wide runtime for one test and uninstalls it on scope
// exit so tests cannot leak cluster state into one another.
class ClusterRuntimeGuard {
 public:
  explicit ClusterRuntimeGuard(
      std::unique_ptr<cluster::ClusterRuntime> runtime) {
    cluster::InstallClusterRuntime(std::move(runtime));
  }
  ~ClusterRuntimeGuard() { cluster::InstallClusterRuntime(nullptr); }
};

keylane::CommandRequest MakeRequest(std::vector<std::string> args,
                                    bool connection_tls = false) {
  keylane::CommandRequest request;
  request.kind_ = keylane::CommandKind::kCluster;
  request.args_ = std::move(args);
  request.connection_tls_ = connection_tls;
  return request;
}

// Drives a handler coroutine to completion on the test thread. The cluster
// subcommands never suspend; a pending handle here means the handler grew an
// await and this driver must be revisited.
std::string RunClusterCommand(
    const keylane::CommandRequest& request,
    keylane::RespVersion version = keylane::RespVersion::k2) {
  keylane::ReplyBuilder builder(version);
  auto task = keylane::ExecuteClusterModeCommand(request, builder);
  auto handle = std::move(task).ReleaseHandle();
  handle.resume();
  EXPECT_TRUE(handle.done());
  std::string encoded;
  // A completed Task<T> always constructed its result: unhandled exceptions
  // terminate, so there is no completed-without-value state to probe here.
  if (handle.done()) {
    encoded = std::string(handle.promise().value_.encoded_);
  }
  handle.destroy();
  return encoded;
}

std::string RunDispatch(keylane::ConnectionContext& context,
                        std::vector<std::string> args) {
  auto request = keylane::BuildCommandRequest(
      keylane::RespCommand{.args_ = std::move(args)}, context.selected_db_);
  EXPECT_TRUE(request.ok()) << request.status();
  if (!request.ok()) return {};
  keylane::ReplyBuilder builder(context.resp_version());
  auto task = keylane::DispatchCommand(context, *request, builder);
  auto handle = std::move(task).ReleaseHandle();
  handle.resume();
  EXPECT_TRUE(handle.done());
  std::string encoded;
  if (handle.done()) encoded = std::string(handle.promise().value_.encoded_);
  handle.destroy();
  return encoded;
}

std::string ChannelInGroupARange(std::uint16_t different_from = 101) {
  for (std::uint32_t suffix = 0; suffix < 100000; ++suffix) {
    const std::string candidate = "issue41-channel-" + std::to_string(suffix);
    const std::uint16_t slot = keylane::storage::RedisSlot(candidate);
    if (slot <= 100 && slot != different_from) return candidate;
  }
  ADD_FAILURE() << "failed to find deterministic channel in group-a range";
  return "issue41-channel-fallback";
}

// Extracts the payload of a $<len>\r\n<payload>\r\n reply, failing the test
// when the framing does not match exactly.
std::string_view BulkPayload(std::string_view reply) {
  const auto fail = [reply](std::string_view why) {
    ADD_FAILURE() << why << ": " << reply;
    return std::string_view{};
  };
  if (!reply.starts_with('$')) return fail("not a bulk string");
  const std::size_t header_end = reply.find("\r\n");
  if (header_end == std::string_view::npos || header_end < 2) {
    return fail("bad bulk header");
  }
  std::uint64_t length = 0;
  for (const char c : reply.substr(1, header_end - 1)) {
    if (c < '0' || c > '9') return fail("bad bulk length");
    length = length * 10 + static_cast<std::uint64_t>(c - '0');
  }
  if (reply.size() != header_end + 2 + length + 2 || !reply.ends_with("\r\n")) {
    return fail("bulk length mismatch");
  }
  return reply.substr(header_end + 2, length);
}

// [host, port, node-id] entry of a CLUSTER SLOTS range, as nested arrays.
std::string SlotsNode(std::string_view host, std::uint16_t port,
                      std::string_view node_id) {
  return absl::StrCat("*3\r\n", keylane::EncodeBulkString(host), ":", port,
                      "\r\n", keylane::EncodeBulkString(node_id));
}

TEST(ClusterCommandTest, KeySlotWorksWithoutClusterState) {
  cluster::InstallClusterRuntime(nullptr);
  EXPECT_EQ(RunClusterCommand(MakeRequest({"CLUSTER", "KEYSLOT", "foo"})),
            keylane::EncodeInteger(keylane::storage::RedisSlot("foo")));
  // The {hashtag} portion decides the slot, so these must agree.
  EXPECT_EQ(RunClusterCommand(MakeRequest({"CLUSTER", "KEYSLOT", "{k}a"})),
            RunClusterCommand(MakeRequest({"CLUSTER", "KEYSLOT", "{k}b"})));
  // Subcommand matching is case-insensitive.
  EXPECT_EQ(RunClusterCommand(MakeRequest({"cluster", "keyslot", "foo"})),
            keylane::EncodeInteger(keylane::storage::RedisSlot("foo")));
}

TEST(ClusterCommandTest, PublishUsesItsChannelSlotAndHonorsControlledPause) {
  const auto state = BuildThreeNodeState(/*full_coverage=*/true, kNodeA,
                                         /*pause_group_a=*/true);
  ASSERT_NE(state, nullptr);
  ClusterRuntimeGuard guard(MakeRuntime(state));
  keylane::ConnectionContext context;

  EXPECT_EQ(
      RunDispatch(context, {"PUBLISH", ChannelInGroupARange(), "payload"}),
      "-TRYAGAIN Failover in progress\r\n");
}

TEST(ClusterCommandTest, SubcommandErrorsMatchRedis) {
  cluster::InstallClusterRuntime(nullptr);
  const auto expect_error = [](std::vector<std::string> args,
                               std::string_view subcommand) {
    EXPECT_EQ(RunClusterCommand(MakeRequest(std::move(args))),
              keylane::EncodeError(absl::StrCat(
                  "ERR Unknown CLUSTER subcommand or wrong number of "
                  "arguments for '",
                  subcommand, "'")));
  };
  // Unsupported and unknown subcommands use the same error path.
  for (const char* sub :
       {"SHARDS", "SETSLOT", "MEET", "FORGET", "REPLICATE", "ADDSLOTS",
        "DELSLOTS", "FAILOVER", "RESET", "BUMPEPOCH", "SAVECONFIG",
        "SET-CONFIG-EPOCH", "COUNTKEYSINSLOT", "GETKEYSINSLOT", "ASKING",
        "WHATEVER"}) {
    expect_error({"CLUSTER", sub}, sub);
  }
  // The error names the subcommand exactly as typed by the client.
  expect_error({"CLUSTER", "meet", "127.0.0.1", "7000"}, "meet");
  // Wrong argument counts for known subcommands produce the same text.
  expect_error({"CLUSTER", "KEYSLOT"}, "KEYSLOT");
  expect_error({"CLUSTER", "KEYSLOT", "a", "b"}, "KEYSLOT");
  expect_error({"CLUSTER", "MYID", "x"}, "MYID");
  expect_error({"CLUSTER", "INFO", "x"}, "INFO");
  expect_error({"CLUSTER", "SLOTS", "x"}, "SLOTS");
  expect_error({"CLUSTER", "NODES", "x"}, "NODES");
  // Below the command's minimum arity the table-level error text applies.
  EXPECT_EQ(RunClusterCommand(MakeRequest({"CLUSTER"})),
            keylane::EncodeError(
                "ERR wrong number of arguments for 'cluster' command"));
}

TEST(ClusterCommandTest, MyIdReportsSelfNodeId) {
  const std::shared_ptr<const cluster::ServingState> state =
      BuildThreeNodeState(false);
  ASSERT_NE(state, nullptr);
  ClusterRuntimeGuard guard(MakeRuntime(state));
  EXPECT_EQ(RunClusterCommand(MakeRequest({"CLUSTER", "MYID"})),
            keylane::EncodeBulkString(kNodeA));
}

TEST(ClusterCommandTest, MyIdIsEmptyWithoutSelf) {
  // No runtime installed at all.
  cluster::InstallClusterRuntime(nullptr);
  EXPECT_EQ(RunClusterCommand(MakeRequest({"CLUSTER", "MYID"})),
            keylane::EncodeBulkString(""));
  // Runtime installed but no ServingState published yet.
  {
    ClusterRuntimeGuard guard(MakeRuntime(nullptr));
    EXPECT_EQ(RunClusterCommand(MakeRequest({"CLUSTER", "MYID"})),
              keylane::EncodeBulkString(""));
  }
  // Published state whose topology does not name this node.
  const std::shared_ptr<const cluster::ServingState> state =
      BuildThreeNodeState(false, "");
  ASSERT_NE(state, nullptr);
  ClusterRuntimeGuard guard(MakeRuntime(state));
  EXPECT_EQ(RunClusterCommand(MakeRequest({"CLUSTER", "MYID"})),
            keylane::EncodeBulkString(""));
}

TEST(ClusterCommandTest, InfoReportsPinnedFieldSet) {
  const std::shared_ptr<const cluster::ServingState> state =
      BuildThreeNodeState(true);
  ASSERT_NE(state, nullptr);
  ClusterRuntimeGuard guard(MakeRuntime(state));
  EXPECT_EQ(RunClusterCommand(MakeRequest({"CLUSTER", "INFO"})),
            keylane::EncodeBulkString("cluster_state:ok\r\n"
                                      "cluster_slots_assigned:16384\r\n"
                                      "cluster_slots_ok:16384\r\n"
                                      "cluster_slots_pfail:0\r\n"
                                      "cluster_slots_fail:0\r\n"
                                      "cluster_known_nodes:3\r\n"
                                      "cluster_size:2\r\n"
                                      "cluster_current_epoch:2\r\n"
                                      "cluster_my_epoch:1\r\n"
                                      "cluster_stats_messages_sent:0\r\n"
                                      "cluster_stats_messages_received:0\r\n"));
}

TEST(ClusterCommandTest, InfoReportsCoverageGap) {
  // Only [0,200] of 16384 slots is assigned: state fails but the counts still
  // report the partial assignment.
  const std::shared_ptr<const cluster::ServingState> state =
      BuildThreeNodeState(false);
  ASSERT_NE(state, nullptr);
  ClusterRuntimeGuard guard(MakeRuntime(state));
  EXPECT_EQ(RunClusterCommand(MakeRequest({"CLUSTER", "INFO"})),
            keylane::EncodeBulkString("cluster_state:fail\r\n"
                                      "cluster_slots_assigned:201\r\n"
                                      "cluster_slots_ok:201\r\n"
                                      "cluster_slots_pfail:0\r\n"
                                      "cluster_slots_fail:0\r\n"
                                      "cluster_known_nodes:3\r\n"
                                      "cluster_size:2\r\n"
                                      "cluster_current_epoch:2\r\n"
                                      "cluster_my_epoch:1\r\n"
                                      "cluster_stats_messages_sent:0\r\n"
                                      "cluster_stats_messages_received:0\r\n"));
}

TEST(ClusterCommandTest, InfoWithoutStateFailsAndZeroes) {
  ClusterRuntimeGuard guard(MakeRuntime(nullptr));
  EXPECT_EQ(RunClusterCommand(MakeRequest({"CLUSTER", "INFO"})),
            keylane::EncodeBulkString("cluster_state:fail\r\n"
                                      "cluster_slots_assigned:0\r\n"
                                      "cluster_slots_ok:0\r\n"
                                      "cluster_slots_pfail:0\r\n"
                                      "cluster_slots_fail:0\r\n"
                                      "cluster_known_nodes:0\r\n"
                                      "cluster_size:0\r\n"
                                      "cluster_current_epoch:0\r\n"
                                      "cluster_my_epoch:0\r\n"
                                      "cluster_stats_messages_sent:0\r\n"
                                      "cluster_stats_messages_received:0\r\n"));
}

TEST(ClusterCommandTest, SlotsGroupsRangesByOwningGroup) {
  const std::shared_ptr<const cluster::ServingState> state =
      BuildThreeNodeState(false);
  ASSERT_NE(state, nullptr);
  // Empty announce address: the wildcard-bind convention leaves the self
  // host empty so clients dial the startup node's address.
  ClusterRuntimeGuard guard(MakeRuntime(state));
  const std::string expected = absl::StrCat(
      "*2\r\n", "*4\r\n", ":0\r\n", ":100\r\n", SlotsNode("", 7000, kNodeA),
      SlotsNode("127.0.0.3", 7002, kNodeR), "*3\r\n", ":101\r\n", ":200\r\n",
      SlotsNode("127.0.0.2", 7001, kNodeB));
  EXPECT_EQ(RunClusterCommand(MakeRequest({"CLUSTER", "SLOTS"})), expected);
  // RESP2 and RESP3 share the discovery wire shape.
  EXPECT_EQ(RunClusterCommand(MakeRequest({"CLUSTER", "SLOTS"}),
                              keylane::RespVersion::k3),
            expected);
}

TEST(ClusterCommandTest, SlotsSelectsTlsPortForTlsConnections) {
  const std::shared_ptr<const cluster::ServingState> state =
      BuildThreeNodeState(false);
  ASSERT_NE(state, nullptr);
  ClusterRuntimeGuard guard(MakeRuntime(state, "192.0.2.1"));
  // TLS connections get the TLS ports; the self entry advertises the
  // configured announce address.
  const std::string expected =
      absl::StrCat("*2\r\n", "*4\r\n", ":0\r\n", ":100\r\n",
                   SlotsNode("192.0.2.1", 17001, kNodeA),
                   SlotsNode("127.0.0.3", 17003, kNodeR), "*3\r\n", ":101\r\n",
                   ":200\r\n", SlotsNode("127.0.0.2", 17002, kNodeB));
  EXPECT_EQ(RunClusterCommand(MakeRequest({"CLUSTER", "SLOTS"},
                                          /*connection_tls=*/true)),
            expected);
}

TEST(ClusterCommandTest, SlotsCompactsAdjacentRanges) {
  const std::shared_ptr<const cluster::ServingState> state =
      BuildCompactionState();
  ASSERT_NE(state, nullptr);
  ClusterRuntimeGuard guard(MakeRuntime(state));
  // The ranges are added out of order; after sorting, the adjacent [7,9] and
  // [10,10] merge into [7,10] while the singleton [5,5] stays a bare range.
  const std::string expected = absl::StrCat(
      "*3\r\n", "*3\r\n", ":5\r\n", ":5\r\n", SlotsNode("", 7000, kNodeA),
      "*3\r\n", ":7\r\n", ":10\r\n", SlotsNode("", 7000, kNodeA), "*3\r\n",
      ":16383\r\n", ":16383\r\n", SlotsNode("127.0.0.2", 7001, kNodeB));
  EXPECT_EQ(RunClusterCommand(MakeRequest({"CLUSTER", "SLOTS"})), expected);
}

TEST(ClusterCommandTest, SlotsIsEmptyWithoutState) {
  ClusterRuntimeGuard guard(MakeRuntime(nullptr));
  EXPECT_EQ(RunClusterCommand(MakeRequest({"CLUSTER", "SLOTS"})), "*0\r\n");
}

TEST(ClusterCommandTest, NodesLinesMatchRedisFormat) {
  const std::shared_ptr<const cluster::ServingState> state =
      BuildThreeNodeState(false);
  ASSERT_NE(state, nullptr);
  ClusterRuntimeGuard guard(MakeRuntime(state));
  const std::string reply =
      RunClusterCommand(MakeRequest({"CLUSTER", "NODES"}));
  const std::string_view payload = BulkPayload(reply);
  const std::string self_line =
      absl::StrCat(kNodeA, " :7000@0 myself,master - 0 0 1 connected 0-100\n");
  const std::string replica_line = absl::StrCat(
      kNodeR, " 127.0.0.3:7002@0 slave ", kNodeA, " 0 0 1 connected\n");
  const std::string peer_line = absl::StrCat(
      kNodeB, " 127.0.0.2:7001@0 master - 0 0 2 connected 101-200\n");
  // The node order inside the payload is unspecified; assert membership and
  // the exact total size instead.
  EXPECT_NE(payload.find(self_line), std::string_view::npos) << payload;
  EXPECT_NE(payload.find(replica_line), std::string_view::npos) << payload;
  EXPECT_NE(payload.find(peer_line), std::string_view::npos) << payload;
  EXPECT_EQ(payload.size(),
            self_line.size() + replica_line.size() + peer_line.size())
      << payload;
}

TEST(ClusterCommandTest, NodesUsesTlsPortsForTlsConnections) {
  const std::shared_ptr<const cluster::ServingState> state =
      BuildThreeNodeState(false);
  ASSERT_NE(state, nullptr);
  ClusterRuntimeGuard guard(MakeRuntime(state));
  const std::string reply =
      RunClusterCommand(MakeRequest({"CLUSTER", "NODES"},
                                    /*connection_tls=*/true));
  const std::string_view payload = BulkPayload(reply);
  EXPECT_NE(payload.find(absl::StrCat(kNodeA, " :17001@0 myself,master")),
            std::string_view::npos)
      << payload;
  EXPECT_NE(payload.find(absl::StrCat(kNodeR, " 127.0.0.3:17003@0 slave")),
            std::string_view::npos)
      << payload;
  EXPECT_NE(payload.find(absl::StrCat(kNodeB, " 127.0.0.2:17002@0 master")),
            std::string_view::npos)
      << payload;
}

TEST(ClusterCommandTest, NodesMarksReplicaSelfAndAnnouncesWildcard) {
  const std::shared_ptr<const cluster::ServingState> state =
      BuildThreeNodeState(false, kNodeR);
  ASSERT_NE(state, nullptr);
  ClusterRuntimeGuard guard(MakeRuntime(state));
  const std::string reply =
      RunClusterCommand(MakeRequest({"CLUSTER", "NODES"}));
  const std::string_view payload = BulkPayload(reply);
  // The replica is self: myself,slave, its primary's id, and the empty
  // wildcard-bind host. A is no longer myself and advertises its concrete
  // address. Self advertises the announce ports (MakeRuntime resolves 7000),
  // not its projected port 7002.
  EXPECT_NE(payload.find(absl::StrCat(kNodeR, " :7000@0 myself,slave ", kNodeA,
                                      " 0 0 1 connected\n")),
            std::string_view::npos)
      << payload;
  EXPECT_NE(payload.find(absl::StrCat(kNodeA,
                                      " 127.0.0.1:7000@0 master - 0 0 1 "
                                      "connected 0-100\n")),
            std::string_view::npos)
      << payload;
}

TEST(ClusterCommandTest, NodesCompactsSlotRangesAndFlagsLinkState) {
  const std::shared_ptr<const cluster::ServingState> state =
      BuildCompactionState();
  ASSERT_NE(state, nullptr);
  ClusterRuntimeGuard guard(MakeRuntime(state));
  const std::string reply =
      RunClusterCommand(MakeRequest({"CLUSTER", "NODES"}));
  const std::string_view payload = BulkPayload(reply);
  EXPECT_NE(payload.find(absl::StrCat(kNodeA,
                                      " :7000@0 myself,master - 0 0 1 "
                                      "connected 5 7-10\n")),
            std::string_view::npos)
      << payload;
  EXPECT_NE(payload.find(absl::StrCat(kNodeB,
                                      " 127.0.0.2:7001@0 master - 0 0 2 "
                                      "disconnected 16383\n")),
            std::string_view::npos)
      << payload;
}

TEST(ClusterCommandTest, NodesIsEmptyWithoutState) {
  ClusterRuntimeGuard guard(MakeRuntime(nullptr));
  EXPECT_EQ(RunClusterCommand(MakeRequest({"CLUSTER", "NODES"})),
            keylane::EncodeBulkString(""));
}

TEST(ClusterRequestAuthorityTest, SessionLossRevokesCapturedWriteAdmission) {
  const std::shared_ptr<const cluster::ServingState> state =
      BuildThreeNodeState(false);
  ASSERT_NE(state, nullptr);

  auto runtime = std::make_unique<cluster::ClusterRuntime>();
  ASSERT_TRUE(runtime->node_control_installer_.SetStorageReady(true).ok());
  cluster::Sha256Digest projection_hash{};
  projection_hash.fill(0x11);
  cluster::Sha256Digest object_hash{};
  object_hash.fill(0x22);
  const cluster::ProjectionBasis projection{
      .source_meta_applied_index_ = 7,
      .projection_hash_ = projection_hash,
  };
  ASSERT_TRUE(runtime->node_control_installer_
                  .InstallFullState(
                      cluster::PreparedFullState{
                          .serving_state_ = state,
                          .object_hash_ = object_hash,
                          .control_groups_ = {},
                      },
                      projection)
                  .ok());
  const std::shared_ptr<const cluster::ServingState> installed =
      runtime->topology_cache_.Current();
  ASSERT_NE(installed, nullptr);
  const cluster::GroupView* group = installed->FindGroup("group-a");
  ASSERT_NE(group, nullptr);

  std::array<std::uint8_t, cluster::SessionId::kByteSize> session_bytes{};
  session_bytes.back() = 1;
  const cluster::SessionIdentity session{
      .session_id_ = cluster::SessionId::FromBytes(session_bytes),
      .generation_ = 1,
      .data_boot_id_ = ParseNodeId(kNodeR),
  };
  const cluster::AuthorityAnchor anchor{
      .group_id_ = group->group_id_,
      .assignment_id_ = group->assignment_id_,
      .group_term_ = group->group_term_,
  };
  const auto now = cluster::LeaseClockNow();
  ASSERT_TRUE(runtime->node_control_installer_
                  .ApplyAuthority(
                      {
                          .kind_ = cluster::AuthorityMessage::Kind::kLeaseGrant,
                          .session_ = session,
                          .projection_ = projection,
                          .anchor_ = anchor,
                          .sent_at_ = now,
                          .granted_duration_ = std::chrono::hours(1),
                      },
                      now)
                  .ok());

  keylane::CommandRequest request;
  request.AddClusterSlot(42);
  const cluster::RequestView view{
      .slots_ = request.ClusterSlots(),
      .is_write_ = true,
  };
  request.cluster_authority_admission_ =
      std::make_shared<const cluster::AuthorityAdmission>(
          runtime->authority_guard_.CaptureAndAdmit(view, now));
  ASSERT_EQ(request.cluster_authority_admission_->decision().kind_,
            cluster::Decision::Kind::kServe);

  ClusterRuntimeGuard runtime_guard(std::move(runtime));
  EXPECT_TRUE(keylane::RecheckClusterRequestAuthority(request).ok());
  ASSERT_TRUE(
      cluster::GetClusterRuntime()
          ->node_control_installer_.LoseSession(session, "test disconnect")
          .ok());
  EXPECT_TRUE(keylane::IsClusterAuthorityChanged(
      keylane::RecheckClusterRequestAuthority(request)));

  keylane::ReplyBuilder reply_builder(keylane::RespVersion::k2);
  const keylane::CommandReply reply = keylane::ClusterAuthorityChangedReply(
      request.ClusterSlots(), /*connection_tls=*/false, reply_builder);
  EXPECT_FALSE(reply.close_connection_);
  EXPECT_EQ(reply.encoded_, "-CLUSTERDOWN Hash slot not served\r\n");
}

TEST(ClusterRequestAuthorityTest,
     BlockingAttemptRegistersOnlyItsConcreteMutationWindow) {
  const std::shared_ptr<const cluster::ServingState> state =
      BuildThreeNodeState(false);
  ASSERT_NE(state, nullptr);
  auto runtime = std::make_unique<cluster::ClusterRuntime>();
  cluster::testing::TestTopologyInstaller topology(
      runtime->node_control_installer_, runtime->topology_cache_);
  ASSERT_TRUE(topology.Install(state, cluster::LeaseClockNow()).ok());
  ClusterRuntimeGuard runtime_guard(std::move(runtime));

  keylane::CommandRequest request;
  request.kind_ = keylane::CommandKind::kBLPop;
  request.AddClusterSlot(42);
  keylane::ReplyBuilder reply_builder(keylane::RespVersion::k2);
  cluster::AuthorityInFlightGuards guards;

  const std::optional<keylane::CommandReply> rejected =
      keylane::RegisterClusterBlockingWriteAttempt(request, reply_builder,
                                                   &guards);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(guards.size(), 1u);
  EXPECT_EQ(state->GroupInFlightCount("group-a"), 1u);

  // ExecuteBlockingWaitLoop destroys this attempt-local guard before it
  // registers or sleeps as a dormant waiter.
  guards.clear();
  EXPECT_EQ(state->GroupInFlightCount("group-a"), 0u);
}

}  // namespace
