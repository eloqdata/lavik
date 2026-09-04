#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "keylane/cluster/control_port.h"
#include "keylane/cluster/topology.h"

namespace {

using keylane::cluster::GroupView;
using keylane::cluster::InMemoryClusterControl;
using keylane::cluster::kSlotCount;
using keylane::cluster::NodeDescriptor;
using keylane::cluster::NodeId;
using keylane::cluster::ServingState;
using keylane::cluster::StaticClusterControl;
using keylane::cluster::TopologyCache;

constexpr std::string_view kIdA =
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";  // 40 hex chars
constexpr std::string_view kIdB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
constexpr std::string_view kIdC = "cccccccccccccccccccccccccccccccccccccccc";
constexpr std::string_view kIdD = "dddddddddddddddddddddddddddddddddddddddd";
static_assert(kIdA.size() == 40);

NodeId ParseNodeId(std::string_view id) {
  const std::optional<NodeId> parsed = NodeId::Parse(id);
  EXPECT_TRUE(parsed.has_value());
  return parsed.value_or(NodeId{});
}

std::string NodeLine(std::string_view id, std::string_view address,
                     std::string_view flags, std::string_view primary,
                     std::string_view epoch, std::string_view link,
                     std::string_view slots = "") {
  std::string line = absl::StrCat(id, " ", address, " ", flags, " ", primary,
                                  " 0 0 ", epoch, " ", link);
  if (!slots.empty()) absl::StrAppend(&line, " ", slots);
  line.push_back('\n');
  return line;
}

// A full-coverage three-primary topology with one replica. Node B carries a
// `myself` gossip flag and a `,hostname` suffix (the shape a real redis-server
// writes), node C uses a bracketed IPv6 address, node D is a disconnected
// replica of A.
std::string ValidTopology() {
  return NodeLine(kIdA, "127.0.0.1:7001@17001", "master", "-", "1", "connected",
                  "0-5460") +
         NodeLine(kIdB, "127.0.0.1:7002@17002,node-b.internal", "myself,master",
                  "-", "2", "connected", "5461-10922") +
         NodeLine(kIdC, "[::1]:7003@17003", "master", "-", "3", "connected",
                  "10923-16383") +
         NodeLine(kIdD, "127.0.0.1:7004@17004", "slave", kIdA, "1",
                  "disconnected") +
         "vars currentEpoch 3 lastVoteEpoch 0\n";
}

class TempNodesFile {
 public:
  TempNodesFile() {
    const auto suffix =
        std::chrono::steady_clock::now().time_since_epoch().count();
    path_ = std::filesystem::temp_directory_path() /
            absl::StrCat("keylane-control-port-test-", suffix, ".conf");
  }
  explicit TempNodesFile(std::string_view contents) : TempNodesFile() {
    Write(contents);
  }
  ~TempNodesFile() {
    std::error_code ignored;
    std::filesystem::remove(path_, ignored);
  }

  void Write(std::string_view contents) {
    std::ofstream output(path_, std::ios::trunc);
    output << contents;
  }
  void Remove() {
    std::error_code ignored;
    std::filesystem::remove(path_, ignored);
  }
  const std::filesystem::path& path() const { return path_; }

 private:
  std::filesystem::path path_;
};

TEST(ClusterControlPortTest, ParsesCompleteTopology) {
  auto state = StaticClusterControl::Parse(ValidTopology(), {"127.0.0.1", 7001},
                                           /*cluster_tls_port=*/17011,
                                           /*storage_ready=*/true);
  ASSERT_TRUE(state.ok()) << state.status();
  const ServingState& serving = **state;

  EXPECT_TRUE(serving.CoverageComplete());
  EXPECT_EQ(serving.CoveredSlotCount(), kSlotCount);
  EXPECT_EQ(serving.topology_epoch(), 3);  // max node-line epoch
  EXPECT_EQ(serving.Groups().size(), 3U);
  EXPECT_EQ(serving.Nodes().size(), 4U);
  EXPECT_TRUE(serving.FullyReady());

  ASSERT_NE(serving.Self(), nullptr);
  EXPECT_EQ(serving.Self()->node_id_, ParseNodeId(kIdA));

  const GroupView* group_a = serving.GroupForSlot(0);
  ASSERT_NE(group_a, nullptr);
  EXPECT_EQ(group_a->group_id_, kIdA);
  ASSERT_NE(serving.NodeAt(group_a->primary_node_index_), nullptr);
  EXPECT_EQ(serving.NodeAt(group_a->primary_node_index_)->node_id_,
            ParseNodeId(kIdA));
  EXPECT_EQ(group_a->config_epoch_, 1);
  EXPECT_TRUE(group_a->granted_);
  EXPECT_TRUE(group_a->population_ready_);
  EXPECT_TRUE(group_a->storage_ready_);
  ASSERT_EQ(group_a->replica_node_indices_.size(), 1U);
  ASSERT_NE(serving.NodeAt(group_a->replica_node_indices_[0]), nullptr);
  EXPECT_EQ(serving.NodeAt(group_a->replica_node_indices_[0])->node_id_,
            ParseNodeId(kIdD));
  EXPECT_EQ(serving.GroupForSlot(5460)->group_id_, kIdA);
  EXPECT_EQ(serving.GroupForSlot(5461)->group_id_, kIdB);
  EXPECT_EQ(serving.GroupForSlot(16383)->group_id_, kIdC);
  EXPECT_EQ(serving.FindGroup(kIdA), group_a);
  EXPECT_EQ(serving.FindNode(ParseNodeId(std::string(40, 'f'))), nullptr);

  const NodeDescriptor* node_b = serving.FindNode(ParseNodeId(kIdB));
  ASSERT_NE(node_b, nullptr);
  EXPECT_EQ(node_b->host_, "127.0.0.1");  // ",hostname" suffix stripped
  EXPECT_EQ(node_b->port_, 7002);
  EXPECT_EQ(node_b->tls_port_, 17011);  // uniform cluster TLS port
  EXPECT_TRUE(node_b->is_primary());
  EXPECT_TRUE(node_b->link_connected_);

  const NodeDescriptor* node_c = serving.FindNode(ParseNodeId(kIdC));
  ASSERT_NE(node_c, nullptr);
  EXPECT_EQ(node_c->host_, "::1");  // IPv6 brackets stripped
  EXPECT_EQ(node_c->port_, 7003);

  const NodeDescriptor* node_d = serving.FindNode(ParseNodeId(kIdD));
  ASSERT_NE(node_d, nullptr);
  EXPECT_FALSE(node_d->is_primary());
  ASSERT_NE(serving.NodeAt(node_d->primary_node_index_), nullptr);
  EXPECT_EQ(serving.NodeAt(node_d->primary_node_index_)->node_id_,
            ParseNodeId(kIdA));
  EXPECT_FALSE(node_d->link_connected_);
}

TEST(ClusterControlPortTest, StorageReadinessFlowsIntoGroups) {
  const StaticClusterControl::SelfMatch self{"127.0.0.1", 7001};
  auto not_ready = StaticClusterControl::Parse(ValidTopology(), self, 0,
                                               /*storage_ready=*/false);
  ASSERT_TRUE(not_ready.ok()) << not_ready.status();
  EXPECT_FALSE((*not_ready)->FullyReady());
  for (const GroupView& group : (*not_ready)->Groups()) {
    EXPECT_FALSE(group.storage_ready_);
    EXPECT_FALSE(group.population_ready_);
    // Grants are permanent in the static adapter; only readiness flips.
    EXPECT_TRUE(group.granted_);
  }
}

TEST(ClusterControlPortTest, ParsesSingleSlotsAndCrlfLines) {
  const std::string content = absl::StrCat(
      kIdA, " 127.0.0.1:7001@17001 master - 0 0 0 connected 5 10-12\r\n",
      "\r\n",  // blank line
      "vars currentEpoch 0 lastVoteEpoch 0\r\n");
  auto state = StaticClusterControl::Parse(content, {"127.0.0.1", 7001}, 0,
                                           /*storage_ready=*/true);
  ASSERT_TRUE(state.ok()) << state.status();
  const ServingState& serving = **state;
  EXPECT_EQ(serving.CoveredSlotCount(), 4U);
  EXPECT_FALSE(serving.CoverageComplete());
  const GroupView* group = serving.GroupForSlot(5);
  ASSERT_NE(group, nullptr);
  EXPECT_EQ(group->group_id_, kIdA);
  EXPECT_EQ(serving.GroupForSlot(11), group);
  EXPECT_EQ(serving.GroupForSlot(12), group);
  EXPECT_EQ(serving.GroupForSlot(6), nullptr);
  const NodeDescriptor* node = serving.FindNode(ParseNodeId(kIdA));
  ASSERT_NE(node, nullptr);
  EXPECT_EQ(node->tls_port_, 0);  // cluster_tls_port passthrough
}

TEST(ClusterControlPortTest, SlotlessPrimaryFormsNoGroup) {
  const std::string content =
      NodeLine(kIdA, "127.0.0.1:7001@17001", "master", "-", "0", "connected",
               "0-10") +
      NodeLine(kIdB, "127.0.0.1:7002@17002", "master", "-", "0", "connected") +
      NodeLine(kIdC, "127.0.0.1:7003@17003", "slave", kIdB, "0", "connected");
  auto state = StaticClusterControl::Parse(content, {"127.0.0.1", 7001}, 0,
                                           /*storage_ready=*/true);
  ASSERT_TRUE(state.ok()) << state.status();
  // The empty primary and its replica are visible as nodes but own nothing.
  EXPECT_EQ((*state)->Groups().size(), 1U);
  EXPECT_EQ((*state)->Nodes().size(), 3U);
}

TEST(ClusterControlPortTest, ToleratesNonRoleFlags) {
  const std::string content =
      NodeLine(kIdA, "127.0.0.1:7001@17001", "master,fail?,noaddr", "-", "7",
               "connected", "0-10");
  auto state = StaticClusterControl::Parse(content, {"127.0.0.1", 7001}, 0,
                                           /*storage_ready=*/true);
  ASSERT_TRUE(state.ok()) << state.status();
  EXPECT_EQ((*state)->topology_epoch(), 7);
  const GroupView* group = (*state)->GroupForSlot(10);
  ASSERT_NE(group, nullptr);
  EXPECT_EQ(group->group_id_, kIdA);
}

TEST(ClusterControlPortTest, RejectsMalformedNodeLines) {
  const StaticClusterControl::SelfMatch self{"127.0.0.1", 7001};
  const std::string valid_address = "127.0.0.1:7001@17001";
  std::vector<std::string> cases = {
      NodeLine("abc", valid_address, "master", "-", "1", "connected", "0-1"),
      NodeLine(std::string(40, 'g'), valid_address, "master", "-", "1",
               "connected", "0-1"),
      // Port out of uint16 range.
      NodeLine(kIdA, "127.0.0.1:70000@17001", "master", "-", "1", "connected",
               "0-1"),
      // Missing / malformed bus port.
      NodeLine(kIdA, "127.0.0.1:7001", "master", "-", "1", "connected", "0-1"),
      NodeLine(kIdA, "127.0.0.1:7001@abc", "master", "-", "1", "connected",
               "0-1"),
      // Empty host, bare IPv6, malformed brackets.
      NodeLine(kIdA, ":7001@17001", "master", "-", "1", "connected", "0-1"),
      NodeLine(kIdA, "::1:7001@17001", "master", "-", "1", "connected", "0-1"),
      NodeLine(kIdA, "[::1]7001@17001", "master", "-", "1", "connected", "0-1"),
      // Truncated line (7 fields).
      absl::StrCat(kIdA, " ", valid_address, " master - 0 0 1\n"),
      NodeLine(kIdA, valid_address, "master", "-", "1", "no-link", "0-1"),
      NodeLine(kIdA, valid_address, "master", "-", "x", "connected", "0-1"),
      // Role flag conflicts / absence.
      NodeLine(kIdA, valid_address, "master,slave", "-", "1", "connected",
               "0-1"),
      NodeLine(kIdA, valid_address, "handshake", "-", "1", "connected", "0-1"),
      // A primary carrying a primary id; a replica with a malformed one.
      NodeLine(kIdA, valid_address, "master", kIdB, "1", "connected", "0-1"),
      NodeLine(kIdA, valid_address, "slave", "zz", "1", "connected"),
      // Slots on a replica line.
      NodeLine(kIdA, valid_address, "slave", kIdB, "1", "connected", "0-1"),
      // Slot token problems.
      NodeLine(kIdA, valid_address, "master", "-", "1", "connected", "16384"),
      NodeLine(kIdA, valid_address, "master", "-", "1", "connected", "0-16384"),
      NodeLine(kIdA, valid_address, "master", "-", "1", "connected", "10-2"),
      NodeLine(kIdA, valid_address, "master", "-", "1", "connected", "abc"),
      // Non-numeric ping/pong.
      absl::StrCat(kIdA, " ", valid_address, " master - x 0 1 connected 0-1\n"),
  };
  for (const std::string& content : cases) {
    auto state = StaticClusterControl::Parse(content, self, 0, true);
    ASSERT_FALSE(state.ok()) << content;
    EXPECT_EQ(state.status().code(), absl::StatusCode::kInvalidArgument)
        << state.status() << " for: " << content;
  }
}

TEST(ClusterControlPortTest, RejectsSlotMigrationMarkers) {
  const StaticClusterControl::SelfMatch self{"127.0.0.1", 7001};
  for (const std::string_view marker :
       {absl::StrCat("[11-<-", kIdB, "]"), absl::StrCat("[11->-", kIdB, "]")}) {
    const std::string content =
        NodeLine(kIdA, "127.0.0.1:7001@17001", "master", "-", "1", "connected",
                 absl::StrCat("0-10 ", marker));
    auto state = StaticClusterControl::Parse(content, self, 0, true);
    ASSERT_FALSE(state.ok()) << marker;
    EXPECT_EQ(state.status().code(), absl::StatusCode::kInvalidArgument)
        << state.status();
    EXPECT_NE(state.status().message().find("migration"),
              std::string_view::npos)
        << state.status();
  }
}

TEST(ClusterControlPortTest, RejectsBrokenReplicaWiring) {
  const StaticClusterControl::SelfMatch self{"127.0.0.1", 7001};
  const std::string master = NodeLine(kIdA, "127.0.0.1:7001@17001", "master",
                                      "-", "0", "connected", "0-10");
  // Replica pointing at a node absent from the file.
  auto unknown = StaticClusterControl::Parse(
      master + NodeLine(kIdB, "127.0.0.1:7002@17002", "slave", kIdC, "0",
                        "connected"),
      self, 0, true);
  ASSERT_FALSE(unknown.ok());
  EXPECT_EQ(unknown.status().code(), absl::StatusCode::kInvalidArgument)
      << unknown.status();
  // Replica pointing at another replica.
  auto non_primary = StaticClusterControl::Parse(
      master +
          NodeLine(kIdB, "127.0.0.1:7002@17002", "slave", kIdA, "0",
                   "connected") +
          NodeLine(kIdC, "127.0.0.1:7003@17003", "slave", kIdB, "0",
                   "connected"),
      self, 0, true);
  ASSERT_FALSE(non_primary.ok());
  EXPECT_EQ(non_primary.status().code(), absl::StatusCode::kInvalidArgument)
      << non_primary.status();
}

TEST(ClusterControlPortTest, RejectsOverlappingSlotsViaBuilder) {
  const StaticClusterControl::SelfMatch self{"127.0.0.1", 7001};
  // Across two primaries.
  const std::string across = NodeLine(kIdA, "127.0.0.1:7001@17001", "master",
                                      "-", "0", "connected", "0-100") +
                             NodeLine(kIdB, "127.0.0.1:7002@17002", "master",
                                      "-", "0", "connected", "50-150");
  EXPECT_FALSE(StaticClusterControl::Parse(across, self, 0, true).ok());
  // Within one node's own token list.
  const std::string within = NodeLine(kIdA, "127.0.0.1:7001@17001", "master",
                                      "-", "0", "connected", "0-100 50-150");
  EXPECT_FALSE(StaticClusterControl::Parse(within, self, 0, true).ok());
}

TEST(ClusterControlPortTest, RejectsDuplicateNodeIdsViaBuilder) {
  const std::string content = NodeLine(kIdA, "127.0.0.1:7001@17001", "master",
                                       "-", "0", "connected", "0-100") +
                              NodeLine(kIdA, "127.0.0.1:7002@17002", "master",
                                       "-", "0", "connected", "101-200");
  EXPECT_FALSE(
      StaticClusterControl::Parse(content, {"127.0.0.1", 7001}, 0, true).ok());
}

TEST(ClusterControlPortTest, MatchesSelfByExactHostAndPort) {
  auto state = StaticClusterControl::Parse(ValidTopology(), {"127.0.0.1", 7002},
                                           0, true);
  ASSERT_TRUE(state.ok()) << state.status();
  ASSERT_NE((*state)->Self(), nullptr);
  EXPECT_EQ((*state)->Self()->node_id_, ParseNodeId(kIdB));
}

TEST(ClusterControlPortTest, WildcardSelfHostMatchesOnPortAlone) {
  for (const char* wildcard : {"0.0.0.0", "::", ""}) {
    auto state =
        StaticClusterControl::Parse(ValidTopology(), {wildcard, 7003}, 0, true);
    ASSERT_TRUE(state.ok()) << wildcard << ": " << state.status();
    // Node C's file host is ::1, so only the port can have matched.
    ASSERT_NE((*state)->Self(), nullptr);
    EXPECT_EQ((*state)->Self()->node_id_, ParseNodeId(kIdC)) << wildcard;
  }
}

TEST(ClusterControlPortTest, RequiresExactlyOneSelfMatch) {
  // An empty file has no entry to match at all.
  EXPECT_FALSE(
      StaticClusterControl::Parse("", {"127.0.0.1", 7001}, 0, true).ok());
  auto zero = StaticClusterControl::Parse(ValidTopology(), {"127.0.0.1", 7999},
                                          0, true);
  ASSERT_FALSE(zero.ok());
  EXPECT_EQ(zero.status().code(), absl::StatusCode::kInvalidArgument)
      << zero.status();
  EXPECT_FALSE(
      StaticClusterControl::Parse(ValidTopology(), {"0.0.0.0", 7999}, 0, true)
          .ok());

  // Two entries share a port: a wildcard bind cannot pick between them...
  const std::string duplicated_port =
      NodeLine(kIdA, "127.0.0.1:7001@17001", "master", "-", "0", "connected",
               "0-100") +
      NodeLine(kIdB, "10.0.0.2:7001@17001", "master", "-", "0", "connected",
               "101-200");
  auto multi =
      StaticClusterControl::Parse(duplicated_port, {"0.0.0.0", 7001}, 0, true);
  ASSERT_FALSE(multi.ok());
  EXPECT_EQ(multi.status().code(), absl::StatusCode::kInvalidArgument)
      << multi.status();
  // ...but an exact host match still selects one.
  auto exact =
      StaticClusterControl::Parse(duplicated_port, {"10.0.0.2", 7001}, 0, true);
  ASSERT_TRUE(exact.ok()) << exact.status();
  ASSERT_NE((*exact)->Self(), nullptr);
  EXPECT_EQ((*exact)->Self()->node_id_, ParseNodeId(kIdB));
}

TEST(ClusterControlPortTest, RefreshPublishesFileContent) {
  TempNodesFile file(ValidTopology());
  StaticClusterControl control(file.path().string(), {"127.0.0.1", 7001},
                               /*cluster_tls_port=*/17011);
  control.SetStorageReady(true);
  TopologyCache cache;
  ASSERT_TRUE(control.RefreshTarget(cache).ok());
  ASSERT_NE(cache.Current(), nullptr);
  EXPECT_TRUE(cache.Current()->CoverageComplete());
  EXPECT_TRUE(cache.Current()->FullyReady());
  const std::uint64_t version = cache.version();
  EXPECT_GT(version, 0U);
  // Re-reading identical content does not republish.
  ASSERT_TRUE(control.RefreshTarget(cache).ok());
  EXPECT_EQ(cache.version(), version);
}

TEST(ClusterControlPortTest, RefreshWithMissingFilePublishesNothing) {
  const std::filesystem::path missing =
      std::filesystem::temp_directory_path() /
      absl::StrCat("keylane-control-port-missing-",
                   std::chrono::steady_clock::now().time_since_epoch().count(),
                   ".conf");
  StaticClusterControl control(missing.string(), {"127.0.0.1", 7001}, 0);
  TopologyCache cache;
  EXPECT_FALSE(control.RefreshTarget(cache).ok());
  EXPECT_EQ(cache.Current(), nullptr);
  EXPECT_EQ(cache.version(), 0U);
}

TEST(ClusterControlPortTest, RefreshFailureKeepsPublishedState) {
  TempNodesFile file(ValidTopology());
  StaticClusterControl control(file.path().string(), {"127.0.0.1", 7001}, 0);
  control.SetStorageReady(true);
  TopologyCache cache;
  ASSERT_TRUE(control.RefreshTarget(cache).ok());
  const std::shared_ptr<const ServingState> published = cache.Current();
  ASSERT_NE(published, nullptr);
  const std::uint64_t version = cache.version();

  // Unparsable content: the previous state stays in effect.
  file.Write("this is not a nodes.conf line\n");
  EXPECT_FALSE(control.RefreshTarget(cache).ok());
  EXPECT_EQ(cache.version(), version);
  EXPECT_EQ(cache.Current().get(), published.get());

  // A vanished file likewise keeps the old state.
  file.Remove();
  EXPECT_FALSE(control.RefreshTarget(cache).ok());
  EXPECT_EQ(cache.version(), version);
  EXPECT_EQ(cache.Current().get(), published.get());
}

TEST(ClusterControlPortTest, StorageReadyTakesEffectOnNextRefresh) {
  TempNodesFile file(ValidTopology());
  StaticClusterControl control(file.path().string(), {"127.0.0.1", 7001}, 0);
  TopologyCache cache;
  ASSERT_TRUE(control.RefreshTarget(cache).ok());
  ASSERT_NE(cache.Current(), nullptr);
  EXPECT_FALSE(cache.Current()->FullyReady());
  const std::uint64_t not_ready_version = cache.version();

  control.SetStorageReady(true);
  ASSERT_TRUE(control.RefreshTarget(cache).ok());
  ASSERT_NE(cache.Current(), nullptr);
  EXPECT_TRUE(cache.Current()->FullyReady());
  // Readiness is part of the published content, so the flip republishes.
  EXPECT_GT(cache.version(), not_ready_version);
}

TEST(ClusterControlPortTest, InMemoryPublishesOnlyPendingTargets) {
  auto state = StaticClusterControl::Parse(ValidTopology(), {"127.0.0.1", 7001},
                                           0, true);
  ASSERT_TRUE(state.ok()) << state.status();

  TopologyCache cache;
  InMemoryClusterControl control;
  EXPECT_TRUE(control.RefreshTarget(cache).ok());  // nothing pending
  EXPECT_EQ(cache.Current(), nullptr);
  EXPECT_EQ(cache.version(), 0U);

  control.SetTarget(*state);
  ASSERT_TRUE(control.RefreshTarget(cache).ok());
  EXPECT_EQ(cache.Current().get(), state->get());
  const std::uint64_t version = cache.version();
  EXPECT_GT(version, 0U);

  // The pending target is consumed by the refresh.
  EXPECT_TRUE(control.RefreshTarget(cache).ok());
  EXPECT_EQ(cache.version(), version);

  // Re-setting identical content republishes nothing.
  control.SetTarget(*state);
  EXPECT_TRUE(control.RefreshTarget(cache).ok());
  EXPECT_EQ(cache.version(), version);

  // A null target publishes nothing.
  control.SetTarget(nullptr);
  EXPECT_TRUE(control.RefreshTarget(cache).ok());
  EXPECT_EQ(cache.version(), version);
  EXPECT_EQ(cache.Current().get(), state->get());
}

}  // namespace
