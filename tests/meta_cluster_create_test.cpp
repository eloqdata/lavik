#include "keylane/meta/cluster_create.h"

#include <chrono>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "gtest/gtest.h"
#include "keylane/meta/cluster_status.h"

namespace keylane::meta {
namespace {

constexpr std::string_view kNodeA =
    "0123456789abcdef0123456789abcdef01234567";
constexpr std::string_view kNodeB =
    "1123456789abcdef0123456789abcdef01234567";
constexpr std::string_view kNodeC =
    "2123456789abcdef0123456789abcdef01234567";
constexpr std::string_view kNodeD =
    "3123456789abcdef0123456789abcdef01234567";
constexpr std::string_view kValidManifest = R"toml(
schema_version = 1
slot_strategy = "contiguous-even"

[[meta_members]]
id = 1

[[data_nodes]]
id = "0123456789abcdef0123456789abcdef01234567"
client_endpoint = "tcp://127.0.0.1:6379"

[[data_nodes]]
id = "1123456789abcdef0123456789abcdef01234567"
client_endpoint = "tcp://127.0.0.1:6380"

[[groups]]
id = "group-1"
primary = "0123456789abcdef0123456789abcdef01234567"
replicas = []

[[groups]]
id = "group-2"
primary = "1123456789abcdef0123456789abcdef01234567"
replicas = []
)toml";

std::string ReplaceOnce(std::string input, std::string_view from,
                        std::string_view to) {
  const std::size_t position = input.find(from);
  EXPECT_NE(position, std::string::npos);
  if (position != std::string::npos) input.replace(position, from.size(), to);
  return input;
}

std::string AutoManifest(std::size_t count) {
  std::string result =
      "schema_version = 1\nslot_strategy = \"contiguous-even\"\n"
      "[[meta_members]]\nid = 1\n";
  for (std::size_t index = 0; index < count; ++index) {
    const char digit = "0123456789abcdef"[index];
    result += "[[data_nodes]]\nid = \"" + std::string(40, digit) +
              "\"\nclient_endpoint = \"tcp://127.0.0.1:" +
              std::to_string(6400 + index) + "\"\n";
  }
  for (std::size_t index = 0; index < count; ++index) {
    const char digit = "0123456789abcdef"[index];
    result += "[[groups]]\nid = \"group-" + std::to_string(index) +
              "\"\nprimary = \"" + std::string(40, digit) + "\"\n";
  }
  return result;
}

TEST(ClusterCreateManifestTest, NormalizesMultipleGroupsAndAllocatesSlots) {
  auto manifest = ParseClusterCreateManifest(kValidManifest);

  ASSERT_TRUE(manifest.ok()) << manifest.status();
  EXPECT_EQ(manifest->schema_version_, 1);
  EXPECT_EQ(manifest->meta_member_id_, 1);
  ASSERT_EQ(manifest->data_nodes_.size(), 2U);
  EXPECT_EQ(manifest->data_nodes_[0].node_id_, kNodeA);
  ASSERT_EQ(manifest->groups_.size(), 2U);
  EXPECT_EQ(manifest->groups_[0].group_id_, "group-1");
  EXPECT_EQ(manifest->groups_[1].group_id_, "group-2");
  ASSERT_EQ(manifest->slot_ranges_.size(), 2U);
  EXPECT_EQ(manifest->slot_ranges_[0],
            (ClusterCreateManifestV1::SlotRange{0, 8191, "group-1"}));
  EXPECT_EQ(manifest->slot_ranges_[1],
            (ClusterCreateManifestV1::SlotRange{8192, 16383, "group-2"}));
}

TEST(ClusterCreateManifestTest, AllocatesNonDivisorGroupCountsExactly) {
  const std::vector<std::vector<std::pair<std::uint16_t, std::uint16_t>>>
      expected = {
          {{0, 16383}},
          {{0, 8191}, {8192, 16383}},
          {{0, 5460}, {5461, 10921}, {10922, 16383}},
          {{0, 3275},
           {3276, 6552},
           {6553, 9829},
           {9830, 13106},
           {13107, 16383}},
      };
  const std::vector<std::size_t> counts = {1, 2, 3, 5};
  for (std::size_t case_index = 0; case_index < counts.size(); ++case_index) {
    auto manifest = ParseClusterCreateManifest(AutoManifest(counts[case_index]));
    ASSERT_TRUE(manifest.ok()) << manifest.status();
    ASSERT_EQ(manifest->slot_ranges_.size(), expected[case_index].size());
    for (std::size_t index = 0; index < expected[case_index].size(); ++index) {
      EXPECT_EQ(manifest->slot_ranges_[index].first_,
                expected[case_index][index].first);
      EXPECT_EQ(manifest->slot_ranges_[index].last_,
                expected[case_index][index].second);
    }
  }
}

TEST(ClusterCreateManifestTest, ExplicitRangesAreCanonicalAndFullyCovered) {
  constexpr std::string_view text = R"toml(
schema_version = 1
[[meta_members]]
id = 1
[[data_nodes]]
id = "1123456789abcdef0123456789abcdef01234567"
client_endpoint = "tcp://127.0.0.1:6380"
[[data_nodes]]
id = "0123456789abcdef0123456789abcdef01234567"
client_endpoint = "tcp://127.0.0.1:6379"
[[groups]]
id = "group-2"
primary = "1123456789abcdef0123456789abcdef01234567"
[[groups]]
id = "group-1"
primary = "0123456789abcdef0123456789abcdef01234567"
[[slot_ranges]]
first = 8192
last = 16383
group = "group-2"
[[slot_ranges]]
first = 0
last = 4095
group = "group-1"
[[slot_ranges]]
first = 4096
last = 8191
group = "group-1"
)toml";

  auto manifest = ParseClusterCreateManifest(text);

  ASSERT_TRUE(manifest.ok()) << manifest.status();
  ASSERT_EQ(manifest->slot_ranges_.size(), 2U);
  EXPECT_EQ(manifest->slot_ranges_[0],
            (ClusterCreateManifestV1::SlotRange{0, 8191, "group-1"}));
  EXPECT_EQ(manifest->slot_ranges_[1],
            (ClusterCreateManifestV1::SlotRange{8192, 16383, "group-2"}));
}

TEST(ClusterCreateManifestTest, InputOrderCannotChangeNormalizedWire) {
  const auto manifest = [](bool shuffled) {
    std::string text =
        "schema_version = 1\nslot_strategy = \"contiguous-even\"\n"
        "[[meta_members]]\nid = 1\n";
    const std::vector<std::pair<std::string_view, std::uint16_t>> nodes =
        shuffled
            ? std::vector<std::pair<std::string_view, std::uint16_t>>{
                  {kNodeD, 6382}, {kNodeC, 6381}, {kNodeB, 6380},
                  {kNodeA, 6379}}
            : std::vector<std::pair<std::string_view, std::uint16_t>>{
                  {kNodeA, 6379}, {kNodeB, 6380}, {kNodeC, 6381},
                  {kNodeD, 6382}};
    for (const auto& [node_id, port] : nodes) {
      text += "[[data_nodes]]\nid = \"" + std::string(node_id) +
              "\"\nclient_endpoint = \"tcp://127.0.0.1:" +
              std::to_string(port) + "\"\n";
    }
    const std::string group_1 =
        "[[groups]]\nid = \"group-1\"\nprimary = \"" +
        std::string(kNodeA) + "\"\nreplicas = [\"" + std::string(kNodeB) +
        "\", \"" + std::string(kNodeC) + "\"]\n";
    const std::string group_2 =
        "[[groups]]\nid = \"group-2\"\nprimary = \"" +
        std::string(kNodeD) + "\"\nreplicas = []\n";
    text += shuffled ? group_2 + ReplaceOnce(
                                   group_1,
                                   std::string(kNodeB) + "\", \"" +
                                       std::string(kNodeC),
                                   std::string(kNodeC) + "\", \"" +
                                       std::string(kNodeB))
                     : group_1 + group_2;
    return ParseClusterCreateManifest(text);
  };

  auto canonical = manifest(false);
  auto shuffled = manifest(true);

  ASSERT_TRUE(canonical.ok()) << canonical.status();
  ASSERT_TRUE(shuffled.ok()) << shuffled.status();
  EXPECT_EQ(*shuffled, *canonical);
  ASSERT_EQ(shuffled->groups_.front().replica_node_ids_.size(), 2U);
  EXPECT_EQ(shuffled->groups_.front().replica_node_ids_[0], kNodeB);
  EXPECT_EQ(shuffled->groups_.front().replica_node_ids_[1], kNodeC);
  EXPECT_EQ(EncodeClusterCreateRequest(*shuffled, 10'000),
            EncodeClusterCreateRequest(*canonical, 10'000));
}

TEST(ClusterCreateManifestTest,
     RejectsUnsupportedVersionsAndInvalidTopologyShapes) {
  const std::vector<std::string> invalid = {
      ReplaceOnce(std::string(kValidManifest), "schema_version = 1",
                  "schema_version = 2"),
      ReplaceOnce(std::string(kValidManifest),
                  "client_endpoint = \"tcp://127.0.0.1:6380\"",
                  "client_endpoint = \"tcp://127.0.0.1:6379\""),
      ReplaceOnce(std::string(kValidManifest),
                  "primary = \"1123456789abcdef0123456789abcdef01234567\"",
                  "primary = \"0123456789abcdef0123456789abcdef01234567\""),
      ReplaceOnce(std::string(kValidManifest),
                  "primary = \"1123456789abcdef0123456789abcdef01234567\"",
                  "primary = \"2123456789abcdef0123456789abcdef01234567\""),
      ReplaceOnce(std::string(kValidManifest), "id = \"group-2\"",
                  "id = \"group-1\""),
      ReplaceOnce(std::string(kValidManifest),
                  "id = \"1123456789abcdef0123456789abcdef01234567\"",
                  "id = \"0123456789abcdef0123456789abcdef01234567\""),
      ReplaceOnce(std::string(kValidManifest),
                  "client_endpoint = \"tcp://127.0.0.1:6380\"",
                  "client_endpoint = \"tcp://localhost:6380\""),
      std::string(kValidManifest) +
          "\n[[slot_ranges]]\nfirst = 0\nlast = 16383\n"
          "group = \"group-1\"\n",
      std::string(kValidManifest) + "\n[storage]\npath = \"/data\"\n",
  };
  for (const std::string& candidate : invalid) {
    EXPECT_FALSE(ParseClusterCreateManifest(candidate).ok()) << candidate;
  }
  EXPECT_EQ(ParseClusterCreateManifest(std::string(64 * 1024 + 1, 'x'))
                .status()
                .code(),
            absl::StatusCode::kResourceExhausted);
}

TEST(ClusterCreateManifestTest, RejectsInvalidReplicaMembership) {
  const std::string base =
      "schema_version = 1\nslot_strategy = \"contiguous-even\"\n"
      "[[meta_members]]\nid = 1\n"
      "[[data_nodes]]\nid = \"" + std::string(kNodeA) +
      "\"\nclient_endpoint = \"tcp://127.0.0.1:6379\"\n"
      "[[data_nodes]]\nid = \"" + std::string(kNodeB) +
      "\"\nclient_endpoint = \"tcp://127.0.0.1:6380\"\n"
      "[[groups]]\nid = \"group-1\"\nprimary = \"" +
      std::string(kNodeA) + "\"\nreplicas = [\"" + std::string(kNodeB) +
      "\"]\n";
  const std::vector<std::string> invalid = {
      ReplaceOnce(base, "replicas = [\"" + std::string(kNodeB) + "\"]",
                  "replicas = [\"" + std::string(kNodeB) + "\", \"" +
                      std::string(kNodeB) + "\"]"),
      ReplaceOnce(base, "replicas = [\"" + std::string(kNodeB) + "\"]",
                  "replicas = [\"" + std::string(kNodeA) + "\"]"),
      ReplaceOnce(base, "replicas = [\"" + std::string(kNodeB) + "\"]",
                  "replicas = []"),
      ReplaceOnce(base, std::string(kNodeB) + "\"]",
                  std::string(kNodeC) + "\"]"),
  };
  for (const std::string& candidate : invalid) {
    EXPECT_FALSE(ParseClusterCreateManifest(candidate).ok()) << candidate;
  }
}

TEST(ClusterCreateManifestTest, RejectsGapsOverlapsAndGroupsWithoutSlots) {
  std::string explicit_manifest =
      ReplaceOnce(std::string(kValidManifest),
                  "slot_strategy = \"contiguous-even\"\n", "");
  explicit_manifest +=
      "[[slot_ranges]]\nfirst = 0\nlast = 8191\ngroup = \"group-1\"\n"
      "[[slot_ranges]]\nfirst = 8192\nlast = 16383\ngroup = \"group-2\"\n";
  EXPECT_TRUE(ParseClusterCreateManifest(explicit_manifest).ok());
  EXPECT_FALSE(ParseClusterCreateManifest(ReplaceOnce(
                   explicit_manifest, "first = 8192", "first = 8193"))
                   .ok());
  EXPECT_FALSE(ParseClusterCreateManifest(ReplaceOnce(
                   explicit_manifest, "first = 8192", "first = 8191"))
                   .ok());
  EXPECT_FALSE(ParseClusterCreateManifest(ReplaceOnce(
                   explicit_manifest, "group = \"group-2\"",
                   "group = \"group-1\""))
                   .ok());
  EXPECT_FALSE(ParseClusterCreateManifest(ReplaceOnce(
                   explicit_manifest, "group = \"group-2\"",
                   "group = \"unknown\""))
                   .ok());
  EXPECT_FALSE(ParseClusterCreateManifest(ReplaceOnce(
                   explicit_manifest, "last = 16383", "last = 16384"))
                   .ok());
  EXPECT_FALSE(ParseClusterCreateManifest(ReplaceOnce(
                   explicit_manifest, "last = 8191", "last = 8190"))
                   .ok());
  EXPECT_FALSE(ParseClusterCreateManifest(ReplaceOnce(
                   explicit_manifest, "first = 8192", "first = 16383"))
                   .ok());
  EXPECT_FALSE(ParseClusterCreateManifest(ReplaceOnce(
                   explicit_manifest, "schema_version = 1",
                   "schema_version = 1\nslot_strategy = \"contiguous-even\""))
                   .ok());
  EXPECT_FALSE(ParseClusterCreateManifest(ReplaceOnce(
                   std::string(kValidManifest),
                   "slot_strategy = \"contiguous-even\"\n", ""))
                   .ok());
}

TEST(ClusterCreateProtocolTest, RoundTripsOnlyCanonicalV1Requests) {
  const ClusterCreateManifestV1 manifest =
      *ParseClusterCreateManifest(kValidManifest);
  auto request = EncodeClusterCreateRequest(manifest, 120'000);
  ASSERT_TRUE(request.ok()) << request.status();
  EXPECT_TRUE(request->starts_with("clustercreate 1 "));
  std::uint32_t timeout = 0;

  auto decoded = DecodeClusterCreateRequest(*request, &timeout);

  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(*decoded, manifest);
  EXPECT_EQ(timeout, 120'000U);
  EXPECT_FALSE(DecodeClusterCreateRequest(*request + "00", &timeout).ok());
  EXPECT_FALSE(DecodeClusterCreateRequest("clustercreate 2 00", &timeout).ok());
}

TEST(ClusterCreateProtocolTest, DecodesPerGroupOutcome) {
  auto outcome = DecodeClusterCreateReply(
      "OK clustercreate 1 25 2 "
      "67726f75702d31 23 00112233445566778899aabbccddeeff "
      "67726f75702d32 24 10112233445566778899aabbccddeeff");

  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->committed_index_, 25U);
  ASSERT_EQ(outcome->groups_.size(), 2U);
  EXPECT_EQ(outcome->groups_[0].group_id_, "group-1");
  EXPECT_EQ(outcome->groups_[1].operation_id_,
            "10112233445566778899aabbccddeeff");
}

ClusterStatusWireV1 EmptyStatus(const ClusterHeadWireV1& head) {
  ClusterStatusWireV1 status;
  status.capture_ = {.responder_id_ = 1,
                     .term_ = head.term_,
                     .config_index_ = head.config_index_,
                     .committed_index_ = 8};
  status.meta_available_ = true;
  status.meta_membership_stable_ = true;
  status.meta_members_ = head.meta_members_;
  return status;
}

ClusterStatusWireV1 ReadyStatus(const ClusterCreateManifestV1& manifest,
                                const ClusterHeadWireV1& head) {
  ClusterStatusWireV1 status = EmptyStatus(head);
  status.capture_.committed_index_ = 24;
  status.capture_.topology_epoch_ = 10;
  status.topology_converged_ = true;
  status.serving_ready_ = true;
  status.cluster_ready_ = true;
  for (const auto& group : manifest.groups_) {
    status.data_nodes_.push_back({
        .node_id_ = group.primary_node_id_,
        .role_ = ClusterDataNodeRole::kPrimary,
        .group_id_ = group.group_id_,
        .current_session_ = true,
        .projection_current_ = true,
        .health_fresh_ = true,
        .population_current_ = true,
        .lease_status_ = ClusterLeaseStatus::kRecentlyGranted,
    });
    for (const std::string& replica : group.replica_node_ids_) {
      status.data_nodes_.push_back({
          .node_id_ = replica,
          .role_ = ClusterDataNodeRole::kReplica,
          .group_id_ = group.group_id_,
          .current_session_ = true,
          .projection_current_ = true,
          .health_fresh_ = true,
          .population_current_ = true,
      });
    }
    status.groups_.push_back({.group_id_ = group.group_id_,
                              .term_ = 1,
                              .owner_node_id_ = group.primary_node_id_,
                              .config_epoch_ = 1,
                              .grant_revision_ = 18,
                              .serving_ready_ = true,
                              .topology_converged_ = true});
  }
  for (const auto& range : manifest.slot_ranges_) {
    status.slot_ranges_.push_back(
        {range.first_, range.last_, range.group_id_});
  }
  return status;
}

TEST(ClusterCreateOperatorTest, WaitsForTheExactMultiGroupTopology) {
  const ClusterCreateManifestV1 manifest =
      *ParseClusterCreateManifest(kValidManifest);
  const ClusterHeadWireV1 head{
      .responder_id_ = 1,
      .role_ = ClusterMetaRole::kLeader,
      .term_ = 4,
      .leader_id_ = 1,
      .config_index_ = 7,
      .meta_members_ = {{.server_id_ = 1, .is_leader_ = true}},
  };
  const std::string head_reply = *EncodeClusterHeadReply(head);
  const std::string empty_reply = *EncodeClusterStatusReply(EmptyStatus(head));
  const std::string ready_reply =
      *EncodeClusterStatusReply(ReadyStatus(manifest, head));
  std::vector<std::string> calls;
  std::size_t status_calls = 0;
  ClusterOperator op([&](const MetaAdminTarget&, std::string_view command,
                         auto) -> absl::StatusOr<std::string> {
    calls.emplace_back(command);
    if (command == "clusterhead 1") return head_reply;
    if (command == "clusterstatus 1")
      return status_calls++ == 0 ? empty_reply : ready_reply;
    if (command.starts_with("clustercreate 1 ")) {
      return "OK clustercreate 1 24 2 "
             "67726f75702d31 23 00112233445566778899aabbccddeeff "
             "67726f75702d32 24 10112233445566778899aabbccddeeff";
    }
    return absl::InvalidArgumentError("unexpected command");
  });
  ClusterStatusOptions options;
  options.deadline_ =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);

  auto outcome = op.Create(
      {.transport_ = MetaAdminTarget::Transport::kUnix,
       .endpoint_ = "/tmp/meta.sock"},
      manifest, options);

  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->committed_index_, 24U);
  ASSERT_EQ(outcome->groups_.size(), 2U);
  ASSERT_EQ(calls.size(), 5U);
  EXPECT_TRUE(calls[2].starts_with("clustercreate 1 "));
}

TEST(ClusterCreateOperatorTest, PreservesUncertainServerFailures) {
  const ClusterCreateManifestV1 manifest =
      *ParseClusterCreateManifest(kValidManifest);
  const ClusterHeadWireV1 head{
      .responder_id_ = 1,
      .role_ = ClusterMetaRole::kLeader,
      .term_ = 1,
      .leader_id_ = 1,
      .config_index_ = 1,
      .meta_members_ = {{.server_id_ = 1, .is_leader_ = true}},
  };
  const ClusterStatusWireV1 empty = EmptyStatus(head);
  ClusterOperator op([&](const MetaAdminTarget&, std::string_view command,
                         auto) -> absl::StatusOr<std::string> {
    if (command == "clusterhead 1") return *EncodeClusterHeadReply(head);
    if (command == "clusterstatus 1")
      return *EncodeClusterStatusReply(empty);
    return "ERR clustercreate 1 initialize-data uncertain-outcome timed out";
  });
  ClusterStatusOptions options;
  options.deadline_ =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);

  auto outcome = op.Create(
      {.transport_ = MetaAdminTarget::Transport::kUnix,
       .endpoint_ = "/tmp/meta.sock"},
      manifest, options);

  EXPECT_EQ(outcome.status().code(), absl::StatusCode::kAborted);
}

TEST(ClusterCreateOperatorTest, RejectsActiveCreateBeforeMutation) {
  const ClusterCreateManifestV1 manifest =
      *ParseClusterCreateManifest(kValidManifest);
  const ClusterHeadWireV1 head{
      .responder_id_ = 1,
      .role_ = ClusterMetaRole::kLeader,
      .term_ = 1,
      .leader_id_ = 1,
      .config_index_ = 1,
      .meta_members_ = {{.server_id_ = 1, .is_leader_ = true}},
  };
  ClusterStatusWireV1 empty = EmptyStatus(head);
  empty.blockers_.push_back(
      {.code_ = std::string(kClusterCreateActiveBlockerCode),
       .scope_ = "cluster",
       .detail_ = "non_terminal_cluster_create_operation_exists"});
  bool mutation_sent = false;
  ClusterOperator op([&](const MetaAdminTarget&, std::string_view command,
                         auto) -> absl::StatusOr<std::string> {
    if (command == "clusterhead 1") return *EncodeClusterHeadReply(head);
    if (command == "clusterstatus 1")
      return *EncodeClusterStatusReply(empty);
    mutation_sent = true;
    return absl::InternalError("unexpected mutation");
  });
  ClusterStatusOptions options;
  options.deadline_ =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);

  auto outcome = op.Create(
      {.transport_ = MetaAdminTarget::Transport::kUnix,
       .endpoint_ = "/tmp/meta.sock"},
      manifest, options);

  EXPECT_EQ(outcome.status().code(), absl::StatusCode::kFailedPrecondition);
  EXPECT_FALSE(mutation_sent);
}

TEST(ClusterCreateOperatorTest, NamesTheNodeThatPreventsExactReadiness) {
  const ClusterCreateManifestV1 manifest =
      *ParseClusterCreateManifest(kValidManifest);
  const ClusterHeadWireV1 head{
      .responder_id_ = 1,
      .role_ = ClusterMetaRole::kLeader,
      .term_ = 1,
      .leader_id_ = 1,
      .config_index_ = 1,
      .meta_members_ = {{.server_id_ = 1, .is_leader_ = true}},
  };
  const ClusterStatusWireV1 empty = EmptyStatus(head);
  ClusterStatusWireV1 incomplete = ReadyStatus(manifest, head);
  incomplete.data_nodes_[1].population_current_ = false;
  incomplete.data_nodes_[1].lease_status_ = ClusterLeaseStatus::kUnknown;
  incomplete.groups_[1].serving_ready_ = false;
  incomplete.groups_[1].topology_converged_ = false;
  incomplete.cluster_ready_ = false;
  incomplete.serving_ready_ = false;
  incomplete.topology_converged_ = false;
  incomplete.blockers_.push_back(
      {.code_ = "node_runtime_not_ready",
       .scope_ = "node:" + std::string(kNodeB),
       .detail_ = "group=group-2;missing=population"});
  std::size_t status_calls = 0;
  ClusterOperator op([&](const MetaAdminTarget&, std::string_view command,
                         auto) -> absl::StatusOr<std::string> {
    if (command == "clusterhead 1") return *EncodeClusterHeadReply(head);
    if (command == "clusterstatus 1") {
      return *EncodeClusterStatusReply(status_calls++ == 0 ? empty
                                                            : incomplete);
    }
    return "OK clustercreate 1 24 2 "
           "67726f75702d31 23 00112233445566778899aabbccddeeff "
           "67726f75702d32 24 10112233445566778899aabbccddeeff";
  });
  ClusterStatusOptions options;
  options.deadline_ =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(75);

  auto outcome = op.Create(
      {.transport_ = MetaAdminTarget::Transport::kUnix,
       .endpoint_ = "/tmp/meta.sock"},
      manifest, options);

  EXPECT_EQ(outcome.status().code(), absl::StatusCode::kDeadlineExceeded);
  EXPECT_NE(outcome.status().message().find("node:" + std::string(kNodeB)),
            std::string_view::npos);
  EXPECT_NE(outcome.status().message().find("group=group-2"),
            std::string_view::npos);
}

TEST(ClusterCreateOperatorTest, DistinguishesFailureBeforeMutation) {
  const ClusterCreateManifestV1 manifest =
      *ParseClusterCreateManifest(kValidManifest);
  ClusterOperator op([](const MetaAdminTarget&, std::string_view,
                        auto) -> absl::StatusOr<std::string> {
    return absl::UnavailableError("seed is offline");
  });
  ClusterStatusOptions options;
  options.deadline_ =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);

  auto outcome = op.Create(
      {.transport_ = MetaAdminTarget::Transport::kUnix,
       .endpoint_ = "/tmp/meta.sock"},
      manifest, options);

  EXPECT_EQ(outcome.status().code(), absl::StatusCode::kCancelled);
  EXPECT_NE(outcome.status().message().find("before sending a mutation"),
            std::string_view::npos);
}

}  // namespace
}  // namespace keylane::meta
