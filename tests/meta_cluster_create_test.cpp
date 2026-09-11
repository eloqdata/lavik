#include "keylane/meta/cluster_create.h"

#include <string_view>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "keylane/meta/cluster_status.h"

namespace keylane::meta {
namespace {

constexpr std::string_view kValidManifest = R"toml(
schema_version = 1

[[meta_members]]
id = 1

[[data_nodes]]
id = "0123456789abcdef0123456789abcdef01234567"
client_endpoint = "tcp://127.0.0.1:6379"

[[groups]]
id = "group-1"
primary = "0123456789abcdef0123456789abcdef01234567"

[[slot_ranges]]
first = 0
last = 16383
group = "group-1"
)toml";

std::string ReplaceOnce(std::string input, std::string_view from,
                        std::string_view to) {
  const std::size_t position = input.find(from);
  EXPECT_NE(position, std::string::npos);
  if (position != std::string::npos) input.replace(position, from.size(), to);
  return input;
}

TEST(ClusterCreateManifestTest, ParsesTheMinimalVersionedTopology) {
  auto manifest = ParseClusterCreateManifest(kValidManifest);

  ASSERT_TRUE(manifest.ok()) << manifest.status();
  EXPECT_EQ(manifest->schema_version_, 1);
  EXPECT_EQ(manifest->meta_member_id_, 1);
  EXPECT_EQ(manifest->data_node_id_,
            "0123456789abcdef0123456789abcdef01234567");
  EXPECT_EQ(manifest->client_endpoint_, "tcp://127.0.0.1:6379");
  EXPECT_EQ(manifest->group_id_, "group-1");
  EXPECT_EQ(manifest->primary_node_id_, manifest->data_node_id_);
  EXPECT_EQ(manifest->first_slot_, 0);
  EXPECT_EQ(manifest->last_slot_, 16383);
}

TEST(ClusterCreateManifestTest, RejectsFieldsOutsideTheTopologyContract) {
  const std::string manifest =
      std::string(kValidManifest) + "\n[data_nodes.storage]\npath = \"/data\"\n";

  auto parsed = ParseClusterCreateManifest(manifest);

  ASSERT_FALSE(parsed.ok());
  EXPECT_EQ(parsed.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_NE(parsed.status().message().find("unknown"), std::string_view::npos);
}

TEST(ClusterCreateManifestTest, RejectsEvenAnEmptyUnknownTopLevelSection) {
  const std::string manifest = std::string(kValidManifest) + "\n[storage]\n";

  auto parsed = ParseClusterCreateManifest(manifest);

  ASSERT_FALSE(parsed.ok());
  EXPECT_EQ(parsed.status().code(), absl::StatusCode::kInvalidArgument);
}

TEST(ClusterCreateManifestTest, RejectsDuplicateArraysAndCrossReferences) {
  const std::string duplicate =
      std::string(kValidManifest) + "\n[[groups]]\nid = \"other\"\n"
                                    "primary = \"0123456789abcdef0123456789abcdef01234567\"\n";
  EXPECT_FALSE(ParseClusterCreateManifest(duplicate).ok());

  std::string wrong_primary(kValidManifest);
  const std::string_view expected =
      "primary = \"0123456789abcdef0123456789abcdef01234567\"";
  const std::size_t primary = wrong_primary.find(expected);
  ASSERT_NE(primary, std::string::npos);
  wrong_primary.replace(primary, expected.size(),
                        "primary = \"1123456789abcdef0123456789abcdef01234567\"");
  EXPECT_FALSE(ParseClusterCreateManifest(wrong_primary).ok());
}

TEST(ClusterCreateManifestTest, RejectsNoncanonicalIdentityAndEndpointText) {
  std::string uppercase_id(kValidManifest);
  const std::size_t id = uppercase_id.find(
      "0123456789abcdef0123456789abcdef01234567");
  ASSERT_NE(id, std::string::npos);
  uppercase_id[id + 10] = 'A';
  EXPECT_FALSE(ParseClusterCreateManifest(uppercase_id).ok());

  std::string padded_port(kValidManifest);
  const std::size_t endpoint = padded_port.find("127.0.0.1:6379");
  ASSERT_NE(endpoint, std::string::npos);
  padded_port.replace(endpoint, std::string_view("127.0.0.1:6379").size(),
                      "127.0.0.1:06379");
  EXPECT_FALSE(ParseClusterCreateManifest(padded_port).ok());

  EXPECT_EQ(ParseClusterCreateManifest(std::string(64 * 1024 + 1, 'x'))
                .status()
                .code(),
            absl::StatusCode::kResourceExhausted);
}

TEST(ClusterCreateManifestTest, RejectsEveryUnsupportedV1ShapeClass) {
  const std::vector<std::string> invalid = {
      "",
      "schema_version = [",
      ReplaceOnce(std::string(kValidManifest), "schema_version = 1",
                  "schema_version = 2"),
      ReplaceOnce(std::string(kValidManifest), "id = 1", "id = 0"),
      ReplaceOnce(std::string(kValidManifest), "schema_version = 1\n", ""),
      std::string(kValidManifest) + "\nschema_version = 1\n",
      ReplaceOnce(std::string(kValidManifest), "id = \"group-1\"",
                  "id = \"\""),
      ReplaceOnce(std::string(kValidManifest), "id = \"group-1\"",
                  "id = \"" + std::string(65, 'g') + "\""),
      ReplaceOnce(std::string(kValidManifest), "first = 0", "first = 1"),
      ReplaceOnce(std::string(kValidManifest), "last = 16383",
                  "last = 16382"),
      ReplaceOnce(std::string(kValidManifest), "group = \"group-1\"",
                  "group = \"group-2\""),
      ReplaceOnce(std::string(kValidManifest),
                  "client_endpoint = \"tcp://127.0.0.1:6379\"",
                  "client_endpoint = \"tcp://data.example:6379\""),
      ReplaceOnce(std::string(kValidManifest),
                  "client_endpoint = \"tcp://127.0.0.1:6379\"",
                  "client_endpoint = \"tls://127.0.0.1:6379\""),
  };
  for (const std::string& candidate : invalid) {
    EXPECT_FALSE(ParseClusterCreateManifest(candidate).ok()) << candidate;
  }
}

TEST(ClusterCreateProtocolTest, RoundTripsTheBoundedVersionedRequest) {
  const ClusterCreateManifestV1 manifest =
      *ParseClusterCreateManifest(kValidManifest);
  auto request = EncodeClusterCreateRequest(manifest, 120'000);
  ASSERT_TRUE(request.ok()) << request.status();
  std::uint32_t timeout = 0;

  auto decoded = DecodeClusterCreateRequest(*request, &timeout);

  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(*decoded, manifest);
  EXPECT_EQ(timeout, 120'000U);
  EXPECT_FALSE(DecodeClusterCreateRequest(*request + "00", &timeout).ok());
}

TEST(ClusterCreateOperatorTest, CreatesOnlyFromOneEmptyMetaAndWaitsForReady) {
  const ClusterCreateManifestV1 manifest =
      *ParseClusterCreateManifest(kValidManifest);
  ClusterHeadWireV1 head{
      .responder_id_ = 1,
      .role_ = ClusterMetaRole::kLeader,
      .term_ = 4,
      .leader_id_ = 1,
      .config_index_ = 7,
      .meta_members_ = {{.server_id_ = 1, .is_leader_ = true}},
  };
  ClusterStatusWireV1 empty;
  empty.capture_ = {.responder_id_ = 1,
                    .term_ = 4,
                    .config_index_ = 7,
                    .committed_index_ = 8};
  empty.meta_available_ = true;
  empty.meta_membership_stable_ = true;
  empty.meta_members_ = head.meta_members_;
  ClusterStatusWireV1 ready = empty;
  ready.capture_.committed_index_ = 22;
  ready.capture_.topology_epoch_ = 5;
  ready.topology_converged_ = true;
  ready.serving_ready_ = true;
  ready.cluster_ready_ = true;
  ready.data_nodes_.push_back({
      .node_id_ = manifest.data_node_id_,
      .role_ = ClusterDataNodeRole::kPrimary,
      .group_id_ = manifest.group_id_,
      .current_session_ = true,
      .projection_current_ = true,
      .health_fresh_ = true,
      .population_current_ = true,
      .lease_status_ = ClusterLeaseStatus::kRecentlyGranted,
  });
  ready.groups_.push_back({.group_id_ = manifest.group_id_,
                           .term_ = 1,
                           .owner_node_id_ = manifest.data_node_id_,
                           .config_epoch_ = 1,
                           .grant_revision_ = 18,
                           .serving_ready_ = true,
                           .topology_converged_ = true});
  ready.slot_ranges_.push_back(
      {.first_ = 0, .last_ = 16383, .group_id_ = manifest.group_id_});

  const std::string head_reply = *EncodeClusterHeadReply(head);
  const std::string empty_reply = *EncodeClusterStatusReply(empty);
  const std::string ready_reply = *EncodeClusterStatusReply(ready);
  std::vector<std::string> calls;
  std::size_t status_calls = 0;
  ClusterOperator op([&](const MetaAdminTarget&, std::string_view command,
                         auto) -> absl::StatusOr<std::string> {
    calls.emplace_back(command);
    if (command == "clusterhead 1") return head_reply;
    if (command == "clusterstatus 1") {
      return status_calls++ == 0 ? empty_reply : ready_reply;
    }
    if (command.starts_with("clustercreate 1 ")) {
      return "OK clustercreate 1 22 00112233445566778899aabbccddeeff";
    }
    return absl::InvalidArgumentError("unexpected command");
  });
  ClusterStatusOptions options;
  options.deadline_ =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);
  MetaAdminTarget seed{.transport_ = MetaAdminTarget::Transport::kUnix,
                       .endpoint_ = "/tmp/meta.sock"};

  auto outcome = op.Create(seed, manifest, options);

  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->committed_index_, 22);
  EXPECT_EQ(outcome->operation_id_, "00112233445566778899aabbccddeeff");
  ASSERT_EQ(calls.size(), 5U);
  EXPECT_TRUE(calls[2].starts_with("clustercreate 1 "));
}

TEST(ClusterCreateOperatorTest, PreservesUncertainServerFailures) {
  const ClusterCreateManifestV1 manifest =
      *ParseClusterCreateManifest(kValidManifest);
  ClusterHeadWireV1 head{
      .responder_id_ = 1,
      .role_ = ClusterMetaRole::kLeader,
      .term_ = 1,
      .leader_id_ = 1,
      .config_index_ = 1,
      .meta_members_ = {{.server_id_ = 1, .is_leader_ = true}},
  };
  ClusterStatusWireV1 empty;
  empty.capture_ = {.responder_id_ = 1,
                    .term_ = 1,
                    .config_index_ = 1,
                    .committed_index_ = 2};
  empty.meta_available_ = true;
  empty.meta_membership_stable_ = true;
  empty.meta_members_ = head.meta_members_;
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

TEST(ClusterCreateOperatorTest, RejectsAnActiveCreateBeforeMutation) {
  const ClusterCreateManifestV1 manifest =
      *ParseClusterCreateManifest(kValidManifest);
  ClusterHeadWireV1 head{
      .responder_id_ = 1,
      .role_ = ClusterMetaRole::kLeader,
      .term_ = 1,
      .leader_id_ = 1,
      .config_index_ = 1,
      .meta_members_ = {{.server_id_ = 1, .is_leader_ = true}},
  };
  ClusterStatusWireV1 empty;
  empty.capture_ = {.responder_id_ = 1,
                    .term_ = 1,
                    .config_index_ = 1,
                    .committed_index_ = 2};
  empty.meta_available_ = true;
  empty.meta_membership_stable_ = true;
  empty.blockers_.push_back(
      {.code_ = std::string(kClusterCreateActiveBlockerCode),
       .scope_ = "cluster",
       .detail_ = "non_terminal_cluster_create_operation_exists"});
  empty.meta_members_ = head.meta_members_;
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

TEST(ClusterCreateOperatorTest, ClassifiesRuntimeRejectionAsDomainFailure) {
  const ClusterCreateManifestV1 manifest =
      *ParseClusterCreateManifest(kValidManifest);
  ClusterHeadWireV1 head{
      .responder_id_ = 1,
      .role_ = ClusterMetaRole::kLeader,
      .term_ = 1,
      .leader_id_ = 1,
      .config_index_ = 1,
      .meta_members_ = {{.server_id_ = 1, .is_leader_ = true}},
  };
  ClusterStatusWireV1 empty;
  empty.capture_ = {.responder_id_ = 1,
                    .term_ = 1,
                    .config_index_ = 1,
                    .committed_index_ = 2};
  empty.meta_available_ = true;
  empty.meta_membership_stable_ = true;
  empty.meta_members_ = head.meta_members_;
  ClusterOperator op([&](const MetaAdminTarget&, std::string_view command,
                         auto) -> absl::StatusOr<std::string> {
    if (command == "clusterhead 1") return *EncodeClusterHeadReply(head);
    if (command == "clusterstatus 1")
      return *EncodeClusterStatusReply(empty);
    return "ERR clustercreate 1 wait-data-projection runtime-invalid "
           "data session changed";
  });
  ClusterStatusOptions options;
  options.deadline_ =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);

  auto outcome = op.Create(
      {.transport_ = MetaAdminTarget::Transport::kUnix,
       .endpoint_ = "/tmp/meta.sock"},
      manifest, options);

  EXPECT_EQ(outcome.status().code(), absl::StatusCode::kFailedPrecondition);
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
