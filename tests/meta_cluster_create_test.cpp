/*
 * Copyright (C) 2026 EloqData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <chrono>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "gtest/gtest.h"
#include "lavik/meta/cluster_create.h"
#include "lavik/meta/cluster_status.h"

namespace lavik::meta {
namespace {

constexpr std::string_view kNodeA = "0123456789abcdef0123456789abcdef01234567";
constexpr std::string_view kNodeB = "1123456789abcdef0123456789abcdef01234567";
constexpr std::string_view kNodeC = "2123456789abcdef0123456789abcdef01234567";
constexpr std::string_view kNodeD = "3123456789abcdef0123456789abcdef01234567";
constexpr std::string_view kValidManifest = R"toml(
schema_version = 1
slot_strategy = "contiguous-even"

[[meta_members]]
id = 1
raft_endpoint = "tcp://127.0.0.1:7101"
data_control_endpoint = "tcp://127.0.0.1:7301"
ctl_endpoint = "tcp://127.0.0.1:7201"

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

MetaOperationId OperationId(std::uint8_t seed) {
  MetaOperationId id{};
  id.fill(seed);
  return id;
}

std::string OperationIdHex(const MetaOperationId& id) {
  static constexpr char kHex[] = "0123456789abcdef";
  std::string result;
  for (const std::uint8_t byte : id) {
    result.push_back(kHex[byte >> 4]);
    result.push_back(kHex[byte & 0x0f]);
  }
  return result;
}

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
      "[[meta_members]]\nid = 1\n"
      "raft_endpoint = \"tcp://127.0.0.1:7101\"\n"
      "data_control_endpoint = \"tcp://127.0.0.1:7301\"\n"
      "ctl_endpoint = \"tcp://127.0.0.1:7201\"\n";
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
  ASSERT_EQ(manifest->meta_members_.size(), 1U);
  EXPECT_EQ(manifest->meta_members_.front(),
            (ClusterCreateManifestV1::MetaMember{1, "tcp://127.0.0.1:7101",
                                                 "tcp://127.0.0.1:7301",
                                                 "tcp://127.0.0.1:7201"}));
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
  EXPECT_EQ(manifest->automatic_uncontrolled_failover_suspect_after_ms_, 5000u);
  EXPECT_EQ(manifest->authority_lease_duration_ms_, 5000u);
}

TEST(ClusterCreateManifestTest,
     RecoveryBudgetOverrideSurvivesRequestRoundTrip) {
  const std::string input =
      std::string(kValidManifest) +
      "\n[bootstrap_policy]\ncandidate_recovery_budget_ms = 0\n";
  const auto manifest = ParseClusterCreateManifest(input);
  ASSERT_TRUE(manifest.ok()) << manifest.status();
  const auto encoded = EncodeClusterCreateRequest(*manifest, OperationId(1));
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  MetaOperationId root;
  const auto decoded = DecodeClusterCreateRequest(*encoded, &root);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(*decoded, *manifest);
  const auto defaults = ParseClusterCreateManifest(kValidManifest);
  ASSERT_TRUE(defaults.ok());
  EXPECT_NE(*defaults, *decoded);
}

TEST(ClusterCreateManifestTest, ParsesStrictBootstrapPolicyOverrides) {
  const std::string configured = std::string(kValidManifest) +
                                 R"toml(
[bootstrap_policy]
automatic_uncontrolled_failover_suspect_after_ms = 9000
authority_lease_duration_ms = 3000
)toml";

  auto manifest = ParseClusterCreateManifest(configured);

  ASSERT_TRUE(manifest.ok()) << manifest.status();
  EXPECT_EQ(manifest->automatic_uncontrolled_failover_suspect_after_ms_, 9000u);
  EXPECT_EQ(manifest->authority_lease_duration_ms_, 3000u);

  for (const std::string& invalid : {
           configured + "unknown = 1\n",
           configured + "automatic_uncontrolled_failover_enabled = false\n",
           configured + "automatic_uncontrolled_failover_enabled = true\n",
           ReplaceOnce(configured, "9000", "999"),
           ReplaceOnce(configured, "3000", "99"),
           configured +
               "\n[bootstrap_policy]\nauthority_lease_duration_ms = 4000\n",
       }) {
    SCOPED_TRACE(invalid);
    EXPECT_FALSE(ParseClusterCreateManifest(invalid).ok());
  }
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
    auto manifest =
        ParseClusterCreateManifest(AutoManifest(counts[case_index]));
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
raft_endpoint = "tcp://127.0.0.1:7101"
data_control_endpoint = "tcp://127.0.0.1:7301"
ctl_endpoint = "tcp://127.0.0.1:7201"
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
        "[[meta_members]]\nid = 1\n"
        "raft_endpoint = \"tcp://127.0.0.1:7101\"\n"
        "data_control_endpoint = \"tcp://127.0.0.1:7301\"\n"
        "ctl_endpoint = \"tcp://127.0.0.1:7201\"\n";
    const std::vector<std::pair<std::string_view, std::uint16_t>> nodes =
        shuffled
            ? std::vector<std::pair<std::string_view, std::uint16_t>>{{kNodeD,
                                                                       6382},
                                                                      {kNodeC,
                                                                       6381},
                                                                      {kNodeB,
                                                                       6380},
                                                                      {kNodeA,
                                                                       6379}}
            : std::vector<std::pair<std::string_view, std::uint16_t>>{
                  {kNodeA, 6379},
                  {kNodeB, 6380},
                  {kNodeC, 6381},
                  {kNodeD, 6382}};
    for (const auto& [node_id, port] : nodes) {
      text +=
          "[[data_nodes]]\nid = \"" + std::string(node_id) +
          "\"\nclient_endpoint = \"tcp://127.0.0.1:" + std::to_string(port) +
          "\"\n";
    }
    const std::string group_1 = "[[groups]]\nid = \"group-1\"\nprimary = \"" +
                                std::string(kNodeA) + "\"\nreplicas = [\"" +
                                std::string(kNodeB) + "\", \"" +
                                std::string(kNodeC) + "\"]\n";
    const std::string group_2 = "[[groups]]\nid = \"group-2\"\nprimary = \"" +
                                std::string(kNodeD) + "\"\nreplicas = []\n";
    text += shuffled
                ? group_2 +
                      ReplaceOnce(
                          group_1,
                          std::string(kNodeB) + "\", \"" + std::string(kNodeC),
                          std::string(kNodeC) + "\", \"" + std::string(kNodeB))
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
  EXPECT_EQ(EncodeClusterCreateRequest(*shuffled, OperationId(1)),
            EncodeClusterCreateRequest(*canonical, OperationId(1)));
}

TEST(ClusterCreateManifestTest, NormalizesOneThreeAndFiveInitialMetaMembers) {
  for (const std::size_t count : {1U, 3U, 5U}) {
    std::string text = AutoManifest(1);
    text = ReplaceOnce(std::move(text),
                       "[[meta_members]]\nid = 1\n"
                       "raft_endpoint = \"tcp://127.0.0.1:7101\"\n"
                       "data_control_endpoint = \"tcp://127.0.0.1:7301\"\n"
                       "ctl_endpoint = \"tcp://127.0.0.1:7201\"\n",
                       "");
    for (std::size_t offset = 0; offset < count; ++offset) {
      const std::size_t id = count - offset;
      text +=
          "[[meta_members]]\nid = " + std::to_string(id) +
          "\nraft_endpoint = \"tcp://127.0.0.1:" + std::to_string(7100 + id) +
          "\"\ndata_control_endpoint = \"tcp://127.0.0.1:" +
          std::to_string(7300 + id) +
          "\"\nctl_endpoint = \"tcp://127.0.0.1:" + std::to_string(7200 + id) +
          "\"\n";
    }

    auto manifest = ParseClusterCreateManifest(text);

    ASSERT_TRUE(manifest.ok()) << manifest.status();
    ASSERT_EQ(manifest->meta_members_.size(), count);
    for (std::size_t index = 0; index < count; ++index) {
      EXPECT_EQ(manifest->meta_members_[index].server_id_, index + 1);
    }
  }
}

TEST(ClusterCreateManifestTest, RejectsDuplicateMetaIdentityAndEndpoints) {
  const std::string base = AutoManifest(1);
  const std::string second =
      "[[meta_members]]\nid = 2\n"
      "raft_endpoint = \"tcp://127.0.0.1:7102\"\n"
      "data_control_endpoint = \"tcp://127.0.0.1:7302\"\n"
      "ctl_endpoint = \"tcp://127.0.0.1:7202\"\n";
  ASSERT_TRUE(ParseClusterCreateManifest(base + second).ok());
  for (const auto& duplicate : {
           ReplaceOnce(second, "id = 2", "id = 1"),
           ReplaceOnce(second, "tcp://127.0.0.1:7102", "tcp://127.0.0.1:7101"),
           ReplaceOnce(second, "tcp://127.0.0.1:7302", "tcp://127.0.0.1:7301"),
           ReplaceOnce(second, "tcp://127.0.0.1:7202", "tcp://127.0.0.1:7201"),
       }) {
    EXPECT_FALSE(ParseClusterCreateManifest(base + duplicate).ok());
  }
}

TEST(ClusterCreateManifestTest, RejectsLegacyScalarMetaMemberShape) {
  const std::string legacy =
      ReplaceOnce(AutoManifest(1),
                  "raft_endpoint = \"tcp://127.0.0.1:7101\"\n"
                  "data_control_endpoint = \"tcp://127.0.0.1:7301\"\n"
                  "ctl_endpoint = \"tcp://127.0.0.1:7201\"\n",
                  "");

  EXPECT_FALSE(ParseClusterCreateManifest(legacy).ok());
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
      "raft_endpoint = \"tcp://127.0.0.1:7101\"\n"
      "data_control_endpoint = \"tcp://127.0.0.1:7301\"\n"
      "ctl_endpoint = \"tcp://127.0.0.1:7201\"\n"
      "[[data_nodes]]\nid = \"" +
      std::string(kNodeA) +
      "\"\nclient_endpoint = \"tcp://127.0.0.1:6379\"\n"
      "[[data_nodes]]\nid = \"" +
      std::string(kNodeB) +
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
  std::string explicit_manifest = ReplaceOnce(
      std::string(kValidManifest), "slot_strategy = \"contiguous-even\"\n", "");
  explicit_manifest +=
      "[[slot_ranges]]\nfirst = 0\nlast = 8191\ngroup = \"group-1\"\n"
      "[[slot_ranges]]\nfirst = 8192\nlast = 16383\ngroup = \"group-2\"\n";
  EXPECT_TRUE(ParseClusterCreateManifest(explicit_manifest).ok());
  EXPECT_FALSE(
      ParseClusterCreateManifest(
          ReplaceOnce(explicit_manifest, "first = 8192", "first = 8193"))
          .ok());
  EXPECT_FALSE(
      ParseClusterCreateManifest(
          ReplaceOnce(explicit_manifest, "first = 8192", "first = 8191"))
          .ok());
  EXPECT_FALSE(ParseClusterCreateManifest(ReplaceOnce(explicit_manifest,
                                                      "group = \"group-2\"",
                                                      "group = \"group-1\""))
                   .ok());
  EXPECT_FALSE(ParseClusterCreateManifest(ReplaceOnce(explicit_manifest,
                                                      "group = \"group-2\"",
                                                      "group = \"unknown\""))
                   .ok());
  EXPECT_FALSE(
      ParseClusterCreateManifest(
          ReplaceOnce(explicit_manifest, "last = 16383", "last = 16384"))
          .ok());
  EXPECT_FALSE(ParseClusterCreateManifest(
                   ReplaceOnce(explicit_manifest, "last = 8191", "last = 8190"))
                   .ok());
  EXPECT_FALSE(
      ParseClusterCreateManifest(
          ReplaceOnce(explicit_manifest, "first = 8192", "first = 16383"))
          .ok());
  EXPECT_FALSE(
      ParseClusterCreateManifest(
          ReplaceOnce(
              explicit_manifest, "schema_version = 1",
              "schema_version = 1\nslot_strategy = \"contiguous-even\""))
          .ok());
  EXPECT_FALSE(ParseClusterCreateManifest(
                   ReplaceOnce(std::string(kValidManifest),
                               "slot_strategy = \"contiguous-even\"\n", ""))
                   .ok());
}

TEST(ClusterCreateProtocolTest, RoundTripsOnlyCanonicalV1Requests) {
  const ClusterCreateManifestV1 manifest =
      *ParseClusterCreateManifest(kValidManifest);
  const MetaOperationId root = OperationId(7);
  auto request = EncodeClusterCreateRequest(manifest, root);
  ASSERT_TRUE(request.ok()) << request.status();
  EXPECT_TRUE(request->starts_with("clustercreate 1 "));
  MetaOperationId decoded_root{};

  auto decoded = DecodeClusterCreateRequest(*request, &decoded_root);

  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(*decoded, manifest);
  EXPECT_EQ(decoded_root, root);
  EXPECT_FALSE(DecodeClusterCreateRequest(*request + "00", &decoded_root).ok());
  EXPECT_FALSE(
      DecodeClusterCreateRequest("clustercreate 2 00", &decoded_root).ok());
}

TEST(ClusterCreateManifestTest, RoundTripsDualAndTlsOnlyListeners) {
  for (const std::string host : {"127.0.0.1", "[::1]"}) {
    for (const bool tls_only : {false, true}) {
      const std::string tcp = "tcp://" + host + ":6379";
      const std::string tls = "tls://" + host + ":16379";
      const std::string declaration =
          (tls_only ? "" : "client_endpoint = \"" + tcp + "\"\n") +
          "tls_endpoint = \"" + tls + "\"";
      auto manifest = ParseClusterCreateManifest(ReplaceOnce(
          std::string(kValidManifest),
          "client_endpoint = \"tcp://127.0.0.1:6379\"", declaration));
      ASSERT_TRUE(manifest.ok()) << manifest.status();
      EXPECT_EQ(manifest->data_nodes_[0].client_endpoint_, tls_only ? "" : tcp);
      EXPECT_EQ(manifest->data_nodes_[0].tls_endpoint_, tls);
      auto request = EncodeClusterCreateRequest(*manifest, OperationId(7));
      ASSERT_TRUE(request.ok()) << request.status();
      EXPECT_TRUE(request->starts_with("clustercreate 1 0001"));
      MetaOperationId root{};
      auto decoded = DecodeClusterCreateRequest(*request, &root);
      ASSERT_TRUE(decoded.ok()) << decoded.status();
      EXPECT_EQ(*decoded, *manifest);
      EXPECT_EQ(root, OperationId(7));
      for (const auto marker :
           {"0000", "0002", "0003", "0004", "0005", "ffff"}) {
        auto unsupported = *request;
        unsupported.replace(std::string("clustercreate 1 ").size(), 4, marker);
        EXPECT_FALSE(DecodeClusterCreateRequest(unsupported, &root).ok());
      }
    }
  }
}

TEST(ClusterCreateManifestTest, RejectsInvalidOrConflictingTlsListeners) {
  for (const std::string tls :
       {"", "tcp://127.0.0.1:16379", "tls://localhost:16379",
        "tls://127.0.0.1:0", "tls://127.0.0.1:65536", "tls://127.0.0.1:016379",
        "tls://127.0.0.2:16379", "tls://127.0.0.1:6379",
        "tls://127.0.0.1:6380"}) {
    SCOPED_TRACE(tls);
    EXPECT_FALSE(ParseClusterCreateManifest(
                     ReplaceOnce(std::string(kValidManifest),
                                 "client_endpoint = \"tcp://127.0.0.1:6379\"",
                                 "client_endpoint = "
                                 "\"tcp://127.0.0.1:6379\"\ntls_endpoint = \"" +
                                     tls + "\""))
                     .ok());
  }
  EXPECT_FALSE(ParseClusterCreateManifest(
                   ReplaceOnce(std::string(kValidManifest),
                               "client_endpoint = \"tcp://127.0.0.1:6379\"",
                               "tls_endpoint = \"tls://127.0.0.1:16379\"\n"
                               "tls_endpoint = \"tls://127.0.0.1:16380\""))
                   .ok());
  EXPECT_FALSE(
      ParseClusterCreateManifest(
          ReplaceOnce(std::string(kValidManifest),
                      "client_endpoint = \"tcp://127.0.0.1:6379\"", ""))
          .ok());
}

TEST(ClusterCreateProtocolTest, RejectsOldPersistedIntentFormat) {
  constexpr std::string_view request =
      "clustercreate 1 "
      "0003070707070707070707070707070707070000000100000001000000147463703a2f2f"
      "3132372e302e302e313a37313031000000147463703a2f2f3132372e302e302e313a3733"
      "3031000000147463703a2f2f3132372e302e302e313a3732303100000000000100000028"
      "303132333435363738396162636465663031323334353637383961626364656630313233"
      "34353637000000147463703a2f2f3132372e302e302e313a363337390000000100000006"
      "6c6567616379000000283031323334353637383961626364656630313233343536373839"
      "6162636465663031323334353637000000000000000100003fff000000066c656761637"
      "9";
  MetaOperationId root{};
  EXPECT_FALSE(DecodeClusterCreateRequest(request, &root).ok());
}

TEST(ClusterCreateProtocolTest, DecodesGenesisOutcome) {
  auto outcome = DecodeClusterCreateReply(
      "OK clustercreate 1 25 00112233445566778899aabbccddeeff");

  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->genesis_commit_index_, 25U);
  EXPECT_EQ(outcome->operation_id_, "00112233445566778899aabbccddeeff");
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
                              .serving_ready_ = true,
                              .topology_converged_ = true,
                              .effective_threshold_ms_ = 1'000});
  }
  for (const auto& range : manifest.slot_ranges_) {
    status.slot_ranges_.push_back({range.first_, range.last_, range.group_id_});
  }
  return status;
}

TEST(ClusterCreateOperatorTest, ReturnsImmediatelyAfterGenesisCommit) {
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
  std::vector<std::string> calls;
  ClusterOperator op([&](const MetaAdminTarget&, std::string_view command,
                         auto) -> absl::StatusOr<std::string> {
    calls.emplace_back(command);
    if (command == "clusterhead 1") return head_reply;
    if (command == "clusterstatus 1") return empty_reply;
    if (command.starts_with("clustercreate 1 ")) {
      MetaOperationId root{};
      auto decoded = DecodeClusterCreateRequest(command, &root);
      if (!decoded.ok()) return decoded.status();
      return "OK clustercreate 1 24 " + OperationIdHex(root);
    }
    return absl::InvalidArgumentError("unexpected command");
  });
  ClusterStatusOptions options;
  options.deadline_ =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);

  auto outcome = op.Create({.transport_ = MetaAdminTarget::Transport::kUnix,
                            .endpoint_ = "/tmp/meta.sock"},
                           manifest, options);

  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->genesis_commit_index_, 24U);
  EXPECT_EQ(outcome->operation_id_.size(), 32U);
  ASSERT_EQ(calls.size(), 3U);
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
    if (command == "clusterstatus 1") return *EncodeClusterStatusReply(empty);
    return "ERR clustercreate 1 initialize-data uncertain-outcome timed out";
  });
  ClusterStatusOptions options;
  options.deadline_ =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);

  auto outcome = op.Create({.transport_ = MetaAdminTarget::Transport::kUnix,
                            .endpoint_ = "/tmp/meta.sock"},
                           manifest, options);

  EXPECT_EQ(outcome.status().code(), absl::StatusCode::kAborted);
  EXPECT_NE(outcome.status().message().find("operation="),
            std::string_view::npos);
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
  empty.cluster_state_ = ClusterStateWireV1::kCreating;
  empty.lifecycle_revision_ = 1;
  empty.root_operation_id_ = "00112233445566778899aabbccddeeff";
  empty.genesis_commit_index_ = 8;
  empty.cluster_create_phase_ = "register-data";
  empty.blockers_.push_back(
      {.code_ = std::string(kClusterCreateActiveBlockerCode),
       .scope_ = "cluster",
       .detail_ = "non_terminal_cluster_create_operation_exists"});
  bool mutation_sent = false;
  ClusterOperator op([&](const MetaAdminTarget&, std::string_view command,
                         auto) -> absl::StatusOr<std::string> {
    if (command == "clusterhead 1") return *EncodeClusterHeadReply(head);
    if (command == "clusterstatus 1") return *EncodeClusterStatusReply(empty);
    mutation_sent = true;
    return absl::InternalError("unexpected mutation");
  });
  ClusterStatusOptions options;
  options.deadline_ =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);

  auto outcome = op.Create({.transport_ = MetaAdminTarget::Transport::kUnix,
                            .endpoint_ = "/tmp/meta.sock"},
                           manifest, options);

  EXPECT_EQ(outcome.status().code(), absl::StatusCode::kFailedPrecondition);
  EXPECT_NE(outcome.status().message().find("already-created"),
            std::string_view::npos);
  EXPECT_FALSE(mutation_sent);
}

TEST(ClusterCreateOperatorTest,
     LifecycleRejectionPrecedesMetaMembershipMismatch) {
  const ClusterCreateManifestV1 manifest =
      *ParseClusterCreateManifest(kValidManifest);
  const ClusterHeadWireV1 head{
      .responder_id_ = 1,
      .role_ = ClusterMetaRole::kLeader,
      .term_ = 2,
      .leader_id_ = 1,
      .config_index_ = 30,
      .meta_members_ = {{.server_id_ = 1,
                         .ctl_endpoint_ = "127.0.0.1:7201",
                         .is_leader_ = true},
                        {.server_id_ = 2, .ctl_endpoint_ = "127.0.0.1:7202"}},
  };
  // The original manifest remains valid after a Meta member is added, but no
  // longer describes current membership. It must not hide the Genesis binding.
  for (const auto state :
       {ClusterStateWireV1::kUninitialized, ClusterStateWireV1::kNonPristine,
        ClusterStateWireV1::kCreating, ClusterStateWireV1::kCreated,
        ClusterStateWireV1::kProvisioningFailed}) {
    SCOPED_TRACE(static_cast<int>(state));
    ClusterStatusWireV1 status = EmptyStatus(head);
    status.capture_.committed_index_ = 30;
    status.cluster_state_ = state;
    std::string_view expected_error = "already-created";
    if (state == ClusterStateWireV1::kUninitialized) {
      expected_error = "bad-request";
    } else if (state == ClusterStateWireV1::kNonPristine) {
      expected_error = "non-pristine";
    } else {
      status.root_operation_id_ = "00112233445566778899aabbccddeeff";
      status.genesis_commit_index_ = 8;
      status.lifecycle_revision_ = 2;
      if (state == ClusterStateWireV1::kCreating) {
        status.lifecycle_revision_ = 1;
        status.cluster_create_phase_ = "register-data";
      } else if (state == ClusterStateWireV1::kProvisioningFailed) {
        status.provisioning_failure_summary_ = "initial population failed";
      }
    }
    bool mutation_sent = false;
    ClusterOperator op([&](const MetaAdminTarget&, std::string_view command,
                           auto) -> absl::StatusOr<std::string> {
      if (command == "clusterhead 1") return EncodeClusterHeadReply(head);
      if (command == "clusterstatus 1") return EncodeClusterStatusReply(status);
      mutation_sent = true;
      return absl::InternalError("unexpected mutation");
    });
    ClusterStatusOptions options;
    options.deadline_ =
        std::chrono::steady_clock::now() + std::chrono::seconds(1);

    auto outcome = op.Create({.transport_ = MetaAdminTarget::Transport::kUnix,
                              .endpoint_ = "/tmp/meta.sock"},
                             manifest, options);

    EXPECT_EQ(outcome.status().code(), absl::StatusCode::kFailedPrecondition);
    EXPECT_NE(outcome.status().message().find(expected_error),
              std::string_view::npos)
        << outcome.status();
    EXPECT_FALSE(mutation_sent);
  }
}

TEST(ClusterCreateOperatorTest, DoesNotWaitForRuntimeReadiness) {
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
  std::size_t status_calls = 0;
  ClusterOperator op([&](const MetaAdminTarget&, std::string_view command,
                         auto) -> absl::StatusOr<std::string> {
    if (command == "clusterhead 1") return *EncodeClusterHeadReply(head);
    if (command == "clusterstatus 1") {
      ++status_calls;
      return *EncodeClusterStatusReply(empty);
    }
    MetaOperationId root{};
    auto decoded = DecodeClusterCreateRequest(command, &root);
    if (!decoded.ok()) return decoded.status();
    return "OK clustercreate 1 24 " + OperationIdHex(root);
  });
  ClusterStatusOptions options;
  options.deadline_ =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(75);

  auto outcome = op.Create({.transport_ = MetaAdminTarget::Transport::kUnix,
                            .endpoint_ = "/tmp/meta.sock"},
                           manifest, options);

  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->genesis_commit_index_, 24U);
  EXPECT_EQ(status_calls, 1U);
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

  auto outcome = op.Create({.transport_ = MetaAdminTarget::Transport::kUnix,
                            .endpoint_ = "/tmp/meta.sock"},
                           manifest, options);

  EXPECT_EQ(outcome.status().code(), absl::StatusCode::kCancelled);
  EXPECT_NE(outcome.status().message().find("before sending a mutation"),
            std::string_view::npos);
}

TEST(ClusterCreateOperatorTest,
     DistinguishesSecondConnectionFailureBeforeMutationWrite) {
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
    if (command == "clusterstatus 1") return *EncodeClusterStatusReply(empty);
    return MarkMetaAdminRequestNotSent(
        absl::UnavailableError("leader connection refused"));
  });
  ClusterStatusOptions options;
  options.deadline_ =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);

  auto outcome = op.Create({.transport_ = MetaAdminTarget::Transport::kUnix,
                            .endpoint_ = "/tmp/meta.sock"},
                           manifest, options);

  EXPECT_EQ(outcome.status().code(), absl::StatusCode::kCancelled);
  EXPECT_NE(outcome.status().message().find("before sending a mutation"),
            std::string_view::npos);
}

}  // namespace
}  // namespace lavik::meta
