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

#include "lavik/cluster/meta_control.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include "../src/redis/cluster_command.h"
#include "absl/cleanup/cleanup.h"
#include "gtest/gtest.h"
#include "lavik/cluster/control_protocol.h"
#include "lavik/cluster/runtime.h"
#include "lavik/replication_group.h"
#include "lavik/resp.h"

namespace {

namespace cluster = lavik::cluster;
namespace control = lavik::cluster::control;

constexpr char kNode1[] = "1111111111111111111111111111111111111111";
constexpr char kNode2[] = "2222222222222222222222222222222222222222";

control::FullDesiredState DesiredState() {
  control::WireId128 assignment{};
  assignment[0] = 9;
  control::FullDesiredState desired;
  desired.control_revision = 42;
  desired.topology_epoch = 17;
  desired.authority_lease_duration_ms = 1'000;
  desired.nodes = {
      {.node_id = kNode1, .host = "10.0.0.1", .port = 7001, .tls_port = 17001},
      {.node_id = kNode2, .host = "10.0.0.2", .port = 7002, .tls_port = 17002},
  };
  desired.groups = {{
      .group_id = "group-a",
      .members = {{.node_id = kNode1, .assignment_id = assignment},
                  {.node_id = kNode2, .assignment_id = control::WireId128{3}}},
      .owner_node_id = kNode1,
      .owner_assignment_id = assignment,
      .group_term = 7,
      .grant_active = true,
      .slot_ranges = {{0, 8191}},
      .manifest_revision = 5,
      .partition_replication_epoch = 13,
      .steady_replication_enabled = true,
  }};
  auto manifest = lavik::PopulationManifest::Create({});
  EXPECT_TRUE(manifest.ok()) << manifest.status();
  if (manifest.ok()) {
    desired.groups[0].manifest_digest = manifest->id().bytes_;
    desired.manifests.push_back({
        .revision = 5,
        .digest = manifest->id().bytes_,
        .entries = {},
    });
  }
  return desired;
}

TEST(MetaControlMapperTest, SelectsLocalControlAndRetainsRemoteRouting) {
  auto bootstrap = DesiredState();
  constexpr char remote[] = "3333333333333333333333333333333333333333";
  bootstrap.nodes.push_back(
      {.node_id = remote, .host = "10.0.0.3", .port = 7003});
  auto group = bootstrap.groups.front();
  group.group_id = "remote";
  group.members = {{remote, control::WireId128{4}}};
  group.owner_node_id = remote;
  group.owner_assignment_id = control::WireId128{4};
  group.group_term = 9;
  group.slot_ranges = {{8192, 16383}};
  bootstrap.groups.push_back(group);
  auto selected = control::SelectNodeControlState(bootstrap, kNode1);
  ASSERT_EQ(selected.local.groups.size(), 1u);
  ASSERT_EQ(selected.local.manifests.size(), 1u);
  auto prepared = cluster::PrepareNodeControlState(selected, kNode1, 1);
  ASSERT_TRUE(prepared.ok()) << prepared.status();
  ASSERT_EQ(prepared->desired_cluster_controls_.size(), 1u);
  const auto* route = prepared->serving_state_->FindGroup("remote");
  ASSERT_NE(route, nullptr);
  EXPECT_EQ(route->group_term_, 9u);
  EXPECT_EQ(route->manifest_revision_, 0u);
  EXPECT_TRUE(route->population_ready_);
  EXPECT_EQ(prepared->serving_state_->Nodes().size(), 3u);
  selected.routing.groups.front().term++;
  EXPECT_FALSE(cluster::PrepareNodeControlState(selected, kNode1, 1).ok());
}

TEST(MetaControlMapperTest, LocalEndpointChangeAdvancesOnlyRelevantControl) {
  auto bootstrap = DesiredState();
  const auto before = control::SelectNodeControlState(bootstrap, kNode1);
  bootstrap.nodes.front().port++;
  auto after = control::SelectNodeControlState(bootstrap, kNode1);
  const auto update = control::DiffNodeControlState(before, after);
  ASSERT_TRUE(update.local);
  ASSERT_TRUE(update.routing);
  EXPECT_FALSE(update.tasks);
  EXPECT_EQ(after.local.revision, before.local.revision + 1);
}

TEST(MetaControlMapperTest, BuildsCompleteImmutableServingState) {
  auto prepared = cluster::PrepareMetaFullState(DesiredState(), kNode1, 4);
  ASSERT_TRUE(prepared.ok()) << prepared.status();
  ASSERT_NE(prepared->serving_state_, nullptr);
  EXPECT_EQ(prepared->serving_state_->topology_epoch(), 17);
  EXPECT_EQ(prepared->serving_state_->Self()->node_id_.ToHexString(), kNode1);
  EXPECT_EQ(prepared->serving_state_->InFlightStripeCount(), 4);
  const cluster::GroupView* group =
      prepared->serving_state_->FindGroup("group-a");
  ASSERT_NE(group, nullptr);
  EXPECT_EQ(group->group_term_, 7);
  EXPECT_EQ(group->manifest_revision_, 5);
  EXPECT_TRUE(group->granted_);
  EXPECT_FALSE(group->population_ready_);
  ASSERT_EQ(group->replica_node_indices_.size(), 1);
  ASSERT_EQ(prepared->desired_cluster_controls_.size(), 1U);
  ASSERT_EQ(prepared->desired_cluster_controls_[0].identity_.members_.size(),
            2U);
  EXPECT_EQ(prepared->desired_cluster_controls_[0]
                .identity_.members_[0]
                .assignment_id_,
            cluster::AssignmentId::FromBytes(
                DesiredState().groups[0].members[0].assignment_id));
  EXPECT_EQ(prepared->desired_cluster_controls_[0]
                .identity_.members_[1]
                .assignment_id_,
            cluster::AssignmentId::FromBytes(
                DesiredState().groups[0].members[1].assignment_id));
  EXPECT_EQ(prepared->desired_cluster_controls_[0].identity_.manifest_digest_,
            DesiredState().groups[0].manifest_digest);
  EXPECT_EQ(prepared->desired_cluster_controls_[0]
                .identity_.partition_replication_epoch_,
            DesiredState().groups[0].partition_replication_epoch);
  ASSERT_EQ(prepared->desired_cluster_controls_.size(), 1U);
  EXPECT_EQ(prepared->authority_lease_duration_ms_, 1'000U);
  const cluster::DesiredClusterControl& desired_control =
      prepared->desired_cluster_controls_.front();
  ASSERT_TRUE(desired_control.owner_endpoint_.has_value());
  EXPECT_EQ(desired_control.owner_endpoint_->node_id_.ToHexString(), kNode1);
  EXPECT_EQ(desired_control.owner_endpoint_->host_, "10.0.0.1");
  EXPECT_EQ(desired_control.owner_endpoint_->port_, 7001);
  EXPECT_EQ(desired_control.owner_endpoint_->tls_port_, 17001);
  EXPECT_TRUE(desired_control.manifest_entries_.empty());
  EXPECT_TRUE(desired_control.steady_replication_enabled_);
}

TEST(MetaControlMapperTest, SourceIndexDoesNotBecomeTopologyEpoch) {
  auto desired = DesiredState();
  desired.control_revision = 100;
  auto encoded = control::EncodeFullDesiredState(desired);
  ASSERT_TRUE(encoded.ok()) << encoded.status();

  auto prepared = cluster::PrepareMetaFullState(desired, kNode1, 1);
  ASSERT_TRUE(prepared.ok()) << prepared.status();
  EXPECT_EQ(prepared->serving_state_->topology_epoch(), 17);
}

TEST(MetaControlMapperTest,
     RedisEpochsFollowGroupTermsIncludingFencedAndEmptyGroups) {
  constexpr char kNode3[] = "3333333333333333333333333333333333333333";
  auto desired = DesiredState();
  desired.nodes.push_back(
      {.node_id = kNode3, .host = "10.0.0.3", .port = 7003});
  auto other = desired.groups.front();
  other.group_id = "group-b";
  other.members = {{.node_id = kNode3, .assignment_id = control::WireId128{4}}};
  other.owner_node_id = kNode3;
  other.owner_assignment_id = other.members.front().assignment_id;
  other.group_term = 11;
  other.slot_ranges = {{8192, 16383}};
  desired.groups.push_back(std::move(other));
  // A fenced Group can lose its last member without losing its durable term.
  // It has no node row or routing entry but still contributes to INFO's max.
  desired.groups.push_back({.group_id = "empty-group", .group_term = 13});

  const auto command = [](std::string subcommand) {
    lavik::CommandRequest request;
    request.kind_ = lavik::CommandKind::kCluster;
    request.args_ = {"CLUSTER", std::move(subcommand)};
    lavik::ReplyBuilder reply;
    auto task = lavik::ExecuteClusterModeCommand(request, reply);
    auto handle = std::move(task).ReleaseHandle();
    handle.resume();
    EXPECT_TRUE(handle.done());
    const std::string result(handle.promise().value_.encoded_);
    handle.destroy();
    return result;
  };
  for (bool fenced : {false, true}) {
    // An uncontrolled Begin advances term before installing a new Owner.
    // The compatibility epoch still reflects that term without advertising
    // serving authority or depending on an active routing GroupView.
    desired.groups.front().group_term = fenced ? 8 : 7;
    desired.groups.front().grant_active = !fenced;
    for (const char* self : {kNode1, kNode2}) {
      auto prepared = cluster::PrepareMetaFullState(desired, self, 1);
      ASSERT_TRUE(prepared.ok()) << prepared.status();
      auto runtime = std::make_unique<cluster::ClusterRuntime>();
      ASSERT_TRUE(
          runtime->node_control_installer_
              .InstallFullState(std::move(*prepared),
                                {.control_revision_ = desired.control_revision})
              .ok());
      // Local readiness publication rebuilds the routing view. It must retain
      // the derived maximum even for Groups absent from that routing view.
      ASSERT_TRUE(runtime->node_control_installer_.SetStorageReady(true).ok());
      cluster::SetClientMode(lavik::ClientMode::kCluster);
      cluster::InstallClusterRuntime(std::move(runtime));
      absl::Cleanup reset_runtime = [] {
        cluster::SetClientMode(lavik::ClientMode::kSingle);
        cluster::InstallClusterRuntime(nullptr);
      };
      const std::string info = command("INFO");
      EXPECT_NE(info.find("cluster_current_epoch:13\r\n"), std::string::npos);
      EXPECT_NE(info.find(fenced ? "cluster_my_epoch:8\r\n"
                                 : "cluster_my_epoch:7\r\n"),
                std::string::npos);
      const std::string nodes = command("NODES");
      const std::string epoch =
          fenced ? " 0 0 8 connected" : " 0 0 7 connected";
      const auto first = nodes.find(epoch);
      ASSERT_NE(first, std::string::npos);
      EXPECT_NE(nodes.find(epoch, first + epoch.size()), std::string::npos);
      EXPECT_NE(nodes.find(" 0 0 11 connected"), std::string::npos);
    }
  }
}

TEST(MetaControlMapperTest, InstallsInitialEmptyTopologyAtEpochZero) {
  control::FullDesiredState desired;
  desired.control_revision = 1;
  desired.topology_epoch = 0;
  desired.authority_lease_duration_ms = 1'000;
  desired.nodes = {
      {.node_id = kNode1, .host = "10.0.0.1", .port = 7001, .tls_port = 17001}};

  auto prepared = cluster::PrepareMetaFullState(desired, kNode1, 1);
  ASSERT_TRUE(prepared.ok()) << prepared.status();
  cluster::TopologyCache topology;
  cluster::AuthorityGuard authority(topology);
  cluster::NullNodeControlActions actions;
  cluster::NodeControlInstaller installer(topology, authority, actions);
  ASSERT_TRUE(installer.SetStorageReady(true).ok());
  ASSERT_TRUE(
      installer.InstallFullState(std::move(*prepared), {.control_revision_ = 1})
          .ok());
  ASSERT_NE(topology.Current(), nullptr);
  EXPECT_EQ(topology.Current()->topology_epoch(), 0U);
  EXPECT_TRUE(topology.Current()->Groups().empty());
}

TEST(MetaControlMapperTest, AcceptsCommittedOwnerlessGroupBeforeActivation) {
  control::FullDesiredState desired;
  desired.control_revision = 2;
  desired.topology_epoch = 1;
  desired.authority_lease_duration_ms = 1'000;
  desired.nodes = {
      {.node_id = kNode1, .host = "10.0.0.1", .port = 7001, .tls_port = 17001}};
  desired.groups = {{.group_id = "group-pending"}};

  auto prepared = cluster::PrepareMetaFullState(desired, kNode1, 1);
  ASSERT_TRUE(prepared.ok()) << prepared.status();
  ASSERT_NE(prepared->serving_state_, nullptr);
  EXPECT_TRUE(prepared->serving_state_->Groups().empty());
  ASSERT_EQ(prepared->desired_cluster_controls_.size(), 1U);
  EXPECT_EQ(prepared->desired_cluster_controls_.front().identity_.group_id_,
            "group-pending");
  EXPECT_EQ(prepared->desired_cluster_controls_.front().identity_.group_term_,
            0U);
}

TEST(MetaControlMapperTest, AcceptsGrantlessGroupWithOwnerIntent) {
  auto desired = DesiredState();
  auto& group = desired.groups.front();
  group.grant_active = false;

  auto prepared = cluster::PrepareMetaFullState(desired, kNode1, 1);
  ASSERT_TRUE(prepared.ok()) << prepared.status();
  EXPECT_TRUE(prepared->serving_state_->Groups().empty());
  ASSERT_EQ(prepared->desired_cluster_controls_.size(), 1U);
  EXPECT_EQ(prepared->desired_cluster_controls_.front().identity_.group_term_,
            group.group_term);
}

TEST(MetaControlMapperTest, RejectsOwnerAssignmentMismatch) {
  auto desired = DesiredState();
  ASSERT_TRUE(desired.groups[0].owner_assignment_id.has_value());
  (*desired.groups[0].owner_assignment_id)[0] = 10;
  EXPECT_EQ(cluster::PrepareMetaFullState(desired, kNode1, 1).status().code(),
            absl::StatusCode::kInvalidArgument);
}

TEST(MetaControlMapperTest, RejectsNodeAssignedToMultipleGroups) {
  auto desired = DesiredState();
  desired.groups.push_back(desired.groups.front());
  desired.groups.back().group_id = "group-b";
  desired.groups.back().slot_ranges.clear();
  EXPECT_EQ(cluster::PrepareMetaFullState(desired, kNode1, 1).status().code(),
            absl::StatusCode::kInvalidArgument);
}

TEST(MetaControlMapperTest, RejectsMissingLocalNodeAndZeroAppliedIndex) {
  auto desired = DesiredState();
  EXPECT_EQ(cluster::PrepareMetaFullState(desired, std::string(40, '3'), 1)
                .status()
                .code(),
            absl::StatusCode::kInvalidArgument);
  desired.control_revision = 0;
  EXPECT_EQ(cluster::PrepareMetaFullState(desired, kNode1, 1).status().code(),
            absl::StatusCode::kInvalidArgument);
}

TEST(MetaControlMapperTest, RejectsNonNumericOrNonCanonicalNodeHosts) {
  auto desired = DesiredState();
  desired.nodes[0].host = "data.example";
  EXPECT_EQ(cluster::PrepareMetaFullState(desired, kNode1, 1).status().code(),
            absl::StatusCode::kInvalidArgument);

  desired = DesiredState();
  desired.nodes[0].host = "2001:0db8:0:0:0:0:0:1";
  EXPECT_EQ(cluster::PrepareMetaFullState(desired, kNode1, 1).status().code(),
            absl::StatusCode::kInvalidArgument);

  desired = DesiredState();
  desired.nodes[0].host = "2001:db8::1";
  EXPECT_TRUE(cluster::PrepareMetaFullState(desired, kNode1, 1).ok());
}

TEST(MetaControlMapperTest,
     RejectsMalformedMemberIncarnationsAndZeroOwnerTerm) {
  auto desired = DesiredState();
  desired.groups[0].members[1].assignment_id = {};
  EXPECT_EQ(cluster::PrepareMetaFullState(desired, kNode1, 1).status().code(),
            absl::StatusCode::kInvalidArgument);

  desired = DesiredState();
  desired.groups[0].members.push_back(desired.groups[0].members.back());
  EXPECT_EQ(cluster::PrepareMetaFullState(desired, kNode1, 1).status().code(),
            absl::StatusCode::kInvalidArgument);

  desired = DesiredState();
  desired.groups[0].group_term = 0;
  EXPECT_EQ(cluster::PrepareMetaFullState(desired, kNode1, 1).status().code(),
            absl::StatusCode::kInvalidArgument);
}

}  // namespace
