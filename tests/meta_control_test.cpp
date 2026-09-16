#include "keylane/cluster/meta_control.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include "gtest/gtest.h"
#include "keylane/cluster/control_protocol.h"
#include "keylane/replication_group.h"

namespace {

namespace cluster = keylane::cluster;
namespace control = keylane::cluster::control;

constexpr char kNode1[] = "1111111111111111111111111111111111111111";
constexpr char kNode2[] = "2222222222222222222222222222222222222222";

void Rehash(control::FullDesiredState* desired) {
  desired->object_hash = {};
  auto directive_digest =
      control::ComputeDirectiveSetDigest(desired->current_directives);
  ASSERT_TRUE(directive_digest.ok()) << directive_digest.status();
  desired->directive_set_digest = *directive_digest;
  auto projection_hash = control::ComputeProjectionHash(*desired);
  ASSERT_TRUE(projection_hash.ok()) << projection_hash.status();
  desired->projection_hash = *projection_hash;
  auto encoded = control::EncodeFullDesiredState(*desired);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  desired->object_hash = control::ComputeSha256(*encoded);
}

control::FullDesiredState DesiredState() {
  control::WireId128 assignment{};
  assignment[0] = 9;
  control::FullDesiredState desired;
  desired.source_meta_applied_index = 42;
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
      .config_epoch = 12,
      .slot_ranges = {{0, 8191}},
      .manifest_revision = 5,
      .partition_replication_epoch = 13,
      .steady_replication_enabled = true,
  }};
  auto manifest = keylane::PopulationManifest::Create({});
  EXPECT_TRUE(manifest.ok()) << manifest.status();
  if (manifest.ok()) {
    desired.groups[0].manifest_digest = manifest->id().bytes_;
    desired.manifests.push_back({
        .revision = 5,
        .digest = manifest->id().bytes_,
        .entries = {},
    });
  }
  Rehash(&desired);
  return desired;
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
  EXPECT_EQ(prepared->object_hash_, DesiredState().object_hash);
  ASSERT_EQ(prepared->control_groups_.size(), 1U);
  ASSERT_EQ(prepared->control_groups_[0].members_.size(), 2U);
  EXPECT_EQ(prepared->control_groups_[0].members_[0].assignment_id_,
            cluster::AssignmentId::FromBytes(
                DesiredState().groups[0].members[0].assignment_id));
  EXPECT_EQ(prepared->control_groups_[0].members_[1].assignment_id_,
            cluster::AssignmentId::FromBytes(
                DesiredState().groups[0].members[1].assignment_id));
  EXPECT_EQ(prepared->control_groups_[0].manifest_digest_,
            DesiredState().groups[0].manifest_digest);
  EXPECT_EQ(prepared->control_groups_[0].partition_replication_epoch_,
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
  desired.source_meta_applied_index = 100;
  desired.object_hash = {};
  auto projection_hash = control::ComputeProjectionHash(desired);
  ASSERT_TRUE(projection_hash.ok()) << projection_hash.status();
  desired.projection_hash = *projection_hash;
  auto encoded = control::EncodeFullDesiredState(desired);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  desired.object_hash = control::ComputeSha256(*encoded);

  auto prepared = cluster::PrepareMetaFullState(desired, kNode1, 1);
  ASSERT_TRUE(prepared.ok()) << prepared.status();
  EXPECT_EQ(prepared->serving_state_->topology_epoch(), 17);
}

TEST(MetaControlMapperTest, InstallsInitialEmptyTopologyAtEpochZero) {
  control::FullDesiredState desired;
  desired.source_meta_applied_index = 1;
  desired.topology_epoch = 0;
  desired.authority_lease_duration_ms = 1'000;
  desired.nodes = {
      {.node_id = kNode1, .host = "10.0.0.1", .port = 7001, .tls_port = 17001}};
  Rehash(&desired);

  auto prepared = cluster::PrepareMetaFullState(desired, kNode1, 1);
  ASSERT_TRUE(prepared.ok()) << prepared.status();
  cluster::TopologyCache topology;
  cluster::AuthorityGuard authority(topology);
  cluster::NullNodeControlActions actions;
  cluster::NodeControlInstaller installer(topology, authority, actions);
  ASSERT_TRUE(installer.SetStorageReady(true).ok());
  ASSERT_TRUE(
      installer
          .InstallFullState(std::move(*prepared),
                            {.source_meta_applied_index_ = 1,
                             .projection_hash_ = desired.projection_hash})
          .ok());
  ASSERT_NE(topology.Current(), nullptr);
  EXPECT_EQ(topology.Current()->topology_epoch(), 0U);
  EXPECT_TRUE(topology.Current()->Groups().empty());
}

TEST(MetaControlMapperTest, AcceptsCommittedOwnerlessGroupBeforeActivation) {
  control::FullDesiredState desired;
  desired.source_meta_applied_index = 2;
  desired.topology_epoch = 1;
  desired.authority_lease_duration_ms = 1'000;
  desired.nodes = {
      {.node_id = kNode1, .host = "10.0.0.1", .port = 7001, .tls_port = 17001}};
  desired.groups = {{.group_id = "group-pending"}};
  Rehash(&desired);

  auto prepared = cluster::PrepareMetaFullState(desired, kNode1, 1);
  ASSERT_TRUE(prepared.ok()) << prepared.status();
  ASSERT_NE(prepared->serving_state_, nullptr);
  EXPECT_TRUE(prepared->serving_state_->Groups().empty());
  ASSERT_EQ(prepared->control_groups_.size(), 1U);
  EXPECT_EQ(prepared->control_groups_.front().group_id_, "group-pending");
  EXPECT_EQ(prepared->control_groups_.front().group_term_, 0U);
}

TEST(MetaControlMapperTest, AcceptsGrantlessGroupWithOwnerIntent) {
  auto desired = DesiredState();
  auto& group = desired.groups.front();
  group.grant_active = false;
  Rehash(&desired);

  auto prepared = cluster::PrepareMetaFullState(desired, kNode1, 1);
  ASSERT_TRUE(prepared.ok()) << prepared.status();
  EXPECT_TRUE(prepared->serving_state_->Groups().empty());
  ASSERT_EQ(prepared->control_groups_.size(), 1U);
  EXPECT_EQ(prepared->control_groups_.front().group_term_, group.group_term);
}

TEST(MetaControlMapperTest, RejectsOwnerAssignmentMismatch) {
  auto desired = DesiredState();
  ASSERT_TRUE(desired.groups[0].owner_assignment_id.has_value());
  (*desired.groups[0].owner_assignment_id)[0] = 10;
  Rehash(&desired);
  EXPECT_EQ(cluster::PrepareMetaFullState(desired, kNode1, 1).status().code(),
            absl::StatusCode::kInvalidArgument);
}

TEST(MetaControlMapperTest, RejectsNodeAssignedToMultipleGroups) {
  auto desired = DesiredState();
  desired.groups.push_back(desired.groups.front());
  desired.groups.back().group_id = "group-b";
  desired.groups.back().slot_ranges.clear();
  Rehash(&desired);
  EXPECT_EQ(cluster::PrepareMetaFullState(desired, kNode1, 1).status().code(),
            absl::StatusCode::kInvalidArgument);
}

TEST(MetaControlMapperTest, RejectsMissingLocalNodeAndZeroHashes) {
  auto desired = DesiredState();
  EXPECT_EQ(cluster::PrepareMetaFullState(desired, std::string(40, '3'), 1)
                .status()
                .code(),
            absl::StatusCode::kInvalidArgument);
  desired.object_hash = {};
  EXPECT_EQ(cluster::PrepareMetaFullState(desired, kNode1, 1).status().code(),
            absl::StatusCode::kInvalidArgument);
}

TEST(MetaControlMapperTest, RejectsNonNumericOrNonCanonicalNodeHosts) {
  auto desired = DesiredState();
  desired.nodes[0].host = "data.example";
  Rehash(&desired);
  EXPECT_EQ(cluster::PrepareMetaFullState(desired, kNode1, 1).status().code(),
            absl::StatusCode::kInvalidArgument);

  desired = DesiredState();
  desired.nodes[0].host = "2001:0db8:0:0:0:0:0:1";
  Rehash(&desired);
  EXPECT_EQ(cluster::PrepareMetaFullState(desired, kNode1, 1).status().code(),
            absl::StatusCode::kInvalidArgument);

  desired = DesiredState();
  desired.nodes[0].host = "2001:db8::1";
  Rehash(&desired);
  EXPECT_TRUE(cluster::PrepareMetaFullState(desired, kNode1, 1).ok());
}

TEST(MetaControlMapperTest, RejectsMalformedMemberIncarnationsAndConfigEpoch) {
  auto desired = DesiredState();
  desired.groups[0].members[1].assignment_id = {};
  Rehash(&desired);
  EXPECT_EQ(cluster::PrepareMetaFullState(desired, kNode1, 1).status().code(),
            absl::StatusCode::kInvalidArgument);

  desired = DesiredState();
  desired.groups[0].members.push_back(desired.groups[0].members.back());
  Rehash(&desired);
  EXPECT_EQ(cluster::PrepareMetaFullState(desired, kNode1, 1).status().code(),
            absl::StatusCode::kInvalidArgument);

  desired = DesiredState();
  desired.groups[0].config_epoch = 0;
  Rehash(&desired);
  EXPECT_EQ(cluster::PrepareMetaFullState(desired, kNode1, 1).status().code(),
            absl::StatusCode::kInvalidArgument);
}

}  // namespace
