#include "keylane/cluster/meta_control.h"

#include <cstddef>
#include <cstdint>
#include <string>

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
      .authority_version = 8,
      .grant_revision = 40,
      .grant_duration_ms = 1000,
      .grant_active = true,
      .config_epoch = 12,
      .slot_ranges = {{0, 8191}},
      .manifest_revision = 5,
      .partition_replication_epoch = 13,
      .grant_policy_id = "lease-policy",
      .grant_policy_version = 1,
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
  const std::string policy_content = "lease policy";
  desired.policies.push_back({
      .policy_id = "lease-policy",
      .version = 1,
      .content_hash = control::ComputeSha256(policy_content),
      .content = policy_content,
  });
  Rehash(&desired);
  return desired;
}

void AddValidSourceHistoryHold(control::FullDesiredState* desired) {
  const control::WireDesiredGroup& group = desired->groups.front();
  desired->groups.front().source_history_hold = control::WireSourceHistoryHold{
      .generation = 23,
      .source_assignment_id = group.members.front().assignment_id,
      .source_boot_id = std::string(40, 'b'),
      .source_replication_history_id = std::string(40, 'c'),
      .manifest_revision = group.manifest_revision,
      .manifest_digest = group.manifest_digest,
      .partition_replication_epoch = group.partition_replication_epoch,
  };
  Rehash(desired);
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
  EXPECT_EQ(group->authority_version_, 8);
  EXPECT_EQ(group->grant_revision_, 40);
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
}

TEST(MetaControlMapperTest,
     MapsSourceHistoryHoldToControlIdentityWithoutRestoringServing) {
  auto desired = DesiredState();
  auto& group = desired.groups.front();
  group.grant_active = false;
  group.grant_duration_ms = 0;
  group.grant_policy_id.clear();
  group.grant_policy_version = 0;
  AddValidSourceHistoryHold(&desired);

  auto prepared = cluster::PrepareMetaFullState(desired, kNode1, 4);
  ASSERT_TRUE(prepared.ok()) << prepared.status();
  ASSERT_EQ(prepared->control_groups_.size(), 1U);
  ASSERT_TRUE(
      prepared->control_groups_.front().source_history_hold_.has_value());
  const cluster::SourceHistoryHoldDesired& hold =
      *prepared->control_groups_.front().source_history_hold_;
  EXPECT_EQ(hold.group_id_, "group-a");
  EXPECT_EQ(hold.recovery_generation_, 23U);
  EXPECT_EQ(hold.source_assignment_id_,
            cluster::AssignmentId::FromBytes(
                desired.groups.front().members.front().assignment_id));
  EXPECT_EQ(hold.source_boot_id_.ToHexString(), std::string(40, 'b'));
  EXPECT_EQ(hold.source_replication_history_id_.ToHexString(),
            std::string(40, 'c'));
  EXPECT_EQ(hold.manifest_revision_, 5U);
  EXPECT_EQ(hold.manifest_digest_, desired.groups.front().manifest_digest);
  EXPECT_EQ(hold.partition_replication_epoch_, 13U);
  // A history hold retains replication data; it never restores the fenced
  // source as a serving group.
  EXPECT_TRUE(prepared->serving_state_->Groups().empty());
}

TEST(MetaControlMapperTest, RejectsSourceHistoryHoldForAnotherAssignment) {
  auto desired = DesiredState();
  AddValidSourceHistoryHold(&desired);
  desired.groups.front().source_history_hold->source_assignment_id =
      desired.groups.front().members.back().assignment_id;
  Rehash(&desired);

  EXPECT_EQ(cluster::PrepareMetaFullState(desired, kNode1, 1).status().code(),
            absl::StatusCode::kInvalidArgument);
}

TEST(MetaControlMapperTest, RejectsSourceHistoryHoldProjectedToNonlocalGroup) {
  auto desired = DesiredState();
  auto& group = desired.groups.front();
  group.members.erase(group.members.begin());
  group.owner_node_id = kNode2;
  group.owner_assignment_id = group.members.front().assignment_id;
  AddValidSourceHistoryHold(&desired);

  EXPECT_EQ(cluster::PrepareMetaFullState(desired, kNode1, 1).status().code(),
            absl::StatusCode::kInvalidArgument);
}

TEST(MetaControlMapperTest,
     RejectsSourceHistoryHoldWithZeroGenerationOrStalePopulation) {
  auto expect_rejected = [](control::FullDesiredState desired) {
    Rehash(&desired);
    EXPECT_EQ(cluster::PrepareMetaFullState(desired, kNode1, 1).status().code(),
              absl::StatusCode::kInvalidArgument);
  };

  auto desired = DesiredState();
  AddValidSourceHistoryHold(&desired);
  desired.groups.front().source_history_hold->generation = 0;
  EXPECT_EQ(cluster::PrepareMetaFullState(desired, kNode1, 1).status().code(),
            absl::StatusCode::kInvalidArgument);

  desired = DesiredState();
  AddValidSourceHistoryHold(&desired);
  ++desired.groups.front().source_history_hold->manifest_revision;
  expect_rejected(std::move(desired));

  desired = DesiredState();
  AddValidSourceHistoryHold(&desired);
  desired.groups.front().source_history_hold->manifest_digest[0] ^= 0xff;
  expect_rejected(std::move(desired));

  desired = DesiredState();
  AddValidSourceHistoryHold(&desired);
  ++desired.groups.front().source_history_hold->partition_replication_epoch;
  expect_rejected(std::move(desired));
}

TEST(MetaControlMapperTest,
     RejectsSourceHistoryHoldWithNoncanonicalBootOrHistory) {
  auto desired = DesiredState();
  AddValidSourceHistoryHold(&desired);
  desired.groups.front().source_history_hold->source_boot_id =
      std::string(40, 'B');
  EXPECT_EQ(cluster::PrepareMetaFullState(desired, kNode1, 1).status().code(),
            absl::StatusCode::kInvalidArgument);

  desired = DesiredState();
  AddValidSourceHistoryHold(&desired);
  desired.groups.front().source_history_hold->source_replication_history_id =
      "short";
  EXPECT_EQ(cluster::PrepareMetaFullState(desired, kNode1, 1).status().code(),
            absl::StatusCode::kInvalidArgument);
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
  desired.nodes = {
      {.node_id = kNode1, .host = "10.0.0.1", .port = 7001, .tls_port = 17001}};
  Rehash(&desired);

  auto prepared = cluster::PrepareMetaFullState(desired, kNode1, 1);
  ASSERT_TRUE(prepared.ok()) << prepared.status();
  cluster::TopologyCache topology;
  cluster::AuthorityGuard authority(
      topology, cluster::AuthorityGuard::LeaseMode::kFinite);
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
  EXPECT_EQ(prepared->control_groups_.front().authority_version_, 0U);
  EXPECT_EQ(prepared->control_groups_.front().grant_revision_, 0U);
}

TEST(MetaControlMapperTest,
     AcceptsFencedGroupWithOwnerIntentAndHistoricalAuthorityCounters) {
  auto desired = DesiredState();
  auto& group = desired.groups.front();
  group.grant_active = false;
  group.grant_duration_ms = 0;
  group.grant_policy_id.clear();
  group.grant_policy_version = 0;
  Rehash(&desired);

  auto prepared = cluster::PrepareMetaFullState(desired, kNode1, 1);
  ASSERT_TRUE(prepared.ok()) << prepared.status();
  EXPECT_TRUE(prepared->serving_state_->Groups().empty());
  ASSERT_EQ(prepared->control_groups_.size(), 1U);
  EXPECT_EQ(prepared->control_groups_.front().group_term_, group.group_term);
  EXPECT_EQ(prepared->control_groups_.front().authority_version_,
            group.authority_version);
  EXPECT_EQ(prepared->control_groups_.front().grant_revision_,
            group.grant_revision);
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

TEST(MetaControlMapperTest, RejectsNonCanonicalGrantLeaseParameters) {
  auto desired = DesiredState();
  desired.groups[0].grant_duration_ms = 0;
  desired.object_hash = {};
  auto projection_hash = control::ComputeProjectionHash(desired);
  ASSERT_TRUE(projection_hash.ok()) << projection_hash.status();
  desired.projection_hash = *projection_hash;
  auto encoded = control::EncodeFullDesiredState(desired);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  desired.object_hash = control::ComputeSha256(*encoded);
  EXPECT_EQ(cluster::PrepareMetaFullState(desired, kNode1, 1).status().code(),
            absl::StatusCode::kInvalidArgument);

  desired = DesiredState();
  desired.groups[0].grant_active = false;
  desired.object_hash = {};
  projection_hash = control::ComputeProjectionHash(desired);
  ASSERT_TRUE(projection_hash.ok()) << projection_hash.status();
  desired.projection_hash = *projection_hash;
  encoded = control::EncodeFullDesiredState(desired);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  desired.object_hash = control::ComputeSha256(*encoded);
  EXPECT_EQ(cluster::PrepareMetaFullState(desired, kNode1, 1).status().code(),
            absl::StatusCode::kInvalidArgument);
}

TEST(MetaControlMapperTest, RejectsTamperedMissingAndDuplicatePolicies) {
  auto desired = DesiredState();
  desired.policies[0].content.append(" tampered");
  Rehash(&desired);
  EXPECT_EQ(cluster::PrepareMetaFullState(desired, kNode1, 1).status().code(),
            absl::StatusCode::kInvalidArgument);

  desired = DesiredState();
  desired.policies.clear();
  Rehash(&desired);
  EXPECT_EQ(cluster::PrepareMetaFullState(desired, kNode1, 1).status().code(),
            absl::StatusCode::kInvalidArgument);

  desired = DesiredState();
  desired.policies.push_back(desired.policies.front());
  Rehash(&desired);
  EXPECT_EQ(cluster::PrepareMetaFullState(desired, kNode1, 1).status().code(),
            absl::StatusCode::kInvalidArgument);

  desired = DesiredState();
  desired.policies[0].policy_id.clear();
  Rehash(&desired);
  EXPECT_EQ(cluster::PrepareMetaFullState(desired, kNode1, 1).status().code(),
            absl::StatusCode::kInvalidArgument);

  desired = DesiredState();
  desired.policies[0].version = 0;
  Rehash(&desired);
  EXPECT_EQ(cluster::PrepareMetaFullState(desired, kNode1, 1).status().code(),
            absl::StatusCode::kInvalidArgument);
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
