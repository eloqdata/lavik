#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "gtest/gtest.h"
#include "keylane/cluster/control_protocol.h"
#include "keylane/cluster/meta_control.h"
#include "keylane/meta/commands.h"
#include "keylane/meta/control_projector.h"
#include "keylane/meta/hash.h"
#include "keylane/meta/policy_store.h"
#include "keylane/meta/population_manifest_store.h"
#include "keylane/meta/state_apply.h"

namespace {

namespace control = keylane::cluster::control;
using keylane::meta::ApplyCommitted;
using keylane::meta::MetaApplyResult;
using keylane::meta::MetaAuditVerdict;
using keylane::meta::MetaCommand;
using keylane::meta::MetaCommittedView;
using keylane::meta::MetaControlProjector;
using keylane::meta::MetaStores;

constexpr std::string_view kActor = "keylane://operator/projector-test";
constexpr std::string_view kTime = "2026-09-07T00:00:00Z";

template <std::size_t N>
std::array<std::uint8_t, N> Bytes(std::uint8_t seed) {
  std::array<std::uint8_t, N> result{};
  for (std::size_t i = 0; i < result.size(); ++i) {
    result[i] = static_cast<std::uint8_t>(seed + i);
  }
  return result;
}

std::string NodeId(std::uint8_t value) {
  constexpr std::string_view kHex = "0123456789abcdef";
  std::string id(40, '0');
  id[38] = kHex[value >> 4];
  id[39] = kHex[value & 0x0f];
  return id;
}

void Commit(MetaStores& stores, std::uint64_t index,
            const MetaCommand& command) {
  const MetaApplyResult result =
      ApplyCommitted(stores, index, command, kActor, kTime);
  ASSERT_EQ(result.verdict_, MetaAuditVerdict::kAccepted) << result.detail_;
}

keylane::meta::RegisterNode Register(std::uint8_t node,
                                     std::vector<std::string> endpoints) {
  keylane::meta::RegisterNode command;
  command.request_id_ = Bytes<16>(node);
  command.node_id_ = NodeId(node);
  command.principal_ = "keylane://node/" + command.node_id_;
  command.endpoints_ = std::move(endpoints);
  command.role_ = keylane::meta::MetaNodeRole::kPrimary;
  return command;
}

keylane::meta::PutPolicy PutPolicy(std::uint8_t request_seed,
                                   std::string policy_id, std::uint64_t version,
                                   std::string content) {
  keylane::meta::PutPolicy command;
  command.request_id_ = Bytes<16>(request_seed);
  command.policy_id_ = std::move(policy_id);
  command.version_ = version;
  command.content_ = std::move(content);
  command.content_hash_ =
      keylane::meta::MetaPolicyStore::ContentHash(command.content_);
  return command;
}

absl::Status ReplaceEndpoints(MetaStores& stores, std::uint8_t node,
                              std::vector<std::string> endpoints) {
  keylane::meta::UpdateNode update;
  update.request_id_ = Bytes<16>(0x75);
  update.node_id_ = NodeId(node);
  update.expected_revision_ = 1;
  update.endpoints_ = std::move(endpoints);
  update.new_topology_epoch_ = 8;
  return stores.identity_.Apply(update);
}

struct Fixture {
  MetaStores stores;
  std::string target = NodeId(1);
  std::string source = NodeId(2);
  keylane::meta::MetaAssignmentId target_assignment = Bytes<16>(0x21);
  keylane::meta::MetaAssignmentId source_assignment = Bytes<16>(0x31);
  keylane::meta::MetaHash256 manifest_digest{};
  keylane::meta::MetaOperationId operation_id = Bytes<16>(0x41);
  keylane::meta::MetaDirectiveId directive_id = Bytes<16>(0x51);
  keylane::meta::MetaAttemptId attempt_id = Bytes<16>(0x61);
  keylane::meta::MetaBootIncarnation target_boot = Bytes<20>(0x71);
  keylane::meta::MetaBootIncarnation source_boot = Bytes<20>(0x81);
  keylane::meta::MetaReplicationHistoryId source_history = Bytes<20>(0x91);
  std::uint64_t grant_revision = 0;
  std::uint64_t directive_revision = 0;
};

Fixture CompleteFixture() {
  Fixture fixture;
  std::uint64_t index = 1;

  keylane::meta::BindMetaMember meta2;
  meta2.request_id_ = Bytes<16>(0x02);
  meta2.server_id_ = 2;
  meta2.principal_ = "keylane://meta/2";
  meta2.data_control_endpoint_ = "10.0.0.12:7200";
  Commit(fixture.stores, index++, meta2);
  keylane::meta::BindMetaMember meta1;
  meta1.request_id_ = Bytes<16>(0x01);
  meta1.server_id_ = 1;
  meta1.principal_ = "keylane://meta/1";
  meta1.data_control_endpoint_ = "10.0.0.11:7100";
  Commit(fixture.stores, index++, meta1);

  Commit(fixture.stores, index++,
         Register(1, {"tls://10.0.0.1:17000", "tcp://10.0.0.1:7000"}));
  Commit(fixture.stores, index++, Register(2, {"tls://10.0.0.2:17001"}));
  Commit(fixture.stores, index++, Register(3, {"10.0.0.3:7002"}));
  keylane::meta::RetireNode retire;
  retire.request_id_ = Bytes<16>(0x03);
  retire.node_id_ = NodeId(3);
  retire.expected_revision_ = 1;
  Commit(fixture.stores, index++, retire);

  keylane::meta::CreateGroup create;
  create.request_id_ = Bytes<16>(0x10);
  create.group_id_ = "group-a";
  create.new_topology_epoch_ = 1;
  Commit(fixture.stores, index++, create);

  keylane::meta::AssignNodeToGroup assign_source;
  assign_source.request_id_ = Bytes<16>(0x11);
  assign_source.group_id_ = "group-a";
  assign_source.node_id_ = fixture.source;
  assign_source.assignment_id_ = fixture.source_assignment;
  assign_source.role_ = keylane::meta::MetaNodeRole::kReplica;
  assign_source.expected_revision_ = 1;
  assign_source.new_topology_epoch_ = 2;
  Commit(fixture.stores, index++, assign_source);

  keylane::meta::AssignNodeToGroup assign_target;
  assign_target.request_id_ = Bytes<16>(0x12);
  assign_target.group_id_ = "group-a";
  assign_target.node_id_ = fixture.target;
  assign_target.assignment_id_ = fixture.target_assignment;
  assign_target.role_ = keylane::meta::MetaNodeRole::kPrimary;
  assign_target.expected_revision_ = 2;
  assign_target.new_topology_epoch_ = 3;
  Commit(fixture.stores, index++, assign_target);

  keylane::meta::PutPopulationManifest put_manifest;
  put_manifest.request_id_ = Bytes<16>(0x13);
  put_manifest.entries_ = {{1, 11}, {2, 22}, {7, 77}};
  put_manifest.manifest_digest_ =
      keylane::meta::MetaPopulationManifestStore::CanonicalDigest(
          put_manifest.entries_);
  fixture.manifest_digest = put_manifest.manifest_digest_;
  Commit(fixture.stores, index++, put_manifest);

  keylane::meta::SetGroupReplicationState replication;
  replication.request_id_ = Bytes<16>(0x14);
  replication.group_id_ = "group-a";
  replication.new_population_manifest_revision_ = 1;
  replication.new_population_manifest_digest_ = fixture.manifest_digest;
  replication.new_partition_replication_epoch_ = 1;
  replication.new_topology_epoch_ = 4;
  Commit(fixture.stores, index++, replication);

  Commit(fixture.stores, index++,
         PutPolicy(0x15, "lease-policy", 3, "lease-policy-v3"));
  Commit(fixture.stores, index++,
         PutPolicy(0x16, "operation-policy", 2, "operation-policy-v2"));
  Commit(fixture.stores, index++,
         PutPolicy(0x17, "unused-policy", 1, "not-referenced"));

  keylane::meta::BeginGroupTerm begin;
  begin.request_id_ = Bytes<16>(0x18);
  begin.group_id_ = "group-a";
  begin.expected_term_ = 0;
  begin.new_term_ = 1;
  Commit(fixture.stores, index++, begin);

  keylane::meta::SetSlotMap slots;
  slots.request_id_ = Bytes<16>(0x19);
  slots.ranges_ = {{0, 9, "group-a"}, {20, 29, "group-a"}, {30, 30, "group-a"}};
  slots.new_topology_epoch_ = 5;
  slots.config_epochs_ = {{"group-a", 11}};
  Commit(fixture.stores, index++, slots);

  keylane::meta::ActivateAuthority activate;
  activate.request_id_ = Bytes<16>(0x1a);
  activate.group_id_ = "group-a";
  activate.expected_term_ = 1;
  activate.new_owner_ = fixture.target;
  activate.grant_.lease_duration_ms_ = 5000;
  activate.grant_.policy_id_ = "lease-policy";
  activate.grant_.policy_version_ = 3;
  activate.new_authority_version_ = 1;
  activate.new_topology_epoch_ = 6;
  activate.new_config_epoch_ = 12;
  fixture.grant_revision = index;
  Commit(fixture.stores, index++, activate);

  keylane::meta::CreateGroup create_empty;
  create_empty.request_id_ = Bytes<16>(0x1b);
  create_empty.group_id_ = "group-empty";
  create_empty.new_topology_epoch_ = 7;
  Commit(fixture.stores, index++, create_empty);

  keylane::meta::SubmitOperation submit;
  submit.request_id_ = Bytes<16>(0x1c);
  submit.operation_id_ = fixture.operation_id;
  submit.kind_ = "population-rebuild";
  submit.intent_ = "rebuild group-a";
  submit.intent_hash_ = keylane::meta::MetaSha256(submit.intent_);
  submit.policy_references_ = {{"operation-policy", 2}};
  Commit(fixture.stores, index++, submit);

  keylane::meta::MetaDirectiveSpec directive;
  directive.directive_id_ = fixture.directive_id;
  directive.attempt_id_ = fixture.attempt_id;
  directive.recipient_node_id_ = fixture.target;
  directive.target_node_id_ = fixture.target;
  directive.target_boot_id_ = fixture.target_boot;
  directive.assignment_id_ = fixture.target_assignment;
  directive.source_node_id_ = fixture.source;
  directive.source_assignment_id_ = fixture.source_assignment;
  directive.source_boot_id_ = fixture.source_boot;
  directive.source_replication_history_id_ = fixture.source_history;
  directive.group_id_ = "group-a";
  directive.group_term_ = 1;
  directive.authority_version_ = 1;
  directive.grant_revision_ = fixture.grant_revision;
  directive.population_manifest_revision_ = 1;
  directive.population_manifest_digest_ = fixture.manifest_digest;
  directive.partition_replication_epoch_ = 1;
  directive.kind_ = "rebuild";
  directive.storage_mutating_ = true;

  keylane::meta::TransitionOperationPhase transition;
  transition.request_id_ = Bytes<16>(0x1d);
  transition.operation_id_ = fixture.operation_id;
  transition.kind_phase_blob_ = "dispatch";
  transition.current_directives_ = {directive};
  fixture.directive_revision = index;
  Commit(fixture.stores, index++, transition);
  return fixture;
}

bool NonZero(const control::WireHash256& hash) {
  for (const std::uint8_t byte : hash) {
    if (byte != 0) return true;
  }
  return false;
}

TEST(MetaControlProjector, ProjectsRegisteredNodeBeforeAnyGroupExists) {
  MetaStores stores;
  const auto registration = Register(1, {"tcp://10.0.0.1:7000"});
  Commit(stores, 1, registration);

  const auto projected = MetaControlProjector::ProjectNode(
      MetaCommittedView(std::move(stores), 1), registration.node_id_);
  ASSERT_TRUE(projected.ok()) << projected.status();
  EXPECT_EQ(projected->full_state.topology_epoch, 0u);
  EXPECT_TRUE(projected->full_state.groups.empty());
  ASSERT_EQ(projected->full_state.nodes.size(), 1u);
  EXPECT_EQ(projected->full_state.nodes.front().node_id, registration.node_id_);
}

TEST(MetaControlProjector, ProjectsCompleteCanonicalStateForOneNode) {
  const Fixture fixture = CompleteFixture();
  const MetaCommittedView view(fixture.stores, 99);

  const auto projected =
      MetaControlProjector::ProjectNode(view, fixture.target);
  ASSERT_TRUE(projected.ok()) << projected.status();
  const control::FullDesiredState& state = projected->full_state;

  EXPECT_EQ(state.source_meta_applied_index, 99u);
  EXPECT_EQ(state.topology_epoch, 7u);
  ASSERT_EQ(state.meta_directory.size(), 2u);
  EXPECT_EQ(
      state.meta_directory[0],
      (control::WireMetaEndpoint{1, "10.0.0.11", 7100, "keylane://meta/1"}));
  EXPECT_EQ(
      state.meta_directory[1],
      (control::WireMetaEndpoint{2, "10.0.0.12", 7200, "keylane://meta/2"}));

  ASSERT_EQ(state.nodes.size(), 2u);
  EXPECT_EQ(state.nodes[0], (control::WireDataEndpoint{
                                fixture.target, "10.0.0.1", 7000, 17000}));
  EXPECT_EQ(state.nodes[1],
            (control::WireDataEndpoint{fixture.source, "10.0.0.2", 0, 17001}));

  ASSERT_EQ(state.groups.size(), 2u);
  const control::WireDesiredGroup& group = state.groups[0];
  EXPECT_EQ(group.group_id, "group-a");
  ASSERT_EQ(group.members.size(), 2u);
  EXPECT_EQ(group.members[0].node_id, fixture.target);
  EXPECT_EQ(group.members[0].assignment_id, fixture.target_assignment);
  EXPECT_EQ(group.members[1].node_id, fixture.source);
  EXPECT_EQ(group.members[1].assignment_id, fixture.source_assignment);
  EXPECT_EQ(group.owner_node_id, fixture.target);
  EXPECT_EQ(group.owner_assignment_id, fixture.target_assignment);
  EXPECT_EQ(group.group_term, 1u);
  EXPECT_EQ(group.authority_version, 1u);
  EXPECT_EQ(group.grant_revision, fixture.grant_revision);
  EXPECT_EQ(group.grant_duration_ms, 5000u);
  EXPECT_TRUE(group.grant_active);
  EXPECT_EQ(group.config_epoch, 12u);
  EXPECT_EQ(group.slot_ranges,
            (std::vector<control::WireSlotRange>{{0, 9}, {20, 30}}));
  EXPECT_EQ(group.manifest_revision, 1u);
  EXPECT_EQ(group.manifest_digest, fixture.manifest_digest);
  EXPECT_EQ(group.partition_replication_epoch, 1u);
  EXPECT_EQ(group.grant_policy_id, "lease-policy");
  EXPECT_EQ(group.grant_policy_version, 3u);

  const control::WireDesiredGroup& empty_group = state.groups[1];
  EXPECT_EQ(empty_group.group_id, "group-empty");
  EXPECT_FALSE(empty_group.owner_node_id.has_value());
  EXPECT_FALSE(empty_group.owner_assignment_id.has_value());
  EXPECT_FALSE(empty_group.grant_active);

  ASSERT_EQ(state.manifests.size(), 1u);
  EXPECT_EQ(state.manifests[0].revision, 1u);
  EXPECT_EQ(state.manifests[0].digest, fixture.manifest_digest);
  EXPECT_EQ(
      state.manifests[0].entries,
      (std::vector<control::WireManifestEntry>{{1, 11}, {2, 22}, {7, 77}}));

  ASSERT_EQ(state.policies.size(), 2u);
  EXPECT_EQ(state.policies[0].policy_id, "lease-policy");
  EXPECT_EQ(state.policies[0].version, 3u);
  EXPECT_EQ(state.policies[1].policy_id, "operation-policy");
  EXPECT_EQ(state.policies[1].version, 2u);

  ASSERT_EQ(state.current_directives.size(), 1u);
  const control::WireProjectedDirective& directive =
      state.current_directives[0];
  EXPECT_EQ(directive.basis.source_meta_applied_index, 99u);
  EXPECT_EQ(directive.basis.projection_hash, state.projection_hash);
  EXPECT_EQ(directive.authority.group_id, "group-a");
  EXPECT_EQ(directive.authority.assignment_id, fixture.target_assignment);
  EXPECT_EQ(directive.authority.group_term, 1u);
  EXPECT_EQ(directive.authority.authority_version, 1u);
  EXPECT_EQ(directive.authority.grant_revision, fixture.grant_revision);
  EXPECT_EQ(directive.identity.operation_id, fixture.operation_id);
  EXPECT_EQ(directive.identity.directive_id, fixture.directive_id);
  EXPECT_EQ(directive.identity.attempt_id, fixture.attempt_id);
  EXPECT_EQ(directive.identity.directive_revision, fixture.directive_revision);
  EXPECT_EQ(directive.recipient_node_id, fixture.target);
  EXPECT_EQ(directive.recipient_boot_id,
            "7172737475767778797a7b7c7d7e7f8081828384");
  EXPECT_EQ(directive.target_node_id, fixture.target);
  EXPECT_EQ(directive.target_boot_id,
            "7172737475767778797a7b7c7d7e7f8081828384");
  EXPECT_EQ(directive.source_node_id, fixture.source);
  EXPECT_EQ(directive.source_boot_id,
            "8182838485868788898a8b8c8d8e8f9091929394");
  EXPECT_EQ(directive.source_replication_history_id,
            "9192939495969798999a9b9c9d9e9fa0a1a2a3a4");
  EXPECT_EQ(directive.manifest_revision, 1u);
  EXPECT_EQ(directive.manifest_digest, fixture.manifest_digest);
  EXPECT_EQ(directive.partition_replication_epoch, 1u);
  EXPECT_EQ(directive.kind, control::WireDirectiveKind::kRebuild);
  EXPECT_TRUE(directive.payload.empty());
  EXPECT_TRUE(directive.preconditions.empty());
  EXPECT_TRUE(directive.storage_mutating);
  EXPECT_FALSE(directive.force);

  EXPECT_TRUE(NonZero(state.directive_set_digest));
  EXPECT_TRUE(NonZero(state.projection_hash));
  EXPECT_TRUE(NonZero(state.object_hash));
  EXPECT_EQ(state.object_hash,
            control::ComputeSha256(projected->encoded_full_state));
  const auto decoded =
      control::DecodeFullDesiredState(projected->encoded_full_state);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(*decoded, state);
}

TEST(MetaControlProjector, ProjectedManifestPassesDataPlaneValidation) {
  const Fixture fixture = CompleteFixture();
  const auto projected = MetaControlProjector::ProjectNode(
      MetaCommittedView(fixture.stores, 99), fixture.target);
  ASSERT_TRUE(projected.ok()) << projected.status();

  const auto prepared = keylane::cluster::PrepareMetaFullState(
      projected->full_state, fixture.target, 4);
  ASSERT_TRUE(prepared.ok()) << prepared.status();
  ASSERT_NE(prepared->serving_state_, nullptr);
  ASSERT_EQ(prepared->control_groups_.size(), 2u);
  EXPECT_EQ(prepared->control_groups_.front().manifest_digest_,
            fixture.manifest_digest);
}

TEST(MetaControlProjector,
     ProjectionHashIgnoresDiagnosticIndexAndUnreferencedDocuments) {
  const Fixture fixture = CompleteFixture();
  const auto first = MetaControlProjector::ProjectNode(
      MetaCommittedView(fixture.stores, 99), fixture.target);
  const auto later = MetaControlProjector::ProjectNode(
      MetaCommittedView(fixture.stores, 1000), fixture.target);
  ASSERT_TRUE(first.ok()) << first.status();
  ASSERT_TRUE(later.ok()) << later.status();

  EXPECT_EQ(first->full_state.projection_hash,
            later->full_state.projection_hash);
  EXPECT_EQ(first->full_state.directive_set_digest,
            later->full_state.directive_set_digest);
  EXPECT_NE(first->full_state.object_hash, later->full_state.object_hash);
  EXPECT_NE(first->encoded_full_state, later->encoded_full_state);
  ASSERT_EQ(later->full_state.current_directives.size(), 1u);
  EXPECT_EQ(
      later->full_state.current_directives[0].basis.source_meta_applied_index,
      1000u);
  EXPECT_EQ(later->full_state.current_directives[0].basis.projection_hash,
            later->full_state.projection_hash);

  MetaStores with_unreferenced_change = fixture.stores;
  ASSERT_TRUE(with_unreferenced_change.policy_
                  .Apply(PutPolicy(0x70, "unused-policy", 2,
                                   "new-unreferenced-version"))
                  .ok());
  const auto changed = MetaControlProjector::ProjectNode(
      MetaCommittedView(std::move(with_unreferenced_change), 1001),
      fixture.target);
  ASSERT_TRUE(changed.ok()) << changed.status();
  EXPECT_EQ(changed->full_state.projection_hash,
            first->full_state.projection_hash);
  EXPECT_EQ(changed->full_state.policies, first->full_state.policies);
}

TEST(MetaControlProjector,
     SupportsLegacyEndpointsAndFiltersDirectivesForAnotherNode) {
  Fixture fixture = CompleteFixture();
  ASSERT_TRUE(
      ReplaceEndpoints(fixture.stores, 2, {"10.0.0.2:7001", "10.0.0.2:17001"})
          .ok());

  const auto projected = MetaControlProjector::ProjectNode(
      MetaCommittedView(std::move(fixture.stores), 101), fixture.source);
  ASSERT_TRUE(projected.ok()) << projected.status();
  ASSERT_EQ(projected->full_state.nodes.size(), 2u);
  EXPECT_EQ(
      projected->full_state.nodes[1],
      (control::WireDataEndpoint{fixture.source, "10.0.0.2", 7001, 17001}));
  EXPECT_TRUE(projected->full_state.current_directives.empty());
  ASSERT_EQ(projected->full_state.policies.size(), 1u);
  EXPECT_EQ(projected->full_state.policies[0].policy_id, "lease-policy");
}

TEST(MetaControlProjector,
     GrantlessGroupDoesNotProjectHistoricalOwnerAfterMemberRemoval) {
  Fixture fixture = CompleteFixture();

  keylane::meta::RevokeGrant revoke;
  revoke.request_id_ = Bytes<16>(0x78);
  revoke.group_id_ = "group-a";
  revoke.expected_term_ = 1;
  Commit(fixture.stores, 21, revoke);

  keylane::meta::RemoveNodeFromGroup remove;
  remove.request_id_ = Bytes<16>(0x79);
  remove.group_id_ = "group-a";
  remove.node_id_ = fixture.target;
  remove.expected_revision_ = 3;
  remove.new_topology_epoch_ = 8;
  Commit(fixture.stores, 22, remove);

  const auto projected = MetaControlProjector::ProjectNode(
      MetaCommittedView(std::move(fixture.stores), 22), fixture.source);
  ASSERT_TRUE(projected.ok()) << projected.status();
  ASSERT_EQ(projected->full_state.groups.size(), 2u);
  const control::WireDesiredGroup& group = projected->full_state.groups[0];
  EXPECT_EQ(group.group_id, "group-a");
  EXPECT_FALSE(group.grant_active);
  EXPECT_FALSE(group.owner_node_id.has_value());
  EXPECT_FALSE(group.owner_assignment_id.has_value());
}

TEST(MetaControlProjector,
     FencedGroupProjectsCommittedOwnerRoleWithoutAnActiveGrant) {
  Fixture fixture = CompleteFixture();

  keylane::meta::RevokeGrant revoke;
  revoke.request_id_ = Bytes<16>(0x78);
  revoke.group_id_ = "group-a";
  revoke.expected_term_ = 1;
  Commit(fixture.stores, 21, revoke);

  const auto projected = MetaControlProjector::ProjectNode(
      MetaCommittedView(std::move(fixture.stores), 21), fixture.source);
  ASSERT_TRUE(projected.ok()) << projected.status();
  const control::WireDesiredGroup& group = projected->full_state.groups[0];
  EXPECT_FALSE(group.grant_active);
  EXPECT_EQ(group.owner_node_id, fixture.target);
  EXPECT_EQ(group.owner_assignment_id, fixture.target_assignment);
}

TEST(MetaControlProjector,
     RoutesSourceActionsToSourceWithoutRebindingTheRebuildTarget) {
  Fixture fixture = CompleteFixture();
  const auto operation =
      fixture.stores.operation_.FindOperation(fixture.operation_id);
  ASSERT_TRUE(operation.has_value());
  ASSERT_EQ(operation->current_directives_.size(), 1u);

  keylane::meta::MetaDirectiveSpec rebuild =
      operation->current_directives_[0].spec_;
  rebuild.directive_id_ = Bytes<16>(0x54);
  rebuild.attempt_id_ = Bytes<16>(0x64);
  keylane::meta::MetaDirectiveSpec authorize =
      operation->current_directives_[0].spec_;
  authorize.directive_id_ = Bytes<16>(0x52);
  authorize.attempt_id_ = Bytes<16>(0x62);
  authorize.recipient_node_id_ = fixture.source;
  authorize.kind_ = "authorize-source";
  authorize.storage_mutating_ = false;
  keylane::meta::MetaDirectiveSpec revoke = authorize;
  revoke.directive_id_ = Bytes<16>(0x53);
  revoke.attempt_id_ = Bytes<16>(0x63);
  revoke.kind_ = "revoke-sources";

  keylane::meta::TransitionOperationPhase transition;
  transition.request_id_ = Bytes<16>(0x77);
  transition.operation_id_ = fixture.operation_id;
  transition.expected_revision_ = 1;
  transition.kind_phase_blob_ = "source-actions";
  transition.current_directives_ = {rebuild, authorize, revoke};
  Commit(fixture.stores, 21, transition);

  const auto target = MetaControlProjector::ProjectNode(
      MetaCommittedView(fixture.stores, 104), fixture.target);
  ASSERT_TRUE(target.ok()) << target.status();
  ASSERT_EQ(target->full_state.current_directives.size(), 1u);
  const control::WireProjectedDirective& projected_rebuild =
      target->full_state.current_directives.front();
  EXPECT_EQ(projected_rebuild.kind, control::WireDirectiveKind::kRebuild);
  EXPECT_EQ(projected_rebuild.recipient_node_id, fixture.target);
  EXPECT_EQ(projected_rebuild.identity.directive_revision, 21u);

  const auto source = MetaControlProjector::ProjectNode(
      MetaCommittedView(std::move(fixture.stores), 104), fixture.source);
  ASSERT_TRUE(source.ok()) << source.status();
  ASSERT_EQ(source->full_state.current_directives.size(), 2u);
  for (const control::WireProjectedDirective& directive :
       source->full_state.current_directives) {
    EXPECT_NE(directive.identity.directive_id,
              projected_rebuild.identity.directive_id);
    EXPECT_NE(directive.identity.attempt_id,
              projected_rebuild.identity.attempt_id);
    EXPECT_EQ(directive.identity.directive_revision, 21u);
    EXPECT_EQ(directive.manifest_revision, projected_rebuild.manifest_revision);
    EXPECT_EQ(directive.manifest_digest, projected_rebuild.manifest_digest);
    EXPECT_EQ(directive.partition_replication_epoch,
              projected_rebuild.partition_replication_epoch);
    EXPECT_EQ(directive.recipient_node_id, fixture.source);
    EXPECT_EQ(directive.recipient_boot_id,
              "8182838485868788898a8b8c8d8e8f9091929394");
    EXPECT_EQ(directive.target_node_id, fixture.target);
    EXPECT_EQ(directive.target_boot_id,
              "7172737475767778797a7b7c7d7e7f8081828384");
    EXPECT_EQ(directive.source_node_id, fixture.source);
    EXPECT_EQ(directive.source_assignment_id, fixture.source_assignment);
    EXPECT_EQ(directive.source_boot_id,
              "8182838485868788898a8b8c8d8e8f9091929394");
  }
}

TEST(MetaControlProjector,
     DurableStoreRejectsAmbiguousOrUndialableDataEndpoints) {
  const Fixture fixture = CompleteFixture();
  const std::vector<std::vector<std::string>> invalid_endpoints = {
      {},
      {"10.0.0.1:7000", "10.0.0.1:17000", "10.0.0.1:27000"},
      {"tcp://10.0.0.1:7000", "tcp://10.0.0.1:7001"},
      {"tls://10.0.0.1:0"},
      {"tls://data.example:17000"},
      {"tcp://2001:db8::1:7000"},
      {"tcp://10.0.0.1:7000", "tls://10.0.0.2:17000"},
      {"tcp://10.0.0.1:7000", "10.0.0.1:17000"},
  };
  for (const auto& endpoints : invalid_endpoints) {
    SCOPED_TRACE(testing::PrintToString(endpoints));
    MetaStores stores = fixture.stores;
    EXPECT_FALSE(ReplaceEndpoints(stores, 1, endpoints).ok());
    // A rejected update cannot poison the global projection for every node.
    const auto projected = MetaControlProjector::ProjectNode(
        MetaCommittedView(std::move(stores), 102), fixture.target);
    EXPECT_TRUE(projected.ok()) << projected.status();
  }
}

TEST(MetaControlProjector,
     TransitionGuardAndProjectorAllowOnlyExplicitDirectiveStorageClasses) {
  const Fixture fixture = CompleteFixture();
  struct DirectiveCase {
    std::string_view kind;
    bool storage_mutating;
    std::optional<control::WireDirectiveKind> expected;
  };
  const std::vector<DirectiveCase> cases = {
      {"rebuild", true, control::WireDirectiveKind::kRebuild},
      {"authorize-source", false, control::WireDirectiveKind::kAuthorizeSource},
      {"revoke-sources", false, control::WireDirectiveKind::kRevokeSources},
      {"unknown", false, std::nullopt},
      {"rebuild", false, std::nullopt},
      {"authorize-source", true, std::nullopt},
      {"revoke-sources", true, std::nullopt},
  };

  for (const DirectiveCase& test : cases) {
    SCOPED_TRACE(test.kind);
    MetaStores stores = fixture.stores;
    const auto operation =
        stores.operation_.FindOperation(fixture.operation_id);
    ASSERT_TRUE(operation.has_value());
    ASSERT_EQ(operation->current_directives_.size(), 1u);
    keylane::meta::MetaDirectiveSpec directive =
        operation->current_directives_[0].spec_;
    directive.kind_ = test.kind;
    directive.storage_mutating_ = test.storage_mutating;
    if (test.kind == "authorize-source" || test.kind == "revoke-sources") {
      directive.recipient_node_id_ = fixture.source;
    }
    keylane::meta::TransitionOperationPhase transition;
    transition.request_id_ = Bytes<16>(0x76);
    transition.operation_id_ = fixture.operation_id;
    transition.expected_revision_ = 1;
    transition.kind_phase_blob_ = "redispatch";
    transition.current_directives_ = {std::move(directive)};
    const MetaApplyResult applied =
        ApplyCommitted(stores, 21, transition, kActor, kTime);
    if (!test.expected.has_value()) {
      EXPECT_EQ(applied.verdict_, MetaAuditVerdict::kRejected);
      EXPECT_EQ(
          stores.operation_.FindOperation(fixture.operation_id)->revision_, 1u);
      continue;
    }
    ASSERT_EQ(applied.verdict_, MetaAuditVerdict::kAccepted) << applied.detail_;

    const std::string& recipient =
        test.kind == "authorize-source" || test.kind == "revoke-sources"
            ? fixture.source
            : fixture.target;
    const auto projected = MetaControlProjector::ProjectNode(
        MetaCommittedView(std::move(stores), 103), recipient);
    ASSERT_TRUE(projected.ok()) << projected.status();
    ASSERT_EQ(projected->full_state.current_directives.size(), 1u);
    EXPECT_EQ(projected->full_state.current_directives[0].kind, *test.expected);
  }
}

}  // namespace
