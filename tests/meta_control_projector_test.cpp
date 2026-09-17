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
#include "lavik/cluster/control_protocol.h"
#include "lavik/cluster/meta_control.h"
#include "lavik/meta/commands.h"
#include "lavik/meta/control_projector.h"
#include "lavik/meta/hash.h"
#include "lavik/meta/policy_store.h"
#include "lavik/meta/population_manifest_store.h"
#include "lavik/meta/state_apply.h"
#include "meta_topology_test_access.h"

namespace {

namespace control = lavik::cluster::control;
using lavik::meta::ApplyCommitted;
using lavik::meta::MetaApplyResult;
using lavik::meta::MetaAuditVerdict;
using lavik::meta::MetaCommand;
using lavik::meta::MetaCommittedView;
using lavik::meta::MetaControlProjector;
using lavik::meta::MetaStores;

constexpr std::string_view kActor = "lavik://operator/projector-test";
constexpr std::string_view kTime = "2026-09-07T00:00:00Z";

template <std::size_t N>
std::array<std::uint8_t, N> Bytes(std::uint8_t seed) {
  std::array<std::uint8_t, N> result{};
  for (std::size_t i = 0; i < result.size(); ++i) {
    result[i] = static_cast<std::uint8_t>(seed + i);
  }
  return result;
}

template <std::size_t N>
std::string Hex(const std::array<std::uint8_t, N>& bytes) {
  constexpr std::string_view kHex = "0123456789abcdef";
  std::string result(bytes.size() * 2, '0');
  for (std::size_t i = 0; i < bytes.size(); ++i) {
    result[i * 2] = kHex[bytes[i] >> 4];
    result[i * 2 + 1] = kHex[bytes[i] & 0x0f];
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

lavik::meta::RegisterNode Register(std::uint8_t node,
                                   std::vector<std::string> endpoints) {
  lavik::meta::RegisterNode command;
  command.request_id_ = Bytes<16>(node);
  command.node_id_ = NodeId(node);
  command.principal_ = "lavik://node/" + command.node_id_;
  command.endpoints_ = std::move(endpoints);
  command.role_ = lavik::meta::MetaNodeRole::kPrimary;
  return command;
}

lavik::meta::PutPolicy PutPolicy(std::uint8_t request_seed,
                                 std::string policy_id, std::uint64_t version,
                                 std::string content) {
  lavik::meta::PutPolicy command;
  command.request_id_ = Bytes<16>(request_seed);
  command.policy_id_ = std::move(policy_id);
  command.version_ = version;
  command.content_ = std::move(content);
  return command;
}

absl::Status ReplaceEndpoints(MetaStores& stores, std::uint8_t node,
                              std::vector<std::string> endpoints) {
  lavik::meta::UpdateNode update;
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
  lavik::meta::MetaAssignmentId target_assignment = Bytes<16>(0x21);
  lavik::meta::MetaAssignmentId source_assignment = Bytes<16>(0x31);
  lavik::meta::MetaHash256 manifest_digest{};
  lavik::meta::MetaOperationId operation_id = Bytes<16>(0x41);
  lavik::meta::MetaDirectiveId directive_id = Bytes<16>(0x51);
  lavik::meta::MetaAttemptId attempt_id = Bytes<16>(0x61);
  lavik::meta::MetaBootIncarnation target_boot = Bytes<20>(0x71);
  lavik::meta::MetaBootIncarnation source_boot = Bytes<20>(0x81);
  lavik::meta::MetaReplicationHistoryId source_history = Bytes<20>(0x91);
  std::uint64_t directive_revision = 0;
};

Fixture CompleteFixture(std::string operation_kind = "population-rebuild") {
  Fixture fixture;
  std::uint64_t index = 1;

  Commit(fixture.stores, index++,
         PutPolicy(0x15, std::string(lavik::meta::kAuthorityLeasePolicyId), 1,
                   R"({"kind":"authority-lease-v1","duration_ms":5000})"));
  Commit(
      fixture.stores, index++,
      PutPolicy(
          0x16,
          std::string(lavik::meta::kAutomaticUncontrolledFailoverPolicyId), 1,
          R"({"kind":"automatic-uncontrolled-failover-v1","suspect_after_ms":5000})"));

  lavik::meta::BindMetaMember meta2;
  meta2.request_id_ = Bytes<16>(0x02);
  meta2.server_id_ = 2;
  meta2.principal_ = "lavik://meta/2";
  meta2.data_control_endpoint_ = "10.0.0.12:7200";
  Commit(fixture.stores, index++, meta2);
  lavik::meta::BindMetaMember meta1;
  meta1.request_id_ = Bytes<16>(0x01);
  meta1.server_id_ = 1;
  meta1.principal_ = "lavik://meta/1";
  meta1.data_control_endpoint_ = "10.0.0.11:7100";
  Commit(fixture.stores, index++, meta1);

  Commit(fixture.stores, index++,
         Register(1, {"tls://10.0.0.1:17000", "tcp://10.0.0.1:7000"}));
  Commit(fixture.stores, index++, Register(2, {"tls://10.0.0.2:17001"}));
  Commit(fixture.stores, index++, Register(3, {"10.0.0.3:7002"}));
  lavik::meta::RetireNode retire;
  retire.request_id_ = Bytes<16>(0x03);
  retire.node_id_ = NodeId(3);
  retire.expected_revision_ = 1;
  Commit(fixture.stores, index++, retire);

  lavik::meta::CreateGroup create;
  create.request_id_ = Bytes<16>(0x10);
  create.group_id_ = "group-a";
  create.new_topology_epoch_ = 1;
  Commit(fixture.stores, index++, create);

  lavik::meta::AssignNodeToGroup assign_source;
  assign_source.request_id_ = Bytes<16>(0x11);
  assign_source.group_id_ = "group-a";
  assign_source.node_id_ = fixture.source;
  assign_source.assignment_id_ = fixture.source_assignment;
  assign_source.role_ = lavik::meta::MetaNodeRole::kReplica;
  assign_source.expected_revision_ = 1;
  assign_source.new_topology_epoch_ = 2;
  Commit(fixture.stores, index++, assign_source);

  lavik::meta::AssignNodeToGroup assign_target;
  assign_target.request_id_ = Bytes<16>(0x12);
  assign_target.group_id_ = "group-a";
  assign_target.node_id_ = fixture.target;
  assign_target.assignment_id_ = fixture.target_assignment;
  assign_target.role_ = lavik::meta::MetaNodeRole::kPrimary;
  assign_target.expected_revision_ = 2;
  assign_target.new_topology_epoch_ = 3;
  Commit(fixture.stores, index++, assign_target);

  lavik::meta::PutPopulationManifest put_manifest;
  put_manifest.request_id_ = Bytes<16>(0x13);
  put_manifest.entries_ = {{1, 11}, {2, 22}, {7, 77}};
  put_manifest.manifest_digest_ =
      lavik::meta::MetaPopulationManifestStore::CanonicalDigest(
          put_manifest.entries_);
  fixture.manifest_digest = put_manifest.manifest_digest_;
  Commit(fixture.stores, index++, put_manifest);

  lavik::meta::SetGroupReplicationState replication;
  replication.request_id_ = Bytes<16>(0x14);
  replication.group_id_ = "group-a";
  replication.new_population_manifest_revision_ = 1;
  replication.new_population_manifest_digest_ = fixture.manifest_digest;
  replication.new_partition_replication_epoch_ = 1;
  replication.new_topology_epoch_ = 4;
  Commit(fixture.stores, index++, replication);

  lavik::meta::BeginGroupTerm begin;
  begin.request_id_ = Bytes<16>(0x18);
  begin.group_id_ = "group-a";
  begin.expected_term_ = 0;
  begin.new_term_ = 1;
  Commit(fixture.stores, index++, begin);

  lavik::meta::SetSlotMap slots;
  slots.request_id_ = Bytes<16>(0x19);
  slots.ranges_ = {{0, 9, "group-a"}, {20, 29, "group-a"}, {30, 30, "group-a"}};
  slots.new_topology_epoch_ = 5;
  Commit(fixture.stores, index++, slots);

  lavik::meta::ActivateAuthority activate;
  activate.request_id_ = Bytes<16>(0x1a);
  activate.group_id_ = "group-a";
  activate.expected_term_ = 1;
  activate.new_owner_ = fixture.target;
  activate.new_topology_epoch_ = 6;
  Commit(fixture.stores, index++, activate);

  lavik::meta::CreateGroup create_empty;
  create_empty.request_id_ = Bytes<16>(0x1b);
  create_empty.group_id_ = "group-empty";
  create_empty.new_topology_epoch_ = 7;
  Commit(fixture.stores, index++, create_empty);

  lavik::meta::SubmitOperation submit;
  submit.request_id_ = Bytes<16>(0x1c);
  submit.operation_id_ = fixture.operation_id;
  submit.kind_ = std::move(operation_kind);
  submit.intent_ = "rebuild group-a";
  submit.intent_hash_ = lavik::meta::MetaSha256(submit.intent_);
  submit.replication_history_id_ = Bytes<20>(0x33);
  Commit(fixture.stores, index++, submit);

  lavik::meta::MetaDirectiveSpec directive;
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
  directive.population_manifest_revision_ = 1;
  directive.population_manifest_digest_ = fixture.manifest_digest;
  directive.partition_replication_epoch_ = 1;
  directive.kind_ = "rebuild";
  directive.payload_ = *lavik::cluster::control::EncodeRebuildRequest({3});

  lavik::meta::TransitionOperationPhase transition;
  transition.request_id_ = Bytes<16>(0x1d);
  transition.operation_id_ = fixture.operation_id;
  transition.kind_phase_blob_ = "dispatch";
  transition.current_directives_ = {directive};
  fixture.directive_revision = index;
  Commit(fixture.stores, index++, transition);
  return fixture;
}

TEST(MetaControlProjector, ProjectsRegisteredNodeBeforeAnyGroupExists) {
  MetaStores stores;
  Commit(stores, 1,
         PutPolicy(0x15, std::string(lavik::meta::kAuthorityLeasePolicyId), 1,
                   R"({"kind":"authority-lease-v1","duration_ms":3000})"));
  const auto registration = Register(1, {"tcp://10.0.0.1:7000"});
  Commit(stores, 2, registration);

  const auto projected = MetaControlProjector::ProjectNode(
      MetaCommittedView(std::move(stores), 2), registration.node_id_);
  ASSERT_TRUE(projected.ok()) << projected.status();
  EXPECT_EQ(projected->full_state.topology_epoch, 0u);
  EXPECT_EQ(projected->full_state.authority_lease_duration_ms, 3000u);
  EXPECT_EQ(control::DataHeartbeatIntervalMs(
                projected->full_state.authority_lease_duration_ms),
            1000u);
  EXPECT_TRUE(projected->full_state.groups.empty());
  ASSERT_EQ(projected->full_state.nodes.size(), 1u);
  EXPECT_EQ(projected->full_state.nodes.front().node_id, registration.node_id_);
}

TEST(MetaControlProjector, MissingAuthorityLeasePolicyFailsClosed) {
  MetaStores stores;
  const auto registration = Register(1, {"tcp://10.0.0.1:7000"});
  ASSERT_TRUE(stores.identity_.Apply(registration).ok());

  const auto projected = MetaControlProjector::ProjectNode(
      MetaCommittedView(std::move(stores), 1), registration.node_id_);
  ASSERT_FALSE(projected.ok());
  EXPECT_EQ(projected.status().code(), absl::StatusCode::kFailedPrecondition);
  EXPECT_NE(projected.status().message().find("Authority Lease Policy"),
            std::string_view::npos);
}

TEST(MetaControlProjector, ProjectsCompleteCanonicalStateForOneNode) {
  const Fixture fixture = CompleteFixture();
  const MetaCommittedView view(fixture.stores, 99);

  const auto projected =
      MetaControlProjector::ProjectNode(view, fixture.target);
  ASSERT_TRUE(projected.ok()) << projected.status();
  const control::FullDesiredState& state = projected->full_state;

  EXPECT_EQ(state.control_revision, 99u);
  EXPECT_EQ(state.topology_epoch, 7u);
  EXPECT_EQ(state.authority_lease_duration_ms, 5000u);
  EXPECT_EQ(control::DataHeartbeatIntervalMs(state.authority_lease_duration_ms),
            1666u);
  ASSERT_EQ(state.meta_directory.size(), 2u);
  EXPECT_EQ(
      state.meta_directory[0],
      (control::WireMetaEndpoint{1, "10.0.0.11", 7100, "lavik://meta/1"}));
  EXPECT_EQ(
      state.meta_directory[1],
      (control::WireMetaEndpoint{2, "10.0.0.12", 7200, "lavik://meta/2"}));

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
  EXPECT_TRUE(group.grant_active);
  EXPECT_EQ(group.slot_ranges,
            (std::vector<control::WireSlotRange>{{0, 9}, {20, 30}}));
  EXPECT_EQ(group.manifest_revision, 1u);
  EXPECT_EQ(group.manifest_digest, fixture.manifest_digest);
  EXPECT_EQ(group.partition_replication_epoch, 1u);

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

  ASSERT_EQ(state.current_directives.size(), 1u);
  const control::WireProjectedDirective& directive =
      state.current_directives[0];

  EXPECT_EQ(directive.authority.group_id, "group-a");
  EXPECT_EQ(directive.authority.assignment_id, fixture.target_assignment);
  EXPECT_EQ(directive.authority.group_term, 1u);
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
  EXPECT_EQ(directive.payload, *control::EncodeRebuildRequest({3}));

  EXPECT_GT(state.control_revision, 0u);
  const auto decoded =
      control::DecodeFullDesiredState(projected->encoded_full_state);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(*decoded, state);
}

TEST(MetaControlProjector, ProjectsCommittedFailoverExecutionSubset) {
  Fixture fixture = CompleteFixture();
  lavik::meta::MetaFailoverTransition transition;
  transition.transition_id_ = Bytes<16>(0xa1);
  transition.mode_ = lavik::meta::MetaFailoverMode::kControlled;
  transition.target_term_ = 2;
  transition.candidate_action_ = lavik::meta::MetaFailoverCandidateAction{
      .action_id_ = Bytes<16>(0xa2),
      .candidate_ = {.node_id_ = fixture.source,
                     .assignment_id_ = fixture.source_assignment,
                     .boot_id_ = fixture.source_boot},
      .domain_ = {.source_group_term_ = 1,
                  .source_node_id_ = fixture.target,
                  .source_assignment_id_ = fixture.target_assignment,
                  .source_boot_id_ = fixture.target_boot,
                  .source_history_id_ = fixture.source_history,
                  .flow_count_ = 3},
      .authorization_ = lavik::meta::MetaFailoverAuthorization{
          .authorized_revision_ = 100,
          .loss_if_cutover_ = lavik::meta::MetaFailoverLoss::kNone}};
  transition.controlled_ = lavik::meta::MetaControlledFailover{
      .operation_id_ = fixture.operation_id,
      .absolute_deadline_unix_ms_ = 30'000,
  };
  ASSERT_TRUE(fixture.stores.topology_
                  .InstallFailoverTransition("group-a", transition, 100)
                  .ok());

  const auto projected = MetaControlProjector::ProjectNode(
      MetaCommittedView(std::move(fixture.stores), 100), fixture.source);
  ASSERT_TRUE(projected.ok()) << projected.status();
  ASSERT_EQ(projected->full_state.groups.size(), 2u);
  const control::WireDesiredGroup& group = projected->full_state.groups[0];
  ASSERT_TRUE(group.failover_transition.has_value());
  const control::WireFailoverTransition& wire = *group.failover_transition;
  EXPECT_EQ(wire.transition_id, transition.transition_id_);
  EXPECT_EQ(wire.revision, 100u);
  EXPECT_EQ(wire.mode, control::WireFailoverMode::kControlled);
  EXPECT_EQ(wire.target_term, 2u);
  ASSERT_TRUE(wire.candidate_action.has_value());
  EXPECT_EQ(wire.candidate_action->action_id, Bytes<16>(0xa2));
  EXPECT_EQ(wire.candidate_action->candidate.node_id, fixture.source);
  EXPECT_EQ(wire.candidate_action->candidate.assignment_id,
            fixture.source_assignment);
  EXPECT_EQ(wire.candidate_action->candidate.boot_id, Hex(fixture.source_boot));
  EXPECT_EQ(wire.candidate_action->domain.source_group_term, 1u);
  EXPECT_EQ(wire.candidate_action->domain.source_node_id, fixture.target);
  EXPECT_EQ(wire.candidate_action->domain.source_assignment_id,
            fixture.target_assignment);
  EXPECT_EQ(wire.candidate_action->domain.source_boot_id,
            Hex(fixture.target_boot));
  EXPECT_EQ(wire.candidate_action->domain.source_history_id,
            Hex(fixture.source_history));
  ASSERT_TRUE(wire.candidate_action->authorization.has_value());
  EXPECT_EQ(wire.candidate_action->authorization->authorized_revision, 100u);
  EXPECT_EQ(wire.candidate_action->authorization->loss_if_cutover,
            control::WireFailoverLoss::kNone);

  // Controlled-operation scheduling state is Meta-only until Cutover. Data
  // receives only the executable transition subset and current grant facts.
  const auto decoded =
      control::DecodeFullDesiredState(projected->encoded_full_state);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(*decoded, projected->full_state);
}

TEST(MetaControlProjector, ProjectsCurrentGrantActivationActionIdentity) {
  Fixture fixture = CompleteFixture();
  lavik::meta::BeginGroupTerm begin;
  begin.group_id_ = "group-a";
  begin.expected_term_ = 1;
  begin.new_term_ = 2;
  ASSERT_TRUE(fixture.stores.topology_.BeginGroupTerm(begin).ok());
  ASSERT_TRUE(lavik::meta::MetaTopologyTestAccess::SetGroupTerm(
                  fixture.stores.topology_, "group-a", 2)
                  .ok());

  lavik::meta::ActivateAuthority activate;
  activate.group_id_ = "group-a";
  activate.expected_term_ = 2;
  activate.new_owner_ = fixture.target;
  activate.new_topology_epoch_ = 8;
  const lavik::meta::MetaFailoverActionId action_id = Bytes<16>(0xb1);
  ASSERT_TRUE(
      fixture.stores.topology_.ValidateActivate(activate, action_id).ok());
  ASSERT_TRUE(fixture.stores.topology_.SetTopologyEpoch(8).ok());
  ASSERT_TRUE(
      fixture.stores.topology_.ActivateAuthority(activate, action_id).ok());

  // Project the other member so the fixture's deliberately old target-only
  // directive is outside this node-specific batch.
  const auto projected = MetaControlProjector::ProjectNode(
      MetaCommittedView(std::move(fixture.stores), 100), fixture.source);
  ASSERT_TRUE(projected.ok()) << projected.status();
  ASSERT_EQ(projected->full_state.groups.size(), 2u);
  const control::WireDesiredGroup& group = projected->full_state.groups[0];
  EXPECT_TRUE(group.grant_active);
  ASSERT_TRUE(group.activation_action_id.has_value());
  EXPECT_EQ(*group.activation_action_id, action_id);
  EXPECT_FALSE(group.failover_transition.has_value());
}

TEST(MetaControlProjector, ProjectedManifestPassesDataPlaneValidation) {
  const Fixture fixture = CompleteFixture();
  const auto projected = MetaControlProjector::ProjectNode(
      MetaCommittedView(fixture.stores, 99), fixture.target);
  ASSERT_TRUE(projected.ok()) << projected.status();

  const auto prepared = lavik::cluster::PrepareNodeControlState(
      control::SelectNodeControlState(projected->full_state, fixture.target),
      fixture.target, 4);
  ASSERT_TRUE(prepared.ok()) << prepared.status();
  ASSERT_NE(prepared->serving_state_, nullptr);
  ASSERT_EQ(prepared->desired_cluster_controls_.size(), 1u);
  EXPECT_EQ(
      prepared->desired_cluster_controls_.front().identity_.manifest_digest_,
      fixture.manifest_digest);
}

TEST(MetaControlProjector,
     EnablesSteadyReplicationOnlyAfterClusterCreationCompletes) {
  Fixture fixture = CompleteFixture();
  auto creating = MetaControlProjector::ProjectNode(
      MetaCommittedView(fixture.stores, 99), fixture.target);
  ASSERT_TRUE(creating.ok()) << creating.status();
  ASSERT_FALSE(creating->full_state.groups.empty());
  EXPECT_FALSE(creating->full_state.groups.front().steady_replication_enabled);

  const lavik::meta::MetaOperationId root = Bytes<16>(0xc1);
  ASSERT_TRUE(fixture.stores.topology_.BeginClusterCreate(root, 1).ok());
  ASSERT_TRUE(fixture.stores.topology_.CompleteClusterCreate(root).ok());
  auto created = MetaControlProjector::ProjectNode(
      MetaCommittedView(std::move(fixture.stores), 100), fixture.target);
  ASSERT_TRUE(created.ok()) << created.status();
  ASSERT_FALSE(created->full_state.groups.empty());
  EXPECT_TRUE(created->full_state.groups.front().steady_replication_enabled);
}

TEST(MetaControlProjector,
     DesiredContentIgnoresSourceIndexAndTracksCurrentLeasePolicy) {
  const Fixture fixture = CompleteFixture();
  const auto first = MetaControlProjector::ProjectNode(
      MetaCommittedView(fixture.stores, 99), fixture.target);
  const auto later = MetaControlProjector::ProjectNode(
      MetaCommittedView(fixture.stores, 1000), fixture.target);
  ASSERT_TRUE(first.ok()) << first.status();
  ASSERT_TRUE(later.ok()) << later.status();

  EXPECT_TRUE(control::SameDesiredState(first->full_state, later->full_state));
  EXPECT_NE(first->encoded_full_state, later->encoded_full_state);
  ASSERT_EQ(later->full_state.current_directives.size(), 1u);
  EXPECT_EQ(later->full_state.control_revision, 1000u);

  MetaStores with_policy_change = fixture.stores;
  ASSERT_TRUE(with_policy_change.policy_
                  .Apply(PutPolicy(
                      0x70, std::string(lavik::meta::kAuthorityLeasePolicyId),
                      2, R"({"kind":"authority-lease-v1","duration_ms":6000})"))
                  .ok());
  const auto changed = MetaControlProjector::ProjectNode(
      MetaCommittedView(std::move(with_policy_change), 1001), fixture.target);
  ASSERT_TRUE(changed.ok()) << changed.status();
  EXPECT_FALSE(
      control::SameDesiredState(changed->full_state, first->full_state));
  EXPECT_EQ(changed->full_state.authority_lease_duration_ms, 6000u);
  EXPECT_EQ(control::DataHeartbeatIntervalMs(
                changed->full_state.authority_lease_duration_ms),
            2000u);
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
}

TEST(MetaControlProjector,
     GrantlessGroupDoesNotProjectHistoricalOwnerAfterMemberRemoval) {
  Fixture fixture = CompleteFixture();

  lavik::meta::FenceGroup fence;
  fence.request_id_ = Bytes<16>(0x78);
  fence.group_id_ = "group-a";
  fence.expected_term_ = 1;
  fence.new_term_ = 2;
  Commit(fixture.stores, 21, fence);

  lavik::meta::RemoveNodeFromGroup remove;
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

  lavik::meta::FenceGroup fence;
  fence.request_id_ = Bytes<16>(0x78);
  fence.group_id_ = "group-a";
  fence.expected_term_ = 1;
  fence.new_term_ = 2;
  Commit(fixture.stores, 21, fence);

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

  lavik::meta::MetaDirectiveSpec rebuild =
      operation->current_directives_[0].spec_;
  rebuild.directive_id_ = Bytes<16>(0x54);
  rebuild.attempt_id_ = Bytes<16>(0x64);
  lavik::meta::MetaDirectiveSpec authorize =
      operation->current_directives_[0].spec_;
  authorize.directive_id_ = Bytes<16>(0x52);
  authorize.attempt_id_ = Bytes<16>(0x62);
  authorize.recipient_node_id_ = fixture.source;
  authorize.kind_ = "authorize-source";
  authorize.payload_ = *lavik::cluster::control::EncodeRebuildRequest({3});

  lavik::meta::MetaDirectiveSpec revoke = authorize;
  revoke.directive_id_ = Bytes<16>(0x53);
  revoke.attempt_id_ = Bytes<16>(0x63);
  revoke.kind_ = "revoke-sources";
  revoke.payload_.clear();

  lavik::meta::TransitionOperationPhase transition;
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
     ClusterCreateRebuildWaitsForCommittedSourceAuthorization) {
  for (const bool mismatched_layout : {false, true}) {
    SCOPED_TRACE(mismatched_layout);
    Fixture fixture = CompleteFixture(
        std::string(lavik::meta::kMetaClusterCreateV1GroupOperationKind));
    const auto operation =
        fixture.stores.operation_.FindOperation(fixture.operation_id);
    ASSERT_TRUE(operation.has_value());
    ASSERT_EQ(operation->current_directives_.size(), 1u);

    lavik::meta::MetaDirectiveSpec rebuild =
        operation->current_directives_[0].spec_;
    rebuild.directive_id_ = Bytes<16>(0x54);
    rebuild.attempt_id_ = Bytes<16>(0x64);
    lavik::meta::MetaDirectiveSpec authorize = rebuild;
    authorize.directive_id_ = Bytes<16>(0x52);
    authorize.attempt_id_ = Bytes<16>(0x62);
    authorize.recipient_node_id_ = fixture.source;
    authorize.kind_ = lavik::meta::kMetaDirectiveAuthorizeSource;

    lavik::meta::TransitionOperationPhase authorize_transition;
    authorize_transition.request_id_ = Bytes<16>(0x77);
    authorize_transition.operation_id_ = fixture.operation_id;
    authorize_transition.expected_revision_ = 1;
    authorize_transition.kind_phase_blob_ = "authorizing-replica-sources";
    authorize_transition.current_directives_ = {authorize};
    Commit(fixture.stores, 21, authorize_transition);

    auto target = MetaControlProjector::ProjectNode(
        MetaCommittedView(fixture.stores, 104), fixture.target);
    ASSERT_TRUE(target.ok()) << target.status();
    EXPECT_TRUE(target->full_state.current_directives.empty());

    auto source = MetaControlProjector::ProjectNode(
        MetaCommittedView(fixture.stores, 104), fixture.source);
    ASSERT_TRUE(source.ok()) << source.status();
    ASSERT_EQ(source->full_state.current_directives.size(), 1u);
    EXPECT_EQ(source->full_state.current_directives.front().kind,
              control::WireDirectiveKind::kAuthorizeSource);

    lavik::meta::CommitDirectiveResult result;
    result.request_id_ = Bytes<16>(0x78);
    result.operation_id_ = fixture.operation_id;
    result.directive_id_ = authorize.directive_id_;
    result.attempt_id_ = authorize.attempt_id_;
    result.directive_revision_ = 21;
    result.recipient_node_id_ = fixture.source;
    result.recipient_boot_id_ = fixture.source_boot;
    result.assignment_id_ = fixture.target_assignment;
    result.status_ = lavik::meta::MetaDirectiveResultStatus::kSucceeded;
    result.result_ = "source-authorized";

    lavik::meta::MetaStores failed_stores = fixture.stores;
    lavik::meta::CommitDirectiveResult failed = result;
    failed.status_ = lavik::meta::MetaDirectiveResultStatus::kFailed;
    failed.result_ = "source-rejected";
    Commit(failed_stores, 22, failed);
    const auto target_after_failure = MetaControlProjector::ProjectNode(
        MetaCommittedView(std::move(failed_stores), 105), fixture.target);
    ASSERT_TRUE(target_after_failure.ok()) << target_after_failure.status();
    EXPECT_TRUE(target_after_failure->full_state.current_directives.empty());

    Commit(fixture.stores, 22, result);

    if (mismatched_layout)
      rebuild.payload_ = *control::EncodeRebuildRequest({2});
    lavik::meta::TransitionOperationPhase rebuild_transition;
    rebuild_transition.request_id_ = Bytes<16>(0x79);
    rebuild_transition.operation_id_ = fixture.operation_id;
    rebuild_transition.expected_revision_ = 3;
    rebuild_transition.kind_phase_blob_ = "replicating-empty-population";
    rebuild_transition.current_directives_ = {authorize, rebuild};
    Commit(fixture.stores, 23, rebuild_transition);

    target = MetaControlProjector::ProjectNode(
        MetaCommittedView(std::move(fixture.stores), 105), fixture.target);
    ASSERT_TRUE(target.ok()) << target.status();
    if (mismatched_layout) {
      EXPECT_TRUE(target->full_state.current_directives.empty());
      continue;
    }
    ASSERT_EQ(target->full_state.current_directives.size(), 1u);
    EXPECT_EQ(target->full_state.current_directives.front().kind,
              control::WireDirectiveKind::kRebuild);
    EXPECT_EQ(target->full_state.current_directives.front()
                  .identity.directive_revision,
              23u);
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
     TransitionGuardAndProjectorDeriveStorageClassFromKind) {
  const Fixture fixture = CompleteFixture();
  struct DirectiveCase {
    std::string_view kind;
    std::optional<control::WireDirectiveKind> expected;
  };
  const std::vector<DirectiveCase> cases = {
      {"rebuild", control::WireDirectiveKind::kRebuild},
      {"initialize-empty-population",
       control::WireDirectiveKind::kInitializeEmptyPopulation},
      {"authorize-source", control::WireDirectiveKind::kAuthorizeSource},
      {"revoke-sources", control::WireDirectiveKind::kRevokeSources},
      {"unknown", std::nullopt},

  };

  for (const DirectiveCase& test : cases) {
    SCOPED_TRACE(test.kind);
    MetaStores stores = fixture.stores;
    const auto operation =
        stores.operation_.FindOperation(fixture.operation_id);
    ASSERT_TRUE(operation.has_value());
    ASSERT_EQ(operation->current_directives_.size(), 1u);
    lavik::meta::MetaDirectiveSpec directive =
        operation->current_directives_[0].spec_;
    directive.kind_ = test.kind;
    if (test.kind == "unknown") directive.payload_.clear();

    if (test.kind == "initialize-empty-population") {
      directive.source_node_id_ = std::string(40, '0');
      directive.source_assignment_id_ = {};
      directive.source_boot_id_ = {};
      directive.source_replication_history_id_ = {};
      directive.payload_ = Hex(operation->replication_history_id_);
    } else if (test.kind == "authorize-source" ||
               test.kind == "revoke-sources") {
      directive.recipient_node_id_ = fixture.source;
      if (test.kind == "revoke-sources") directive.payload_.clear();
    }
    lavik::meta::TransitionOperationPhase transition;
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
