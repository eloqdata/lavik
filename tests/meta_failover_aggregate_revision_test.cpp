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
#include <cstdint>
#include <string>

#include "gtest/gtest.h"
#include "lavik/meta/cluster_create.h"
#include "lavik/meta/commands.h"
#include "lavik/meta/failover.h"
#include "lavik/meta/hash.h"
#include "lavik/meta/state_apply.h"
#include "meta_topology_test_access.h"

namespace {

namespace meta = lavik::meta;

constexpr std::uint64_t kBaseRevision = 10;

template <std::size_t N>
std::array<std::uint8_t, N> Filled(std::uint8_t seed) {
  std::array<std::uint8_t, N> value{};
  value.fill(seed);
  return value;
}

std::string NodeId(std::uint8_t suffix) {
  std::string id(40, '0');
  constexpr char kHex[] = "0123456789abcdef";
  id[38] = kHex[(suffix >> 4) & 0x0f];
  id[39] = kHex[suffix & 0x0f];
  return id;
}

meta::SubmitOperation ClusterCreateRoot() {
  meta::ClusterCreateManifestV1 manifest;
  manifest.schema_version_ = 1;
  manifest.meta_members_ = {{1, "tcp://127.0.0.1:7101", "tcp://127.0.0.1:7301",
                             "tcp://127.0.0.1:7201"}};
  manifest.data_nodes_ = {{NodeId(1), "tcp://127.0.0.1:6379"}};
  manifest.groups_ = {{"g1", NodeId(1), {}}};
  manifest.slot_ranges_ = {{0, 16383, "g1"}};

  meta::SubmitOperation root;
  root.request_id_ = Filled<16>(0x01);
  root.operation_id_ = Filled<16>(0x02);
  root.kind_ = std::string(meta::kMetaClusterCreateOperationKind);
  const auto intent =
      meta::EncodeClusterCreateRequest(manifest, root.operation_id_);
  EXPECT_TRUE(intent.ok()) << intent.status();
  root.intent_ = intent.value_or("");
  root.intent_hash_ = meta::MetaSha256(root.intent_);
  return root;
}

struct Fixture {
  meta::MetaStores stores;
  std::string owner = NodeId(1);
  std::string candidate = NodeId(2);
  meta::MetaAssignmentId owner_assignment = Filled<16>(0x21);
  meta::MetaAssignmentId candidate_assignment = Filled<16>(0x22);
  meta::MetaOperationId operation_id = Filled<16>(0x41);
};

void RegisterNode(Fixture& fixture, const std::string& node_id,
                  meta::MetaNodeRole role, std::uint8_t request_seed,
                  std::uint16_t port) {
  meta::RegisterNode node;
  node.request_id_ = Filled<16>(request_seed);
  node.node_id_ = node_id;
  node.principal_ = "lavik://node/" + node_id;
  node.endpoints_ = {"tcp://127.0.0.1:" + std::to_string(port)};
  node.role_ = role;
  ASSERT_TRUE(fixture.stores.identity_.Apply(node).ok());
}

void PopulateActivatedFixture(Fixture& fixture,
                              bool install_automatic_policy = true,
                              bool install_authority_lease_policy = true) {
  // Build through the individual stores so the test can manufacture revision
  // orderings that a correctly sequenced ApplyCommitted stream cannot emit.
  const meta::SubmitOperation root = ClusterCreateRoot();
  ASSERT_TRUE(fixture.stores.operation_.SubmitOperation(root, 1).ok());
  ASSERT_TRUE(
      fixture.stores.topology_.BeginClusterCreate(root.operation_id_, 1).ok());

  meta::CompleteOperation complete;
  complete.request_id_ = Filled<16>(0x03);
  complete.operation_id_ = root.operation_id_;
  complete.expected_revision_ = 0;
  complete.result_ = "cluster-created";
  ASSERT_TRUE(fixture.stores.operation_.CompleteOperation(complete).ok());
  ASSERT_TRUE(
      fixture.stores.topology_.CompleteClusterCreate(root.operation_id_).ok());

  RegisterNode(fixture, fixture.owner, meta::MetaNodeRole::kPrimary, 0x04,
               6379);
  RegisterNode(fixture, fixture.candidate, meta::MetaNodeRole::kReplica, 0x05,
               6380);

  meta::CreateGroup group;
  group.request_id_ = Filled<16>(0x06);
  group.group_id_ = "g1";
  group.new_topology_epoch_ = 1;
  ASSERT_TRUE(fixture.stores.topology_.Apply(group).ok());

  meta::AssignNodeToGroup assign_owner;
  assign_owner.request_id_ = Filled<16>(0x07);
  assign_owner.group_id_ = "g1";
  assign_owner.node_id_ = fixture.owner;
  assign_owner.assignment_id_ = fixture.owner_assignment;
  assign_owner.role_ = meta::MetaNodeRole::kPrimary;
  assign_owner.expected_revision_ = 1;
  assign_owner.new_topology_epoch_ = 2;
  ASSERT_TRUE(fixture.stores.topology_.Apply(assign_owner).ok());

  meta::AssignNodeToGroup assign_candidate;
  assign_candidate.request_id_ = Filled<16>(0x08);
  assign_candidate.group_id_ = "g1";
  assign_candidate.node_id_ = fixture.candidate;
  assign_candidate.assignment_id_ = fixture.candidate_assignment;
  assign_candidate.role_ = meta::MetaNodeRole::kReplica;
  assign_candidate.expected_revision_ = 2;
  assign_candidate.new_topology_epoch_ = 3;
  ASSERT_TRUE(fixture.stores.topology_.Apply(assign_candidate).ok());

  meta::PutPolicy recovery;
  recovery.request_id_ = Filled<16>(0x0e);
  recovery.policy_id_ = std::string(meta::kCandidateRecoveryPolicyId);
  recovery.version_ = 1;
  recovery.content_ = R"({"kind":"candidate-recovery-v1","budget_ms":2000})";
  EXPECT_TRUE(fixture.stores.policy_.Apply(recovery).ok());

  if (install_automatic_policy) {
    meta::PutPolicy automatic;
    automatic.request_id_ = Filled<16>(0x09);
    automatic.policy_id_ =
        std::string(meta::kAutomaticUncontrolledFailoverPolicyId);
    automatic.version_ = 1;
    automatic.content_ =
        R"({"kind":"automatic-uncontrolled-failover-v1","suspect_after_ms":5000})";
    ASSERT_TRUE(fixture.stores.policy_.Apply(automatic).ok());
  }
  if (install_authority_lease_policy) {
    meta::PutPolicy lease;
    lease.request_id_ = Filled<16>(0x0c);
    lease.policy_id_ = std::string(meta::kAuthorityLeasePolicyId);
    lease.version_ = 1;
    lease.content_ = R"({"kind":"authority-lease-v1","duration_ms":5000})";
    ASSERT_TRUE(fixture.stores.policy_.Apply(lease).ok());
  }

  meta::BeginGroupTerm begin_term;
  begin_term.request_id_ = Filled<16>(0x0a);
  begin_term.group_id_ = "g1";
  begin_term.expected_term_ = 0;
  begin_term.new_term_ = 1;
  ASSERT_TRUE(fixture.stores.topology_.BeginGroupTerm(begin_term).ok());
  ASSERT_TRUE(lavik::meta::MetaTopologyTestAccess::SetGroupTerm(
                  fixture.stores.topology_, "g1", 1)
                  .ok());

  meta::ActivateAuthority activate;
  activate.request_id_ = Filled<16>(0x0b);
  activate.group_id_ = "g1";
  activate.expected_term_ = 1;
  activate.new_owner_ = fixture.owner;
  activate.new_topology_epoch_ = 4;
  ASSERT_TRUE(fixture.stores.topology_.ValidateActivate(activate).ok());
  ASSERT_TRUE(lavik::meta::MetaTopologyTestAccess::SetOwner(
                  fixture.stores.topology_, "g1", fixture.owner)
                  .ok());
  ASSERT_TRUE(fixture.stores.topology_.SetTopologyEpoch(4).ok());
  ASSERT_TRUE(fixture.stores.topology_.ActivateAuthority(activate).ok());
}

void InstallControlledTransition(Fixture& fixture, std::uint64_t operation_seq,
                                 std::uint64_t transition_revision) {
  meta::FailoverOperationIntent intent;
  intent.group_id_ = "g1";
  intent.absolute_deadline_unix_ms_ = 2'000'000'000'000ULL;
  const auto encoded = meta::EncodeFailoverOperationIntent(intent);
  ASSERT_TRUE(encoded.ok()) << encoded.status();

  meta::SubmitOperation submit;
  submit.request_id_ = Filled<16>(0x40);
  submit.operation_id_ = fixture.operation_id;
  submit.kind_ = std::string(meta::kFailoverOperationKind);
  submit.intent_ = *encoded;
  submit.intent_hash_ = meta::MetaSha256(submit.intent_);
  ASSERT_TRUE(
      fixture.stores.operation_.SubmitOperation(submit, operation_seq).ok());

  meta::MetaFailoverCandidateAction action;
  action.action_id_ = Filled<16>(0x42);
  action.candidate_.node_id_ = fixture.candidate;
  action.candidate_.assignment_id_ = fixture.candidate_assignment;
  action.candidate_.boot_id_ = Filled<meta::kMetaBootIncarnationBytes>(0x43);
  action.domain_.source_group_term_ = 1;
  action.domain_.source_node_id_ = fixture.owner;
  action.domain_.source_assignment_id_ = fixture.owner_assignment;
  action.domain_.source_boot_id_ =
      Filled<meta::kMetaBootIncarnationBytes>(0x44);
  action.domain_.source_history_id_ =
      Filled<meta::kMetaReplicationHistoryIdBytes>(0x45);
  action.domain_.flow_count_ = 1;

  meta::MetaFailoverTransition transition;
  transition.transition_id_ = Filled<16>(0x46);
  transition.mode_ = meta::MetaFailoverMode::kControlled;
  transition.target_term_ = 2;
  transition.candidate_action_ = action;
  transition.controlled_ = meta::MetaControlledFailover{
      fixture.operation_id, intent.absolute_deadline_unix_ms_};
  ASSERT_TRUE(
      fixture.stores.topology_
          .InstallFailoverTransition("g1", transition, transition_revision)
          .ok());
}

absl::StatusOr<meta::MetaStores> Restore(const meta::MetaStores& stores) {
  const auto serialized = stores.Serialize();
  EXPECT_TRUE(serialized.ok()) << serialized.status();
  if (!serialized.ok()) return serialized.status();
  return meta::MetaStores::Deserialize(*serialized);
}

TEST(MetaFailoverAggregateRevision,
     RestoreRejectsCreatedClusterMissingEitherRequiredCurrentPolicy) {
  {
    SCOPED_TRACE("automatic uncontrolled failover Policy missing");
    Fixture fixture;
    PopulateActivatedFixture(fixture, /*install_automatic_policy=*/false,
                             /*install_authority_lease_policy=*/true);
    const auto restored = Restore(fixture.stores);
    ASSERT_FALSE(restored.ok());
    EXPECT_EQ(meta::MetaFailureClassOf(restored.status()),
              meta::MetaFailureClass::kFailStop);
  }
  {
    SCOPED_TRACE("Authority Lease Policy missing");
    Fixture fixture;
    PopulateActivatedFixture(fixture, /*install_automatic_policy=*/true,
                             /*install_authority_lease_policy=*/false);
    const auto restored = Restore(fixture.stores);
    ASSERT_FALSE(restored.ok());
    EXPECT_EQ(meta::MetaFailureClassOf(restored.status()),
              meta::MetaFailureClass::kFailStop);
  }
}

TEST(MetaFailoverAggregateRevision,
     RestoreRejectsControlledTransitionNotAfterOperationSubmission) {
  Fixture fixture;
  PopulateActivatedFixture(fixture);
  InstallControlledTransition(fixture, kBaseRevision + 1, kBaseRevision + 1);

  const auto restored = Restore(fixture.stores);

  ASSERT_FALSE(restored.ok());
  EXPECT_EQ(meta::MetaFailureClassOf(restored.status()),
            meta::MetaFailureClass::kFailStop);
}

TEST(MetaFailoverAggregateRevision,
     RestoreAcceptsStrictlyOrderedControlledTransition) {
  Fixture fixture;
  PopulateActivatedFixture(fixture);
  InstallControlledTransition(fixture, kBaseRevision + 1, kBaseRevision + 2);

  const auto restored = Restore(fixture.stores);

  ASSERT_TRUE(restored.ok()) << restored.status();
  const auto group = restored->topology_.FindGroup("g1");
  ASSERT_TRUE(group.has_value());
  ASSERT_TRUE(group->failover_transition_.has_value());
  EXPECT_EQ(group->failover_transition_->revision_, kBaseRevision + 2);
}

}  // namespace
