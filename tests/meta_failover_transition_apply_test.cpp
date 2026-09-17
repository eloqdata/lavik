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
#include <string_view>
#include <utility>

#include "absl/strings/str_cat.h"
#include "gtest/gtest.h"
#include "lavik/cluster/control_protocol.h"
#include "lavik/meta/cluster_create.h"
#include "lavik/meta/commands.h"
#include "lavik/meta/failover.h"
#include "lavik/meta/hash.h"
#include "lavik/meta/state_apply.h"
#include "meta_topology_test_access.h"

namespace {

namespace meta = lavik::meta;

constexpr std::string_view kActor = "lavik://operator/failover-test";
constexpr std::string_view kTime = "2026-09-13T00:00:00Z";
constexpr std::string_view kAutomaticPreemptionReason =
    "preempted by automatic uncontrolled failover";

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

void ExpectAccepted(meta::MetaStores& stores, std::uint64_t index,
                    const meta::MetaCommand& command) {
  const meta::MetaApplyResult result =
      meta::ApplyCommitted(stores, index, command, kActor, kTime);
  EXPECT_EQ(result.verdict_, meta::MetaAuditVerdict::kAccepted)
      << result.detail_;
}

meta::MetaApplyResult ExpectRejected(meta::MetaStores& stores,
                                     std::uint64_t index,
                                     const meta::MetaCommand& command) {
  const meta::MetaApplyResult result =
      meta::ApplyCommitted(stores, index, command, kActor, kTime);
  EXPECT_EQ(result.verdict_, meta::MetaAuditVerdict::kRejected);
  EXPECT_FALSE(result.detail_.empty());
  return result;
}

std::string DomainBytes(const meta::MetaStores& stores) {
  std::string bytes = stores.identity_.Serialize();
  bytes += stores.topology_.Serialize();
  bytes += stores.policy_.Serialize();
  bytes += stores.operation_.Serialize().value_or("invalid-operation");
  bytes += stores.population_manifest_.Serialize();
  return bytes;
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

void SeedRequiredCurrentPolicies(meta::MetaStores& stores) {
  meta::PutPolicy automatic;
  automatic.request_id_ = Filled<16>(0x0f);
  automatic.policy_id_ =
      std::string(meta::kAutomaticUncontrolledFailoverPolicyId);
  automatic.version_ = 1;
  automatic.content_ =
      R"({"kind":"automatic-uncontrolled-failover-v1","suspect_after_ms":5000})";
  ASSERT_TRUE(stores.policy_.Apply(automatic).ok());

  meta::PutPolicy authority;
  authority.request_id_ = Filled<16>(0x07);
  authority.policy_id_ = std::string(meta::kAuthorityLeasePolicyId);
  authority.version_ = 1;
  authority.content_ = R"({"kind":"authority-lease-v1","duration_ms":5000})";
  ASSERT_TRUE(stores.policy_.Apply(authority).ok());
}

meta::SubmitOperation FailoverSubmit(std::uint8_t seed,
                                     std::string group_id = "g1") {
  meta::SubmitOperation submit;
  submit.request_id_ = Filled<16>(seed);
  submit.operation_id_ = Filled<16>(static_cast<std::uint8_t>(seed + 1));
  submit.kind_ = "failover";
  const auto intent = meta::EncodeFailoverOperationIntent(
      {.group_id_ = std::move(group_id), .absolute_deadline_unix_ms_ = 1000});
  EXPECT_TRUE(intent.ok()) << intent.status();
  submit.intent_ = intent.value_or("");
  submit.intent_hash_ = meta::MetaSha256(submit.intent_);
  return submit;
}

struct ActivatedGroupFixture {
  meta::MetaStores stores;
  std::string owner = NodeId(1);
  meta::MetaAssignmentId owner_assignment = Filled<16>(0x31);
};

void PopulateActivatedGroup(ActivatedGroupFixture& fixture,
                            std::uint64_t first_index) {
  meta::RegisterNode node;
  node.request_id_ = Filled<16>(0x04);
  node.node_id_ = fixture.owner;
  node.principal_ = absl::StrCat("lavik://node/", fixture.owner);
  node.endpoints_ = {"tcp://127.0.0.1:6379"};
  node.role_ = meta::MetaNodeRole::kPrimary;
  ExpectAccepted(fixture.stores, first_index, meta::MetaCommand{node});

  meta::CreateGroup group;
  group.request_id_ = Filled<16>(0x05);
  group.group_id_ = "g1";
  group.new_topology_epoch_ = 1;
  ExpectAccepted(fixture.stores, first_index + 1, meta::MetaCommand{group});

  meta::AssignNodeToGroup assign;
  assign.request_id_ = Filled<16>(0x06);
  assign.group_id_ = "g1";
  assign.node_id_ = fixture.owner;
  assign.assignment_id_ = fixture.owner_assignment;
  assign.role_ = meta::MetaNodeRole::kPrimary;
  assign.expected_revision_ = 1;
  assign.new_topology_epoch_ = 2;
  ExpectAccepted(fixture.stores, first_index + 2, meta::MetaCommand{assign});

  meta::PutPolicy policy;
  policy.request_id_ = Filled<16>(0x07);
  policy.policy_id_ = std::string(meta::kAuthorityLeasePolicyId);
  policy.version_ = 1;
  policy.content_ = R"({"kind":"authority-lease-v1","duration_ms":5000})";
  ExpectAccepted(fixture.stores, first_index + 3, meta::MetaCommand{policy});

  meta::BeginGroupTerm begin;
  begin.request_id_ = Filled<16>(0x08);
  begin.group_id_ = "g1";
  begin.expected_term_ = 0;
  begin.new_term_ = 1;
  ExpectAccepted(fixture.stores, first_index + 4, meta::MetaCommand{begin});

  meta::ActivateAuthority activate;
  activate.request_id_ = Filled<16>(0x09);
  activate.group_id_ = "g1";
  activate.expected_term_ = 1;
  activate.new_owner_ = fixture.owner;
  activate.new_topology_epoch_ = 3;
  ExpectAccepted(fixture.stores, first_index + 5, meta::MetaCommand{activate});
}

ActivatedGroupFixture MakeActivatedGroup() {
  ActivatedGroupFixture fixture;
  meta::SubmitOperation root = ClusterCreateRoot();
  ExpectAccepted(fixture.stores, 1, meta::MetaCommand{root});

  SeedRequiredCurrentPolicies(fixture.stores);

  meta::CompleteOperation complete;
  complete.request_id_ = Filled<16>(0x03);
  complete.operation_id_ = root.operation_id_;
  complete.expected_revision_ = 0;
  complete.result_ = "cluster-created";
  ExpectAccepted(fixture.stores, 2, meta::MetaCommand{complete});

  PopulateActivatedGroup(fixture, 3);
  return fixture;
}

meta::BeginUncontrolledFailover MakeBeginUncontrolled(
    const ActivatedGroupFixture& fixture) {
  meta::BeginUncontrolledFailover begin;
  begin.request_id_ = Filled<16>(0x0a);
  begin.group_id_ = "g1";
  begin.transition_id_ = Filled<16>(0x41);
  begin.target_term_ = 2;
  begin.expected_owner_node_id_ = fixture.owner;
  begin.expected_owner_assignment_id_ = fixture.owner_assignment;
  begin.expected_membership_revision_ = 2;
  begin.expected_group_term_ = 1;
  begin.expected_population_manifest_revision_ = 0;
  begin.expected_population_manifest_digest_.fill(0);
  begin.expected_partition_replication_epoch_ = 0;
  return begin;
}

meta::MetaOperationId InstallCurrentAuthorityDirective(
    ActivatedGroupFixture& fixture) {
  meta::SubmitOperation submit;
  submit.operation_id_ = Filled<16>(0x81);
  submit.kind_ = "test-rebuild";
  submit.intent_ = "rebuild-current-authority";
  submit.intent_hash_ = meta::MetaSha256(submit.intent_);
  EXPECT_TRUE(fixture.stores.operation_.SubmitOperation(submit, 80).ok());

  meta::MetaDirectiveSpec directive;
  directive.directive_id_ = Filled<16>(0x82);
  directive.attempt_id_ = Filled<16>(0x83);
  directive.recipient_node_id_ = fixture.owner;
  directive.target_node_id_ = fixture.owner;
  directive.target_boot_id_ = Filled<meta::kMetaBootIncarnationBytes>(0x84);
  directive.assignment_id_ = fixture.owner_assignment;
  directive.source_node_id_ = fixture.owner;
  directive.source_assignment_id_ = fixture.owner_assignment;
  directive.source_boot_id_ = directive.target_boot_id_;
  directive.source_replication_history_id_ =
      Filled<meta::kMetaReplicationHistoryIdBytes>(0x85);
  directive.group_id_ = "g1";
  directive.group_term_ = 1;
  directive.kind_ = std::string(meta::kMetaDirectiveRebuild);
  directive.payload_ = *lavik::cluster::control::EncodeRebuildRequest({3});

  meta::TransitionOperationPhase phase;
  phase.operation_id_ = submit.operation_id_;
  phase.current_directives_ = {directive};
  EXPECT_TRUE(
      fixture.stores.operation_.TransitionOperationPhase(phase, 81).ok());
  return submit.operation_id_;
}

void InstallUncontrolledPostStateDirectly(ActivatedGroupFixture& fixture,
                                          std::uint64_t revision) {
  meta::BeginGroupTerm begin_term;
  begin_term.group_id_ = "g1";
  begin_term.expected_term_ = 1;
  begin_term.new_term_ = 2;
  ASSERT_TRUE(fixture.stores.topology_.BeginGroupTerm(begin_term).ok());
  ASSERT_TRUE(lavik::meta::MetaTopologyTestAccess::SetGroupTerm(
                  fixture.stores.topology_, "g1", 2)
                  .ok());

  const meta::BeginUncontrolledFailover begin = MakeBeginUncontrolled(fixture);
  meta::MetaFailoverTransition transition;
  transition.transition_id_ = begin.transition_id_;
  transition.mode_ = meta::MetaFailoverMode::kUncontrolled;
  transition.target_term_ = begin.target_term_;
  ASSERT_TRUE(fixture.stores.topology_
                  .InstallFailoverTransition("g1", transition, revision)
                  .ok());
}

void ExpectAggregateRestoreFails(const meta::MetaStores& stores) {
  const auto bytes = stores.Serialize();
  ASSERT_TRUE(bytes.ok()) << bytes.status();
  const auto restored = meta::MetaStores::Deserialize(*bytes);
  ASSERT_FALSE(restored.ok());
  EXPECT_EQ(meta::MetaFailureClassOf(restored.status()),
            meta::MetaFailureClass::kFailStop);
}

TEST(MetaFailoverTransitionApply,
     BeginUncontrolledWithoutCandidateAtomicallyFencesAndPersists) {
  ActivatedGroupFixture fixture = MakeActivatedGroup();
  const meta::BeginUncontrolledFailover begin = MakeBeginUncontrolled(fixture);

  ExpectAccepted(fixture.stores, 9, meta::MetaCommand{begin});

  const auto group = fixture.stores.topology_.FindGroup("g1");
  ASSERT_TRUE(group.has_value());
  EXPECT_EQ(group->record_.owner_, fixture.owner);
  EXPECT_EQ(group->record_.group_term_, 2u);
  ASSERT_TRUE(group->failover_transition_.has_value());
  EXPECT_EQ(group->failover_transition_->transition_id_, begin.transition_id_);
  EXPECT_EQ(group->failover_transition_->revision_, 9u);
  EXPECT_EQ(group->failover_transition_->mode_,
            meta::MetaFailoverMode::kUncontrolled);
  EXPECT_EQ(group->failover_transition_->target_term_, 2u);
  EXPECT_FALSE(group->failover_transition_->candidate_action_.has_value());
  EXPECT_FALSE(group->failover_transition_->controlled_.has_value());
  EXPECT_EQ(group->revision_, 2u);
  EXPECT_EQ(fixture.stores.topology_.TopologyEpoch(), 3u);

  const auto grant = fixture.stores.topology_.AuthorityFor("g1");
  ASSERT_TRUE(grant.has_value());
  EXPECT_EQ(grant->group_term_, 2u);
  EXPECT_FALSE(grant->grant_.has_value());

  const auto serialized = fixture.stores.Serialize();
  ASSERT_TRUE(serialized.ok()) << serialized.status();
  const auto restored = meta::MetaStores::Deserialize(*serialized);
  ASSERT_TRUE(restored.ok()) << restored.status();
  EXPECT_EQ(restored->topology_.FindGroup("g1"), group);

  // The exact committed entry is a post-state no-op: term and transition
  // revision do not advance a second time, and the audit record is unchanged.
  ExpectAccepted(fixture.stores, 9, meta::MetaCommand{begin});
  EXPECT_EQ(fixture.stores.topology_.FindGroup("g1"), group);
  EXPECT_EQ(fixture.stores.audit_.size(), 9u);
}

TEST(MetaFailoverTransitionApply,
     AutomaticBeginAtomicallyPreemptsPristineControlledRequest) {
  ActivatedGroupFixture fixture = MakeActivatedGroup();
  const meta::SubmitOperation controlled = FailoverSubmit(0xa0);
  ExpectAccepted(fixture.stores, 9, meta::MetaCommand{controlled});

  meta::BeginUncontrolledFailover begin = MakeBeginUncontrolled(fixture);
  begin.trigger_reason_ = meta::MetaAutomaticFailoverReason::kHeartbeatExpired;
  begin.suspect_duration_ms_ = 5000;
  begin.preempted_operation_id_ = controlled.operation_id_;
  begin.expected_preempted_operation_revision_ = 0;
  ExpectAccepted(fixture.stores, 10, meta::MetaCommand{begin});

  const auto operation =
      fixture.stores.operation_.FindOperation(controlled.operation_id_);
  ASSERT_TRUE(operation.has_value());
  EXPECT_EQ(operation->lifecycle_, meta::MetaOperationLifecycle::kAborted);
  EXPECT_EQ(operation->revision_, 1u);
  EXPECT_EQ(operation->terminal_result_, kAutomaticPreemptionReason);
  EXPECT_FALSE(operation->data_loss_possible_);

  const auto group = fixture.stores.topology_.FindGroup("g1");
  ASSERT_TRUE(group.has_value());
  ASSERT_TRUE(group->failover_transition_.has_value());
  EXPECT_EQ(group->failover_transition_->mode_,
            meta::MetaFailoverMode::kUncontrolled);
  EXPECT_EQ(group->failover_transition_->revision_, 10u);
  EXPECT_EQ(group->record_.group_term_, 2u);
  const auto grant = fixture.stores.topology_.AuthorityFor("g1");
  ASSERT_TRUE(grant.has_value());
  EXPECT_FALSE(grant->grant_.has_value());

  const std::string post_state = DomainBytes(fixture.stores);
  const std::size_t audit_size = fixture.stores.audit_.size();
  ExpectAccepted(fixture.stores, 10, meta::MetaCommand{begin});
  EXPECT_EQ(DomainBytes(fixture.stores), post_state);
  EXPECT_EQ(fixture.stores.audit_.size(), audit_size);
}

TEST(
    MetaFailoverTransitionApply,
    ControlValidationRejectionRestoresAuthorityPreemptedOperationAndDirectives) {
  ActivatedGroupFixture fixture = MakeActivatedGroup();
  const meta::SubmitOperation controlled = FailoverSubmit(0xa0);
  ExpectAccepted(fixture.stores, 9, meta::MetaCommand{controlled});
  const auto population_operation = InstallCurrentAuthorityDirective(fixture);

  // Force a post-mutation control validation failure. The earlier
  // term/transition, preemption, and stale-directive cleanup must all be rolled
  // back together.
  fixture.stores.policy_ = meta::MetaPolicyStore{};
  const std::string before = DomainBytes(fixture.stores);
  const auto active_before = fixture.stores.operation_.ActiveCount();
  meta::BeginUncontrolledFailover begin = MakeBeginUncontrolled(fixture);
  begin.trigger_reason_ = meta::MetaAutomaticFailoverReason::kHeartbeatExpired;
  begin.suspect_duration_ms_ = 5000;
  begin.preempted_operation_id_ = controlled.operation_id_;
  begin.expected_preempted_operation_revision_ = 0;
  const auto rejected =
      ExpectRejected(fixture.stores, 90, meta::MetaCommand{begin});
  EXPECT_NE(rejected.detail_.find("Authority Lease policy"), std::string::npos);
  EXPECT_EQ(DomainBytes(fixture.stores), before);
  EXPECT_EQ(fixture.stores.operation_.ActiveCount(), active_before);
  ASSERT_EQ(fixture.stores.operation_.FindOperation(population_operation)
                ->current_directives_.size(),
            1u);

  // A fresh command succeeds after repairing the injected fault: rejection
  // did not consume the Group term, operation revision, or task identity.
  SeedRequiredCurrentPolicies(fixture.stores);
  ExpectAccepted(fixture.stores, 91, meta::MetaCommand{begin});
  EXPECT_EQ(fixture.stores.topology_.FindGroup("g1")->record_.group_term_, 2u);
  EXPECT_TRUE(fixture.stores.operation_.FindOperation(population_operation)
                  ->current_directives_.empty());
  EXPECT_EQ(fixture.stores.operation_.FindOperation(controlled.operation_id_)
                ->lifecycle_,
            meta::MetaOperationLifecycle::kAborted);
}

TEST(MetaFailoverTransitionApply,
     AutomaticBeginRejectsCandidateWithoutDomainMutation) {
  ActivatedGroupFixture fixture = MakeActivatedGroup();
  meta::BeginUncontrolledFailover begin = MakeBeginUncontrolled(fixture);
  begin.trigger_reason_ = meta::MetaAutomaticFailoverReason::kHeartbeatExpired;
  begin.suspect_duration_ms_ = 5'000;
  begin.candidate_action_ = meta::MetaFailoverCandidateAction{
      .action_id_ = Filled<16>(0xc0),
      .candidate_ =
          {
              .node_id_ = fixture.owner,
              .assignment_id_ = fixture.owner_assignment,
              .boot_id_ = Filled<meta::kMetaBootIncarnationBytes>(0xc1),
          },
      .domain_ =
          {
              .source_group_term_ = 1,
              .source_node_id_ = fixture.owner,
              .source_assignment_id_ = fixture.owner_assignment,
              .source_boot_id_ = Filled<meta::kMetaBootIncarnationBytes>(0xc2),
              .source_history_id_ =
                  Filled<meta::kMetaReplicationHistoryIdBytes>(0xc3),
              .flow_count_ = 1,
          },
  };
  const std::string before = DomainBytes(fixture.stores);

  const meta::MetaApplyResult result =
      ExpectRejected(fixture.stores, 9, meta::MetaCommand{begin});

  EXPECT_NE(result.detail_.find("automatic uncontrolled failover"),
            std::string::npos);
  EXPECT_EQ(DomainBytes(fixture.stores), before);
  EXPECT_EQ(fixture.stores.topology_.FindGroup("g1")->record_.group_term_, 1u);
  EXPECT_FALSE(fixture.stores.topology_.FindGroup("g1")
                   ->failover_transition_.has_value());
}

TEST(MetaFailoverTransitionApply,
     AutomaticBeginPreemptsAllRequestsPresentAtApplyNotOnlyItsHint) {
  ActivatedGroupFixture fixture = MakeActivatedGroup();
  const meta::SubmitOperation hinted = FailoverSubmit(0xb0);
  ExpectAccepted(fixture.stores, 9, meta::MetaCommand{hinted});

  meta::BeginUncontrolledFailover begin = MakeBeginUncontrolled(fixture);
  begin.trigger_reason_ = meta::MetaAutomaticFailoverReason::kSessionMissing;
  begin.suspect_duration_ms_ = 5000;
  begin.preempted_operation_id_ = hinted.operation_id_;
  begin.expected_preempted_operation_revision_ = 0;

  // This request commits after the leader built Begin and is therefore absent
  // from its CAS hint, but it is present before Begin reaches deterministic
  // apply. The command must not leave this race winner behind.
  const meta::SubmitOperation racing = FailoverSubmit(0xb2);
  ExpectAccepted(fixture.stores, 10, meta::MetaCommand{racing});
  const meta::SubmitOperation other_group = FailoverSubmit(0xb4, "g2");
  ExpectAccepted(fixture.stores, 11, meta::MetaCommand{other_group});

  ExpectAccepted(fixture.stores, 12, meta::MetaCommand{begin});

  for (const meta::MetaOperationId& operation_id :
       {hinted.operation_id_, racing.operation_id_}) {
    const auto operation =
        fixture.stores.operation_.FindOperation(operation_id);
    ASSERT_TRUE(operation.has_value());
    EXPECT_EQ(operation->lifecycle_, meta::MetaOperationLifecycle::kAborted);
    EXPECT_EQ(operation->revision_, 1u);
    EXPECT_EQ(operation->terminal_result_, kAutomaticPreemptionReason);
    EXPECT_FALSE(operation->data_loss_possible_);
  }
  const auto unrelated =
      fixture.stores.operation_.FindOperation(other_group.operation_id_);
  ASSERT_TRUE(unrelated.has_value());
  EXPECT_EQ(unrelated->lifecycle_, meta::MetaOperationLifecycle::kSubmitted);
}

TEST(MetaFailoverTransitionApply,
     ActiveTransitionRejectsNewControlledRequestButPreservesSubmitReplay) {
  ActivatedGroupFixture fixture = MakeActivatedGroup();
  const meta::SubmitOperation existing = FailoverSubmit(0xb6);
  ExpectAccepted(fixture.stores, 9, meta::MetaCommand{existing});

  meta::BeginUncontrolledFailover begin = MakeBeginUncontrolled(fixture);
  begin.trigger_reason_ = meta::MetaAutomaticFailoverReason::kDraining;
  begin.suspect_duration_ms_ = 5000;
  begin.preempted_operation_id_ = existing.operation_id_;
  begin.expected_preempted_operation_revision_ = 0;
  ExpectAccepted(fixture.stores, 10, meta::MetaCommand{begin});

  // Retrying the durable Submit identity remains an idempotent lookup of the
  // now-aborted record; only a new controlled request is barred.
  ExpectAccepted(fixture.stores, 11, meta::MetaCommand{existing});
  const meta::SubmitOperation late = FailoverSubmit(0xb8);
  const meta::MetaApplyResult rejected =
      ExpectRejected(fixture.stores, 12, meta::MetaCommand{late});
  EXPECT_NE(rejected.detail_.find("active failover transition"),
            std::string::npos);
  EXPECT_FALSE(fixture.stores.operation_.OperationKnown(late.operation_id_));
}

TEST(MetaFailoverTransitionApply,
     AutomaticBeginRejectsPreemptionForAnotherGroupWithoutMutation) {
  ActivatedGroupFixture fixture = MakeActivatedGroup();
  const meta::SubmitOperation controlled = FailoverSubmit(0xa2, "g2");
  ExpectAccepted(fixture.stores, 9, meta::MetaCommand{controlled});

  meta::BeginUncontrolledFailover begin = MakeBeginUncontrolled(fixture);
  begin.trigger_reason_ = meta::MetaAutomaticFailoverReason::kSessionMissing;
  begin.suspect_duration_ms_ = 5000;
  begin.preempted_operation_id_ = controlled.operation_id_;
  begin.expected_preempted_operation_revision_ = 0;
  const std::string before = DomainBytes(fixture.stores);

  const meta::MetaApplyResult result =
      ExpectRejected(fixture.stores, 10, meta::MetaCommand{begin});

  EXPECT_NE(result.detail_.find("another group"), std::string::npos);
  EXPECT_EQ(DomainBytes(fixture.stores), before);
}

TEST(MetaFailoverTransitionApply,
     AutomaticBeginRejectsOperationOnlyReplayHalfWithoutRepair) {
  ActivatedGroupFixture fixture = MakeActivatedGroup();
  const meta::SubmitOperation controlled = FailoverSubmit(0xa4);
  ExpectAccepted(fixture.stores, 9, meta::MetaCommand{controlled});

  meta::AbortOperation abort;
  abort.operation_id_ = controlled.operation_id_;
  abort.expected_revision_ = 0;
  abort.reason_ = std::string(kAutomaticPreemptionReason);
  ASSERT_TRUE(fixture.stores.operation_.AbortOperation(abort).ok());

  meta::BeginUncontrolledFailover begin = MakeBeginUncontrolled(fixture);
  begin.trigger_reason_ = meta::MetaAutomaticFailoverReason::kStorageUnready;
  begin.suspect_duration_ms_ = 5000;
  begin.preempted_operation_id_ = controlled.operation_id_;
  begin.expected_preempted_operation_revision_ = 0;
  const std::string before = DomainBytes(fixture.stores);

  ExpectRejected(fixture.stores, 10, meta::MetaCommand{begin});

  EXPECT_EQ(DomainBytes(fixture.stores), before);
  EXPECT_EQ(fixture.stores.topology_.FindGroup("g1")->record_.group_term_, 1u);
  EXPECT_FALSE(fixture.stores.topology_.FindGroup("g1")
                   ->failover_transition_.has_value());
}

TEST(MetaFailoverTransitionApply, ManualBeginCannotPreemptControlledRequest) {
  ActivatedGroupFixture fixture = MakeActivatedGroup();
  const meta::SubmitOperation controlled = FailoverSubmit(0xa6);
  ExpectAccepted(fixture.stores, 9, meta::MetaCommand{controlled});

  meta::BeginUncontrolledFailover begin = MakeBeginUncontrolled(fixture);
  begin.preempted_operation_id_ = controlled.operation_id_;
  begin.expected_preempted_operation_revision_ = 0;
  const std::string before = DomainBytes(fixture.stores);

  ExpectRejected(fixture.stores, 10, meta::MetaCommand{begin});

  EXPECT_EQ(DomainBytes(fixture.stores), before);
}

TEST(MetaFailoverTransitionApply,
     BeginUncontrolledRejectsStaleTermAnchorWithoutMutation) {
  ActivatedGroupFixture fixture = MakeActivatedGroup();
  meta::BeginUncontrolledFailover begin = MakeBeginUncontrolled(fixture);
  begin.expected_group_term_ = 0;
  const std::string before = DomainBytes(fixture.stores);

  ExpectRejected(fixture.stores, 9, meta::MetaCommand{begin});

  EXPECT_EQ(DomainBytes(fixture.stores), before);
}

TEST(MetaFailoverTransitionApply,
     BeginUncontrolledAtomicallyInvalidatesOldAuthorityDirectives) {
  ActivatedGroupFixture fixture = MakeActivatedGroup();
  const meta::MetaOperationId operation_id =
      InstallCurrentAuthorityDirective(fixture);
  ASSERT_EQ(fixture.stores.operation_.FindOperation(operation_id)
                ->current_directives_.size(),
            1u);

  ExpectAccepted(fixture.stores, 9,
                 meta::MetaCommand{MakeBeginUncontrolled(fixture)});

  const auto operation = fixture.stores.operation_.FindOperation(operation_id);
  ASSERT_TRUE(operation.has_value());
  EXPECT_TRUE(operation->current_directives_.empty());
  EXPECT_EQ(operation->revision_, 2u);
}

TEST(MetaFailoverTransitionApply,
     BeginUncontrolledRejectsOperationCleanupPartialStateWithoutRepair) {
  ActivatedGroupFixture fixture = MakeActivatedGroup();
  const meta::MetaOperationId operation_id =
      InstallCurrentAuthorityDirective(fixture);
  const meta::BeginUncontrolledFailover begin = MakeBeginUncontrolled(fixture);
  InstallUncontrolledPostStateDirectly(fixture, 9);
  const std::string before = DomainBytes(fixture.stores);

  ExpectRejected(fixture.stores, 9, meta::MetaCommand{begin});

  EXPECT_EQ(DomainBytes(fixture.stores), before);
  const auto operation = fixture.stores.operation_.FindOperation(operation_id);
  ASSERT_TRUE(operation.has_value());
  EXPECT_EQ(operation->current_directives_.size(), 1u);
  EXPECT_EQ(operation->revision_, 1u);
}

TEST(MetaFailoverTransitionApply,
     BeginUncontrolledSamePayloadAtLaterIndexIsNotReplay) {
  ActivatedGroupFixture fixture = MakeActivatedGroup();
  const meta::BeginUncontrolledFailover begin = MakeBeginUncontrolled(fixture);
  ExpectAccepted(fixture.stores, 9, meta::MetaCommand{begin});
  const std::string before = DomainBytes(fixture.stores);

  ExpectRejected(fixture.stores, 10, meta::MetaCommand{begin});

  EXPECT_EQ(DomainBytes(fixture.stores), before);
  EXPECT_EQ(
      fixture.stores.topology_.FindGroup("g1")->failover_transition_->revision_,
      9u);
}

TEST(MetaFailoverTransitionApply,
     BeginUncontrolledRejectsTermAdvanceWithoutTransition) {
  ActivatedGroupFixture fixture = MakeActivatedGroup();
  const meta::BeginUncontrolledFailover begin = MakeBeginUncontrolled(fixture);
  meta::BeginGroupTerm partial;
  partial.group_id_ = "g1";
  partial.expected_term_ = 1;
  partial.new_term_ = 2;
  ASSERT_TRUE(fixture.stores.topology_.BeginGroupTerm(partial).ok());
  const std::string before = DomainBytes(fixture.stores);

  ExpectRejected(fixture.stores, 9, meta::MetaCommand{begin});

  EXPECT_EQ(DomainBytes(fixture.stores), before);
  EXPECT_EQ(fixture.stores.topology_.FindGroup("g1")->record_.group_term_, 2u);
  EXPECT_FALSE(fixture.stores.topology_.AuthorityFor("g1")->grant_);
  EXPECT_FALSE(fixture.stores.topology_.FindGroup("g1")
                   ->failover_transition_.has_value());
}

TEST(MetaFailoverTransitionApply,
     BeginUncontrolledRejectsTransitionWithoutFencing) {
  ActivatedGroupFixture fixture = MakeActivatedGroup();
  const meta::BeginUncontrolledFailover begin = MakeBeginUncontrolled(fixture);
  ASSERT_TRUE(lavik::meta::MetaTopologyTestAccess::SetGroupTerm(
                  fixture.stores.topology_, "g1", 2)
                  .ok());
  meta::MetaFailoverTransition partial;
  partial.transition_id_ = begin.transition_id_;
  partial.mode_ = meta::MetaFailoverMode::kUncontrolled;
  partial.target_term_ = begin.target_term_;
  ASSERT_TRUE(
      fixture.stores.topology_.InstallFailoverTransition("g1", partial, 9)
          .ok());
  const std::string before = DomainBytes(fixture.stores);

  ExpectRejected(fixture.stores, 9, meta::MetaCommand{begin});

  EXPECT_EQ(DomainBytes(fixture.stores), before);
  EXPECT_EQ(fixture.stores.topology_.AuthorityFor("g1")->group_term_, 2u);
  EXPECT_TRUE(fixture.stores.topology_.AuthorityFor("g1")->grant_.has_value());
}

TEST(MetaFailoverTransitionApply,
     FailoverSubmitRequiresCreatedClusterLifecycle) {
  {
    meta::MetaStores uninitialized;
    ExpectRejected(uninitialized, 1, meta::MetaCommand{FailoverSubmit(0x51)});
    EXPECT_FALSE(uninitialized.operation_.OperationKnown(Filled<16>(0x52)));
  }

  {
    meta::MetaStores creating;
    const meta::SubmitOperation root = ClusterCreateRoot();
    ExpectAccepted(creating, 1, meta::MetaCommand{root});
    ExpectRejected(creating, 2, meta::MetaCommand{FailoverSubmit(0x53)});
    EXPECT_FALSE(creating.operation_.OperationKnown(Filled<16>(0x54)));
  }

  {
    meta::MetaStores failed;
    const meta::SubmitOperation root = ClusterCreateRoot();
    ExpectAccepted(failed, 1, meta::MetaCommand{root});
    meta::AbortOperation abort;
    abort.request_id_ = Filled<16>(0x55);
    abort.operation_id_ = root.operation_id_;
    abort.expected_revision_ = 0;
    abort.reason_ = "provisioning failed";
    ExpectAccepted(failed, 2, meta::MetaCommand{abort});
    ASSERT_EQ(failed.topology_.ClusterLifecycle().state_,
              meta::MetaClusterLifecycle::kProvisioningFailed);
    ExpectRejected(failed, 3, meta::MetaCommand{FailoverSubmit(0x56)});
    EXPECT_FALSE(failed.operation_.OperationKnown(Filled<16>(0x57)));
  }

  {
    meta::MetaStores created;
    const meta::SubmitOperation root = ClusterCreateRoot();
    ExpectAccepted(created, 1, meta::MetaCommand{root});
    SeedRequiredCurrentPolicies(created);
    meta::CompleteOperation complete;
    complete.request_id_ = Filled<16>(0x58);
    complete.operation_id_ = root.operation_id_;
    complete.expected_revision_ = 0;
    complete.result_ = "cluster-created";
    ExpectAccepted(created, 2, meta::MetaCommand{complete});
    const meta::SubmitOperation failover = FailoverSubmit(0x59);
    ExpectAccepted(created, 3, meta::MetaCommand{failover});
    EXPECT_TRUE(created.operation_.OperationKnown(failover.operation_id_));
  }
}

TEST(MetaFailoverTransitionApply,
     ActiveTransitionBlocksOrdinaryAuthorityAndAnchorMutations) {
  auto expect_blocked = [](const meta::MetaCommand& command) {
    ActivatedGroupFixture fixture = MakeActivatedGroup();
    ExpectAccepted(fixture.stores, 9,
                   meta::MetaCommand{MakeBeginUncontrolled(fixture)});
    const std::string before = DomainBytes(fixture.stores);
    ExpectRejected(fixture.stores, 10, command);
    EXPECT_EQ(DomainBytes(fixture.stores), before);
  };

  meta::ActivateAuthority activate;
  activate.request_id_ = Filled<16>(0x61);
  activate.group_id_ = "g1";
  activate.expected_term_ = 2;
  activate.new_owner_ = NodeId(1);
  activate.new_topology_epoch_ = 4;
  expect_blocked(meta::MetaCommand{activate});

  meta::RemoveNodeFromGroup remove;
  remove.request_id_ = Filled<16>(0x62);
  remove.group_id_ = "g1";
  remove.node_id_ = NodeId(1);
  remove.expected_revision_ = 2;
  remove.new_topology_epoch_ = 4;
  expect_blocked(meta::MetaCommand{remove});

  meta::SetGroupReplicationState replication;
  replication.request_id_ = Filled<16>(0x63);
  replication.group_id_ = "g1";
  replication.expected_population_manifest_revision_ = 0;
  replication.expected_population_manifest_digest_.fill(0);
  replication.new_population_manifest_revision_ = 0;
  replication.new_population_manifest_digest_.fill(0);
  replication.expected_partition_replication_epoch_ = 0;
  replication.new_partition_replication_epoch_ = 1;
  replication.new_topology_epoch_ = 4;
  expect_blocked(meta::MetaCommand{replication});

  meta::BeginGroupTerm begin_term;
  begin_term.request_id_ = Filled<16>(0x64);
  begin_term.group_id_ = "g1";
  begin_term.expected_term_ = 2;
  begin_term.new_term_ = 3;
  expect_blocked(meta::MetaCommand{begin_term});

  meta::SetSlotMap slots;
  slots.request_id_ = Filled<16>(0x65);
  slots.ranges_ = {{0, 16383, "g1"}};
  slots.new_topology_epoch_ = 4;
  expect_blocked(meta::MetaCommand{slots});
}

TEST(MetaFailoverTransitionApply,
     BeginUncontrolledRequiresCreatedClusterLifecycle) {
  const meta::BeginUncontrolledFailover command =
      MakeBeginUncontrolled(MakeActivatedGroup());

  {
    meta::MetaStores uninitialized;
    ExpectRejected(uninitialized, 1, meta::MetaCommand{command});
  }
  {
    meta::MetaStores creating;
    ExpectAccepted(creating, 1, meta::MetaCommand{ClusterCreateRoot()});
    ExpectRejected(creating, 2, meta::MetaCommand{command});
  }
  {
    meta::MetaStores failed;
    const meta::SubmitOperation root = ClusterCreateRoot();
    ExpectAccepted(failed, 1, meta::MetaCommand{root});
    meta::AbortOperation abort;
    abort.request_id_ = Filled<16>(0x71);
    abort.operation_id_ = root.operation_id_;
    abort.expected_revision_ = 0;
    abort.reason_ = "provisioning failed";
    ExpectAccepted(failed, 2, meta::MetaCommand{abort});
    ExpectRejected(failed, 3, meta::MetaCommand{command});
  }
}

TEST(MetaFailoverTransitionApply,
     AggregateRestoreAllowsTransitionOnlyInCreatedLifecycle) {
  {
    ActivatedGroupFixture uninitialized;
    PopulateActivatedGroup(uninitialized, 1);
    InstallUncontrolledPostStateDirectly(uninitialized, 7);
    ExpectAggregateRestoreFails(uninitialized.stores);
  }
  {
    ActivatedGroupFixture creating;
    ExpectAccepted(creating.stores, 1, meta::MetaCommand{ClusterCreateRoot()});
    PopulateActivatedGroup(creating, 2);
    InstallUncontrolledPostStateDirectly(creating, 8);
    ExpectAggregateRestoreFails(creating.stores);
  }
  {
    ActivatedGroupFixture failed;
    const meta::SubmitOperation root = ClusterCreateRoot();
    ExpectAccepted(failed.stores, 1, meta::MetaCommand{root});
    meta::AbortOperation abort;
    abort.request_id_ = Filled<16>(0x72);
    abort.operation_id_ = root.operation_id_;
    abort.expected_revision_ = 0;
    abort.reason_ = "provisioning failed";
    ExpectAccepted(failed.stores, 2, meta::MetaCommand{abort});
    PopulateActivatedGroup(failed, 3);
    InstallUncontrolledPostStateDirectly(failed, 9);
    ExpectAggregateRestoreFails(failed.stores);
  }
  {
    ActivatedGroupFixture created = MakeActivatedGroup();
    InstallUncontrolledPostStateDirectly(created, 9);
    const auto bytes = created.stores.Serialize();
    ASSERT_TRUE(bytes.ok()) << bytes.status();
    const auto restored = meta::MetaStores::Deserialize(*bytes);
    ASSERT_TRUE(restored.ok()) << restored.status();
    EXPECT_EQ(restored->topology_.FindGroup("g1"),
              created.stores.topology_.FindGroup("g1"));
  }
}

TEST(MetaFailoverTransitionApply,
     AggregateRestoreRejectsTransitionWhoseHistoricalOwnerWasRemoved) {
  ActivatedGroupFixture fixture = MakeActivatedGroup();
  InstallUncontrolledPostStateDirectly(fixture, 9);

  meta::RemoveNodeFromGroup remove;
  remove.group_id_ = "g1";
  remove.node_id_ = fixture.owner;
  remove.expected_revision_ = 2;
  remove.new_topology_epoch_ = 4;
  ASSERT_TRUE(fixture.stores.topology_.Apply(remove).ok());

  ExpectAggregateRestoreFails(fixture.stores);
}

}  // namespace
