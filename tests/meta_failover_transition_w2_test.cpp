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
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "gtest/gtest.h"
#include "lavik/meta/cluster_create.h"
#include "lavik/meta/commands.h"
#include "lavik/meta/failover.h"
#include "lavik/meta/hash.h"
#include "lavik/meta/state_apply.h"
#include "meta_topology_test_access.h"

namespace {

namespace meta = lavik::meta;

constexpr std::string_view kActor = "lavik://operator/failover-w2-test";
constexpr std::string_view kTime = "2026-09-15T00:00:00Z";

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

std::array<std::string, 6> DomainBytes(const meta::MetaStores& stores) {
  // A rejected command still appends its mandatory audit record. Compare the
  // canonical serialization of every other committed store to prove that no
  // partial domain effect escaped before the rejection.
  return {stores.identity_.Serialize(), stores.topology_.Serialize(),
          stores.policy_.Serialize(),
          stores.operation_.Serialize().value_or("invalid-operation"),
          stores.population_manifest_.Serialize()};
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
  meta::MetaBootIncarnation owner_boot =
      Filled<meta::kMetaBootIncarnationBytes>(0x31);
  meta::MetaBootIncarnation candidate_boot =
      Filled<meta::kMetaBootIncarnationBytes>(0x32);
  meta::MetaReplicationHistoryId source_history =
      Filled<meta::kMetaReplicationHistoryIdBytes>(0x33);
  meta::MetaOperationId operation_id = Filled<16>(0x41);
  meta::MetaFailoverTransitionId transition_id = Filled<16>(0x42);
  std::uint64_t absolute_deadline_unix_ms = 2'000'000'000'000ULL;
  std::uint64_t next_index = 1;
};

std::uint64_t AcceptFresh(Fixture& fixture, const meta::MetaCommand& command) {
  const std::uint64_t index = fixture.next_index++;
  ExpectAccepted(fixture.stores, index, command);
  return index;
}

meta::MetaApplyResult RejectFresh(Fixture& fixture,
                                  const meta::MetaCommand& command) {
  const std::uint64_t index = fixture.next_index++;
  const auto before = DomainBytes(fixture.stores);
  const std::size_t audit_size = fixture.stores.audit_.size();
  meta::MetaApplyResult result = ExpectRejected(fixture.stores, index, command);
  EXPECT_EQ(DomainBytes(fixture.stores), before);
  EXPECT_EQ(fixture.stores.audit_.size(), audit_size + 1);
  const auto audit = fixture.stores.audit_.Find(index);
  EXPECT_TRUE(audit.has_value());
  if (audit.has_value()) {
    EXPECT_EQ(audit->verdict_, meta::MetaAuditVerdict::kRejected);
  }
  return result;
}

void RejectFreshWithDetail(Fixture& fixture, const meta::MetaCommand& command,
                           std::string_view expected_detail) {
  const meta::MetaApplyResult result = RejectFresh(fixture, command);
  EXPECT_NE(result.detail_.find(expected_detail), std::string::npos)
      << result.detail_;
}

void ExpectExactReplay(Fixture& fixture, std::uint64_t index,
                       const meta::MetaCommand& command) {
  const auto before = DomainBytes(fixture.stores);
  const std::size_t audit_size = fixture.stores.audit_.size();
  ExpectAccepted(fixture.stores, index, command);
  EXPECT_EQ(DomainBytes(fixture.stores), before);
  EXPECT_EQ(fixture.stores.audit_.size(), audit_size);
}

void RegisterNode(Fixture& fixture, const std::string& node_id,
                  meta::MetaNodeRole role, std::uint8_t request_seed,
                  std::uint16_t port) {
  meta::RegisterNode node;
  node.request_id_ = Filled<16>(request_seed);
  node.node_id_ = node_id;
  node.principal_ = "lavik://node/" + node_id;
  node.endpoints_ = {"tcp://127.0.0.1:" + std::to_string(port)};
  node.role_ = role;
  AcceptFresh(fixture, meta::MetaCommand{node});
}

std::unique_ptr<Fixture> MakeFixture() {
  // Keep the large topology store off the stack: Debug builds can reserve a
  // separate stack slot for every scoped fixture in the rejection matrix.
  auto fixture_owner = std::make_unique<Fixture>();
  Fixture& fixture = *fixture_owner;
  const meta::SubmitOperation root = ClusterCreateRoot();
  AcceptFresh(fixture, meta::MetaCommand{root});

  meta::PutPolicy automatic;
  automatic.request_id_ = Filled<16>(0x0f);
  automatic.policy_id_ =
      std::string(meta::kAutomaticUncontrolledFailoverPolicyId);
  automatic.version_ = 1;
  automatic.content_ =
      R"({"kind":"automatic-uncontrolled-failover-v1","suspect_after_ms":5000})";
  EXPECT_TRUE(fixture.stores.policy_.Apply(automatic).ok());

  meta::PutPolicy authority;
  authority.request_id_ = Filled<16>(0x09);
  authority.policy_id_ = std::string(meta::kAuthorityLeasePolicyId);
  authority.version_ = 1;
  authority.content_ = R"({"kind":"authority-lease-v1","duration_ms":5000})";
  EXPECT_TRUE(fixture.stores.policy_.Apply(authority).ok());

  meta::CompleteOperation complete;
  complete.request_id_ = Filled<16>(0x03);
  complete.operation_id_ = root.operation_id_;
  complete.expected_revision_ = 0;
  complete.result_ = "cluster-created";
  AcceptFresh(fixture, meta::MetaCommand{complete});

  RegisterNode(fixture, fixture.owner, meta::MetaNodeRole::kPrimary, 0x04,
               6379);
  RegisterNode(fixture, fixture.candidate, meta::MetaNodeRole::kReplica, 0x05,
               6380);

  meta::CreateGroup group;
  group.request_id_ = Filled<16>(0x06);
  group.group_id_ = "g1";
  group.new_topology_epoch_ = 1;
  AcceptFresh(fixture, meta::MetaCommand{group});

  meta::AssignNodeToGroup assign_owner;
  assign_owner.request_id_ = Filled<16>(0x07);
  assign_owner.group_id_ = "g1";
  assign_owner.node_id_ = fixture.owner;
  assign_owner.assignment_id_ = fixture.owner_assignment;
  assign_owner.role_ = meta::MetaNodeRole::kPrimary;
  assign_owner.expected_revision_ = 1;
  assign_owner.new_topology_epoch_ = 2;
  AcceptFresh(fixture, meta::MetaCommand{assign_owner});

  meta::AssignNodeToGroup assign_candidate;
  assign_candidate.request_id_ = Filled<16>(0x08);
  assign_candidate.group_id_ = "g1";
  assign_candidate.node_id_ = fixture.candidate;
  assign_candidate.assignment_id_ = fixture.candidate_assignment;
  assign_candidate.role_ = meta::MetaNodeRole::kReplica;
  assign_candidate.expected_revision_ = 2;
  assign_candidate.new_topology_epoch_ = 3;
  AcceptFresh(fixture, meta::MetaCommand{assign_candidate});

  meta::PutPolicy policy;
  policy.request_id_ = Filled<16>(0x09);
  policy.policy_id_ = std::string(meta::kAuthorityLeasePolicyId);
  policy.version_ = 1;
  policy.content_ = R"({"kind":"authority-lease-v1","duration_ms":5000})";
  AcceptFresh(fixture, meta::MetaCommand{policy});

  meta::BeginGroupTerm begin_term;
  begin_term.request_id_ = Filled<16>(0x0a);
  begin_term.group_id_ = "g1";
  begin_term.expected_term_ = 0;
  begin_term.new_term_ = 1;
  AcceptFresh(fixture, meta::MetaCommand{begin_term});

  meta::ActivateAuthority activate;
  activate.request_id_ = Filled<16>(0x0b);
  activate.group_id_ = "g1";
  activate.expected_term_ = 1;
  activate.new_owner_ = fixture.owner;
  activate.new_topology_epoch_ = 4;
  AcceptFresh(fixture, meta::MetaCommand{activate});

  EXPECT_EQ(fixture.next_index, 11u);
  return fixture_owner;
}

meta::MetaFailoverCandidateAction CandidateAction(const Fixture& fixture,
                                                  std::uint8_t action_seed) {
  meta::MetaFailoverCandidateAction action;
  action.action_id_ = Filled<16>(action_seed);
  action.candidate_.node_id_ = fixture.candidate;
  action.candidate_.assignment_id_ = fixture.candidate_assignment;
  action.candidate_.boot_id_ = fixture.candidate_boot;
  action.domain_.source_group_term_ = 1;
  action.domain_.source_node_id_ = fixture.owner;
  action.domain_.source_assignment_id_ = fixture.owner_assignment;
  action.domain_.source_boot_id_ = fixture.owner_boot;
  action.domain_.source_history_id_ = fixture.source_history;
  action.domain_.flow_count_ = 2;
  return action;
}

template <typename Command>
void SetGroupAnchors(Command& command, const Fixture& fixture,
                     std::uint64_t expected_term) {
  command.expected_owner_node_id_ = fixture.owner;
  command.expected_owner_assignment_id_ = fixture.owner_assignment;
  command.expected_membership_revision_ = 3;
  command.expected_group_term_ = expected_term;
  command.expected_population_manifest_revision_ = 0;
  command.expected_population_manifest_digest_.fill(0);
  command.expected_partition_replication_epoch_ = 0;
}

void SubmitControlledOperation(Fixture& fixture) {
  meta::FailoverOperationIntent intent;
  intent.group_id_ = "g1";
  intent.absolute_deadline_unix_ms_ = fixture.absolute_deadline_unix_ms;
  const auto encoded = meta::EncodeFailoverOperationIntent(intent);
  ASSERT_TRUE(encoded.ok()) << encoded.status();

  meta::SubmitOperation submit;
  submit.request_id_ = Filled<16>(0x40);
  submit.operation_id_ = fixture.operation_id;
  submit.kind_ = std::string(meta::kFailoverOperationKind);
  submit.intent_ = *encoded;
  submit.intent_hash_ = meta::MetaSha256(submit.intent_);
  AcceptFresh(fixture, meta::MetaCommand{submit});
}

meta::BeginControlledFailover MakeBeginControlled(const Fixture& fixture) {
  meta::BeginControlledFailover begin;
  begin.request_id_ = Filled<16>(0x43);
  begin.group_id_ = "g1";
  begin.transition_id_ = fixture.transition_id;
  begin.target_term_ = 2;
  begin.candidate_action_ = CandidateAction(fixture, 0x44);
  begin.operation_id_ = fixture.operation_id;
  begin.expected_operation_revision_ = 0;
  begin.absolute_deadline_unix_ms_ = fixture.absolute_deadline_unix_ms;
  SetGroupAnchors(begin, fixture, 1);
  return begin;
}

meta::BeginUncontrolledFailover MakeBeginUncontrolled(
    const Fixture& fixture, bool include_candidate = true) {
  meta::BeginUncontrolledFailover begin;
  begin.request_id_ = Filled<16>(0x45);
  begin.group_id_ = "g1";
  begin.transition_id_ = fixture.transition_id;
  begin.target_term_ = 2;
  if (include_candidate) {
    begin.candidate_action_ = CandidateAction(fixture, 0x46);
  }
  SetGroupAnchors(begin, fixture, 1);
  return begin;
}

meta::AuthorizeFailoverPrepare MakeAuthorize(
    const Fixture& fixture, std::uint64_t transition_revision,
    const meta::MetaFailoverCandidateAction& action,
    meta::MetaFailoverLoss loss) {
  meta::AuthorizeFailoverPrepare authorize;
  authorize.request_id_ = Filled<16>(0x47);
  authorize.group_id_ = "g1";
  authorize.expected_transition_ = {fixture.transition_id, transition_revision};
  authorize.action_id_ = action.action_id_;
  authorize.loss_if_cutover_ = loss;
  return authorize;
}

meta::AbortControlledFailover MakeAbort(
    const Fixture& fixture,
    std::optional<meta::MetaFailoverTransitionRef> transition,
    std::string reason = "candidate disconnected") {
  meta::AbortControlledFailover abort;
  abort.request_id_ = Filled<16>(0x48);
  abort.operation_id_ = fixture.operation_id;
  abort.expected_operation_revision_ = 0;
  abort.group_id_ = "g1";
  abort.expected_transition_ = transition;
  abort.reason_ = std::move(reason);
  return abort;
}

meta::DegradeControlledFailover MakeDegrade(
    const Fixture& fixture, std::uint64_t transition_revision,
    const meta::MetaFailoverCandidateAction& expected_action, bool retain,
    std::string reason) {
  meta::DegradeControlledFailover degrade;
  degrade.request_id_ = Filled<16>(0x49);
  degrade.operation_id_ = fixture.operation_id;
  degrade.expected_operation_revision_ = 0;
  degrade.group_id_ = "g1";
  degrade.expected_transition_ = {fixture.transition_id, transition_revision};
  degrade.expected_candidate_action_ = expected_action;
  degrade.retain_candidate_action_ = retain;
  degrade.reason_ = std::move(reason);
  return degrade;
}

meta::CommitControlledFailover MakeCommitControlled(
    const Fixture& fixture, std::uint64_t transition_revision,
    std::uint64_t authorized_revision,
    const meta::MetaFailoverCandidateAction& action) {
  meta::CommitControlledFailover commit;
  commit.request_id_ = Filled<16>(0x4a);
  commit.operation_id_ = fixture.operation_id;
  commit.expected_operation_revision_ = 0;
  commit.group_id_ = "g1";
  commit.expected_transition_ = {fixture.transition_id, transition_revision};
  commit.action_id_ = action.action_id_;
  commit.authorized_revision_ = authorized_revision;
  commit.expected_candidate_ = action.candidate_;
  SetGroupAnchors(commit, fixture, 1);
  commit.new_topology_epoch_ = 5;
  return commit;
}

meta::CommitUncontrolledFailover MakeCommitUncontrolled(
    const Fixture& fixture, std::uint64_t transition_revision,
    std::uint64_t authorized_revision,
    const meta::MetaFailoverCandidateAction& action,
    meta::MetaFailoverLoss loss) {
  meta::CommitUncontrolledFailover commit;
  commit.request_id_ = Filled<16>(0x4b);
  commit.group_id_ = "g1";
  commit.expected_transition_ = {fixture.transition_id, transition_revision};
  commit.action_id_ = action.action_id_;
  commit.authorized_revision_ = authorized_revision;
  commit.loss_if_cutover_ = loss;
  commit.expected_candidate_ = action.candidate_;
  SetGroupAnchors(commit, fixture, 2);
  commit.new_topology_epoch_ = 5;
  return commit;
}

void ExpectAuthorityUnchanged(const Fixture& fixture) {
  const auto group = fixture.stores.topology_.FindGroup("g1");
  ASSERT_TRUE(group.has_value());
  EXPECT_EQ(group->record_.owner_, fixture.owner);
  EXPECT_EQ(group->record_.group_term_, 1u);
  EXPECT_EQ(fixture.stores.topology_.TopologyEpoch(), 4u);

  const auto state = fixture.stores.topology_.AuthorityFor("g1");
  ASSERT_TRUE(state.has_value());
  ASSERT_TRUE(state->grant_.has_value());
  EXPECT_EQ(state->group_term_, 1u);
  EXPECT_EQ(state->grant_->owner_, fixture.owner);
  EXPECT_FALSE(state->grant_->activation_action_id_.has_value());
}

void ExpectCutover(const Fixture& fixture,
                   const meta::MetaFailoverActionId& action_id) {
  const auto group = fixture.stores.topology_.FindGroup("g1");
  ASSERT_TRUE(group.has_value());
  EXPECT_EQ(group->record_.owner_, fixture.candidate);
  EXPECT_EQ(group->record_.group_term_, 2u);
  EXPECT_FALSE(group->failover_transition_.has_value());
  EXPECT_EQ(fixture.stores.topology_.TopologyEpoch(), 5u);

  const auto state = fixture.stores.topology_.AuthorityFor("g1");
  ASSERT_TRUE(state.has_value());
  EXPECT_EQ(state->group_term_, 2u);
  ASSERT_TRUE(state->grant_.has_value());
  EXPECT_EQ(state->grant_->owner_, fixture.candidate);
  ASSERT_TRUE(state->grant_->activation_action_id_.has_value());
  EXPECT_EQ(*state->grant_->activation_action_id_, action_id);
}

TEST(MetaFailoverTransitionW2,
     ControlledBeginPersistsTransitionWithoutChangingAuthority) {
  auto fixture_owner = MakeFixture();
  Fixture& fixture = *fixture_owner;
  SubmitControlledOperation(fixture);
  const meta::BeginControlledFailover begin = MakeBeginControlled(fixture);

  const std::uint64_t begin_index =
      AcceptFresh(fixture, meta::MetaCommand{begin});

  ExpectAuthorityUnchanged(fixture);
  const auto group = fixture.stores.topology_.FindGroup("g1");
  ASSERT_TRUE(group->failover_transition_.has_value());
  const meta::MetaFailoverTransition& transition = *group->failover_transition_;
  EXPECT_EQ(transition.transition_id_, fixture.transition_id);
  EXPECT_EQ(transition.revision_, begin_index);
  EXPECT_EQ(transition.mode_, meta::MetaFailoverMode::kControlled);
  EXPECT_EQ(transition.target_term_, 2u);
  ASSERT_TRUE(transition.candidate_action_.has_value());
  EXPECT_EQ(*transition.candidate_action_, begin.candidate_action_);
  ASSERT_TRUE(transition.controlled_.has_value());
  EXPECT_EQ(transition.controlled_->operation_id_, fixture.operation_id);
  EXPECT_EQ(transition.controlled_->absolute_deadline_unix_ms_,
            fixture.absolute_deadline_unix_ms);

  const auto operation =
      fixture.stores.operation_.FindOperation(fixture.operation_id);
  ASSERT_TRUE(operation.has_value());
  EXPECT_EQ(operation->lifecycle_, meta::MetaOperationLifecycle::kSubmitted);
  EXPECT_EQ(operation->revision_, 0u);
  ExpectExactReplay(fixture, begin_index, meta::MetaCommand{begin});
  RejectFresh(fixture, meta::MetaCommand{begin});
}

TEST(MetaFailoverTransitionW2,
     UncontrolledCandidateReplacementAndClearUseExactTransitionCas) {
  auto fixture_owner = MakeFixture();
  Fixture& fixture = *fixture_owner;
  const meta::BeginUncontrolledFailover begin = MakeBeginUncontrolled(fixture);
  const std::uint64_t begin_index =
      AcceptFresh(fixture, meta::MetaCommand{begin});
  ExpectExactReplay(fixture, begin_index, meta::MetaCommand{begin});

  const meta::MetaFailoverCandidateAction replacement =
      CandidateAction(fixture, 0x51);
  meta::SetUncontrolledCandidate replace;
  replace.request_id_ = Filled<16>(0x52);
  replace.group_id_ = "g1";
  replace.expected_transition_ = {fixture.transition_id, begin_index};
  replace.candidate_action_ = replacement;
  const std::uint64_t replace_index =
      AcceptFresh(fixture, meta::MetaCommand{replace});
  ExpectExactReplay(fixture, replace_index, meta::MetaCommand{replace});

  auto transition =
      fixture.stores.topology_.FindGroup("g1")->failover_transition_;
  ASSERT_TRUE(transition.has_value());
  EXPECT_EQ(transition->revision_, replace_index);
  ASSERT_TRUE(transition->candidate_action_.has_value());
  EXPECT_EQ(*transition->candidate_action_, replacement);
  EXPECT_FALSE(transition->candidate_action_->authorization_.has_value());

  meta::SetUncontrolledCandidate stale = replace;
  stale.request_id_ = Filled<16>(0x53);
  stale.candidate_action_ = CandidateAction(fixture, 0x54);
  RejectFresh(fixture, meta::MetaCommand{stale});

  meta::SetUncontrolledCandidate reuses_current_action = replace;
  reuses_current_action.request_id_ = Filled<16>(0x55);
  reuses_current_action.expected_transition_ = {fixture.transition_id,
                                                replace_index};
  RejectFresh(fixture, meta::MetaCommand{reuses_current_action});

  meta::SetUncontrolledCandidate clear;
  clear.request_id_ = Filled<16>(0x56);
  clear.group_id_ = "g1";
  clear.expected_transition_ = {fixture.transition_id, replace_index};
  const std::uint64_t clear_index =
      AcceptFresh(fixture, meta::MetaCommand{clear});
  ExpectExactReplay(fixture, clear_index, meta::MetaCommand{clear});

  transition = fixture.stores.topology_.FindGroup("g1")->failover_transition_;
  ASSERT_TRUE(transition.has_value());
  EXPECT_EQ(transition->revision_, clear_index);
  EXPECT_FALSE(transition->candidate_action_.has_value());
  EXPECT_EQ(transition->mode_, meta::MetaFailoverMode::kUncontrolled);
  EXPECT_EQ(transition->target_term_, 2u);
  EXPECT_FALSE(fixture.stores.topology_.AuthorityFor("g1")->grant_.has_value());
}

TEST(MetaFailoverTransitionW2,
     UncontrolledCandidateClearRequiresAnInstalledCandidate) {
  auto fixture_owner = MakeFixture();
  Fixture& fixture = *fixture_owner;
  const meta::BeginUncontrolledFailover begin =
      MakeBeginUncontrolled(fixture, false);
  const std::uint64_t begin_index =
      AcceptFresh(fixture, meta::MetaCommand{begin});

  meta::SetUncontrolledCandidate clear;
  clear.request_id_ = Filled<16>(0x57);
  clear.group_id_ = "g1";
  clear.expected_transition_ = {fixture.transition_id, begin_index};
  RejectFreshWithDetail(fixture, meta::MetaCommand{clear},
                        "uncontrolled failover candidate is absent");
}

TEST(MetaFailoverTransitionW2,
     AuthorizationLatchesModeSpecificLossAndReplaysExactly) {
  {
    auto fixture_owner = MakeFixture();
    Fixture& fixture = *fixture_owner;
    SubmitControlledOperation(fixture);
    const meta::BeginControlledFailover begin = MakeBeginControlled(fixture);
    const std::uint64_t begin_index =
        AcceptFresh(fixture, meta::MetaCommand{begin});

    RejectFresh(fixture, meta::MetaCommand{MakeAuthorize(
                             fixture, begin_index, begin.candidate_action_,
                             meta::MetaFailoverLoss::kUnknown)});
    const meta::AuthorizeFailoverPrepare authorize =
        MakeAuthorize(fixture, begin_index, begin.candidate_action_,
                      meta::MetaFailoverLoss::kNone);
    const std::uint64_t authorize_index =
        AcceptFresh(fixture, meta::MetaCommand{authorize});
    ExpectExactReplay(fixture, authorize_index, meta::MetaCommand{authorize});

    const auto transition =
        fixture.stores.topology_.FindGroup("g1")->failover_transition_;
    ASSERT_TRUE(transition->candidate_action_->authorization_.has_value());
    EXPECT_EQ(transition->revision_, authorize_index);
    EXPECT_EQ(
        transition->candidate_action_->authorization_->authorized_revision_,
        authorize_index);
    EXPECT_EQ(transition->candidate_action_->authorization_->loss_if_cutover_,
              meta::MetaFailoverLoss::kNone);

    meta::AuthorizeFailoverPrepare relatch = authorize;
    relatch.request_id_ = Filled<16>(0x61);
    relatch.expected_transition_ = {fixture.transition_id, authorize_index};
    RejectFresh(fixture, meta::MetaCommand{relatch});
  }

  {
    auto fixture_owner = MakeFixture();
    Fixture& fixture = *fixture_owner;
    const meta::BeginUncontrolledFailover begin =
        MakeBeginUncontrolled(fixture);
    const std::uint64_t begin_index =
        AcceptFresh(fixture, meta::MetaCommand{begin});
    const meta::MetaFailoverCandidateAction action = *begin.candidate_action_;

    RejectFresh(fixture, meta::MetaCommand{
                             MakeAuthorize(fixture, begin_index, action,
                                           meta::MetaFailoverLoss::kNone)});
    const meta::AuthorizeFailoverPrepare authorize = MakeAuthorize(
        fixture, begin_index, action, meta::MetaFailoverLoss::kUnknown);
    const std::uint64_t authorize_index =
        AcceptFresh(fixture, meta::MetaCommand{authorize});
    ExpectExactReplay(fixture, authorize_index, meta::MetaCommand{authorize});

    const auto transition =
        fixture.stores.topology_.FindGroup("g1")->failover_transition_;
    ASSERT_TRUE(transition->candidate_action_->authorization_.has_value());
    EXPECT_EQ(transition->candidate_action_->authorization_->loss_if_cutover_,
              meta::MetaFailoverLoss::kUnknown);
  }
}

TEST(MetaFailoverTransitionW2,
     PreBeginTypedAbortTerminatesOnlyItsOperationAndSurvivesUnrelatedState) {
  auto fixture_owner = MakeFixture();
  Fixture& fixture = *fixture_owner;
  SubmitControlledOperation(fixture);
  const meta::AbortControlledFailover abort =
      MakeAbort(fixture, std::nullopt, "no eligible candidate");
  const std::uint64_t abort_index =
      AcceptFresh(fixture, meta::MetaCommand{abort});
  ExpectExactReplay(fixture, abort_index, meta::MetaCommand{abort});

  const auto operation =
      fixture.stores.operation_.FindOperation(fixture.operation_id);
  ASSERT_TRUE(operation.has_value());
  EXPECT_EQ(operation->lifecycle_, meta::MetaOperationLifecycle::kAborted);
  EXPECT_EQ(operation->revision_, 1u);
  EXPECT_EQ(operation->terminal_result_, abort.reason_);
  EXPECT_FALSE(operation->data_loss_possible_);
  ExpectAuthorityUnchanged(fixture);

  const meta::BeginUncontrolledFailover unrelated =
      MakeBeginUncontrolled(fixture);
  const std::uint64_t begin_index =
      AcceptFresh(fixture, meta::MetaCommand{unrelated});
  ExpectExactReplay(fixture, abort_index, meta::MetaCommand{abort});
  const auto transition =
      fixture.stores.topology_.FindGroup("g1")->failover_transition_;
  ASSERT_TRUE(transition.has_value());
  EXPECT_EQ(transition->revision_, begin_index);
  EXPECT_EQ(transition->mode_, meta::MetaFailoverMode::kUncontrolled);
}

TEST(MetaFailoverTransitionW2,
     PostBeginTypedAbortAtomicallyClearsTransitionAndRejectsPartialRepair) {
  {
    auto fixture_owner = MakeFixture();
    Fixture& fixture = *fixture_owner;
    SubmitControlledOperation(fixture);
    const meta::BeginControlledFailover begin = MakeBeginControlled(fixture);
    const std::uint64_t begin_index =
        AcceptFresh(fixture, meta::MetaCommand{begin});
    const meta::AbortControlledFailover abort = MakeAbort(
        fixture,
        meta::MetaFailoverTransitionRef{fixture.transition_id, begin_index});

    const std::uint64_t abort_index =
        AcceptFresh(fixture, meta::MetaCommand{abort});
    ExpectExactReplay(fixture, abort_index, meta::MetaCommand{abort});
    EXPECT_FALSE(fixture.stores.topology_.FindGroup("g1")
                     ->failover_transition_.has_value());
    const auto operation =
        fixture.stores.operation_.FindOperation(fixture.operation_id);
    ASSERT_TRUE(operation.has_value());
    EXPECT_EQ(operation->lifecycle_, meta::MetaOperationLifecycle::kAborted);
    EXPECT_EQ(operation->terminal_result_, abort.reason_);
    ExpectAuthorityUnchanged(fixture);
  }

  {
    auto fixture_owner = MakeFixture();
    Fixture& fixture = *fixture_owner;
    SubmitControlledOperation(fixture);
    const meta::BeginControlledFailover begin = MakeBeginControlled(fixture);
    const std::uint64_t begin_index =
        AcceptFresh(fixture, meta::MetaCommand{begin});
    const meta::AbortControlledFailover abort = MakeAbort(
        fixture,
        meta::MetaFailoverTransitionRef{fixture.transition_id, begin_index});

    meta::AbortOperation operation_only;
    operation_only.operation_id_ = fixture.operation_id;
    operation_only.expected_revision_ = 0;
    operation_only.reason_ = abort.reason_;
    ASSERT_TRUE(fixture.stores.operation_.AbortOperation(operation_only).ok());
    RejectFresh(fixture, meta::MetaCommand{abort});
    EXPECT_TRUE(fixture.stores.topology_.FindGroup("g1")
                    ->failover_transition_.has_value());
  }
}

TEST(MetaFailoverTransitionW2,
     ControlledDegradeCanRetainOnlyTheExactLosslessAuthorizedAction) {
  auto fixture_owner = MakeFixture();
  Fixture& fixture = *fixture_owner;
  SubmitControlledOperation(fixture);
  const meta::BeginControlledFailover begin = MakeBeginControlled(fixture);
  const std::uint64_t begin_index =
      AcceptFresh(fixture, meta::MetaCommand{begin});
  const meta::AuthorizeFailoverPrepare authorize =
      MakeAuthorize(fixture, begin_index, begin.candidate_action_,
                    meta::MetaFailoverLoss::kNone);
  const std::uint64_t authorize_index =
      AcceptFresh(fixture, meta::MetaCommand{authorize});
  meta::MetaFailoverCandidateAction authorized_action = begin.candidate_action_;
  authorized_action.authorization_ = meta::MetaFailoverAuthorization{
      authorize_index, meta::MetaFailoverLoss::kNone};

  const meta::DegradeControlledFailover degrade =
      MakeDegrade(fixture, authorize_index, authorized_action, true,
                  "source authority unavailable");
  const std::uint64_t degrade_index =
      AcceptFresh(fixture, meta::MetaCommand{degrade});
  ExpectExactReplay(fixture, degrade_index, meta::MetaCommand{degrade});

  const auto transition =
      fixture.stores.topology_.FindGroup("g1")->failover_transition_;
  ASSERT_TRUE(transition.has_value());
  EXPECT_EQ(transition->revision_, degrade_index);
  EXPECT_EQ(transition->mode_, meta::MetaFailoverMode::kUncontrolled);
  EXPECT_EQ(transition->target_term_, 2u);
  EXPECT_FALSE(transition->controlled_.has_value());
  ASSERT_TRUE(transition->candidate_action_.has_value());
  EXPECT_EQ(*transition->candidate_action_, authorized_action);

  const auto group = fixture.stores.topology_.FindGroup("g1");
  EXPECT_EQ(group->record_.owner_, fixture.owner);
  EXPECT_EQ(group->record_.group_term_, 2u);
  EXPECT_EQ(fixture.stores.topology_.TopologyEpoch(), 4u);
  const auto grant = fixture.stores.topology_.AuthorityFor("g1");
  EXPECT_FALSE(grant->grant_.has_value());

  const auto operation =
      fixture.stores.operation_.FindOperation(fixture.operation_id);
  ASSERT_TRUE(operation.has_value());
  EXPECT_EQ(operation->lifecycle_, meta::MetaOperationLifecycle::kAborted);
  EXPECT_EQ(operation->revision_, 1u);
  EXPECT_EQ(operation->terminal_result_, degrade.reason_);
}

TEST(MetaFailoverTransitionW2,
     ControlledDegradeClearsActionAndRejectsAStaleActionSnapshot) {
  auto fixture_owner = MakeFixture();
  Fixture& fixture = *fixture_owner;
  SubmitControlledOperation(fixture);
  const meta::BeginControlledFailover begin = MakeBeginControlled(fixture);
  const std::uint64_t begin_index =
      AcceptFresh(fixture, meta::MetaCommand{begin});

  meta::MetaFailoverCandidateAction stale_action = begin.candidate_action_;
  stale_action.candidate_.boot_id_ =
      Filled<meta::kMetaBootIncarnationBytes>(0x71);
  RejectFresh(fixture, meta::MetaCommand{
                           MakeDegrade(fixture, begin_index, stale_action,
                                       false, "candidate context replaced")});

  const meta::DegradeControlledFailover degrade =
      MakeDegrade(fixture, begin_index, begin.candidate_action_, false,
                  "candidate disconnected");
  const std::uint64_t degrade_index =
      AcceptFresh(fixture, meta::MetaCommand{degrade});
  ExpectExactReplay(fixture, degrade_index, meta::MetaCommand{degrade});

  const auto transition =
      fixture.stores.topology_.FindGroup("g1")->failover_transition_;
  ASSERT_TRUE(transition.has_value());
  EXPECT_EQ(transition->revision_, degrade_index);
  EXPECT_EQ(transition->mode_, meta::MetaFailoverMode::kUncontrolled);
  EXPECT_FALSE(transition->candidate_action_.has_value());
  EXPECT_FALSE(transition->controlled_.has_value());
  EXPECT_EQ(fixture.stores.operation_.FindOperation(fixture.operation_id)
                ->terminal_result_,
            degrade.reason_);
}

TEST(MetaFailoverTransitionW2,
     ControlledCommitAtomicallyCutsOverAndCompletesItsOperation) {
  auto fixture_owner = MakeFixture();
  Fixture& fixture = *fixture_owner;
  SubmitControlledOperation(fixture);
  const meta::BeginControlledFailover begin = MakeBeginControlled(fixture);
  const std::uint64_t begin_index =
      AcceptFresh(fixture, meta::MetaCommand{begin});
  const meta::AuthorizeFailoverPrepare authorize =
      MakeAuthorize(fixture, begin_index, begin.candidate_action_,
                    meta::MetaFailoverLoss::kNone);
  const std::uint64_t authorize_index =
      AcceptFresh(fixture, meta::MetaCommand{authorize});
  const meta::CommitControlledFailover commit = MakeCommitControlled(
      fixture, authorize_index, authorize_index, begin.candidate_action_);

  const std::uint64_t commit_index =
      AcceptFresh(fixture, meta::MetaCommand{commit});
  ExpectCutover(fixture, begin.candidate_action_.action_id_);
  const auto operation =
      fixture.stores.operation_.FindOperation(fixture.operation_id);
  ASSERT_TRUE(operation.has_value());
  EXPECT_EQ(operation->lifecycle_, meta::MetaOperationLifecycle::kCompleted);
  EXPECT_EQ(operation->revision_, 1u);
  EXPECT_FALSE(operation->terminal_result_.empty());
  EXPECT_FALSE(operation->data_loss_possible_);
  const auto audit = fixture.stores.audit_.Find(commit_index);
  ASSERT_TRUE(audit.has_value());
  EXPECT_NE(audit->command_summary_.find("loss=none"), std::string::npos);
  ExpectExactReplay(fixture, commit_index, meta::MetaCommand{commit});
  const auto committed = DomainBytes(fixture.stores);
  AcceptFresh(fixture, meta::MetaCommand{commit});
  EXPECT_EQ(DomainBytes(fixture.stores), committed);
}

TEST(MetaFailoverTransitionW2,
     ControlledCommitRejectsTermOnlyPartialStateWithoutRepair) {
  auto fixture_owner = MakeFixture();
  Fixture& fixture = *fixture_owner;
  SubmitControlledOperation(fixture);
  const meta::BeginControlledFailover begin = MakeBeginControlled(fixture);
  const std::uint64_t begin_index =
      AcceptFresh(fixture, meta::MetaCommand{begin});
  const meta::AuthorizeFailoverPrepare authorize =
      MakeAuthorize(fixture, begin_index, begin.candidate_action_,
                    meta::MetaFailoverLoss::kNone);
  const std::uint64_t authorize_index =
      AcceptFresh(fixture, meta::MetaCommand{authorize});
  const meta::CommitControlledFailover commit = MakeCommitControlled(
      fixture, authorize_index, authorize_index, begin.candidate_action_);

  meta::BeginGroupTerm term_only;
  term_only.group_id_ = "g1";
  term_only.expected_term_ = 1;
  term_only.new_term_ = 2;
  ASSERT_TRUE(fixture.stores.topology_.BeginGroupTerm(term_only).ok());
  ASSERT_TRUE(lavik::meta::MetaTopologyTestAccess::SetGroupTerm(
                  fixture.stores.topology_, "g1", 2)
                  .ok());
  RejectFresh(fixture, meta::MetaCommand{commit});

  const auto group = fixture.stores.topology_.FindGroup("g1");
  EXPECT_EQ(group->record_.owner_, fixture.owner);
  EXPECT_TRUE(group->failover_transition_.has_value());
  EXPECT_EQ(
      fixture.stores.operation_.FindOperation(fixture.operation_id)->lifecycle_,
      meta::MetaOperationLifecycle::kSubmitted);
}

TEST(MetaFailoverTransitionW2,
     UncontrolledCommitRequiresLatchedLossAndCutsOverWithoutAnOperation) {
  auto fixture_owner = MakeFixture();
  Fixture& fixture = *fixture_owner;
  const meta::BeginUncontrolledFailover begin = MakeBeginUncontrolled(fixture);
  const std::uint64_t begin_index =
      AcceptFresh(fixture, meta::MetaCommand{begin});
  const meta::MetaFailoverCandidateAction action = *begin.candidate_action_;
  const meta::AuthorizeFailoverPrepare authorize = MakeAuthorize(
      fixture, begin_index, action, meta::MetaFailoverLoss::kUnknown);
  const std::uint64_t authorize_index =
      AcceptFresh(fixture, meta::MetaCommand{authorize});

  RejectFresh(fixture, meta::MetaCommand{MakeCommitUncontrolled(
                           fixture, authorize_index, authorize_index, action,
                           meta::MetaFailoverLoss::kNone)});
  const meta::CommitUncontrolledFailover commit =
      MakeCommitUncontrolled(fixture, authorize_index, authorize_index, action,
                             meta::MetaFailoverLoss::kUnknown);
  const std::size_t live_operations = fixture.stores.operation_.LiveCount();
  const std::uint64_t commit_index =
      AcceptFresh(fixture, meta::MetaCommand{commit});

  ExpectCutover(fixture, action.action_id_);
  EXPECT_EQ(fixture.stores.operation_.LiveCount(), live_operations);
  EXPECT_EQ(fixture.stores.operation_.ActiveCount(), 0u);
  const auto audit = fixture.stores.audit_.Find(commit_index);
  ASSERT_TRUE(audit.has_value());
  EXPECT_NE(audit->command_summary_.find("loss=unknown"), std::string::npos);
  ExpectExactReplay(fixture, commit_index, meta::MetaCommand{commit});
  const auto committed = DomainBytes(fixture.stores);
  AcceptFresh(fixture, meta::MetaCommand{commit});
  EXPECT_EQ(DomainBytes(fixture.stores), committed);
}

TEST(MetaFailoverTransitionW2,
     TypedCommandRejectionMatrixLeavesEveryDomainStoreUnchanged) {
  const auto install_controlled = [](Fixture& fixture) {
    SubmitControlledOperation(fixture);
    const meta::BeginControlledFailover begin = MakeBeginControlled(fixture);
    const std::uint64_t revision =
        AcceptFresh(fixture, meta::MetaCommand{begin});
    return std::pair{begin, revision};
  };
  const auto install_uncontrolled = [](Fixture& fixture) {
    const meta::BeginUncontrolledFailover begin =
        MakeBeginUncontrolled(fixture);
    const std::uint64_t revision =
        AcceptFresh(fixture, meta::MetaCommand{begin});
    return std::pair{begin, revision};
  };
  const auto authorize_controlled = [](Fixture& fixture, const auto& attempt) {
    const meta::AuthorizeFailoverPrepare authorize =
        MakeAuthorize(fixture, attempt.second, attempt.first.candidate_action_,
                      meta::MetaFailoverLoss::kNone);
    return AcceptFresh(fixture, meta::MetaCommand{authorize});
  };
  const auto authorize_uncontrolled = [](Fixture& fixture,
                                         const auto& attempt) {
    const meta::AuthorizeFailoverPrepare authorize =
        MakeAuthorize(fixture, attempt.second, *attempt.first.candidate_action_,
                      meta::MetaFailoverLoss::kUnknown);
    return AcceptFresh(fixture, meta::MetaCommand{authorize});
  };

  {
    SCOPED_TRACE("BeginControlledFailover/stale-topology-anchor");
    auto fixture_owner = MakeFixture();
    Fixture& fixture = *fixture_owner;
    SubmitControlledOperation(fixture);
    meta::BeginControlledFailover command = MakeBeginControlled(fixture);
    ++command.expected_membership_revision_;
    RejectFreshWithDetail(fixture, meta::MetaCommand{command},
                          "failover group anchor is stale");
  }
  {
    SCOPED_TRACE("BeginControlledFailover/stale-operation-anchor");
    auto fixture_owner = MakeFixture();
    Fixture& fixture = *fixture_owner;
    SubmitControlledOperation(fixture);
    meta::BeginControlledFailover command = MakeBeginControlled(fixture);
    ++command.expected_operation_revision_;
    RejectFreshWithDetail(
        fixture, meta::MetaCommand{command},
        "controlled failover operation is not pristine Submitted state");
  }
  {
    SCOPED_TRACE("BeginUncontrolledFailover/stale-owner-anchor");
    auto fixture_owner = MakeFixture();
    Fixture& fixture = *fixture_owner;
    meta::BeginUncontrolledFailover command = MakeBeginUncontrolled(fixture);
    command.expected_owner_node_id_ = fixture.candidate;
    RejectFreshWithDetail(fixture, meta::MetaCommand{command},
                          "failover group anchor is stale");
  }

  {
    SCOPED_TRACE("SetUncontrolledCandidate/wrong-mode");
    auto fixture_owner = MakeFixture();
    Fixture& fixture = *fixture_owner;
    const auto attempt = install_controlled(fixture);
    meta::SetUncontrolledCandidate command;
    command.request_id_ = Filled<16>(0x90);
    command.group_id_ = "g1";
    command.expected_transition_ = {fixture.transition_id, attempt.second};
    command.candidate_action_ = CandidateAction(fixture, 0x91);
    RejectFreshWithDetail(fixture, meta::MetaCommand{command},
                          "uncontrolled failover transition CAS mismatch");
  }
  {
    SCOPED_TRACE("SetUncontrolledCandidate/stale-transition-revision");
    auto fixture_owner = MakeFixture();
    Fixture& fixture = *fixture_owner;
    const auto attempt = install_uncontrolled(fixture);
    meta::SetUncontrolledCandidate command;
    command.request_id_ = Filled<16>(0x92);
    command.group_id_ = "g1";
    command.expected_transition_ = {fixture.transition_id, attempt.second - 1};
    command.candidate_action_ = CandidateAction(fixture, 0x93);
    RejectFreshWithDetail(fixture, meta::MetaCommand{command},
                          "uncontrolled failover transition CAS mismatch");
  }

  {
    SCOPED_TRACE("AuthorizeFailoverPrepare/stale-transition-revision");
    auto fixture_owner = MakeFixture();
    Fixture& fixture = *fixture_owner;
    const auto attempt = install_uncontrolled(fixture);
    meta::AuthorizeFailoverPrepare command = MakeAuthorize(
        fixture, attempt.second - 1, *attempt.first.candidate_action_,
        meta::MetaFailoverLoss::kUnknown);
    RejectFreshWithDetail(fixture, meta::MetaCommand{command},
                          "failover authorization CAS mismatch");
  }
  {
    SCOPED_TRACE("AuthorizeFailoverPrepare/mode-specific-loss");
    auto fixture_owner = MakeFixture();
    Fixture& fixture = *fixture_owner;
    const auto attempt = install_controlled(fixture);
    const meta::AuthorizeFailoverPrepare command =
        MakeAuthorize(fixture, attempt.second, attempt.first.candidate_action_,
                      meta::MetaFailoverLoss::kUnknown);
    RejectFreshWithDetail(fixture, meta::MetaCommand{command},
                          "authorization loss does not match failover mode");
  }

  {
    SCOPED_TRACE("AbortControlledFailover/wrong-mode");
    auto fixture_owner = MakeFixture();
    Fixture& fixture = *fixture_owner;
    SubmitControlledOperation(fixture);
    const auto attempt = install_uncontrolled(fixture);
    const meta::AbortControlledFailover command = MakeAbort(
        fixture,
        meta::MetaFailoverTransitionRef{fixture.transition_id, attempt.second});
    RejectFreshWithDetail(fixture, meta::MetaCommand{command},
                          "post-Begin controlled transition CAS mismatch");
  }
  {
    SCOPED_TRACE("AbortControlledFailover/stale-transition-revision");
    auto fixture_owner = MakeFixture();
    Fixture& fixture = *fixture_owner;
    const auto attempt = install_controlled(fixture);
    const meta::AbortControlledFailover command =
        MakeAbort(fixture, meta::MetaFailoverTransitionRef{
                               fixture.transition_id, attempt.second - 1});
    RejectFreshWithDetail(fixture, meta::MetaCommand{command},
                          "post-Begin controlled transition CAS mismatch");
  }
  {
    SCOPED_TRACE("AbortControlledFailover/stale-operation-anchor");
    auto fixture_owner = MakeFixture();
    Fixture& fixture = *fixture_owner;
    const auto attempt = install_controlled(fixture);
    meta::AbortControlledFailover command = MakeAbort(
        fixture,
        meta::MetaFailoverTransitionRef{fixture.transition_id, attempt.second});
    ++command.expected_operation_revision_;
    RejectFreshWithDetail(fixture, meta::MetaCommand{command},
                          "controlled failover operation CAS mismatch");
  }

  {
    SCOPED_TRACE("DegradeControlledFailover/wrong-mode");
    auto fixture_owner = MakeFixture();
    Fixture& fixture = *fixture_owner;
    SubmitControlledOperation(fixture);
    const auto attempt = install_uncontrolled(fixture);
    const meta::DegradeControlledFailover command =
        MakeDegrade(fixture, attempt.second, *attempt.first.candidate_action_,
                    false, "source unavailable");
    RejectFreshWithDetail(fixture, meta::MetaCommand{command},
                          "controlled failover degrade CAS mismatch");
  }
  {
    SCOPED_TRACE("DegradeControlledFailover/stale-transition-revision");
    auto fixture_owner = MakeFixture();
    Fixture& fixture = *fixture_owner;
    const auto attempt = install_controlled(fixture);
    const meta::DegradeControlledFailover command = MakeDegrade(
        fixture, attempt.second - 1, attempt.first.candidate_action_, false,
        "source unavailable");
    RejectFreshWithDetail(fixture, meta::MetaCommand{command},
                          "controlled failover degrade CAS mismatch");
  }
  {
    SCOPED_TRACE("DegradeControlledFailover/stale-operation-anchor");
    auto fixture_owner = MakeFixture();
    Fixture& fixture = *fixture_owner;
    const auto attempt = install_controlled(fixture);
    meta::DegradeControlledFailover command =
        MakeDegrade(fixture, attempt.second, attempt.first.candidate_action_,
                    false, "source unavailable");
    ++command.expected_operation_revision_;
    RejectFreshWithDetail(fixture, meta::MetaCommand{command},
                          "controlled failover operation CAS mismatch");
  }

  {
    SCOPED_TRACE("CommitControlledFailover/wrong-mode");
    auto fixture_owner = MakeFixture();
    Fixture& fixture = *fixture_owner;
    SubmitControlledOperation(fixture);
    const auto attempt = install_uncontrolled(fixture);
    const std::uint64_t authorized_revision =
        authorize_uncontrolled(fixture, attempt);
    const meta::CommitControlledFailover command =
        MakeCommitControlled(fixture, authorized_revision, authorized_revision,
                             *attempt.first.candidate_action_);
    RejectFreshWithDetail(fixture, meta::MetaCommand{command},
                          "failover commit transition CAS mismatch");
  }
  {
    SCOPED_TRACE("CommitControlledFailover/stale-transition-revision");
    auto fixture_owner = MakeFixture();
    Fixture& fixture = *fixture_owner;
    const auto attempt = install_controlled(fixture);
    const std::uint64_t authorized_revision =
        authorize_controlled(fixture, attempt);
    meta::CommitControlledFailover command = MakeCommitControlled(
        fixture, authorized_revision + 1, authorized_revision,
        attempt.first.candidate_action_);
    RejectFreshWithDetail(fixture, meta::MetaCommand{command},
                          "failover commit transition CAS mismatch");
  }
  {
    SCOPED_TRACE("CommitControlledFailover/stale-topology-anchor");
    auto fixture_owner = MakeFixture();
    Fixture& fixture = *fixture_owner;
    const auto attempt = install_controlled(fixture);
    const std::uint64_t authorized_revision =
        authorize_controlled(fixture, attempt);
    meta::CommitControlledFailover command =
        MakeCommitControlled(fixture, authorized_revision, authorized_revision,
                             attempt.first.candidate_action_);
    ++command.expected_membership_revision_;
    RejectFreshWithDetail(fixture, meta::MetaCommand{command},
                          "failover commit group anchor is stale");
  }
  {
    SCOPED_TRACE("CommitControlledFailover/stale-owner-anchor");
    auto fixture_owner = MakeFixture();
    Fixture& fixture = *fixture_owner;
    const auto attempt = install_controlled(fixture);
    const std::uint64_t authorized_revision =
        authorize_controlled(fixture, attempt);
    meta::CommitControlledFailover command =
        MakeCommitControlled(fixture, authorized_revision, authorized_revision,
                             attempt.first.candidate_action_);
    command.expected_owner_node_id_ = fixture.candidate;
    RejectFreshWithDetail(fixture, meta::MetaCommand{command},
                          "failover commit group anchor is stale");
  }
  {
    SCOPED_TRACE("CommitControlledFailover/stale-operation-anchor");
    auto fixture_owner = MakeFixture();
    Fixture& fixture = *fixture_owner;
    const auto attempt = install_controlled(fixture);
    const std::uint64_t authorized_revision =
        authorize_controlled(fixture, attempt);
    meta::CommitControlledFailover command =
        MakeCommitControlled(fixture, authorized_revision, authorized_revision,
                             attempt.first.candidate_action_);
    ++command.expected_operation_revision_;
    RejectFreshWithDetail(
        fixture, meta::MetaCommand{command},
        "controlled failover operation is not pristine Submitted state");
  }

  {
    SCOPED_TRACE("CommitUncontrolledFailover/wrong-mode");
    auto fixture_owner = MakeFixture();
    Fixture& fixture = *fixture_owner;
    const auto attempt = install_controlled(fixture);
    const std::uint64_t authorized_revision =
        authorize_controlled(fixture, attempt);
    const meta::CommitUncontrolledFailover command = MakeCommitUncontrolled(
        fixture, authorized_revision, authorized_revision,
        attempt.first.candidate_action_, meta::MetaFailoverLoss::kNone);
    RejectFreshWithDetail(fixture, meta::MetaCommand{command},
                          "failover commit transition CAS mismatch");
  }
  {
    SCOPED_TRACE("CommitUncontrolledFailover/stale-transition-revision");
    auto fixture_owner = MakeFixture();
    Fixture& fixture = *fixture_owner;
    const auto attempt = install_uncontrolled(fixture);
    const std::uint64_t authorized_revision =
        authorize_uncontrolled(fixture, attempt);
    meta::CommitUncontrolledFailover command = MakeCommitUncontrolled(
        fixture, authorized_revision + 1, authorized_revision,
        *attempt.first.candidate_action_, meta::MetaFailoverLoss::kUnknown);
    RejectFreshWithDetail(fixture, meta::MetaCommand{command},
                          "failover commit transition CAS mismatch");
  }
  {
    SCOPED_TRACE("CommitUncontrolledFailover/stale-topology-anchor");
    auto fixture_owner = MakeFixture();
    Fixture& fixture = *fixture_owner;
    const auto attempt = install_uncontrolled(fixture);
    const std::uint64_t authorized_revision =
        authorize_uncontrolled(fixture, attempt);
    meta::CommitUncontrolledFailover command = MakeCommitUncontrolled(
        fixture, authorized_revision, authorized_revision,
        *attempt.first.candidate_action_, meta::MetaFailoverLoss::kUnknown);
    ++command.expected_membership_revision_;
    RejectFreshWithDetail(fixture, meta::MetaCommand{command},
                          "failover commit group anchor is stale");
  }
  {
    SCOPED_TRACE("CommitUncontrolledFailover/stale-owner-anchor");
    auto fixture_owner = MakeFixture();
    Fixture& fixture = *fixture_owner;
    const auto attempt = install_uncontrolled(fixture);
    const std::uint64_t authorized_revision =
        authorize_uncontrolled(fixture, attempt);
    meta::CommitUncontrolledFailover command = MakeCommitUncontrolled(
        fixture, authorized_revision, authorized_revision,
        *attempt.first.candidate_action_, meta::MetaFailoverLoss::kUnknown);
    command.expected_owner_node_id_ = fixture.candidate;
    RejectFreshWithDetail(fixture, meta::MetaCommand{command},
                          "failover commit group anchor is stale");
  }
}

TEST(MetaFailoverTransitionW2,
     GenericOperationTerminalCommandsRejectFailoverOperations) {
  auto fixture_owner = MakeFixture();
  Fixture& fixture = *fixture_owner;
  SubmitControlledOperation(fixture);

  meta::CompleteOperation complete;
  complete.request_id_ = Filled<16>(0x81);
  complete.operation_id_ = fixture.operation_id;
  complete.expected_revision_ = 0;
  complete.result_ = "bypass typed cutover";
  RejectFresh(fixture, meta::MetaCommand{complete});

  meta::AbortOperation abort;
  abort.request_id_ = Filled<16>(0x82);
  abort.operation_id_ = fixture.operation_id;
  abort.expected_revision_ = 0;
  abort.reason_ = "bypass typed abort";
  RejectFresh(fixture, meta::MetaCommand{abort});

  const auto operation =
      fixture.stores.operation_.FindOperation(fixture.operation_id);
  ASSERT_TRUE(operation.has_value());
  EXPECT_EQ(operation->lifecycle_, meta::MetaOperationLifecycle::kSubmitted);
  EXPECT_EQ(operation->revision_, 0u);
  EXPECT_TRUE(operation->terminal_result_.empty());
}

}  // namespace
