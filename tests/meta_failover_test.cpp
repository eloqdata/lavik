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
#include <limits>
#include <optional>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "lavik/meta/coordinator.h"
#include "lavik/meta/failover.h"
#include "lavik/meta/failover_reconciler.h"
#include "lavik/meta/hash.h"
#include "lavik/meta/state_apply.h"

namespace lavik::meta {
namespace {

template <std::size_t N>
std::array<std::uint8_t, N> Bytes(std::uint8_t value) {
  std::array<std::uint8_t, N> result{};
  result.fill(value);
  return result;
}

FailoverOperationIntent OperationIntent() {
  return FailoverOperationIntent{
      .group_id_ = "group-a",
      .absolute_deadline_unix_ms_ = 1'800'000'000'000,
  };
}

SubmitOperation FailoverSubmit() {
  auto intent = EncodeFailoverOperationIntent(OperationIntent());
  EXPECT_TRUE(intent.ok()) << intent.status();
  SubmitOperation submit;
  submit.operation_id_ = Bytes<16>(9);
  submit.kind_ = std::string(kFailoverOperationKind);
  if (intent.ok()) submit.intent_ = *intent;
  submit.intent_hash_ = MetaSha256(submit.intent_);
  return submit;
}

std::string NodeId(std::uint8_t suffix) {
  std::string id(40, '0');
  constexpr char kHex[] = "0123456789abcdef";
  id[38] = kHex[(suffix >> 4) & 0x0f];
  id[39] = kHex[suffix & 0x0f];
  return id;
}

struct ProposalFixture {
  MetaStores stores;
  MetaObservationStore observations;
  std::string owner = NodeId(1);
  std::string candidate = NodeId(2);
  std::string alternate = NodeId(3);
  MetaAssignmentId owner_assignment = Bytes<16>(0x21);
  MetaAssignmentId candidate_assignment = Bytes<16>(0x22);
  MetaAssignmentId alternate_assignment = Bytes<16>(0x23);
  MetaBootIncarnation owner_boot = Bytes<20>(0x31);
  MetaBootIncarnation candidate_boot = Bytes<20>(0x32);
  MetaBootIncarnation alternate_boot = Bytes<20>(0x33);
  MetaReplicationHistoryId source_history = Bytes<20>(0x41);
  MetaOperationId operation_id = Bytes<16>(0x51);
  std::uint64_t next_index = 1;
  std::uint8_t next_id = 0x80;

  ProposalFixture() {
    const MetaOperationId root = Bytes<16>(0x01);
    EXPECT_TRUE(stores.topology_.BeginClusterCreate(root, 1).ok());
    PutPolicy automatic;
    automatic.request_id_ = Bytes<16>(0x0f);
    automatic.policy_id_ = std::string(kAutomaticUncontrolledFailoverPolicyId);
    automatic.version_ = 1;
    automatic.content_ =
        R"({"kind":"automatic-uncontrolled-failover-v1","suspect_after_ms":5000})";
    EXPECT_TRUE(stores.policy_.Apply(automatic).ok());
    EXPECT_TRUE(stores.topology_.CompleteClusterCreate(root).ok());
    Register(owner, MetaNodeRole::kPrimary, 6379, 0x02);
    Register(candidate, MetaNodeRole::kReplica, 6380, 0x03);

    CreateGroup group;
    group.request_id_ = Bytes<16>(0x04);
    group.group_id_ = "g1";
    group.new_topology_epoch_ = 1;
    Apply(group);

    AssignNodeToGroup assign_owner;
    assign_owner.request_id_ = Bytes<16>(0x05);
    assign_owner.group_id_ = "g1";
    assign_owner.node_id_ = owner;
    assign_owner.assignment_id_ = owner_assignment;
    assign_owner.role_ = MetaNodeRole::kPrimary;
    assign_owner.expected_revision_ = 1;
    assign_owner.new_topology_epoch_ = 2;
    Apply(assign_owner);

    AssignNodeToGroup assign_candidate;
    assign_candidate.request_id_ = Bytes<16>(0x06);
    assign_candidate.group_id_ = "g1";
    assign_candidate.node_id_ = candidate;
    assign_candidate.assignment_id_ = candidate_assignment;
    assign_candidate.role_ = MetaNodeRole::kReplica;
    assign_candidate.expected_revision_ = 2;
    assign_candidate.new_topology_epoch_ = 3;
    Apply(assign_candidate);

    PutPolicy policy;
    policy.request_id_ = Bytes<16>(0x07);
    policy.policy_id_ = std::string(kAuthorityLeasePolicyId);
    policy.version_ = 1;
    policy.content_ = R"({"kind":"authority-lease-v1","duration_ms":5000})";
    Apply(policy);

    BeginGroupTerm begin_term;
    begin_term.request_id_ = Bytes<16>(0x08);
    begin_term.group_id_ = "g1";
    begin_term.expected_term_ = 0;
    begin_term.new_term_ = 1;
    Apply(begin_term);

    ActivateAuthority activate;
    activate.request_id_ = Bytes<16>(0x09);
    activate.group_id_ = "g1";
    activate.expected_term_ = 1;
    activate.new_owner_ = owner;
    activate.new_topology_epoch_ = 4;
    Apply(activate);
  }

  template <typename Command>
  void Apply(const Command& command) {
    const MetaApplyResult result = ApplyCommitted(
        stores, next_index++, MetaCommand{command},
        "lavik://test/failover-proposal", "2026-09-13T00:00:00Z");
    ASSERT_EQ(result.verdict_, MetaAuditVerdict::kAccepted) << result.detail_;
  }

  void Register(const std::string& node_id, MetaNodeRole role,
                std::uint16_t port, std::uint8_t request_seed) {
    RegisterNode node;
    node.request_id_ = Bytes<16>(request_seed);
    node.node_id_ = node_id;
    node.principal_ = "lavik://node/" + node_id;
    node.endpoints_ = {"tcp://127.0.0.1:" + std::to_string(port)};
    node.role_ = role;
    Apply(node);
  }

  void AddAlternate() {
    Register(alternate, MetaNodeRole::kReplica, 6381, 0x0a);
    const auto group = stores.topology_.FindGroup("g1");
    ASSERT_TRUE(group.has_value());
    AssignNodeToGroup assign;
    assign.request_id_ = Bytes<16>(0x0b);
    assign.group_id_ = "g1";
    assign.node_id_ = alternate;
    assign.assignment_id_ = alternate_assignment;
    assign.role_ = MetaNodeRole::kReplica;
    assign.expected_revision_ = group->revision_;
    assign.new_topology_epoch_ = stores.topology_.TopologyEpoch() + 1;
    Apply(assign);
  }

  MetaFailoverCompatibilityDomain Domain() const {
    return {.source_group_term_ = 1,
            .source_node_id_ = owner,
            .source_assignment_id_ = owner_assignment,
            .source_boot_id_ = owner_boot,
            .source_history_id_ = source_history,
            .flow_count_ = 2};
  }

  MetaCandidateProgressObs CandidateProgress(
      std::uint64_t generation = 1,
      std::vector<std::uint64_t> frontier = {10, 20}) const {
    const auto group = stores.topology_.FindGroup("g1");
    EXPECT_TRUE(group.has_value());
    return {.node_id_ = candidate,
            .boot_incarnation_ = candidate_boot,
            .session_generation_ = generation,
            .group_id_ = "g1",
            .assignment_id_ = candidate_assignment,
            .group_term_ = group->record_.group_term_,
            .population_manifest_revision_ =
                group->record_.population_manifest_revision_,
            .population_manifest_digest_ =
                group->record_.population_manifest_digest_,
            .partition_replication_epoch_ =
                group->record_.partition_replication_epoch_,
            .replication_history_id_ = Bytes<20>(0x43),
            .source_group_term_ = Domain().source_group_term_,
            .source_node_id_ = Domain().source_node_id_,
            .source_assignment_id_ = Domain().source_assignment_id_,
            .source_boot_incarnation_ = Domain().source_boot_id_,
            .source_replication_history_id_ = Domain().source_history_id_,
            .applied_next_lsns_ = std::move(frontier),
            .storage_ready_ = true,
            .population_ready_ = true};
  }

  void ReportOwner(
      std::int64_t now,
      std::optional<MetaFailoverObservationObs> failover = std::nullopt,
      std::uint64_t generation = 1) {
    MetaStoresFacts facts(stores);
    const MetaObservationIdentity identity{owner, owner_boot, generation};
    if (observations.CurrentGeneration(owner) != std::optional(generation)) {
      ASSERT_TRUE(
          observations.AdoptSession(identity, now - 1, source_history).ok());
    }
    const auto result = observations.ReplaceHeartbeat(
        identity,
        {.storage_ready_ = true,
         .population_ready_ = true,
         .active_groups_ = 1},
        std::nullopt, std::move(failover), facts, now);
    ASSERT_TRUE(result.boot_status_.ok()) << result.boot_status_;
    ASSERT_TRUE(result.failover_status_.ok()) << result.failover_status_;
  }

  void ReportCandidate(
      std::int64_t now,
      std::optional<MetaFailoverObservationObs> failover = std::nullopt,
      std::uint64_t generation = 1,
      std::vector<std::uint64_t> frontier = {10, 20}) {
    MetaStoresFacts facts(stores);
    const MetaObservationIdentity identity{candidate, candidate_boot,
                                           generation};
    if (observations.CurrentGeneration(candidate) !=
        std::optional(generation)) {
      ASSERT_TRUE(
          observations.AdoptSession(identity, now - 1, Bytes<20>(0x43)).ok());
    }
    const auto result = observations.ReplaceHeartbeat(
        identity,
        {.storage_ready_ = true,
         .population_ready_ = true,
         .active_groups_ = 1},
        CandidateProgress(generation, std::move(frontier)), std::move(failover),
        facts, now);
    ASSERT_TRUE(result.candidate_status_.ok()) << result.candidate_status_;
    ASSERT_TRUE(result.failover_status_.ok()) << result.failover_status_;
  }

  void ReportAlternate(std::int64_t now) {
    MetaStoresFacts facts(stores);
    const MetaObservationIdentity identity{alternate, alternate_boot, 1};
    if (observations.CurrentGeneration(alternate) !=
        std::optional<std::uint64_t>(1)) {
      ASSERT_TRUE(
          observations.AdoptSession(identity, now - 1, Bytes<20>(0x44)).ok());
    }
    MetaCandidateProgressObs progress = CandidateProgress();
    progress.node_id_ = alternate;
    progress.boot_incarnation_ = alternate_boot;
    progress.assignment_id_ = alternate_assignment;
    progress.replication_history_id_ = Bytes<20>(0x44);
    const auto result = observations.ReplaceHeartbeat(
        identity,
        {.storage_ready_ = true,
         .population_ready_ = true,
         .active_groups_ = 1},
        std::move(progress), std::nullopt, facts, now);
    ASSERT_TRUE(result.candidate_status_.ok()) << result.candidate_status_;
  }

  void SubmitControlled(std::uint64_t deadline) {
    auto intent = EncodeFailoverOperationIntent({"g1", deadline});
    ASSERT_TRUE(intent.ok()) << intent.status();
    SubmitOperation submit;
    submit.request_id_ = Bytes<16>(0x52);
    submit.operation_id_ = operation_id;
    submit.kind_ = std::string(kFailoverOperationKind);
    submit.intent_ = *intent;
    submit.intent_hash_ = MetaSha256(*intent);
    Apply(submit);
  }

  BeginUncontrolledFailover UncontrolledBegin(bool with_candidate) const {
    const auto group = stores.topology_.FindGroup("g1");
    const auto grant_state = stores.topology_.AuthorityFor("g1");
    EXPECT_TRUE(group.has_value());
    EXPECT_TRUE(grant_state.has_value());
    EXPECT_TRUE(grant_state->grant_.has_value());
    BeginUncontrolledFailover begin;
    begin.request_id_ = Bytes<16>(0x53);
    begin.group_id_ = "g1";
    begin.transition_id_ = Bytes<16>(0x54);
    begin.target_term_ = group->record_.group_term_ + 1;
    if (with_candidate) {
      begin.candidate_action_ = MetaFailoverCandidateAction{
          .action_id_ = Bytes<16>(0x55),
          .candidate_ = {candidate, candidate_assignment, candidate_boot},
          .domain_ = Domain()};
    }
    begin.expected_owner_node_id_ = group->record_.owner_;
    begin.expected_owner_assignment_id_ = owner_assignment;
    begin.expected_membership_revision_ = group->revision_;
    begin.expected_group_term_ = group->record_.group_term_;
    begin.expected_population_manifest_revision_ =
        group->record_.population_manifest_revision_;
    begin.expected_population_manifest_digest_ =
        group->record_.population_manifest_digest_;
    begin.expected_partition_replication_epoch_ =
        group->record_.partition_replication_epoch_;
    return begin;
  }

  void BeginUncontrolledWithoutCandidate() {
    Apply(UncontrolledBegin(/*with_candidate=*/false));
  }

  MetaFailoverTransition Transition() const {
    const auto group = stores.topology_.FindGroup("g1");
    EXPECT_TRUE(group.has_value());
    EXPECT_TRUE(group->failover_transition_.has_value());
    return *group->failover_transition_;
  }

  void ReportSourcePaused(std::int64_t now,
                          std::vector<std::uint64_t> stable = {10, 20}) {
    const auto transition = Transition();
    const auto& domain = transition.candidate_action_->domain_;
    MetaSourcePausedObs paused{
        .group_id_ = "g1",
        .transition_id_ = transition.transition_id_,
        .source_node_id_ = domain.source_node_id_,
        .source_assignment_id_ = domain.source_assignment_id_,
        .source_boot_id_ = domain.source_boot_id_,
        .source_history_id_ = domain.source_history_id_,
        .source_group_term_ = domain.source_group_term_,
        .stable_next_lsns_ = std::move(stable)};
    ReportOwner(now, MetaFailoverObservationObs{.payload_ = paused});
  }

  void ReportPrepared(std::int64_t now, std::uint64_t generation = 1) {
    const auto transition = Transition();
    const auto& action = *transition.candidate_action_;
    MetaCandidatePreparedObs prepared{
        .group_id_ = "g1",
        .transition_id_ = transition.transition_id_,
        .action_id_ = action.action_id_,
        .candidate_node_id_ = action.candidate_.node_id_,
        .candidate_assignment_id_ = action.candidate_.assignment_id_,
        .candidate_boot_id_ = action.candidate_.boot_id_,
        .prepared_context_id_ = Bytes<16>(0x61)};
    ReportCandidate(now, MetaFailoverObservationObs{.payload_ = prepared},
                    generation);
  }

  absl::StatusOr<std::optional<MetaCommand>> Plan(std::int64_t now) {
    return PlanFailoverStep(
        MetaCommittedView(stores, next_index - 1), observations,
        {.now_unix_ms_ = now,
         .leadership_started_unix_ms_ = now - 101,
         .observation_grace_ms_ = 100,
         .next_id_ = [&]() -> absl::StatusOr<MetaRequestId> {
           return Bytes<16>(next_id++);
         }});
  }
};

TEST(MetaFailoverOperationIntentCodecTest, RoundTripsCanonicalRequest) {
  const FailoverOperationIntent intent = OperationIntent();

  auto first = EncodeFailoverOperationIntent(intent);
  ASSERT_TRUE(first.ok()) << first.status();
  auto second = EncodeFailoverOperationIntent(intent);
  ASSERT_TRUE(second.ok()) << second.status();
  EXPECT_EQ(*second, *first);

  auto decoded = DecodeFailoverOperationIntent(*first);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(*decoded, intent);
}

TEST(MetaFailoverOperationIntentCodecTest,
     RejectsInvalidLocallyConstructedRequest) {
  FailoverOperationIntent intent = OperationIntent();
  intent.group_id_.clear();
  EXPECT_EQ(MetaFailureClassOf(ValidateFailoverOperationIntent(intent)),
            MetaFailureClass::kDomainReject);
  EXPECT_EQ(MetaFailureClassOf(EncodeFailoverOperationIntent(intent).status()),
            MetaFailureClass::kDomainReject);

  intent = OperationIntent();
  intent.group_id_ = std::string(kMaxMetaGroupIdBytes + 1, 'g');
  EXPECT_EQ(MetaFailureClassOf(EncodeFailoverOperationIntent(intent).status()),
            MetaFailureClass::kDomainReject);

  intent = OperationIntent();
  intent.absolute_deadline_unix_ms_ = 0;
  EXPECT_EQ(MetaFailureClassOf(EncodeFailoverOperationIntent(intent).status()),
            MetaFailureClass::kDomainReject);

  intent.absolute_deadline_unix_ms_ =
      static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) + 1;
  EXPECT_EQ(MetaFailureClassOf(EncodeFailoverOperationIntent(intent).status()),
            MetaFailureClass::kDomainReject);
}

TEST(MetaFailoverOperationIntentCodecTest,
     FailsStopOnUnknownVersionTrailingBytesAndInvalidDecodedRequest) {
  auto encoded = EncodeFailoverOperationIntent(OperationIntent());
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  (*encoded)[4] = '\x02';
  EXPECT_EQ(
      MetaFailureClassOf(DecodeFailoverOperationIntent(*encoded).status()),
      MetaFailureClass::kFailStop);

  encoded = EncodeFailoverOperationIntent(OperationIntent());
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  encoded->push_back('\0');
  EXPECT_EQ(
      MetaFailureClassOf(DecodeFailoverOperationIntent(*encoded).status()),
      MetaFailureClass::kFailStop);

  encoded = EncodeFailoverOperationIntent(OperationIntent());
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  encoded->replace(encoded->size() - sizeof(std::uint64_t),
                   sizeof(std::uint64_t), sizeof(std::uint64_t), '\0');
  EXPECT_EQ(
      MetaFailureClassOf(DecodeFailoverOperationIntent(*encoded).status()),
      MetaFailureClass::kFailStop);
}

TEST(MetaFailoverValidationTest, AcceptsOnlyCanonicalRequestOnlySubmit) {
  MetaObservationStore observations;
  MetaStores stores;
  SubmitOperation submit = FailoverSubmit();

  EXPECT_TRUE(ValidateFailoverProposal(MetaCommand(submit),
                                       MetaCommittedView(stores, 0),
                                       observations, 1'000)
                  .ok());

  SubmitOperation malformed = submit;
  malformed.intent_ = "not-a-failover-request";
  malformed.intent_hash_ = MetaSha256(malformed.intent_);
  EXPECT_EQ(MetaFailureClassOf(ValidateFailoverProposal(
                MetaCommand(malformed), MetaCommittedView(stores, 0),
                observations, 1'000)),
            MetaFailureClass::kDomainReject);

  SubmitOperation wrong_hash = submit;
  wrong_hash.intent_hash_ = Bytes<32>(4);
  EXPECT_EQ(MetaFailureClassOf(ValidateFailoverProposal(
                MetaCommand(wrong_hash), MetaCommittedView(stores, 0),
                observations, 1'000)),
            MetaFailureClass::kDomainReject);

  SubmitOperation history_bound = submit;
  history_bound.replication_history_id_ = Bytes<20>(7);
  EXPECT_EQ(MetaFailureClassOf(ValidateFailoverProposal(
                MetaCommand(history_bound), MetaCommittedView(stores, 0),
                observations, 1'000)),
            MetaFailureClass::kDomainReject);
}

TEST(MetaFailoverValidationTest, RejectsGenericMutationOfFailoverOperation) {
  MetaObservationStore observations;
  MetaStores stores;
  SubmitOperation submit = FailoverSubmit();
  ASSERT_TRUE(stores.operation_.SubmitOperation(submit, 1).ok());
  const MetaCommittedView view(stores, 1);

  TransitionOperationPhase transition;
  transition.operation_id_ = submit.operation_id_;
  EXPECT_EQ(MetaFailureClassOf(ValidateFailoverProposal(
                MetaCommand(transition), view, observations, 1'000)),
            MetaFailureClass::kDomainReject);

  CompleteOperation complete;
  complete.operation_id_ = submit.operation_id_;
  EXPECT_EQ(MetaFailureClassOf(ValidateFailoverProposal(
                MetaCommand(complete), view, observations, 1'000)),
            MetaFailureClass::kDomainReject);

  AbortOperation abort;
  abort.operation_id_ = submit.operation_id_;
  EXPECT_EQ(MetaFailureClassOf(ValidateFailoverProposal(
                MetaCommand(abort), view, observations, 1'000)),
            MetaFailureClass::kDomainReject);
}

TEST(MetaFailoverValidationTest,
     RejectsControlledBeginWhenCandidateDisconnectsAfterPlanning) {
  ProposalFixture fixture;
  fixture.SubmitControlled(5'000);
  fixture.ReportOwner(1'000);
  fixture.ReportCandidate(1'000);
  const auto planned = fixture.Plan(1'001);
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  ASSERT_NE(std::get_if<BeginControlledFailover>(&**planned), nullptr);
  EXPECT_TRUE(ValidateFailoverProposal(
                  **planned,
                  MetaCommittedView(fixture.stores, fixture.next_index - 1),
                  fixture.observations, 1'001)
                  .ok());
  EXPECT_EQ(
      MetaFailureClassOf(ValidateFailoverProposal(
          **planned, MetaCommittedView(fixture.stores, fixture.next_index - 1),
          fixture.observations, 5'000)),
      MetaFailureClass::kDomainReject);

  fixture.observations.InvalidateCandidateOnDisconnect(
      {fixture.candidate, fixture.candidate_boot, 1}, 1'002);
  EXPECT_EQ(
      MetaFailureClassOf(ValidateFailoverProposal(
          **planned, MetaCommittedView(fixture.stores, fixture.next_index - 1),
          fixture.observations, 1'002)),
      MetaFailureClass::kDomainReject);
}

TEST(MetaFailoverValidationTest,
     AcceptsControlledBeginAfterSameSessionCandidateProgressAdvances) {
  ProposalFixture fixture;
  fixture.SubmitControlled(5'000);
  fixture.ReportOwner(1'000);
  fixture.ReportCandidate(1'000);
  const auto planned = fixture.Plan(1'001);
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  ASSERT_NE(std::get_if<BeginControlledFailover>(&**planned), nullptr);

  fixture.ReportCandidate(1'002, std::nullopt, 1, {11, 21});
  EXPECT_TRUE(ValidateFailoverProposal(
                  **planned,
                  MetaCommittedView(fixture.stores, fixture.next_index - 1),
                  fixture.observations, 1'002)
                  .ok());
}

TEST(MetaFailoverValidationTest,
     RejectsControlledBeginWhenSourceHeartbeatExpiresAfterPlanning) {
  ProposalFixture fixture;
  fixture.SubmitControlled(100'000);
  fixture.ReportOwner(1'000);
  fixture.ReportCandidate(1'000);
  const auto planned = fixture.Plan(1'001);
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  ASSERT_NE(std::get_if<BeginControlledFailover>(&**planned), nullptr);

  // Keep the candidate observation live while only the source heartbeat crosses
  // the observation TTL. The long operation deadline isolates source
  // freshness from the independent controlled-failover deadline gate.
  fixture.ReportCandidate(31'001);
  EXPECT_EQ(
      MetaFailureClassOf(ValidateFailoverProposal(
          **planned, MetaCommittedView(fixture.stores, fixture.next_index - 1),
          fixture.observations, 31'001)),
      MetaFailureClass::kDomainReject);
}

TEST(MetaFailoverValidationTest,
     RejectsUncontrolledCandidateSetWhenSelectionIsWithdrawnAfterPlanning) {
  ProposalFixture fixture;
  fixture.BeginUncontrolledWithoutCandidate();
  fixture.ReportCandidate(1'010);
  const auto planned = fixture.Plan(1'011);
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  ASSERT_NE(std::get_if<SetUncontrolledCandidate>(&**planned), nullptr);
  EXPECT_TRUE(ValidateFailoverProposal(
                  **planned,
                  MetaCommittedView(fixture.stores, fixture.next_index - 1),
                  fixture.observations, 1'011)
                  .ok());

  fixture.observations.InvalidateCandidateOnDisconnect(
      {fixture.candidate, fixture.candidate_boot, 1}, 1'012);
  EXPECT_EQ(
      MetaFailureClassOf(ValidateFailoverProposal(
          **planned, MetaCommittedView(fixture.stores, fixture.next_index - 1),
          fixture.observations, 1'012)),
      MetaFailureClass::kDomainReject);
}

TEST(MetaFailoverValidationTest,
     RejectsUncontrolledCandidateClearWhenNoActionIsInstalled) {
  ProposalFixture fixture;
  fixture.BeginUncontrolledWithoutCandidate();
  const MetaFailoverTransition transition = fixture.Transition();

  SetUncontrolledCandidate clear;
  clear.request_id_ = Bytes<16>(0x57);
  clear.group_id_ = "g1";
  clear.expected_transition_ = {transition.transition_id_,
                                transition.revision_};

  EXPECT_EQ(MetaFailureClassOf(ValidateFailoverProposal(
                MetaCommand(clear),
                MetaCommittedView(fixture.stores, fixture.next_index - 1),
                fixture.observations, 1'011)),
            MetaFailureClass::kDomainReject);
}

TEST(MetaFailoverValidationTest,
     KeepsUncontrolledReplacementWhenDisconnectedActionReprepares) {
  ProposalFixture fixture;
  fixture.AddAlternate();
  fixture.BeginUncontrolledWithoutCandidate();
  fixture.ReportCandidate(1'010);
  auto planned = fixture.Plan(1'011);
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  ASSERT_NE(std::get_if<SetUncontrolledCandidate>(&**planned), nullptr);
  fixture.Apply(**planned);

  fixture.ReportCandidate(1'020);
  planned = fixture.Plan(1'021);
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  ASSERT_NE(std::get_if<AuthorizeFailoverPrepare>(&**planned), nullptr);
  fixture.Apply(**planned);

  fixture.observations.InvalidateCandidateOnDisconnect(
      {fixture.candidate, fixture.candidate_boot, 1}, 1'030);
  fixture.ReportAlternate(1'030);
  planned = fixture.Plan(1'031);
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  const auto* replacement = std::get_if<SetUncontrolledCandidate>(&**planned);
  ASSERT_NE(replacement, nullptr);
  ASSERT_TRUE(replacement->candidate_action_.has_value());
  EXPECT_EQ(replacement->candidate_action_->candidate_.node_id_,
            fixture.alternate);
  EXPECT_TRUE(ValidateFailoverProposal(
                  **planned,
                  MetaCommittedView(fixture.stores, fixture.next_index - 1),
                  fixture.observations, 1'031)
                  .ok());

  fixture.ReportPrepared(1'032, 2);
  EXPECT_TRUE(ValidateFailoverProposal(
                  **planned,
                  MetaCommittedView(fixture.stores, fixture.next_index - 1),
                  fixture.observations, 1'032)
                  .ok());
}

TEST(MetaFailoverValidationTest,
     RejectsUncontrolledBeginWhenCandidateIsWithdrawnAfterPlanning) {
  ProposalFixture fixture;
  fixture.ReportCandidate(1'000);
  const MetaCommand begin = fixture.UncontrolledBegin(/*with_candidate=*/true);
  EXPECT_TRUE(ValidateFailoverProposal(
                  begin,
                  MetaCommittedView(fixture.stores, fixture.next_index - 1),
                  fixture.observations, 1'001)
                  .ok());

  fixture.observations.InvalidateCandidateOnDisconnect(
      {fixture.candidate, fixture.candidate_boot, 1}, 1'002);
  EXPECT_EQ(
      MetaFailureClassOf(ValidateFailoverProposal(
          begin, MetaCommittedView(fixture.stores, fixture.next_index - 1),
          fixture.observations, 1'002)),
      MetaFailureClass::kDomainReject);
}

TEST(MetaFailoverValidationTest,
     RejectsAuthorizationWhenCandidateObservationIsWithdrawnAfterPlanning) {
  ProposalFixture fixture;
  fixture.SubmitControlled(5'000);
  fixture.ReportOwner(1'000);
  fixture.ReportCandidate(1'000);
  auto planned = fixture.Plan(1'001);
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  ASSERT_NE(std::get_if<BeginControlledFailover>(&**planned), nullptr);
  fixture.Apply(**planned);

  fixture.ReportSourcePaused(1'010);
  fixture.ReportCandidate(1'010);
  planned = fixture.Plan(1'011);
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  ASSERT_NE(std::get_if<AuthorizeFailoverPrepare>(&**planned), nullptr);
  EXPECT_TRUE(ValidateFailoverProposal(
                  **planned,
                  MetaCommittedView(fixture.stores, fixture.next_index - 1),
                  fixture.observations, 1'011)
                  .ok());
  EXPECT_EQ(
      MetaFailureClassOf(ValidateFailoverProposal(
          **planned, MetaCommittedView(fixture.stores, fixture.next_index - 1),
          fixture.observations, 5'000)),
      MetaFailureClass::kDomainReject);

  fixture.observations.InvalidateCandidateOnDisconnect(
      {fixture.candidate, fixture.candidate_boot, 1}, 1'012);
  EXPECT_EQ(
      MetaFailureClassOf(ValidateFailoverProposal(
          **planned, MetaCommittedView(fixture.stores, fixture.next_index - 1),
          fixture.observations, 1'012)),
      MetaFailureClass::kDomainReject);
}

TEST(MetaFailoverValidationTest,
     RejectsAuthorizationWhenSourcePauseIsWithdrawnAfterPlanning) {
  ProposalFixture fixture;
  fixture.SubmitControlled(5'000);
  fixture.ReportOwner(1'000);
  fixture.ReportCandidate(1'000);
  auto planned = fixture.Plan(1'001);
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  fixture.Apply(**planned);

  fixture.ReportSourcePaused(1'010);
  fixture.ReportCandidate(1'010);
  planned = fixture.Plan(1'011);
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  ASSERT_NE(std::get_if<AuthorizeFailoverPrepare>(&**planned), nullptr);

  fixture.ReportOwner(1'012);
  EXPECT_EQ(
      MetaFailureClassOf(ValidateFailoverProposal(
          **planned, MetaCommittedView(fixture.stores, fixture.next_index - 1),
          fixture.observations, 1'012)),
      MetaFailureClass::kDomainReject);
}

TEST(
    MetaFailoverValidationTest,
    RejectsRetainingCandidateOnDegradeWhenObservationIsWithdrawnAfterPlanning) {
  ProposalFixture fixture;
  fixture.SubmitControlled(5'000);
  fixture.ReportOwner(1'000);
  fixture.ReportCandidate(1'000);
  auto planned = fixture.Plan(1'001);
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  fixture.Apply(**planned);

  fixture.ReportSourcePaused(1'010);
  fixture.ReportCandidate(1'010);
  planned = fixture.Plan(1'011);
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  ASSERT_NE(std::get_if<AuthorizeFailoverPrepare>(&**planned), nullptr);
  fixture.Apply(**planned);

  ASSERT_TRUE(fixture.observations
                  .AdoptSession({fixture.owner, fixture.owner_boot, 2}, 1'020,
                                Bytes<20>(0x99))
                  .ok());
  planned = fixture.Plan(1'021);
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  const auto* degrade = std::get_if<DegradeControlledFailover>(&**planned);
  ASSERT_NE(degrade, nullptr);
  ASSERT_TRUE(degrade->retain_candidate_action_);
  EXPECT_TRUE(ValidateFailoverProposal(
                  **planned,
                  MetaCommittedView(fixture.stores, fixture.next_index - 1),
                  fixture.observations, 1'021)
                  .ok());
  EXPECT_EQ(
      MetaFailureClassOf(ValidateFailoverProposal(
          **planned, MetaCommittedView(fixture.stores, fixture.next_index - 1),
          fixture.observations, 5'000)),
      MetaFailureClass::kDomainReject);

  fixture.observations.InvalidateCandidateOnDisconnect(
      {fixture.candidate, fixture.candidate_boot, 1}, 1'022);
  EXPECT_EQ(
      MetaFailureClassOf(ValidateFailoverProposal(
          **planned, MetaCommittedView(fixture.stores, fixture.next_index - 1),
          fixture.observations, 1'022)),
      MetaFailureClass::kDomainReject);
}

TEST(MetaFailoverValidationTest,
     RejectsDegradeWhenExactSourceRecoversAfterPlanning) {
  ProposalFixture fixture;
  fixture.SubmitControlled(5'000);
  fixture.ReportOwner(1'000);
  fixture.ReportCandidate(1'000);
  auto planned = fixture.Plan(1'001);
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  ASSERT_NE(std::get_if<BeginControlledFailover>(&**planned), nullptr);
  fixture.Apply(**planned);

  fixture.observations.InvalidateCandidateOnDisconnect(
      {fixture.owner, fixture.owner_boot, 1}, 1'010);
  fixture.ReportCandidate(1'110);
  planned = fixture.Plan(1'110);
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  const auto* degrade = std::get_if<DegradeControlledFailover>(&**planned);
  ASSERT_NE(degrade, nullptr);
  ASSERT_FALSE(degrade->retain_candidate_action_);
  EXPECT_TRUE(ValidateFailoverProposal(
                  **planned,
                  MetaCommittedView(fixture.stores, fixture.next_index - 1),
                  fixture.observations, 1'110)
                  .ok());

  // Planning from an expired disconnect is only a negative observation. If
  // the exact source lineage proves itself healthy before Raft proposal, the
  // stale degradation must not fence a source that has recovered.
  fixture.ReportOwner(1'111, std::nullopt, 2);
  EXPECT_EQ(
      MetaFailureClassOf(ValidateFailoverProposal(
          **planned, MetaCommittedView(fixture.stores, fixture.next_index - 1),
          fixture.observations, 1'111)),
      MetaFailureClass::kDomainReject);
}

TEST(MetaFailoverValidationTest,
     RejectsControlledCommitWhenPreparedObservationIsWithdrawnAfterPlanning) {
  ProposalFixture fixture;
  fixture.SubmitControlled(5'000);
  fixture.ReportOwner(1'000);
  fixture.ReportCandidate(1'000);
  auto planned = fixture.Plan(1'001);
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  fixture.Apply(**planned);

  fixture.ReportSourcePaused(1'010);
  fixture.ReportCandidate(1'010);
  planned = fixture.Plan(1'011);
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  fixture.Apply(**planned);

  fixture.ReportPrepared(1'020);
  planned = fixture.Plan(1'021);
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  ASSERT_NE(std::get_if<CommitControlledFailover>(&**planned), nullptr);
  EXPECT_TRUE(ValidateFailoverProposal(
                  **planned,
                  MetaCommittedView(fixture.stores, fixture.next_index - 1),
                  fixture.observations, 1'021)
                  .ok());
  EXPECT_EQ(
      MetaFailureClassOf(ValidateFailoverProposal(
          **planned, MetaCommittedView(fixture.stores, fixture.next_index - 1),
          fixture.observations, 5'000)),
      MetaFailureClass::kDomainReject);

  fixture.ReportCandidate(1'022);
  EXPECT_EQ(
      MetaFailureClassOf(ValidateFailoverProposal(
          **planned, MetaCommittedView(fixture.stores, fixture.next_index - 1),
          fixture.observations, 1'022)),
      MetaFailureClass::kDomainReject);
}

TEST(MetaFailoverValidationTest,
     RejectsControlledCommitWhenDisconnectedActionRepreparesAfterPlanning) {
  ProposalFixture fixture;
  fixture.SubmitControlled(5'000);
  fixture.ReportOwner(1'000);
  fixture.ReportCandidate(1'000);
  auto planned = fixture.Plan(1'001);
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  fixture.Apply(**planned);

  fixture.ReportSourcePaused(1'010);
  fixture.ReportCandidate(1'010);
  planned = fixture.Plan(1'011);
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  fixture.Apply(**planned);

  fixture.ReportPrepared(1'020);
  planned = fixture.Plan(1'021);
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  ASSERT_NE(std::get_if<CommitControlledFailover>(&**planned), nullptr);

  fixture.observations.InvalidateCandidateOnDisconnect(
      {fixture.candidate, fixture.candidate_boot, 1}, 1'022);
  fixture.ReportPrepared(1'023, 2);
  EXPECT_EQ(
      MetaFailureClassOf(ValidateFailoverProposal(
          **planned, MetaCommittedView(fixture.stores, fixture.next_index - 1),
          fixture.observations, 1'023)),
      MetaFailureClass::kDomainReject);
}

TEST(MetaFailoverValidationTest,
     RejectsUncontrolledCommitWhenPreparedObservationIsWithdrawnAfterPlanning) {
  ProposalFixture fixture;
  fixture.BeginUncontrolledWithoutCandidate();
  fixture.ReportCandidate(1'010);
  auto planned = fixture.Plan(1'011);
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  ASSERT_NE(std::get_if<SetUncontrolledCandidate>(&**planned), nullptr);
  fixture.Apply(**planned);

  fixture.ReportCandidate(1'020);
  planned = fixture.Plan(1'021);
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  ASSERT_NE(std::get_if<AuthorizeFailoverPrepare>(&**planned), nullptr);
  fixture.Apply(**planned);

  fixture.ReportPrepared(1'030);
  planned = fixture.Plan(1'031);
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  ASSERT_NE(std::get_if<CommitUncontrolledFailover>(&**planned), nullptr);
  EXPECT_TRUE(ValidateFailoverProposal(
                  **planned,
                  MetaCommittedView(fixture.stores, fixture.next_index - 1),
                  fixture.observations, 1'031)
                  .ok());

  fixture.ReportCandidate(1'032);
  EXPECT_EQ(
      MetaFailureClassOf(ValidateFailoverProposal(
          **planned, MetaCommittedView(fixture.stores, fixture.next_index - 1),
          fixture.observations, 1'032)),
      MetaFailureClass::kDomainReject);
}

}  // namespace
}  // namespace lavik::meta
