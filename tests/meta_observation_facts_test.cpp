/* Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0 */

#include <array>
#include <barrier>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "lavik/meta/coordinator.h"
#include "lavik/meta/failover.h"
#include "lavik/meta/hash.h"
#include "lavik/meta/state_machine.h"
#include "support/meta_stores.h"
#include "support/test_data_path.h"

namespace {
namespace meta = lavik::meta;

TEST(MetaObservationFactsTest, CapturedFactsOwnTheirExactCommittedCut) {
  const auto dir = lavik::test::TestDataPath("meta_observation_facts_owned");
  auto opened = meta::MetaStateMachine::Open(dir);
  ASSERT_TRUE(opened.ok()) << opened.status();
  auto machine = std::move(*opened);
  meta::RegisterNode node;
  node.request_id_.fill(1);
  node.node_id_ = std::string(40, 'a');
  node.principal_ = "lavik://node/" + node.node_id_;
  node.endpoints_ = {"tcp://127.0.0.1:6379"};
  node.role_ = meta::MetaNodeRole::kReplica;
  auto encoded = meta::MetaStateMachine::EncodeCommand(node);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  machine->commit(1, **encoded);
  const auto captured = machine->CaptureObservationFacts();
  EXPECT_EQ(captured.applied_index(), 1u);
  EXPECT_EQ(captured.state_change_index(), 1u);
  EXPECT_TRUE(captured.IsActiveNode(node.node_id_));
  machine->Advance(2);
  const auto advanced = machine->CaptureObservationFacts();
  EXPECT_EQ(advanced.applied_index(), 2u);
  EXPECT_EQ(advanced.state_change_index(), 1u);
  machine.reset();
  EXPECT_TRUE(captured.IsActiveNode(node.node_id_));
  EXPECT_EQ(captured.applied_index(), 1u);
  EXPECT_EQ(captured.CurrentGroupTerm("unknown"), 0u);
  std::filesystem::remove_all(dir);
}

template <std::size_t N>
std::array<std::uint8_t, N> Bytes(std::uint8_t value) {
  std::array<std::uint8_t, N> bytes{};
  bytes.fill(value);
  return bytes;
}

std::string Node(char value) { return std::string(40, value); }

struct FactsGroup {
  std::string id;
  char source;
  char candidate;
  std::uint8_t tag;
  bool controlled;
  meta::MetaAssignmentId SourceAssignment() const { return Bytes<16>(tag); }
  meta::MetaAssignmentId CandidateAssignment() const {
    return Bytes<16>(tag + 1);
  }
  meta::MetaBootIncarnation SourceBoot() const { return Bytes<20>(tag + 2); }
  meta::MetaBootIncarnation CandidateBoot() const { return Bytes<20>(tag + 3); }
  meta::MetaReplicationHistoryId SourceHistory() const {
    return Bytes<20>(tag + 4);
  }
  meta::MetaFailoverTransitionId TransitionId() const {
    return Bytes<16>(tag + 5);
  }
  meta::MetaFailoverActionId ActionId() const { return Bytes<16>(tag + 6); }
  meta::MetaObservationIdentity SourceIdentity() const {
    return {Node(source), SourceBoot(), 1};
  }
  meta::MetaObservationIdentity CandidateIdentity() const {
    return {Node(candidate), CandidateBoot(), 1};
  }
};

const FactsGroup kControlled{"a-controlled", 'c', 'd', 0x30, true};
const FactsGroup kRecovering{"z-recovering", 'a', 'b', 0x10, false};

class MetaObservationFactsFixture : public ::testing::Test {
 protected:
  void SetUp() override {
    // Completed lifecycle can outlive its archived/pruned creation root. Use
    // that supported durable state, with the three required current policies.
    ASSERT_TRUE(
        stores_.topology_
            .BeginClusterCreate(Bytes<16>(0x70), 1, lavik::ClientMode::kCluster)
            .ok());
    ASSERT_TRUE(stores_.topology_.CompleteClusterCreate(Bytes<16>(0x70)).ok());
    const std::vector<std::pair<std::string_view, std::string>> policies{
        {meta::kAutomaticUncontrolledFailoverPolicyId,
         R"({"kind":"automatic-uncontrolled-failover-v1","suspect_after_ms":5000})"},
        {meta::kAuthorityLeasePolicyId,
         R"({"kind":"authority-lease-v1","duration_ms":5000})"},
        {meta::kCandidateRecoveryPolicyId,
         R"({"kind":"candidate-recovery-v1","budget_ms":2000})"}};
    for (const auto& [id, content] : policies) {
      meta::PutPolicy policy;
      policy.policy_id_ = id;
      policy.version_ = 1;
      policy.content_ = content;
      ASSERT_TRUE(stores_.policy_.Apply(policy).ok());
    }
    meta::PutPopulationManifest manifest;
    manifest.entries_ = {{1, 1}, {2, 1}};
    manifest.manifest_digest_ =
        meta::MetaPopulationManifestStore::CanonicalDigest(manifest.entries_);
    digest_ = manifest.manifest_digest_;
    ASSERT_TRUE(stores_.population_manifest_.Put(manifest).ok());
    AddGroup(kControlled);
    AddGroup(kRecovering);
    ASSERT_FALSE(HasFatalFailure());
    auto opened = meta::MetaStateMachine::Open("");
    ASSERT_TRUE(opened.ok()) << opened.status();
    machine_ = std::move(*opened);
    Install(stores_, 100);
  }

  void AddGroup(const FactsGroup& group) {
    meta::CreateGroup create;
    create.group_id_ = group.id;
    create.new_topology_epoch_ = stores_.topology_.TopologyEpoch() + 1;
    ASSERT_TRUE(stores_.topology_.Apply(create).ok());
    for (const char value : {group.source, group.candidate}) {
      meta::RegisterNode node;
      node.node_id_ = Node(value);
      node.principal_ = "lavik://node/" + node.node_id_;
      node.endpoints_ = {"tcp://127.0.0.1:6379"};
      node.role_ = value == group.source ? meta::MetaNodeRole::kPrimary
                                         : meta::MetaNodeRole::kReplica;
      ASSERT_TRUE(stores_.identity_.Apply(node).ok());
      meta::AssignNodeToGroup assign;
      assign.group_id_ = group.id;
      assign.node_id_ = node.node_id_;
      assign.role_ = node.role_;
      assign.assignment_id_ = value == group.source
                                  ? group.SourceAssignment()
                                  : group.CandidateAssignment();
      assign.expected_revision_ =
          stores_.topology_.FindGroup(group.id)->revision_;
      assign.new_topology_epoch_ = stores_.topology_.TopologyEpoch() + 1;
      ASSERT_TRUE(stores_.topology_.Apply(assign).ok());
    }
    meta::BeginGroupTerm term;
    term.group_id_ = group.id;
    term.new_term_ = 1;
    ASSERT_TRUE(stores_.topology_.BeginGroupTerm(term).ok());
    meta::ActivateAuthority activate;
    activate.group_id_ = group.id;
    activate.expected_term_ = 1;
    activate.new_owner_ = Node(group.source);
    ASSERT_TRUE(stores_.topology_.ActivateAuthority(activate).ok());
    ASSERT_TRUE(
        stores_.topology_.SetPopulationManifest(group.id, 9, digest_).ok());
    ASSERT_TRUE(
        stores_.topology_.SetPartitionReplicationEpoch(group.id, 4).ok());

    meta::MetaFailoverCandidateAction action;
    action.action_id_ = group.ActionId();
    action.candidate_ = {Node(group.candidate), group.CandidateAssignment(),
                         group.CandidateBoot()};
    action.domain_ = {1,
                      Node(group.source),
                      group.SourceAssignment(),
                      group.SourceBoot(),
                      group.SourceHistory(),
                      2};
    meta::MetaFailoverTransition transition;
    transition.transition_id_ = group.TransitionId();
    transition.target_term_ = 2;
    transition.candidate_action_ = action;
    if (group.controlled) {
      transition.mode_ = meta::MetaFailoverMode::kControlled;
      meta::SubmitOperation operation;
      operation.operation_id_ = Bytes<16>(group.tag + 7);
      operation.kind_ = std::string(meta::kFailoverOperationKind);
      auto intent = meta::EncodeFailoverOperationIntent({group.id, 9999});
      ASSERT_TRUE(intent.ok()) << intent.status();
      operation.intent_ = *intent;
      operation.intent_hash_ = meta::MetaSha256(operation.intent_);
      ASSERT_TRUE(stores_.operation_.SubmitOperation(operation, 10).ok());
      transition.controlled_ = {operation.operation_id_, 9999};
    } else {
      term.expected_term_ = 1;
      term.new_term_ = 2;
      ASSERT_TRUE(stores_.topology_.BeginGroupTerm(term).ok());
      transition.recovery_deadline_unix_ms_ = 9999;
    }
    ASSERT_TRUE(
        stores_.topology_
            .InstallFailoverTransition(group.id, transition, 20 + group.tag)
            .ok());
  }

  void Install(const meta::MetaStores& stores, std::uint64_t index) {
    auto image = stores.Serialize();
    ASSERT_TRUE(image.ok()) << image.status();
    const auto status = machine_->Install(index, *image);
    ASSERT_TRUE(status.ok()) << status;
  }

  meta::MetaCandidateProgressObs Candidate(const FactsGroup& group,
                                           bool source = false) const {
    const auto identity =
        source ? group.SourceIdentity() : group.CandidateIdentity();
    meta::MetaCandidateProgressObs result;
    result.node_id_ = identity.node_id_;
    result.boot_incarnation_ = identity.boot_incarnation_;
    result.session_generation_ = 1;
    result.group_id_ = group.id;
    result.assignment_id_ =
        source ? group.SourceAssignment() : group.CandidateAssignment();
    result.group_term_ = group.controlled ? 1 : 2;
    result.population_manifest_revision_ = 9;
    result.population_manifest_digest_ = digest_;
    result.partition_replication_epoch_ = 4;
    result.replication_history_id_ = group.SourceHistory();
    result.source_group_term_ = 1;
    result.source_node_id_ = Node(group.source);
    result.source_assignment_id_ = group.SourceAssignment();
    result.source_boot_incarnation_ = group.SourceBoot();
    result.source_replication_history_id_ = group.SourceHistory();
    result.applied_next_lsns_ = {10, 20};
    result.storage_ready_ = true;
    result.population_ready_ = true;
    return result;
  }

  meta::MetaStores stores_;
  meta::MetaHash256 digest_{};
  std::unique_ptr<meta::MetaStateMachine> machine_;
};

std::vector<meta::MetaObservation> Observations(
    const meta::MetaCandidateProgressObs& candidate,
    const meta::MetaHash256& digest) {
  const auto& group = kControlled;
  return {
      {group.CandidateIdentity(), meta::MetaNodeBootObs{}},
      {group.CandidateIdentity(),
       meta::MetaNodeHealthObs{.storage_ready_ = true,
                               .population_ready_ = true,
                               .health_ = "ready"}},
      {group.CandidateIdentity(), candidate},
      {group.SourceIdentity(),
       meta::MetaFailoverObservationObs{meta::MetaSourcePausedObs{
           .group_id_ = group.id,
           .transition_id_ = group.TransitionId(),
           .source_node_id_ = Node(group.source),
           .source_assignment_id_ = group.SourceAssignment(),
           .source_boot_id_ = group.SourceBoot(),
           .source_history_id_ = group.SourceHistory(),
           .source_group_term_ = 1,
           .stable_next_lsns_ = {10, 20}}}},
      {group.CandidateIdentity(),
       meta::MetaFailoverObservationObs{meta::MetaCandidatePreparedObs{
           .group_id_ = group.id,
           .transition_id_ = group.TransitionId(),
           .action_id_ = group.ActionId(),
           .candidate_node_id_ = Node(group.candidate),
           .candidate_assignment_id_ = group.CandidateAssignment(),
           .candidate_boot_id_ = group.CandidateBoot(),
           .prepared_context_id_ = Bytes<16>(0x66)}}},
      {group.CandidateIdentity(),
       meta::MetaFailoverObservationObs{meta::MetaActionFailedObs{
           .group_id_ = group.id,
           .transition_id_ = group.TransitionId(),
           .action_id_ = group.ActionId(),
           .candidate_node_id_ = Node(group.candidate),
           .candidate_assignment_id_ = group.CandidateAssignment(),
           .candidate_boot_id_ = group.CandidateBoot(),
           .population_manifest_revision_ = 9,
           .population_manifest_digest_ = digest,
           .partition_replication_epoch_ = 4,
           .failure_class_ = "watchdog-timeout",
           .failure_detail_ = "promotion did not become durable"}}},
      {kRecovering.CandidateIdentity(),
       meta::MetaFailoverObservationObs{meta::MetaCandidateRecoveryCompleteObs{
           .group_id_ = kRecovering.id,
           .transition_id_ = kRecovering.TransitionId(),
           .action_id_ = kRecovering.ActionId(),
           .candidate_node_id_ = Node(kRecovering.candidate),
           .candidate_assignment_id_ = kRecovering.CandidateAssignment(),
           .candidate_boot_id_ = kRecovering.CandidateBoot(),
           .recovery_deadline_unix_ms_ = 9999,
           .applied_next_lsns_ = {10, 20},
           .completion_reason_ = "target-reached"}}},
  };
}

void ExpectEqualObservations(const meta::MetaObservationStore& original,
                             const meta::MetaObservationStore& projected,
                             const meta::MetaCommittedFacts& original_facts,
                             const meta::MetaCommittedFacts& projected_facts,
                             std::int64_t now) {
  EXPECT_EQ(original.size(), projected.size());
  EXPECT_EQ(original.retained_bytes(), projected.retained_bytes());
  for (const auto& group : {kControlled, kRecovering}) {
    EXPECT_EQ(
        original.LiveCandidateProgressFor(group.id, original_facts, now),
        projected.LiveCandidateProgressFor(group.id, projected_facts, now));
    EXPECT_EQ(
        original.SourcePausedFor(group.TransitionId(), original_facts, now),
        projected.SourcePausedFor(group.TransitionId(), projected_facts, now));
    EXPECT_EQ(
        original.CandidatePreparedFor(group.TransitionId(), group.ActionId(),
                                      original_facts, now),
        projected.CandidatePreparedFor(group.TransitionId(), group.ActionId(),
                                       projected_facts, now));
    EXPECT_EQ(original.ActionFailedFor(group.TransitionId(), group.ActionId(),
                                       original_facts, now),
              projected.ActionFailedFor(group.TransitionId(), group.ActionId(),
                                        projected_facts, now));
    EXPECT_EQ(
        original.CandidateRecoveryCompleteFor(
            group.TransitionId(), group.ActionId(), original_facts, now),
        projected.CandidateRecoveryCompleteFor(
            group.TransitionId(), group.ActionId(), projected_facts, now));
    for (char node : {group.source, group.candidate}) {
      const auto before =
          original.LatestForNode(Node(node), original_facts, now);
      const auto after =
          projected.LatestForNode(Node(node), projected_facts, now);
      ASSERT_EQ(before.has_value(), after.has_value());
      if (before) {
        EXPECT_EQ(before->identity_, after->identity_);
        EXPECT_EQ(before->payload_, after->payload_);
        EXPECT_EQ(before->received_unix_ms_, after->received_unix_ms_);
      }
      EXPECT_EQ(original.retained_bytes_for_node(Node(node)),
                projected.retained_bytes_for_node(Node(node)));
    }
  }
  const auto before = original.AuditRing();
  const auto after = projected.AuditRing();
  ASSERT_EQ(before.size(), after.size());
  for (std::size_t i = 0; i < before.size(); ++i) {
    EXPECT_EQ(before[i].kind_, after[i].kind_);
    EXPECT_EQ(before[i].node_id_, after[i].node_id_);
    EXPECT_EQ(before[i].detail_, after[i].detail_);
    EXPECT_EQ(before[i].unix_ms_, after[i].unix_ms_);
  }
}

// Transition and candidate ordering intentionally differ from Group ordering,
// so every global lookup must cover the whole committed set.
TEST_F(MetaObservationFactsFixture,
       EveryObservationTypeMatchesOwnedStoresFacts) {
  const auto captured = machine_->CaptureObservationFacts();
  const meta::StoredFactsTestAdapter original_facts(stores_);
  for (const auto& observation :
       Observations(Candidate(kControlled), digest_)) {
    SCOPED_TRACE(observation.payload_.index());
    meta::MetaObservationStore original;
    meta::MetaObservationStore projected;
    ASSERT_TRUE(original.AdoptSession(observation.identity_, 999).ok());
    ASSERT_TRUE(projected.AdoptSession(observation.identity_, 999).ok());
    ASSERT_TRUE(original.Ingest(observation, original_facts, 1000).ok());
    ASSERT_TRUE(projected.Ingest(observation, captured, 1000).ok());
    EXPECT_EQ(projected.size(), 1u);
    EXPECT_GT(projected.retained_bytes(), 0u);
    ExpectEqualObservations(original, projected, original_facts, captured,
                            1001);
    original.RevalidateAll(original_facts, 1001);
    projected.RevalidateAll(captured, 1001);
    EXPECT_EQ(projected.size(), 1u);
    ExpectEqualObservations(original, projected, original_facts, captured,
                            1001);
  }
  for (const auto& group : {kControlled, kRecovering}) {
    const auto transition =
        captured.FailoverTransitionById(group.TransitionId());
    ASSERT_TRUE(transition.has_value());
    EXPECT_EQ(transition->group_id_, group.id);
    EXPECT_EQ(transition->transition_,
              *stores_.topology_.FindGroup(group.id)->failover_transition_);
    EXPECT_TRUE(captured.IsCurrentFailoverCandidate(Node(group.candidate),
                                                    group.CandidateBoot()));
    EXPECT_FALSE(captured.IsCurrentFailoverCandidate(Node(group.candidate),
                                                     group.SourceBoot()));
    EXPECT_TRUE(captured.IsOwnerAssignment(group.id, Node(group.source),
                                           group.SourceAssignment()));
    EXPECT_FALSE(captured.AssignmentMatches(group.id, Node(group.candidate),
                                            group.SourceAssignment()));
  }
  EXPECT_FALSE(captured.IsActiveNode("unknown"));
  EXPECT_EQ(captured.CurrentGroupTerm("unknown"), 0u);
  EXPECT_EQ(captured.CurrentPopulationManifestRevision("unknown"), 0u);
  EXPECT_EQ(captured.CurrentPopulationManifestDigest("unknown"),
            meta::MetaHash256{});
  EXPECT_EQ(captured.CurrentPartitionReplicationEpoch("unknown"), 0u);
  EXPECT_FALSE(captured.AssignmentMatches("unknown", Node(kControlled.source),
                                          kControlled.SourceAssignment()));
  EXPECT_FALSE(captured.IsOwnerAssignment("unknown", Node(kControlled.source),
                                          kControlled.SourceAssignment()));
  EXPECT_FALSE(captured.IsCurrentFailoverCandidate("unknown", Bytes<20>(1)));
  EXPECT_FALSE(captured.FailoverTransitionById(Bytes<16>(0xff)).has_value());
}

TEST_F(MetaObservationFactsFixture,
       ProposalFactsPreserveCrossGroupEvidenceAndLifetime) {
  meta::StartCandidateRecovery command;
  command.group_id_ = kRecovering.id;
  auto view = machine_->CaptureProposal(command);
  auto copied = view;
  auto retained = std::move(copied);
  const meta::StoredFactsTestAdapter original_facts(stores_);
  // Query the other Group's observations too, including node-wide lookup.
  for (const auto& observation :
       Observations(Candidate(kControlled), digest_)) {
    SCOPED_TRACE(observation.payload_.index());
    meta::MetaObservationStore original;
    meta::MetaObservationStore projected;
    ASSERT_TRUE(original.AdoptSession(observation.identity_, 999).ok());
    ASSERT_TRUE(projected.AdoptSession(observation.identity_, 999).ok());
    ASSERT_TRUE(original.Ingest(observation, original_facts, 1000).ok());
    ASSERT_TRUE(projected.Ingest(observation, retained.facts(), 1000).ok());
    ExpectEqualObservations(original, projected, original_facts,
                            retained.facts(), 1001);
  }
  const auto transition =
      retained.facts().FailoverTransitionById(kControlled.TransitionId());
  ASSERT_TRUE(transition.has_value());
  EXPECT_EQ(transition->group_id_, kControlled.id);
  meta::MetaStores empty;
  Install(empty, 101);
  machine_.reset();
  EXPECT_EQ(retained.applied_index(), 100u);
  EXPECT_EQ(retained.facts()
                .FailoverTransitionById(kControlled.TransitionId())
                ->transition_,
            transition->transition_);
  EXPECT_TRUE(retained.facts().IsActiveNode(Node(kRecovering.source)));
  EXPECT_EQ(retained.group(command.group_id_)->record_.group_term_, 2u);
}

TEST_F(MetaObservationFactsFixture,
       FailoverCapturesKeepGlobalEvidenceAndOnlyTheirCurrentPolicies) {
  const auto discovery = machine_->CaptureFailoverDiscovery();
  ASSERT_EQ(discovery.work_.size(), 2u);
  EXPECT_EQ(discovery.work_[0].group_id_, kControlled.id);
  EXPECT_EQ(discovery.work_[1].group_id_, kRecovering.id);
  ASSERT_EQ(discovery.operations_.size(), 1u);
  EXPECT_EQ(discovery.work_[0].operation_id_,
            discovery.operations_[0].operation_id_);
  const auto controlled =
      machine_->CaptureFailoverPlanningView(discovery.cursor_, kControlled.id);
  const auto recovering =
      machine_->CaptureFailoverPlanningView(discovery.cursor_, kRecovering.id);
  ASSERT_TRUE(controlled);
  ASSERT_TRUE(recovering);
  ASSERT_TRUE(controlled->operation_);
  EXPECT_EQ(controlled->operation_->intent_, discovery.operations_[0].intent_);
  EXPECT_FALSE(controlled->recovery_policy_);
  EXPECT_FALSE(recovering->operation_);
  ASSERT_TRUE(recovering->recovery_policy_);
  EXPECT_EQ(recovering->recovery_policy_->budget_ms_, 2000u);
  const auto detection = machine_->CaptureAutomaticDetectionView();
  ASSERT_TRUE(detection.automatic_);
  ASSERT_TRUE(detection.lease_);
  EXPECT_EQ(detection.automatic_->suspect_after_ms_, 5000u);
  EXPECT_EQ(detection.lease_->duration_ms_, 5000u);
  ASSERT_EQ(detection.groups_.size(), 2u);
  EXPECT_EQ(detection.FindGroup(kControlled.id)->owner_assignment_,
            kControlled.SourceAssignment());
  EXPECT_EQ(detection.FindGroup(kRecovering.id)->transition_->transition_id_,
            kRecovering.TransitionId());
  EXPECT_EQ(detection.FindGroup("absent"), nullptr);

  const meta::StoredFactsTestAdapter original_facts(stores_);
  // The target is recovering, but the newest valid node-wide report can live
  // exclusively in the controlled Group. Every observation lookup keeps its
  // complete domain and must survive later apply/Install independently.
  for (const auto& observation :
       Observations(Candidate(kControlled), digest_)) {
    meta::MetaObservationStore original;
    meta::MetaObservationStore projected;
    ASSERT_TRUE(original.AdoptSession(observation.identity_, 999).ok());
    ASSERT_TRUE(projected.AdoptSession(observation.identity_, 999).ok());
    ASSERT_TRUE(original.Ingest(observation, original_facts, 1000).ok());
    ASSERT_TRUE(projected.Ingest(observation, recovering->facts_, 1000).ok());
    ExpectEqualObservations(original, projected, original_facts,
                            recovering->facts_, 1001);
  }
  machine_->Advance(101);
  const auto advanced =
      machine_->CaptureAutomaticTriggerView(detection.cursor_);
  ASSERT_TRUE(advanced);
  EXPECT_EQ(advanced->cursor_.applied_index(), 101u);
  EXPECT_EQ(advanced->cursor_.state_change_index(), 100u);
  const auto advanced_plan =
      machine_->CaptureFailoverPlanningView(discovery.cursor_, kRecovering.id);
  ASSERT_TRUE(advanced_plan);
  EXPECT_EQ(advanced_plan->facts_.applied_index(), 101u);
  // Equal applied indexes still have different state-change cuts after Install.
  meta::MetaStores empty;
  Install(empty, 101);
  EXPECT_FALSE(machine_->CaptureAutomaticTriggerView(advanced->cursor_));
  EXPECT_FALSE(
      machine_->CaptureFailoverPlanningView({101, 100}, kRecovering.id));
  machine_.reset();
  EXPECT_EQ(recovering->facts_.applied_index(), 100u);
  EXPECT_TRUE(recovering->facts_.IsActiveNode(Node(kControlled.candidate)));
  EXPECT_EQ(controlled->group_->failover_transition_->transition_id_,
            kControlled.TransitionId());
  EXPECT_EQ(detection.FindGroup(kControlled.id)->record_.group_term_, 1u);
  EXPECT_EQ(advanced->operations_[0].intent_, discovery.operations_[0].intent_);
}

void ClearTransition(meta::MetaStores& stores, const FactsGroup& group) {
  const auto transition =
      *stores.topology_.FindGroup(group.id)->failover_transition_;
  ASSERT_TRUE(stores.topology_
                  .ClearFailoverTransition(group.id, {transition.transition_id_,
                                                      transition.revision_})
                  .ok());
}

void RemoveCandidate(meta::MetaStores& stores, const FactsGroup& group) {
  ClearTransition(stores, group);
  meta::RemoveNodeFromGroup remove;
  remove.group_id_ = group.id;
  remove.node_id_ = Node(group.candidate);
  remove.expected_revision_ = stores.topology_.FindGroup(group.id)->revision_;
  remove.new_topology_epoch_ = stores.topology_.TopologyEpoch() + 1;
  ASSERT_TRUE(stores.topology_.Apply(remove).ok());
}

void ReplaceTransition(
    meta::MetaStores& stores, const FactsGroup& group,
    const std::function<void(meta::MetaFailoverTransition&)>& change) {
  auto transition = *stores.topology_.FindGroup(group.id)->failover_transition_;
  const meta::MetaFailoverTransitionRef expected{transition.transition_id_,
                                                 transition.revision_};
  change(transition);
  ASSERT_TRUE(stores.topology_
                  .ReplaceFailoverTransition(group.id, expected, transition,
                                             transition.revision_ + 1)
                  .ok());
}

TEST_F(MetaObservationFactsFixture,
       CommitRevalidationKeepsTheSameObservationsReasonsAndByteCharges) {
  struct Scenario {
    std::string name;
    std::function<void(meta::MetaStores&)> change;
    // Boot, Health, CandidateProgress, SourcePaused, CandidatePrepared,
    // ActionFailed, CandidateRecoveryComplete. Empty means still admissible.
    std::array<std::string, 7> rejection;
  };
  const std::vector<Scenario> scenarios{
      {"retired-node",
       [](auto& stores) {
         RemoveCandidate(stores, kControlled);
         meta::RetireNode retire;
         retire.node_id_ = Node(kControlled.candidate);
         retire.expected_revision_ = 1;
         ASSERT_TRUE(stores.identity_.Apply(retire).ok());
       },
       {"node-not-active", "node-not-active", "node-not-active",
        "failover-transition-mismatch", "node-not-active", "node-not-active",
        ""}},
      {"removed-member",
       [](auto& stores) { RemoveCandidate(stores, kControlled); },
       {"", "", "assignment-mismatch", "failover-transition-mismatch",
        "failover-transition-mismatch", "failover-transition-mismatch", ""}},
      {"new-membership-incarnation",
       [](auto& stores) {
         RemoveCandidate(stores, kControlled);
         meta::AssignNodeToGroup assign;
         assign.group_id_ = kControlled.id;
         assign.node_id_ = Node(kControlled.candidate);
         assign.assignment_id_ = Bytes<16>(0x7f);
         assign.role_ = meta::MetaNodeRole::kReplica;
         assign.expected_revision_ =
             stores.topology_.FindGroup(kControlled.id)->revision_;
         assign.new_topology_epoch_ = stores.topology_.TopologyEpoch() + 1;
         ASSERT_TRUE(stores.topology_.Apply(assign).ok());
       },
       {"", "", "assignment-mismatch", "failover-transition-mismatch",
        "failover-transition-mismatch", "failover-transition-mismatch", ""}},
      {"new-term",
       [](auto& stores) {
         ClearTransition(stores, kControlled);
         meta::BeginGroupTerm term;
         term.group_id_ = kControlled.id;
         term.expected_term_ = 1;
         term.new_term_ = 2;
         ASSERT_TRUE(stores.topology_.BeginGroupTerm(term).ok());
       },
       {"", "", "term-mismatch:committed=2", "failover-transition-mismatch",
        "failover-transition-mismatch", "failover-transition-mismatch", ""}},
      {"manifest-revision",
       [this](auto& stores) {
         ASSERT_TRUE(
             stores.topology_.SetPopulationManifest(kControlled.id, 10, digest_)
                 .ok());
       },
       {"", "", "manifest-mismatch:committed=10", "", "",
        "action-failed-population-mismatch", ""}},
      {"manifest-digest",
       [](auto& stores) {
         meta::PutPopulationManifest manifest;
         manifest.entries_ = {{1, 2}, {2, 2}};
         manifest.manifest_digest_ =
             meta::MetaPopulationManifestStore::CanonicalDigest(
                 manifest.entries_);
         ASSERT_TRUE(stores.population_manifest_.Put(manifest).ok());
         ASSERT_TRUE(stores.topology_
                         .SetPopulationManifest(kControlled.id, 9,
                                                manifest.manifest_digest_)
                         .ok());
       },
       {"", "", "manifest-digest-mismatch", "", "",
        "action-failed-population-mismatch", ""}},
      {"partition-epoch",
       [](auto& stores) {
         ASSERT_TRUE(
             stores.topology_.SetPartitionReplicationEpoch(kControlled.id, 5)
                 .ok());
       },
       {"", "", "partition-epoch-mismatch:committed=5", "", "",
        "action-failed-population-mismatch", ""}},
      {"replacement-action",
       [](auto& stores) {
         ReplaceTransition(stores, kControlled, [](auto& transition) {
           transition.candidate_action_->action_id_ = Bytes<16>(0x7f);
         });
       },
       {"", "", "", "", "failover-action-mismatch", "failover-action-mismatch",
        ""}},
      {"replacement-transition",
       [](auto& stores) {
         auto transition =
             *stores.topology_.FindGroup(kControlled.id)->failover_transition_;
         ClearTransition(stores, kControlled);
         transition.transition_id_ = Bytes<16>(0x7f);
         ASSERT_TRUE(
             stores.topology_
                 .InstallFailoverTransition(kControlled.id, transition, 90)
                 .ok());
       },
       {"", "", "", "failover-transition-mismatch",
        "failover-transition-mismatch", "failover-transition-mismatch", ""}},
      {"deleted-transition",
       [](auto& stores) { ClearTransition(stores, kControlled); },
       {"", "", "", "failover-transition-mismatch",
        "failover-transition-mismatch", "failover-transition-mismatch", ""}},
      {"replacement-candidate-boot",
       [](auto& stores) {
         ReplaceTransition(stores, kControlled, [](auto& transition) {
           transition.candidate_action_->candidate_.boot_id_ = Bytes<20>(0x7f);
         });
       },
       {"", "", "", "", "failover-candidate-anchor-mismatch",
        "failover-candidate-anchor-mismatch", ""}},
      {"replacement-recovery-action",
       [](auto& stores) {
         ReplaceTransition(stores, kRecovering, [](auto& transition) {
           transition.candidate_action_->action_id_ = Bytes<16>(0x7f);
         });
       },
       {"", "", "", "", "", "", "failover-action-mismatch"}},
      {"deleted-recovery-action",
       [](auto& stores) {
         ReplaceTransition(stores, kRecovering, [](auto& transition) {
           transition.candidate_action_.reset();
         });
       },
       {"", "", "", "", "", "", "failover-action-mismatch"}},
      {"deleted-recovery-transition",
       [](auto& stores) { ClearTransition(stores, kRecovering); },
       {"", "", "", "", "", "", "failover-transition-mismatch"}},
  };
  const auto initial = machine_->CaptureObservationFacts();
  const meta::StoredFactsTestAdapter original_initial(stores_);
  const auto observations = Observations(Candidate(kControlled), digest_);
  std::uint64_t index = 100;
  for (const auto& scenario : scenarios) {
    SCOPED_TRACE(scenario.name);
    auto changed = stores_;
    scenario.change(changed);
    ASSERT_FALSE(HasFatalFailure());
    Install(changed, ++index);
    ASSERT_FALSE(HasFatalFailure());
    const auto captured = machine_->CaptureObservationFacts();
    const meta::StoredFactsTestAdapter original_changed(changed);
    for (std::size_t i = 0; i < observations.size(); ++i) {
      SCOPED_TRACE(i);
      const auto& observation = observations[i];
      meta::MetaObservationStore original;
      meta::MetaObservationStore projected;
      ASSERT_TRUE(original.AdoptSession(observation.identity_, 999).ok());
      ASSERT_TRUE(projected.AdoptSession(observation.identity_, 999).ok());
      ASSERT_TRUE(original.Ingest(observation, original_initial, 1000).ok());
      ASSERT_TRUE(projected.Ingest(observation, initial, 1000).ok());
      original.RevalidateAll(original_changed, 1001);
      projected.RevalidateAll(captured, 1001);
      ExpectEqualObservations(original, projected, original_changed, captured,
                              1001);
      EXPECT_EQ(projected.size(), scenario.rejection[i].empty() ? 1u : 0u);
      if (!scenario.rejection[i].empty()) {
        EXPECT_EQ(projected.retained_bytes(), 0u);
        ASSERT_EQ(projected.AuditRing().size(), 1u);
        EXPECT_EQ(projected.AuditRing()[0].detail_,
                  "commit-stale:" + scenario.rejection[i]);
      }
      const auto before = original.Ingest(observation, original_changed, 1002);
      const auto after = projected.Ingest(observation, captured, 1002);
      EXPECT_EQ(before, after);
      EXPECT_EQ(after.message(), scenario.rejection[i]);
      ExpectEqualObservations(original, projected, original_changed, captured,
                              1002);
    }
  }
}

TEST_F(MetaObservationFactsFixture,
       CopiedAndMovedFactsPreserveFencedOwnerEvidenceAfterStateReplacement) {
  const auto retained = [this] {
    const auto original = machine_->CaptureObservationFacts();
    auto copied = original;
    auto moved = std::move(copied);
    return moved;
  }();
  const auto owner_candidate = Candidate(kRecovering, true);
  const auto active_owner_candidate = Candidate(kControlled, true);
  const meta::StoredFactsTestAdapter original_facts(stores_);
  EXPECT_TRUE(retained.MayReportFencedOwnerCandidate(owner_candidate));
  EXPECT_TRUE(original_facts.MayReportFencedOwnerCandidate(owner_candidate));
  EXPECT_FALSE(retained.MayReportFencedOwnerCandidate(active_owner_candidate));
  EXPECT_FALSE(
      original_facts.MayReportFencedOwnerCandidate(active_owner_candidate));

  meta::MetaObservationStore observations;
  ASSERT_TRUE(observations
                  .AdoptSession(kRecovering.SourceIdentity(), 999,
                                kRecovering.SourceHistory())
                  .ok());
  ASSERT_TRUE(observations
                  .Ingest({kRecovering.SourceIdentity(), owner_candidate},
                          retained, 1000)
                  .ok());
  ASSERT_EQ(
      observations.LiveCandidateProgressFor(kRecovering.id, retained, 1000)
          .size(),
      1u);

  meta::RegisterNode node;
  node.request_id_.fill(0x7e);
  node.node_id_ = Node('e');
  node.principal_ = "lavik://node/" + node.node_id_;
  node.endpoints_ = {"tcp://127.0.0.1:6380"};
  node.role_ = meta::MetaNodeRole::kReplica;
  auto encoded = meta::MetaStateMachine::EncodeCommand(node);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  machine_->commit(101, **encoded);
  ASSERT_TRUE(machine_->CaptureObservationFacts().IsActiveNode(node.node_id_));
  EXPECT_FALSE(retained.IsActiveNode(node.node_id_));
  machine_->Advance(102);

  auto changed = machine_->CaptureRecoveryStores().stores_;
  ClearTransition(changed, kRecovering);
  meta::ActivateAuthority activate;
  activate.group_id_ = kRecovering.id;
  activate.expected_term_ = 2;
  activate.new_owner_ = Node(kRecovering.source);
  ASSERT_TRUE(changed.topology_.ActivateAuthority(activate).ok());
  RemoveCandidate(changed, kControlled);
  meta::RetireNode retire;
  retire.node_id_ = Node(kControlled.candidate);
  retire.expected_revision_ = 1;
  ASSERT_TRUE(changed.identity_.Apply(retire).ok());
  meta::PutPopulationManifest replacement_manifest;
  replacement_manifest.entries_ = {{1, 2}, {2, 2}};
  replacement_manifest.manifest_digest_ =
      meta::MetaPopulationManifestStore::CanonicalDigest(
          replacement_manifest.entries_);
  ASSERT_TRUE(changed.population_manifest_.Put(replacement_manifest).ok());
  ASSERT_TRUE(changed.topology_
                  .SetPopulationManifest(kControlled.id, 10,
                                         replacement_manifest.manifest_digest_)
                  .ok());
  ASSERT_TRUE(
      changed.topology_.SetPartitionReplicationEpoch(kControlled.id, 5).ok());
  Install(changed, 200);
  ASSERT_FALSE(HasFatalFailure());
  const auto replaced = machine_->CaptureObservationFacts();
  machine_.reset();

  EXPECT_EQ(retained.applied_index(), 100u);
  EXPECT_EQ(retained.state_change_index(), 100u);
  EXPECT_EQ(replaced.applied_index(), 200u);
  EXPECT_EQ(replaced.state_change_index(), 200u);
  EXPECT_TRUE(retained.IsActiveNode(Node(kControlled.candidate)));
  EXPECT_FALSE(replaced.IsActiveNode(Node(kControlled.candidate)));
  EXPECT_EQ(retained.CurrentPopulationManifestRevision(kControlled.id), 9u);
  EXPECT_EQ(replaced.CurrentPopulationManifestRevision(kControlled.id), 10u);
  EXPECT_EQ(retained.CurrentPopulationManifestDigest(kControlled.id), digest_);
  EXPECT_EQ(replaced.CurrentPopulationManifestDigest(kControlled.id),
            replacement_manifest.manifest_digest_);
  EXPECT_EQ(retained.CurrentPartitionReplicationEpoch(kControlled.id), 4u);
  EXPECT_EQ(replaced.CurrentPartitionReplicationEpoch(kControlled.id), 5u);
  EXPECT_TRUE(retained.MayReportFencedOwnerCandidate(owner_candidate));
  EXPECT_FALSE(replaced.MayReportFencedOwnerCandidate(owner_candidate));
  for (const auto& group : {kControlled, kRecovering}) {
    const auto transition =
        retained.FailoverTransitionById(group.TransitionId());
    ASSERT_TRUE(transition.has_value());
    EXPECT_EQ(transition->group_id_, group.id);
    EXPECT_EQ(transition->transition_,
              *stores_.topology_.FindGroup(group.id)->failover_transition_);
    EXPECT_TRUE(retained.IsCurrentFailoverCandidate(Node(group.candidate),
                                                    group.CandidateBoot()));
    EXPECT_FALSE(
        replaced.FailoverTransitionById(group.TransitionId()).has_value());
    EXPECT_FALSE(replaced.IsCurrentFailoverCandidate(Node(group.candidate),
                                                     group.CandidateBoot()));
  }
  ASSERT_EQ(
      observations.LiveCandidateProgressFor(kRecovering.id, retained, 1001)
          .size(),
      1u);
  EXPECT_TRUE(
      observations.LiveCandidateProgressFor(kRecovering.id, replaced, 1001)
          .empty());
  observations.RevalidateAll(replaced, 1001);
  EXPECT_EQ(observations.size(), 0u);
  EXPECT_EQ(observations.retained_bytes(), 0u);
  ASSERT_EQ(observations.AuditRing().size(), 1u);
  EXPECT_EQ(observations.AuditRing()[0].detail_,
            "commit-stale:candidate-is-committed-owner");
}

template <auto CaptureFacts>
void CheckConcurrentCaptures() {
  constexpr std::uint64_t kRounds = 16;
  const auto node_id = [](std::uint64_t index) {
    constexpr std::string_view digits = "0123456789abcdef";
    std::string id(40, 'a');
    id[38] = digits[(index >> 4) & 15];
    id[39] = digits[index & 15];
    return id;
  };
  meta::MetaStores stores;
  meta::CreateGroup create;
  create.group_id_ = "concurrent";
  create.new_topology_epoch_ = 1;
  ASSERT_TRUE(stores.topology_.Apply(create).ok());
  auto opened = meta::MetaStateMachine::Open("");
  ASSERT_TRUE(opened.ok()) << opened.status();
  auto machine = std::move(*opened);
  const auto initial_image = stores.Serialize();
  ASSERT_TRUE(initial_image.ok()) << initial_image.status();
  ASSERT_TRUE(machine->Install(1, *initial_image).ok());

  enum class Change { kCommit, kAdvance, kInstall };
  struct Step {
    Change change;
    std::uint64_t index;
    std::shared_ptr<meta::MetaRaftBuffer> command;
    std::string image;
  };
  std::vector<Step> steps;
  for (std::uint64_t round = 0; round < kRounds; ++round) {
    meta::BeginGroupTerm term;
    term.request_id_.fill(static_cast<std::uint8_t>(round + 1));
    term.group_id_ = create.group_id_;
    term.expected_term_ = round * 2;
    term.new_term_ = term.expected_term_ + 1;
    auto encoded = meta::MetaStateMachine::EncodeCommand(term);
    ASSERT_TRUE(encoded.ok()) << encoded.status();
    steps.push_back({Change::kCommit, round * 3 + 2, *encoded, {}});
    steps.push_back({Change::kAdvance, round * 3 + 3, {}, {}});
    ASSERT_TRUE(stores.topology_.BeginGroupTerm(term).ok());
    ++term.expected_term_;
    ++term.new_term_;
    ASSERT_TRUE(stores.topology_.BeginGroupTerm(term).ok());
    meta::RegisterNode node;
    node.node_id_ = node_id(round);
    node.principal_ = "lavik://node/" + node.node_id_;
    node.endpoints_ = {"tcp://127.0.0.1:6379"};
    node.role_ = meta::MetaNodeRole::kReplica;
    ASSERT_TRUE(stores.identity_.Apply(node).ok());
    auto image = stores.Serialize();
    ASSERT_TRUE(image.ok()) << image.status();
    steps.push_back({Change::kInstall, round * 3 + 4, {}, std::move(*image)});
  }

  meta::StartCandidateRecovery proposal_command;
  proposal_command.group_id_ = create.group_id_;
  const auto check_cut = [&](const auto& view,
                             const meta::MetaCommittedFacts& facts) {
    const auto index = view.applied_index();
    if (index == 0 || index > kRounds * 3 + 1) {
      ADD_FAILURE() << "unexpected capture index " << index;
      return;
    }
    // Each round commits one term, advances only the applied cursor, then
    // installs the next term together with one additional active node.
    const auto completed = (index - 1) / 3;
    const auto phase = (index - 1) % 3;
    EXPECT_EQ(facts.CurrentGroupTerm(create.group_id_),
              completed * 2 + (phase == 0 ? 0u : 1u));
    EXPECT_EQ(view.state_change_index(), phase == 2 ? index - 1 : index);
    for (std::uint64_t node = 0; node < kRounds; ++node) {
      EXPECT_EQ(facts.IsActiveNode(node_id(node)), node < completed)
          << "at committed cut " << index << ", node " << node;
    }
  };
  std::barrier phase(3);
  const auto reader = [&] {
    for (const auto& step : steps) {
      phase.arrive_and_wait();
      for (int capture = 0; capture < 8; ++capture) {
        const auto facts = (machine.get()->*CaptureFacts)();
        check_cut(facts, facts);
        const auto proposal = machine->CaptureProposal(proposal_command);
        check_cut(proposal, proposal.facts());
        const auto discovery = machine->CaptureFailoverDiscovery();
        const auto planning = machine->CaptureFailoverPlanningView(
            discovery.cursor_, create.group_id_);
        if (planning) {
          check_cut(planning->facts_, planning->facts_);
          EXPECT_EQ(planning->group_->record_.group_term_,
                    planning->facts_.CurrentGroupTerm(create.group_id_));
        }
        const auto detection = machine->CaptureAutomaticDetectionView();
        const auto detected_index = detection.cursor_.applied_index();
        EXPECT_EQ(detection.FindGroup(create.group_id_)->record_.group_term_,
                  ((detected_index - 1) / 3) * 2 +
                      ((detected_index - 1) % 3 == 0 ? 0u : 1u));
        const auto trigger =
            machine->CaptureAutomaticTriggerView(detection.cursor_);
        if (trigger)
          EXPECT_EQ(trigger->cursor_.state_change_index(),
                    detection.cursor_.state_change_index());
      }
      phase.arrive_and_wait();
      // The writer cannot enter the next phase until both readers finish
      // this check, so this also verifies every completed mutation was seen.
      const auto view = (machine.get()->*CaptureFacts)();
      check_cut(view, view);
      const auto proposal = machine->CaptureProposal(proposal_command);
      check_cut(proposal, proposal.facts());
      EXPECT_EQ(proposal.applied_index(), step.index);
      EXPECT_EQ(view.applied_index(), step.index);
      const auto cursor = machine->CaptureCommittedCursor();
      EXPECT_EQ(cursor.applied_index(), view.applied_index());
      EXPECT_EQ(cursor.state_change_index(), view.state_change_index());
    }
  };
  std::jthread first(reader);
  std::jthread second(reader);
  for (const auto& step : steps) {
    phase.arrive_and_wait();
    switch (step.change) {
      case Change::kCommit:
        EXPECT_NE(machine->commit(step.index, *step.command), nullptr);
        break;
      case Change::kAdvance:
        machine->Advance(step.index);
        break;
      case Change::kInstall:
        EXPECT_TRUE(machine->Install(step.index, step.image).ok());
        break;
    }
    phase.arrive_and_wait();
  }
  first.join();
  second.join();
  const auto facts = (machine.get()->*CaptureFacts)();
  check_cut(facts, facts);
  const auto proposal = machine->CaptureProposal(proposal_command);
  check_cut(proposal, proposal.facts());
  EXPECT_EQ(machine->CaptureCommittedCursor().applied_index(), kRounds * 3 + 1);
}
TEST(MetaObservationFactsTest, ConcurrentCommitAdvanceAndInstallKeepOneCut) {
  CheckConcurrentCaptures<&meta::MetaStateMachine::CaptureObservationFacts>();
}
TEST(MetaDataPublicationViewTest, ConcurrentCommitAdvanceAndInstallKeepOneCut) {
  CheckConcurrentCaptures<&meta::MetaStateMachine::CaptureDataPublication>();
}

// Transition and candidate ordering intentionally differ from Group ordering,
// so every global lookup must cover the whole committed set.
TEST_F(MetaObservationFactsFixture, PublicationFactsMatchAllObservationTypes) {
  const auto captured = machine_->CaptureDataPublication();
  const meta::StoredFactsTestAdapter original_facts(stores_);
  for (const auto& observation :
       Observations(Candidate(kControlled), digest_)) {
    SCOPED_TRACE(observation.payload_.index());
    meta::MetaObservationStore original;
    meta::MetaObservationStore projected;
    ASSERT_TRUE(original.AdoptSession(observation.identity_, 999).ok());
    ASSERT_TRUE(projected.AdoptSession(observation.identity_, 999).ok());
    ASSERT_TRUE(original.Ingest(observation, original_facts, 1000).ok());
    ASSERT_TRUE(projected.Ingest(observation, captured, 1000).ok());
    EXPECT_EQ(projected.size(), 1u);
    EXPECT_GT(projected.retained_bytes(), 0u);
    ExpectEqualObservations(original, projected, original_facts, captured,
                            1001);
    original.RevalidateAll(original_facts, 1001);
    projected.RevalidateAll(captured, 1001);
    EXPECT_EQ(projected.size(), 1u);
    ExpectEqualObservations(original, projected, original_facts, captured,
                            1001);
  }
  for (const auto& group : {kControlled, kRecovering}) {
    const auto transition =
        captured.FailoverTransitionById(group.TransitionId());
    ASSERT_TRUE(transition.has_value());
    EXPECT_EQ(transition->group_id_, group.id);
    EXPECT_EQ(transition->transition_,
              *stores_.topology_.FindGroup(group.id)->failover_transition_);
    EXPECT_TRUE(captured.IsCurrentFailoverCandidate(Node(group.candidate),
                                                    group.CandidateBoot()));
    EXPECT_FALSE(captured.IsCurrentFailoverCandidate(Node(group.candidate),
                                                     group.SourceBoot()));
    EXPECT_TRUE(captured.IsOwnerAssignment(group.id, Node(group.source),
                                           group.SourceAssignment()));
    EXPECT_FALSE(captured.AssignmentMatches(group.id, Node(group.candidate),
                                            group.SourceAssignment()));
  }
  EXPECT_FALSE(captured.IsActiveNode("unknown"));
  EXPECT_EQ(captured.CurrentGroupTerm("unknown"), 0u);
  EXPECT_EQ(captured.CurrentPopulationManifestRevision("unknown"), 0u);
  EXPECT_EQ(captured.CurrentPopulationManifestDigest("unknown"),
            meta::MetaHash256{});
  EXPECT_EQ(captured.CurrentPartitionReplicationEpoch("unknown"), 0u);
  EXPECT_FALSE(captured.AssignmentMatches("unknown", Node(kControlled.source),
                                          kControlled.SourceAssignment()));
  EXPECT_FALSE(captured.IsOwnerAssignment("unknown", Node(kControlled.source),
                                          kControlled.SourceAssignment()));
  EXPECT_FALSE(captured.IsCurrentFailoverCandidate("unknown", Bytes<20>(1)));
  EXPECT_FALSE(captured.FailoverTransitionById(Bytes<16>(0xff)).has_value());
}

}  // namespace
