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

#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <future>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "bycorf/runtime/runtime.h"
#include "bycorf/runtime/worker.h"
#include "gtest/gtest.h"
#include "lavik/meta/candidate_plan.h"
#include "lavik/meta/cluster_create.h"
#include "lavik/meta/failover.h"
#include "lavik/meta/failover_reconciler.h"
#include "lavik/meta/hash.h"
#include "lavik/meta/state_apply.h"
#include "lavik/meta/state_machine.h"
#include "support/meta_raft.h"
#include "support/test_data_path.h"

namespace {

namespace meta = lavik::meta;

template <std::size_t N>
std::array<std::uint8_t, N> Bytes(std::uint8_t value) {
  std::array<std::uint8_t, N> result{};
  result.fill(value);
  return result;
}

std::string Node(std::uint8_t value) {
  std::string result(40, '0');
  constexpr char kHex[] = "0123456789abcdef";
  result[38] = kHex[value >> 4];
  result[39] = kHex[value & 0x0f];
  return result;
}

bool WaitUntil(const std::function<bool()>& predicate,
               std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (predicate()) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  return predicate();
}

std::unique_ptr<const meta::MetaStores> StoresSnapshotOnHeap(
    const meta::MetaStateMachine& machine) {
  // These lifecycle tests retain several large snapshots. Direct initialization
  // elides the return-value copy into heap storage; make_unique would first
  // materialize a large stack temporary for its forwarding argument.
  return std::unique_ptr<const meta::MetaStores>(
      new const meta::MetaStores(machine.StoresSnapshot()));
}

TEST(MetaFencedAuthorityGuardTest,
     FenceNeedsElapsedLeaseWindowAndResetsWithLeadership) {
  meta::MetaFencedAuthorityGuard guard(600);
  EXPECT_FALSE(guard.ObserveFence("g", 2, 1000));
  EXPECT_FALSE(guard.ObserveFence("g", 2, 1599));
  EXPECT_TRUE(guard.ObserveFence("g", 2, 1600));
  EXPECT_FALSE(guard.ObserveFence("g", 3, 1600));
  EXPECT_TRUE(guard.ObserveFence("g", 3, 2200));
  guard.Reset();
  EXPECT_FALSE(guard.ObserveFence("g", 3, 2300));
  EXPECT_TRUE(guard.ObserveFence("g", 3, 2900));
}

struct Fixture {
  meta::MetaStores stores;
  meta::MetaObservationStore observations;
  std::string owner = Node(1);
  std::string candidate = Node(2);
  std::string alternate = Node(3);
  meta::MetaAssignmentId owner_assignment = Bytes<16>(0x21);
  meta::MetaAssignmentId candidate_assignment = Bytes<16>(0x22);
  meta::MetaAssignmentId alternate_assignment = Bytes<16>(0x23);
  meta::MetaBootIncarnation owner_boot = Bytes<20>(0x31);
  meta::MetaBootIncarnation candidate_boot = Bytes<20>(0x32);
  meta::MetaBootIncarnation alternate_boot = Bytes<20>(0x33);
  meta::MetaReplicationHistoryId source_history = Bytes<20>(0x41);
  meta::MetaOperationId operation_id = Bytes<16>(0x51);
  std::uint64_t next_index = 1;
  std::vector<meta::MetaCommand> committed_commands;

  Fixture() {
    const meta::MetaOperationId root = Bytes<16>(0x01);
    EXPECT_TRUE(stores.topology_.BeginClusterCreate(root, 1).ok());
    meta::PutPolicy automatic;
    automatic.request_id_ = Bytes<16>(0x0f);
    automatic.policy_id_ =
        std::string(meta::kAutomaticUncontrolledFailoverPolicyId);
    automatic.version_ = 1;
    automatic.content_ =
        R"({"kind":"automatic-uncontrolled-failover-v1","suspect_after_ms":5000})";
    EXPECT_TRUE(stores.policy_.Apply(automatic).ok());
    meta::PutPolicy recovery;
    recovery.request_id_ = Bytes<16>(0x0e);
    recovery.policy_id_ = std::string(meta::kCandidateRecoveryPolicyId);
    recovery.version_ = 1;
    recovery.content_ = R"({"kind":"candidate-recovery-v1","budget_ms":2000})";
    EXPECT_TRUE(stores.policy_.Apply(recovery).ok());
    EXPECT_TRUE(stores.topology_.CompleteClusterCreate(root).ok());

    Register(owner, meta::MetaNodeRole::kPrimary, 6379, 0x02);
    Register(candidate, meta::MetaNodeRole::kReplica, 6380, 0x03);

    meta::CreateGroup group;
    group.request_id_ = Bytes<16>(0x04);
    group.group_id_ = "g1";
    group.new_topology_epoch_ = 1;
    Accept(group);

    meta::AssignNodeToGroup assign_owner;
    assign_owner.request_id_ = Bytes<16>(0x05);
    assign_owner.group_id_ = "g1";
    assign_owner.node_id_ = owner;
    assign_owner.assignment_id_ = owner_assignment;
    assign_owner.role_ = meta::MetaNodeRole::kPrimary;
    assign_owner.expected_revision_ = 1;
    assign_owner.new_topology_epoch_ = 2;
    Accept(assign_owner);

    meta::AssignNodeToGroup assign_candidate;
    assign_candidate.request_id_ = Bytes<16>(0x06);
    assign_candidate.group_id_ = "g1";
    assign_candidate.node_id_ = candidate;
    assign_candidate.assignment_id_ = candidate_assignment;
    assign_candidate.role_ = meta::MetaNodeRole::kReplica;
    assign_candidate.expected_revision_ = 2;
    assign_candidate.new_topology_epoch_ = 3;
    Accept(assign_candidate);

    meta::PutPolicy policy;
    policy.request_id_ = Bytes<16>(0x07);
    policy.policy_id_ = std::string(meta::kAuthorityLeasePolicyId);
    policy.version_ = 1;
    policy.content_ = R"({"kind":"authority-lease-v1","duration_ms":5000})";
    Accept(policy);

    meta::BeginGroupTerm term;
    term.request_id_ = Bytes<16>(0x08);
    term.group_id_ = "g1";
    term.expected_term_ = 0;
    term.new_term_ = 1;
    Accept(term);

    meta::ActivateAuthority activate;
    activate.request_id_ = Bytes<16>(0x09);
    activate.group_id_ = "g1";
    activate.expected_term_ = 1;
    activate.new_owner_ = owner;
    activate.new_topology_epoch_ = 4;
    Accept(activate);
  }

  template <typename Command>
  void Accept(const Command& command) {
    meta::MetaCommand wrapped{command};
    const auto result = meta::ApplyCommitted(stores, next_index++, wrapped,
                                             "lavik://test/failover-reconciler",
                                             "2026-09-15T00:00:00Z");
    ASSERT_EQ(result.verdict_, meta::MetaAuditVerdict::kAccepted)
        << result.detail_;
    committed_commands.push_back(std::move(wrapped));
  }

  void Register(const std::string& node_id, meta::MetaNodeRole role,
                std::uint16_t port, std::uint8_t request) {
    meta::RegisterNode node;
    node.request_id_ = Bytes<16>(request);
    node.node_id_ = node_id;
    node.principal_ = "lavik://node/" + node_id;
    node.endpoints_ = {"tcp://127.0.0.1:" + std::to_string(port)};
    node.role_ = role;
    Accept(node);
  }

  void SubmitControlled(std::uint64_t deadline = 10'000) {
    const auto intent = meta::EncodeFailoverOperationIntent({"g1", deadline});
    ASSERT_TRUE(intent.ok()) << intent.status();
    meta::SubmitOperation submit;
    submit.request_id_ = Bytes<16>(0x52);
    submit.operation_id_ = operation_id;
    submit.kind_ = std::string(meta::kFailoverOperationKind);
    submit.intent_ = *intent;
    submit.intent_hash_ = meta::MetaSha256(*intent);
    Accept(submit);
  }

  meta::MetaCandidateProgressObs CandidateProgress(
      std::string node_id, const meta::MetaAssignmentId& assignment,
      const meta::MetaBootIncarnation& boot,
      std::vector<std::uint64_t> frontier = {10, 20},
      std::optional<meta::MetaFailoverCompatibilityDomain> domain =
          std::nullopt,
      std::uint64_t session_generation = 1) const {
    const auto group = stores.topology_.FindGroup("g1");
    EXPECT_TRUE(group.has_value());
    const meta::MetaFailoverCompatibilityDomain source =
        domain.value_or(meta::MetaFailoverCompatibilityDomain{
            .source_group_term_ = 1,
            .source_node_id_ = owner,
            .source_assignment_id_ = owner_assignment,
            .source_boot_id_ = owner_boot,
            .source_history_id_ = source_history,
            .flow_count_ = 2});
    return {
        .node_id_ = std::move(node_id),
        .boot_incarnation_ = boot,
        .session_generation_ = session_generation,
        .group_id_ = "g1",
        .assignment_id_ = assignment,
        .group_term_ = group->record_.group_term_,
        .population_manifest_revision_ = 0,
        .population_manifest_digest_ = {},
        .partition_replication_epoch_ = 0,
        .replication_history_id_ = Bytes<20>(0x43),
        .source_group_term_ = source.source_group_term_,
        .source_node_id_ = source.source_node_id_,
        .source_assignment_id_ = source.source_assignment_id_,
        .source_boot_incarnation_ = source.source_boot_id_,
        .source_replication_history_id_ = source.source_history_id_,
        .applied_next_lsns_ = std::move(frontier),
        .storage_ready_ = true,
        .population_ready_ = true,
    };
  }

  void ReportCandidate(std::int64_t now = 1'000) {
    ReportCandidate(candidate, candidate_assignment, candidate_boot, now,
                    {10, 20});
  }

  void ReportCandidate(
      const std::string& node_id, const meta::MetaAssignmentId& assignment,
      const meta::MetaBootIncarnation& boot, std::int64_t now,
      std::vector<std::uint64_t> frontier,
      std::optional<meta::MetaFailoverObservationObs> failover = std::nullopt,
      std::optional<meta::MetaFailoverCompatibilityDomain> domain =
          std::nullopt,
      std::uint64_t generation = 1,
      std::optional<meta::MetaObservedFailoverProjection> failover_projection =
          std::nullopt) {
    meta::MetaStoresFacts facts(stores);
    const meta::MetaObservationIdentity identity{node_id, boot, generation};
    if (observations.CurrentGeneration(node_id) != std::optional(generation)) {
      ASSERT_TRUE(
          observations.AdoptSession(identity, now - 1, Bytes<20>(0x43)).ok());
    }
    const auto result = observations.ReplaceHeartbeat(
        identity,
        {.storage_ready_ = true,
         .population_ready_ = true,
         .active_groups_ = 1},
        CandidateProgress(node_id, assignment, boot, std::move(frontier),
                          domain, generation),
        std::move(failover), std::move(failover_projection), facts, now);
    ASSERT_TRUE(result.candidate_status_.ok()) << result.candidate_status_;
    ASSERT_TRUE(result.failover_status_.ok()) << result.failover_status_;
  }

  void ReportOwner(
      std::int64_t now, const meta::MetaBootIncarnation& boot = Bytes<20>(0x31),
      std::uint64_t generation = 1,
      std::optional<meta::MetaFailoverObservationObs> failover = std::nullopt,
      std::optional<meta::MetaReplicationHistoryId> session_history =
          std::nullopt) {
    meta::MetaStoresFacts facts(stores);
    const meta::MetaObservationIdentity identity{owner, boot, generation};
    if (observations.CurrentGeneration(owner) != std::optional(generation)) {
      ASSERT_TRUE(observations
                      .AdoptSession(identity, now - 1,
                                    session_history.value_or(source_history))
                      .ok());
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

  void AddAlternate() {
    Register(alternate, meta::MetaNodeRole::kReplica, 6381, 0x0a);
    const auto group = stores.topology_.FindGroup("g1");
    ASSERT_TRUE(group.has_value());
    meta::AssignNodeToGroup assign;
    assign.request_id_ = Bytes<16>(0x0b);
    assign.group_id_ = "g1";
    assign.node_id_ = alternate;
    assign.assignment_id_ = alternate_assignment;
    assign.role_ = meta::MetaNodeRole::kReplica;
    assign.expected_revision_ = group->revision_;
    assign.new_topology_epoch_ = stores.topology_.TopologyEpoch() + 1;
    Accept(assign);
  }

  meta::MetaFailoverTransition Transition() const {
    const auto group = stores.topology_.FindGroup("g1");
    EXPECT_TRUE(group.has_value());
    EXPECT_TRUE(group->failover_transition_.has_value());
    return *group->failover_transition_;
  }

  meta::MetaObservedFailoverProjection CurrentFailoverProjection() const {
    const auto group = stores.topology_.FindGroup("g1");
    EXPECT_TRUE(group.has_value());
    EXPECT_TRUE(group->failover_transition_.has_value());
    EXPECT_TRUE(group->failover_transition_->candidate_action_.has_value());
    const meta::MetaFailoverTransition& transition =
        *group->failover_transition_;
    const meta::MetaFailoverCandidateAction& action =
        *transition.candidate_action_;
    return {
        .group_id_ = group->group_id_,
        .group_term_ = group->record_.group_term_,
        .transition_id_ = transition.transition_id_,
        .transition_revision_ = transition.revision_,
        .action_id_ = action.action_id_,
        .candidate_node_id_ = action.candidate_.node_id_,
        .candidate_assignment_id_ = action.candidate_.assignment_id_,
        .candidate_boot_id_ = action.candidate_.boot_id_,
    };
  }

  void ReportSourcePaused(std::int64_t now,
                          std::vector<std::uint64_t> stable = {10, 20}) {
    const auto transition = Transition();
    const auto& domain = transition.candidate_action_->domain_;
    meta::MetaSourcePausedObs paused{
        .group_id_ = "g1",
        .transition_id_ = transition.transition_id_,
        .source_node_id_ = domain.source_node_id_,
        .source_assignment_id_ = domain.source_assignment_id_,
        .source_boot_id_ = domain.source_boot_id_,
        .source_history_id_ = domain.source_history_id_,
        .source_group_term_ = domain.source_group_term_,
        .stable_next_lsns_ = std::move(stable)};
    ReportOwner(now, domain.source_boot_id_, 1,
                meta::MetaFailoverObservationObs{.payload_ = paused});
  }

  void ReportPrepared(std::int64_t now,
                      std::vector<std::uint64_t> frontier = {10, 20},
                      std::uint64_t generation = 1) {
    const auto transition = Transition();
    const auto& action = *transition.candidate_action_;
    meta::MetaCandidatePreparedObs prepared{
        .group_id_ = "g1",
        .transition_id_ = transition.transition_id_,
        .action_id_ = action.action_id_,
        .candidate_node_id_ = action.candidate_.node_id_,
        .candidate_assignment_id_ = action.candidate_.assignment_id_,
        .candidate_boot_id_ = action.candidate_.boot_id_,
        .prepared_context_id_ = Bytes<16>(0x71)};
    ReportCandidate(action.candidate_.node_id_,
                    action.candidate_.assignment_id_,
                    action.candidate_.boot_id_, now, std::move(frontier),
                    meta::MetaFailoverObservationObs{.payload_ = prepared},
                    action.domain_, generation);
  }

  void CompleteRecovery(
      std::int64_t now,
      std::optional<std::vector<std::uint64_t>> frontier = std::nullopt) {
    auto transition = Transition();
    const auto action = *transition.candidate_action_;
    if (!transition.recovery_deadline_unix_ms_.has_value()) {
      meta::StartCandidateRecovery start;
      start.request_id_ = Bytes<16>(0xe0);
      start.group_id_ = "g1";
      start.expected_transition_ = {transition.transition_id_,
                                    transition.revision_};
      start.action_id_ = action.action_id_;
      start.recovery_deadline_unix_ms_ = 3000;
      Accept(start);
      transition = Transition();
    }
    if (!frontier.has_value()) {
      auto progress = observations.LiveCandidateProgressFor(
          "g1", meta::MetaStoresFacts(stores), now);
      auto current =
          std::ranges::find(progress, action.candidate_.node_id_,
                            &meta::MetaCandidateProgressObs::node_id_);
      ASSERT_NE(current, progress.end());
      frontier = current->applied_next_lsns_;
    }
    meta::MetaCandidateRecoveryCompleteObs complete{
        .group_id_ = "g1",
        .transition_id_ = transition.transition_id_,
        .action_id_ = action.action_id_,
        .candidate_node_id_ = action.candidate_.node_id_,
        .candidate_assignment_id_ = action.candidate_.assignment_id_,
        .candidate_boot_id_ = action.candidate_.boot_id_,
        .recovery_deadline_unix_ms_ = *transition.recovery_deadline_unix_ms_,
        .applied_next_lsns_ = *frontier,
        .completion_reason_ = "coverage-unavailable"};
    auto generation =
        observations.CurrentGeneration(action.candidate_.node_id_);
    ASSERT_TRUE(generation.has_value());
    ReportCandidate(action.candidate_.node_id_,
                    action.candidate_.assignment_id_,
                    action.candidate_.boot_id_, now, *frontier,
                    meta::MetaFailoverObservationObs{complete}, action.domain_,
                    *generation, CurrentFailoverProjection());
  }

  void ReportActionFailed(std::int64_t now) {
    const auto transition = Transition();
    const auto& action = *transition.candidate_action_;
    const auto group = stores.topology_.FindGroup("g1");
    meta::MetaActionFailedObs failed{
        .group_id_ = "g1",
        .transition_id_ = transition.transition_id_,
        .action_id_ = action.action_id_,
        .candidate_node_id_ = action.candidate_.node_id_,
        .candidate_assignment_id_ = action.candidate_.assignment_id_,
        .candidate_boot_id_ = action.candidate_.boot_id_,
        .population_manifest_revision_ =
            group->record_.population_manifest_revision_,
        .population_manifest_digest_ =
            group->record_.population_manifest_digest_,
        .partition_replication_epoch_ =
            group->record_.partition_replication_epoch_,
        .failure_class_ = "watchdog-timeout",
        .failure_detail_ = "candidate action failed"};
    ReportCandidate(action.candidate_.node_id_,
                    action.candidate_.assignment_id_,
                    action.candidate_.boot_id_, now, {10, 20},
                    meta::MetaFailoverObservationObs{.payload_ = failed},
                    action.domain_, 1, CurrentFailoverProjection());
  }
};

class IdSequence {
 public:
  explicit IdSequence(std::initializer_list<std::uint8_t> values)
      : values_(values) {}

  absl::StatusOr<meta::MetaRequestId> Next() {
    if (next_ == values_.size()) {
      return absl::ResourceExhaustedError("test id sequence exhausted");
    }
    return Bytes<16>(values_[next_++]);
  }

  std::size_t consumed() const { return next_; }

 private:
  std::vector<std::uint8_t> values_;
  std::size_t next_ = 0;
};

void BeginControlled(Fixture& fixture, std::uint8_t first_id = 0x80,
                     std::uint64_t deadline = 10'000) {
  fixture.SubmitControlled(deadline);
  fixture.ReportOwner(1'000);
  fixture.ReportCandidate(1'000);
  std::uint8_t next = first_id;
  const auto planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'001,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&]() -> absl::StatusOr<meta::MetaRequestId> {
         return Bytes<16>(next++);
       }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  ASSERT_NE(std::get_if<meta::BeginControlledFailover>(&**planned), nullptr);
  fixture.Accept(**planned);
}

void BeginUncontrolled(Fixture& fixture,
                       std::optional<meta::MetaFailoverCandidateAction>
                           candidate_action = std::nullopt,
                       std::uint8_t first_id = 0x90) {
  const auto group = fixture.stores.topology_.FindGroup("g1");
  const auto grant = fixture.stores.topology_.AuthorityFor("g1");
  ASSERT_TRUE(group.has_value());
  ASSERT_TRUE(grant.has_value());
  ASSERT_TRUE(grant->grant_.has_value());
  const auto owner = std::ranges::find(group->members_, group->record_.owner_,
                                       &meta::MetaGroupMember::node_id_);
  ASSERT_NE(owner, group->members_.end());
  meta::BeginUncontrolledFailover begin;
  begin.request_id_ = Bytes<16>(first_id);
  begin.group_id_ = "g1";
  begin.transition_id_ = Bytes<16>(static_cast<std::uint8_t>(first_id + 1));
  begin.target_term_ = group->record_.group_term_ + 1;
  begin.expected_owner_node_id_ = group->record_.owner_;
  begin.expected_owner_assignment_id_ = owner->assignment_id_;
  begin.expected_membership_revision_ = group->revision_;
  begin.expected_group_term_ = group->record_.group_term_;
  begin.expected_population_manifest_revision_ =
      group->record_.population_manifest_revision_;
  begin.expected_population_manifest_digest_ =
      group->record_.population_manifest_digest_;
  begin.expected_partition_replication_epoch_ =
      group->record_.partition_replication_epoch_;
  begin.candidate_action_ = std::move(candidate_action);
  fixture.Accept(begin);
}

TEST(MetaFailoverReconcilerPlannerTest,
     RecoveryStartsAfterExclusionAndPrepareWaitsForDrain) {
  Fixture fixture;
  BeginUncontrolled(fixture);
  fixture.ReportCandidate(1000);
  std::uint8_t id = 0xc0;
  bool excluded = false;
  auto plan = [&](std::int64_t now) {
    return meta::PlanFailoverStep(
        meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
        fixture.observations,
        {.now_unix_ms_ = now,
         .leadership_started_unix_ms_ = 900,
         .observation_grace_ms_ = 100,
         .next_id_ = [&]() -> absl::StatusOr<meta::MetaRequestId> {
           return Bytes<16>(id++);
         },
         .authority_excluded_ =
             [&](std::string_view group, std::uint64_t term) {
               EXPECT_EQ(group, "g1");
               EXPECT_EQ(term, 2);
               return excluded;
             }});
  };
  auto selected = plan(1001);
  ASSERT_TRUE(selected.ok());
  ASSERT_TRUE(selected->has_value());
  ASSERT_NE(std::get_if<meta::SetUncontrolledCandidate>(&**selected), nullptr);
  fixture.Accept(**selected);
  auto pending = plan(1002);
  ASSERT_TRUE(pending.ok());
  EXPECT_FALSE(pending->has_value());
  EXPECT_FALSE(fixture.Transition().recovery_deadline_unix_ms_.has_value());
  excluded = true;
  auto started = plan(1003);
  ASSERT_TRUE(started.ok());
  ASSERT_TRUE(started->has_value());
  const auto* start = std::get_if<meta::StartCandidateRecovery>(&**started);
  ASSERT_NE(start, nullptr);
  EXPECT_EQ(start->recovery_deadline_unix_ms_, 3003);
  fixture.Accept(**started);
  fixture.ReportCandidate(3010);
  auto expired = plan(3011);
  ASSERT_TRUE(expired.ok());
  EXPECT_FALSE(
      expired->has_value());  // timeout never stands in for a safe drain
  fixture.CompleteRecovery(3012);
  auto authorized = plan(3013);
  ASSERT_TRUE(authorized.ok());
  ASSERT_TRUE(authorized->has_value());
  const auto* authorize =
      std::get_if<meta::AuthorizeFailoverPrepare>(&**authorized);
  ASSERT_NE(authorize, nullptr);
  EXPECT_EQ(authorize->loss_if_cutover_, meta::MetaFailoverLoss::kUnknown);
  EXPECT_TRUE(
      meta::ValidateFailoverProposal(
          **authorized,
          meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
          fixture.observations, 3013)
          .ok());
  // A changed role update withdraws the terminal report before append.
  fixture.ReportCandidate(3014);
  EXPECT_FALSE(
      meta::ValidateFailoverProposal(
          **authorized,
          meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
          fixture.observations, 3014)
          .ok());
}

TEST(MetaFailoverReconcilerPlannerTest,
     ZeroBudgetStillRequiresRecoveryCompletionAndOverflowIsRejected) {
  Fixture fixture;
  BeginUncontrolled(fixture);
  fixture.ReportCandidate(1000);
  meta::PutPolicy recovery;
  recovery.request_id_ = Bytes<16>(0xe1);
  recovery.policy_id_ = std::string(meta::kCandidateRecoveryPolicyId);
  recovery.version_ = 2;
  recovery.content_ = R"({"kind":"candidate-recovery-v1","budget_ms":0})";
  fixture.Accept(recovery);
  std::uint8_t id = 0xc0;
  auto plan = [&](std::int64_t now) {
    return meta::PlanFailoverStep(
        meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
        fixture.observations,
        {.now_unix_ms_ = now,
         .leadership_started_unix_ms_ = 900,
         .observation_grace_ms_ = 100,
         .next_id_ = [&]() -> absl::StatusOr<meta::MetaRequestId> {
           return Bytes<16>(id++);
         },
         .authority_excluded_ = [](std::string_view,
                                   std::uint64_t) { return true; }});
  };
  auto selected = plan(1001);
  ASSERT_TRUE(selected.ok());
  ASSERT_TRUE(selected->has_value());
  fixture.Accept(**selected);
  auto started = plan(1002);
  ASSERT_TRUE(started.ok());
  ASSERT_TRUE(started->has_value());
  const auto* start = std::get_if<meta::StartCandidateRecovery>(&**started);
  ASSERT_NE(start, nullptr);
  EXPECT_EQ(start->recovery_deadline_unix_ms_, 1002);

  recovery.version_ = 3;
  recovery.content_ = R"({"kind":"candidate-recovery-v1","budget_ms":2000})";
  fixture.Accept(recovery);
  constexpr auto late = std::numeric_limits<std::int64_t>::max() - 1000;
  fixture.ReportCandidate(late);
  EXPECT_FALSE(plan(late).ok());
  // A policy change after proposal cannot alter the carried cutoff at apply.
  fixture.Accept(**started);
  EXPECT_EQ(fixture.Transition().recovery_deadline_unix_ms_, 1002);
  auto pending = plan(late);
  ASSERT_TRUE(pending.ok());
  EXPECT_FALSE(pending->has_value());
  fixture.CompleteRecovery(late);
  auto authorized = plan(late);
  ASSERT_TRUE(authorized.ok());
  ASSERT_TRUE(authorized->has_value());
  EXPECT_NE(std::get_if<meta::AuthorizeFailoverPrepare>(&**authorized),
            nullptr);
}

TEST(MetaFailoverReconcilerPlannerTest,
     SubmittedControlledOperationBeginsWithExactCommittedAnchors) {
  Fixture fixture;
  fixture.SubmitControlled();
  fixture.ReportOwner(1'000);
  fixture.ReportCandidate();
  IdSequence ids{0xa0, 0xa1, 0xa2};

  const auto planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'001,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return ids.Next(); }});

  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  const auto* command = std::get_if<meta::BeginControlledFailover>(&**planned);
  ASSERT_NE(command, nullptr);
  EXPECT_EQ(command->request_id_, Bytes<16>(0xa0));
  EXPECT_EQ(command->transition_id_, Bytes<16>(0xa1));
  EXPECT_EQ(command->candidate_action_.action_id_, Bytes<16>(0xa2));
  EXPECT_EQ(command->group_id_, "g1");
  EXPECT_EQ(command->operation_id_, fixture.operation_id);
  EXPECT_EQ(command->expected_operation_revision_, 0);
  EXPECT_EQ(command->target_term_, 2);
  EXPECT_EQ(command->expected_owner_node_id_, fixture.owner);
  EXPECT_EQ(command->expected_owner_assignment_id_, fixture.owner_assignment);
  EXPECT_EQ(command->expected_membership_revision_, 3);
  EXPECT_EQ(command->expected_group_term_, 1);
  EXPECT_EQ(command->candidate_action_.candidate_.node_id_, fixture.candidate);
  EXPECT_EQ(command->candidate_action_.candidate_.assignment_id_,
            fixture.candidate_assignment);
  EXPECT_EQ(command->candidate_action_.candidate_.boot_id_,
            fixture.candidate_boot);
  EXPECT_EQ(command->candidate_action_.domain_.source_node_id_, fixture.owner);
  EXPECT_EQ(command->candidate_action_.domain_.source_boot_id_,
            fixture.owner_boot);
  EXPECT_EQ(ids.consumed(), 3);
}

TEST(MetaFailoverReconcilerPlannerTest,
     CandidateReconnectBeforeControlledActionDoesNotPoisonNewAction) {
  Fixture fixture;
  fixture.SubmitControlled();
  fixture.ReportOwner(1'000);
  fixture.ReportCandidate(1'000);
  fixture.observations.InvalidateCandidateOnDisconnect(
      {fixture.candidate, fixture.candidate_boot, 1}, 1'001);
  fixture.ReportCandidate(fixture.candidate, fixture.candidate_assignment,
                          fixture.candidate_boot, 1'002, {10, 20}, std::nullopt,
                          std::nullopt, 2);

  IdSequence begin_ids{0xa3, 0xa4, 0xa5};
  auto planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'003,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return begin_ids.Next(); }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  ASSERT_NE(std::get_if<meta::BeginControlledFailover>(&**planned), nullptr);
  fixture.Accept(**planned);

  fixture.ReportSourcePaused(1'004);
  IdSequence authorize_ids{0xa6};
  planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'005,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return authorize_ids.Next(); }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  EXPECT_NE(std::get_if<meta::AuthorizeFailoverPrepare>(&**planned), nullptr);
}

TEST(MetaFailoverReconcilerPlannerTest,
     ControlledCandidateDisconnectAbortsWhileSourceIsHealthy) {
  Fixture fixture;
  fixture.SubmitControlled();
  fixture.ReportOwner(1'000);
  fixture.ReportCandidate();
  IdSequence begin_ids{0xb0, 0xb1, 0xb2};
  auto begin = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'001,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return begin_ids.Next(); }});
  ASSERT_TRUE(begin.ok()) << begin.status();
  ASSERT_TRUE(begin->has_value());
  fixture.Accept(**begin);
  const auto transition =
      fixture.stores.topology_.FindGroup("g1")->failover_transition_.value();

  fixture.ReportOwner(1'010);
  fixture.observations.InvalidateCandidateOnDisconnect(
      {fixture.candidate, fixture.candidate_boot, 1}, 1'020);
  IdSequence abort_ids{0xb3};
  const auto planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'020,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return abort_ids.Next(); }});

  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  const auto* command = std::get_if<meta::AbortControlledFailover>(&**planned);
  ASSERT_NE(command, nullptr);
  EXPECT_EQ(command->request_id_, Bytes<16>(0xb3));
  EXPECT_EQ(command->operation_id_, fixture.operation_id);
  EXPECT_EQ(command->expected_operation_revision_, 0);
  EXPECT_EQ(command->group_id_, "g1");
  EXPECT_EQ(command->expected_transition_,
            (meta::MetaFailoverTransitionRef{transition.transition_id_,
                                             transition.revision_}));
  EXPECT_NE(command->reason_.find("candidate"), std::string::npos);
  EXPECT_EQ(abort_ids.consumed(), 1);
}

TEST(MetaFailoverReconcilerPlannerTest,
     ControlledCandidateDisconnectAbortsWhileSourceIsInGrace) {
  Fixture fixture;
  BeginControlled(fixture);
  const auto transition = fixture.Transition();

  fixture.observations.InvalidateCandidateOnDisconnect(
      {fixture.owner, fixture.owner_boot, 1}, 1'010);
  fixture.observations.InvalidateCandidateOnDisconnect(
      {fixture.candidate, fixture.candidate_boot, 1}, 1'020);

  IdSequence ids{0xb3};
  const auto planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'050,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return ids.Next(); }});

  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  const auto* abort = std::get_if<meta::AbortControlledFailover>(&**planned);
  ASSERT_NE(abort, nullptr);
  EXPECT_EQ(abort->expected_transition_,
            (meta::MetaFailoverTransitionRef{transition.transition_id_,
                                             transition.revision_}));
  EXPECT_NE(abort->reason_.find("candidate"), std::string::npos);
  EXPECT_EQ(ids.consumed(), 1);
}

TEST(MetaFailoverReconcilerPlannerTest,
     ControlledCandidateNewBootAbortsWithoutWaitingForDisconnectGrace) {
  Fixture fixture;
  BeginControlled(fixture);
  const auto transition = fixture.Transition();
  const auto replacement_boot = Bytes<20>(0x34);

  fixture.ReportOwner(1'010);
  fixture.ReportCandidate(fixture.candidate, fixture.candidate_assignment,
                          replacement_boot, 1'011, {10, 20}, std::nullopt,
                          std::nullopt, 2);

  IdSequence ids{0xb3};
  const auto planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'012,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return ids.Next(); }});

  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  const auto* abort = std::get_if<meta::AbortControlledFailover>(&**planned);
  ASSERT_NE(abort, nullptr);
  EXPECT_EQ(abort->expected_transition_,
            (meta::MetaFailoverTransitionRef{transition.transition_id_,
                                             transition.revision_}));
  EXPECT_NE(abort->reason_.find("candidate"), std::string::npos);
  EXPECT_EQ(ids.consumed(), 1);
}

TEST(MetaFailoverReconcilerPlannerTest,
     ControlledCandidateNewBootAbortsWhileSourceIsInGrace) {
  Fixture fixture;
  BeginControlled(fixture);
  const auto transition = fixture.Transition();

  fixture.observations.InvalidateCandidateOnDisconnect(
      {fixture.owner, fixture.owner_boot, 1}, 1'010);
  fixture.ReportCandidate(fixture.candidate, fixture.candidate_assignment,
                          Bytes<20>(0x34), 1'020, {10, 20}, std::nullopt,
                          std::nullopt, 2);

  IdSequence ids{0xb3};
  const auto planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'050,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return ids.Next(); }});

  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  const auto* abort = std::get_if<meta::AbortControlledFailover>(&**planned);
  ASSERT_NE(abort, nullptr);
  EXPECT_EQ(abort->expected_transition_,
            (meta::MetaFailoverTransitionRef{transition.transition_id_,
                                             transition.revision_}));
  EXPECT_NE(abort->reason_.find("candidate"), std::string::npos);
  EXPECT_EQ(ids.consumed(), 1);
}

TEST(MetaFailoverReconcilerPlannerTest,
     ControlledCandidateActionFailureAbortsWhileSourceIsInGrace) {
  Fixture fixture;
  BeginControlled(fixture);
  const auto transition = fixture.Transition();

  fixture.observations.InvalidateCandidateOnDisconnect(
      {fixture.owner, fixture.owner_boot, 1}, 1'010);
  fixture.ReportActionFailed(1'020);

  IdSequence ids{0xb3};
  const auto planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'050,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return ids.Next(); }});

  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  const auto* abort = std::get_if<meta::AbortControlledFailover>(&**planned);
  ASSERT_NE(abort, nullptr);
  EXPECT_EQ(abort->expected_transition_,
            (meta::MetaFailoverTransitionRef{transition.transition_id_,
                                             transition.revision_}));
  EXPECT_NE(abort->reason_.find("candidate"), std::string::npos);
  EXPECT_EQ(ids.consumed(), 1);
}

TEST(MetaFailoverReconcilerPlannerTest,
     ControlledSourceFailureDegradesEvenWhenCandidateAlsoFails) {
  Fixture fixture;
  BeginControlled(fixture);
  const auto transition = fixture.Transition();

  fixture.observations.InvalidateCandidateOnDisconnect(
      {fixture.owner, fixture.owner_boot, 1}, 1'010);
  fixture.observations.InvalidateCandidateOnDisconnect(
      {fixture.candidate, fixture.candidate_boot, 1}, 1'020);

  IdSequence ids{0xb3};
  const auto planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'110,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return ids.Next(); }});

  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  const auto* degrade =
      std::get_if<meta::DegradeControlledFailover>(&**planned);
  ASSERT_NE(degrade, nullptr);
  EXPECT_EQ(degrade->expected_transition_,
            (meta::MetaFailoverTransitionRef{transition.transition_id_,
                                             transition.revision_}));
  EXPECT_FALSE(degrade->retain_candidate_action_);
  EXPECT_NE(degrade->reason_.find("source"), std::string::npos);
  EXPECT_EQ(ids.consumed(), 1);
}

TEST(MetaFailoverReconcilerPlannerTest,
     SameBootReconnectWithOnlyGenericProgressDoesNotReviveCandidate) {
  Fixture fixture;
  BeginControlled(fixture);
  const auto transition = fixture.Transition();

  fixture.observations.InvalidateCandidateOnDisconnect(
      {fixture.candidate, fixture.candidate_boot, 1}, 1'010);
  fixture.ReportCandidate(fixture.candidate, fixture.candidate_assignment,
                          fixture.candidate_boot, 1'011, {10, 20}, std::nullopt,
                          std::nullopt, 2);

  IdSequence ids{0xb3};
  const auto planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'012,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return ids.Next(); }});

  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  const auto* abort = std::get_if<meta::AbortControlledFailover>(&**planned);
  ASSERT_NE(abort, nullptr);
  EXPECT_EQ(abort->expected_transition_,
            (meta::MetaFailoverTransitionRef{transition.transition_id_,
                                             transition.revision_}));
  EXPECT_NE(abort->reason_.find("candidate"), std::string::npos);
}

TEST(MetaFailoverReconcilerPlannerTest,
     MatchingPreparedFromNewSessionDoesNotReviveDisconnectedAction) {
  Fixture fixture;
  BeginControlled(fixture);
  fixture.ReportSourcePaused(1'010);

  IdSequence authorize_ids{0xb4};
  auto planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'011,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return authorize_ids.Next(); }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  ASSERT_NE(std::get_if<meta::AuthorizeFailoverPrepare>(&**planned), nullptr);
  fixture.Accept(**planned);
  const auto authorized = fixture.Transition();

  fixture.observations.InvalidateCandidateOnDisconnect(
      {fixture.candidate, fixture.candidate_boot, 1}, 1'012);
  fixture.ReportPrepared(1'013, {10, 20}, 2);

  IdSequence abort_ids{0xb5};
  planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'014,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return abort_ids.Next(); }});

  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  const auto* abort = std::get_if<meta::AbortControlledFailover>(&**planned);
  ASSERT_NE(abort, nullptr);
  EXPECT_EQ(abort->expected_transition_,
            (meta::MetaFailoverTransitionRef{authorized.transition_id_,
                                             authorized.revision_}));
  EXPECT_NE(abort->reason_.find("candidate"), std::string::npos);
}

TEST(MetaFailoverReconcilerPlannerTest,
     ControlledObservationsAuthorizeThenPreparedCommitsExactSuccessor) {
  Fixture fixture;
  BeginControlled(fixture);
  fixture.ReportSourcePaused(1'010, {11, 19});
  fixture.ReportCandidate(fixture.candidate, fixture.candidate_assignment,
                          fixture.candidate_boot, 1'011, {11, 20});

  IdSequence authorize_ids{0x83};
  auto planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'012,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return authorize_ids.Next(); }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  const auto* authorize =
      std::get_if<meta::AuthorizeFailoverPrepare>(&**planned);
  ASSERT_NE(authorize, nullptr);
  EXPECT_EQ(authorize->loss_if_cutover_, meta::MetaFailoverLoss::kNone);
  EXPECT_EQ(
      authorize->expected_transition_,
      (meta::MetaFailoverTransitionRef{fixture.Transition().transition_id_,
                                       fixture.Transition().revision_}));
  fixture.Accept(**planned);

  const auto authorized = fixture.Transition();
  ASSERT_TRUE(authorized.candidate_action_->authorization_.has_value());
  EXPECT_EQ(authorized.candidate_action_->authorization_->loss_if_cutover_,
            meta::MetaFailoverLoss::kNone);
  fixture.ReportPrepared(1'013, {11, 20});

  IdSequence commit_ids{0x84};
  planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'014,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return commit_ids.Next(); }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  const auto* commit = std::get_if<meta::CommitControlledFailover>(&**planned);
  ASSERT_NE(commit, nullptr);
  EXPECT_EQ(commit->action_id_, authorized.candidate_action_->action_id_);
  EXPECT_EQ(commit->authorized_revision_,
            authorized.candidate_action_->authorization_->authorized_revision_);
  EXPECT_EQ(commit->new_topology_epoch_, 5);
  fixture.Accept(**planned);

  const auto group = fixture.stores.topology_.FindGroup("g1");
  ASSERT_TRUE(group.has_value());
  EXPECT_EQ(group->record_.owner_, fixture.candidate);
  EXPECT_EQ(group->record_.group_term_, 2);
  EXPECT_FALSE(group->failover_transition_.has_value());
  const auto operation =
      fixture.stores.operation_.FindOperation(fixture.operation_id);
  ASSERT_TRUE(operation.has_value());
  EXPECT_EQ(operation->lifecycle_, meta::MetaOperationLifecycle::kCompleted);
}

TEST(MetaFailoverReconcilerPlannerTest,
     AuthorizedControlledActionWaitsThroughExactNoRoleHeartbeatUntilFailure) {
  Fixture fixture;
  BeginControlled(fixture);
  fixture.ReportSourcePaused(1'010);

  IdSequence authorize_ids{0xb6};
  auto planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'011,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return authorize_ids.Next(); }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  ASSERT_NE(std::get_if<meta::AuthorizeFailoverPrepare>(&**planned), nullptr);
  fixture.Accept(**planned);
  const auto authorized = fixture.Transition();

  const meta::MetaObservationIdentity identity{fixture.candidate,
                                               fixture.candidate_boot, 1};
  const auto preparing = fixture.observations.ReplaceHeartbeat(
      identity,
      {.storage_ready_ = true, .population_ready_ = true, .active_groups_ = 1},
      std::nullopt, std::nullopt, fixture.CurrentFailoverProjection(),
      meta::MetaStoresFacts(fixture.stores), 1'012);
  ASSERT_TRUE(preparing.boot_status_.ok()) << preparing.boot_status_;
  ASSERT_TRUE(preparing.health_status_.ok()) << preparing.health_status_;
  ASSERT_TRUE(preparing.candidate_status_.ok()) << preparing.candidate_status_;
  ASSERT_TRUE(preparing.failover_status_.ok()) << preparing.failover_status_;

  IdSequence wait_ids{0xb7};
  planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'013,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return wait_ids.Next(); }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  EXPECT_FALSE(planned->has_value());
  EXPECT_EQ(wait_ids.consumed(), 0);

  fixture.ReportActionFailed(1'014);
  IdSequence abort_ids{0xb8};
  planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'015,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return abort_ids.Next(); }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  const auto* abort = std::get_if<meta::AbortControlledFailover>(&**planned);
  ASSERT_NE(abort, nullptr);
  EXPECT_EQ(abort->expected_transition_,
            (meta::MetaFailoverTransitionRef{authorized.transition_id_,
                                             authorized.revision_}));
  EXPECT_NE(abort->reason_.find("candidate"), std::string::npos);
}

TEST(MetaFailoverReconcilerPlannerTest,
     ControlledDeadlineWinsOverSimultaneousPreparedCommit) {
  Fixture fixture;
  BeginControlled(fixture, 0xb4, 1'014);
  fixture.ReportSourcePaused(1'010);
  IdSequence authorize_ids{0xb7};
  auto planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'011,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return authorize_ids.Next(); }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  fixture.Accept(**planned);
  fixture.ReportPrepared(1'013);

  IdSequence deadline_ids{0xb8};
  planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'014,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return deadline_ids.Next(); }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  const auto* abort = std::get_if<meta::AbortControlledFailover>(&**planned);
  ASSERT_NE(abort, nullptr);
  EXPECT_TRUE(abort->expected_transition_.has_value());
  EXPECT_NE(abort->reason_.find("deadline"), std::string::npos);
}

TEST(MetaFailoverReconcilerPlannerTest,
     ControlledSourceDisconnectWaitsGraceThenDegradesAndClearsCandidate) {
  Fixture fixture;
  BeginControlled(fixture);
  fixture.observations.InvalidateCandidateOnDisconnect(
      {fixture.owner, fixture.owner_boot, 1}, 1'010);

  IdSequence wait_ids{0x85};
  auto planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'050,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return wait_ids.Next(); }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  EXPECT_FALSE(planned->has_value());
  EXPECT_EQ(wait_ids.consumed(), 0);

  IdSequence degrade_ids{0x86};
  planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'110,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return degrade_ids.Next(); }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  const auto* degrade =
      std::get_if<meta::DegradeControlledFailover>(&**planned);
  ASSERT_NE(degrade, nullptr);
  EXPECT_FALSE(degrade->retain_candidate_action_);
  EXPECT_EQ(degrade->expected_candidate_action_,
            fixture.Transition().candidate_action_);
  fixture.Accept(**planned);

  const auto transition = fixture.Transition();
  EXPECT_EQ(transition.mode_, meta::MetaFailoverMode::kUncontrolled);
  EXPECT_FALSE(transition.candidate_action_.has_value());
  const auto grant = fixture.stores.topology_.AuthorityFor("g1");
  ASSERT_TRUE(grant.has_value());
  EXPECT_FALSE(grant->grant_.has_value());
  const auto operation =
      fixture.stores.operation_.FindOperation(fixture.operation_id);
  ASSERT_TRUE(operation.has_value());
  EXPECT_EQ(operation->lifecycle_, meta::MetaOperationLifecycle::kAborted);
}

TEST(MetaFailoverReconcilerPlannerTest,
     ExactSourceReconnectWaitsForFreshHeartbeatUntilDisconnectGraceExpires) {
  Fixture fixture;
  BeginControlled(fixture);
  fixture.observations.InvalidateCandidateOnDisconnect(
      {fixture.owner, fixture.owner_boot, 1}, 1'010);
  ASSERT_TRUE(fixture.observations
                  .AdoptSession({fixture.owner, fixture.owner_boot, 2}, 1'020,
                                fixture.source_history)
                  .ok());

  IdSequence wait_ids{0x87};
  auto planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'050,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return wait_ids.Next(); }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  EXPECT_FALSE(planned->has_value());
  EXPECT_EQ(wait_ids.consumed(), 0);

  IdSequence degrade_ids{0x88};
  planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'110,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return degrade_ids.Next(); }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  ASSERT_NE(std::get_if<meta::DegradeControlledFailover>(&**planned), nullptr);
}

TEST(MetaFailoverReconcilerPlannerTest,
     ExactSourceReconnectBecomesHealthyOnlyAfterCurrentHeartbeat) {
  Fixture fixture;
  BeginControlled(fixture);
  fixture.observations.InvalidateCandidateOnDisconnect(
      {fixture.owner, fixture.owner_boot, 1}, 1'010);
  ASSERT_TRUE(fixture.observations
                  .AdoptSession({fixture.owner, fixture.owner_boot, 2}, 1'020,
                                fixture.source_history)
                  .ok());
  fixture.ReportOwner(1'030, fixture.owner_boot, 2);

  IdSequence ids{0x89};
  const auto planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'050,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return ids.Next(); }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  EXPECT_FALSE(planned->has_value());
  EXPECT_EQ(ids.consumed(), 0);
}

TEST(MetaFailoverReconcilerPlannerTest,
     ControlledSourceNewBootDegradesWithoutWaitingForDisconnectGrace) {
  Fixture fixture;
  BeginControlled(fixture);
  const auto transition = fixture.Transition();
  const auto replacement_boot = Bytes<20>(0x35);

  fixture.ReportOwner(1'010, replacement_boot, 2);

  IdSequence ids{0x8a};
  const auto planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'011,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return ids.Next(); }});

  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  const auto* degrade =
      std::get_if<meta::DegradeControlledFailover>(&**planned);
  ASSERT_NE(degrade, nullptr);
  EXPECT_EQ(degrade->expected_transition_,
            (meta::MetaFailoverTransitionRef{transition.transition_id_,
                                             transition.revision_}));
  EXPECT_EQ(degrade->expected_candidate_action_, transition.candidate_action_);
  EXPECT_FALSE(degrade->retain_candidate_action_);
  EXPECT_NE(degrade->reason_.find("replaced"), std::string::npos);
  EXPECT_EQ(ids.consumed(), 1);
}

TEST(MetaFailoverReconcilerPlannerTest,
     ControlledSourceHistoryReplacementDegradesAndRetainsLosslessAction) {
  Fixture fixture;
  BeginControlled(fixture);
  fixture.ReportSourcePaused(1'010, {10, 20});
  IdSequence authorize_ids{0x8c};
  auto planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'011,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return authorize_ids.Next(); }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  ASSERT_NE(std::get_if<meta::AuthorizeFailoverPrepare>(&**planned), nullptr);
  fixture.Accept(**planned);
  const auto authorized = fixture.Transition().candidate_action_;

  fixture.ReportOwner(1'012, fixture.owner_boot, 2, std::nullopt,
                      Bytes<20>(0x7a));
  IdSequence degrade_ids{0x8d};
  planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'013,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return degrade_ids.Next(); }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  const auto* degrade =
      std::get_if<meta::DegradeControlledFailover>(&**planned);
  ASSERT_NE(degrade, nullptr);
  EXPECT_TRUE(degrade->retain_candidate_action_);
  EXPECT_NE(degrade->reason_.find("replaced"), std::string::npos);
  fixture.Accept(**planned);

  const auto degraded = fixture.Transition();
  EXPECT_EQ(degraded.mode_, meta::MetaFailoverMode::kUncontrolled);
  EXPECT_EQ(degraded.candidate_action_, authorized);
  ASSERT_TRUE(degraded.candidate_action_->authorization_.has_value());
  EXPECT_EQ(degraded.candidate_action_->authorization_->loss_if_cutover_,
            meta::MetaFailoverLoss::kNone);
}

TEST(MetaFailoverReconcilerPlannerTest,
     NewLeaderRebuildsFromCommittedTransitionAfterObservationWarmup) {
  Fixture fixture;
  BeginControlled(fixture);
  meta::MetaObservationStore fresh_observations;

  IdSequence warmup_ids{0x8e};
  auto planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fresh_observations,
      {.now_unix_ms_ = 1'050,
       .leadership_started_unix_ms_ = 1'000,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return warmup_ids.Next(); }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  EXPECT_FALSE(planned->has_value());
  EXPECT_EQ(warmup_ids.consumed(), 0);

  IdSequence degrade_ids{0x8f};
  planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fresh_observations,
      {.now_unix_ms_ = 1'100,
       .leadership_started_unix_ms_ = 1'000,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return degrade_ids.Next(); }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  const auto* degrade =
      std::get_if<meta::DegradeControlledFailover>(&**planned);
  ASSERT_NE(degrade, nullptr);
  EXPECT_FALSE(degrade->retain_candidate_action_);
}

TEST(MetaFailoverReconcilerPlannerTest,
     SubmittedControlledFailuresUseTypedPreBeginAbort) {
  {
    Fixture fixture;
    fixture.SubmitControlled();
    fixture.ReportOwner(1'000);
    IdSequence ids{0x87};
    const auto planned = meta::PlanFailoverStep(
        meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
        fixture.observations,
        {.now_unix_ms_ = 1'001,
         .leadership_started_unix_ms_ = 900,
         .observation_grace_ms_ = 100,
         .next_id_ = [&] { return ids.Next(); }});
    ASSERT_TRUE(planned.ok()) << planned.status();
    ASSERT_TRUE(planned->has_value());
    const auto* abort = std::get_if<meta::AbortControlledFailover>(&**planned);
    ASSERT_NE(abort, nullptr);
    EXPECT_FALSE(abort->expected_transition_.has_value());
    EXPECT_NE(abort->reason_.find("candidate"), std::string::npos);
  }

  {
    Fixture fixture;
    fixture.SubmitControlled(950);
    IdSequence ids{0x88};
    const auto planned = meta::PlanFailoverStep(
        meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
        fixture.observations,
        {.now_unix_ms_ = 950,
         .leadership_started_unix_ms_ = 900,
         .observation_grace_ms_ = 100,
         .next_id_ = [&] { return ids.Next(); }});
    ASSERT_TRUE(planned.ok()) << planned.status();
    ASSERT_TRUE(planned->has_value());
    const auto* abort = std::get_if<meta::AbortControlledFailover>(&**planned);
    ASSERT_NE(abort, nullptr);
    EXPECT_FALSE(abort->expected_transition_.has_value());
    EXPECT_NE(abort->reason_.find("deadline"), std::string::npos);
  }

  {
    Fixture fixture;
    fixture.SubmitControlled();
    BeginUncontrolled(fixture);
    fixture.ReportCandidate(1'000);
    IdSequence ids{0x89};
    const auto planned = meta::PlanFailoverStep(
        meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
        fixture.observations,
        {.now_unix_ms_ = 1'001,
         .leadership_started_unix_ms_ = 900,
         .observation_grace_ms_ = 100,
         .next_id_ = [&] { return ids.Next(); }});
    ASSERT_TRUE(planned.ok()) << planned.status();
    ASSERT_TRUE(planned->has_value());
    const auto* abort = std::get_if<meta::AbortControlledFailover>(&**planned);
    ASSERT_NE(abort, nullptr);
    EXPECT_FALSE(abort->expected_transition_.has_value());
    EXPECT_NE(abort->reason_.find("transition"), std::string::npos);
  }

  {
    Fixture fixture;
    fixture.SubmitControlled();
    meta::FenceGroup fence;
    fence.request_id_ = Bytes<16>(0x8a);
    fence.group_id_ = "g1";
    fence.expected_term_ = 1;
    fence.new_term_ = 2;
    fixture.Accept(fence);
    IdSequence ids{0x8b};
    const auto planned = meta::PlanFailoverStep(
        meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
        fixture.observations,
        {.now_unix_ms_ = 1'001,
         .leadership_started_unix_ms_ = 900,
         .observation_grace_ms_ = 100,
         .next_id_ = [&] { return ids.Next(); }});
    ASSERT_TRUE(planned.ok()) << planned.status();
    ASSERT_TRUE(planned->has_value());
    const auto* abort = std::get_if<meta::AbortControlledFailover>(&**planned);
    ASSERT_NE(abort, nullptr);
    EXPECT_FALSE(abort->expected_transition_.has_value());
    EXPECT_NE(abort->reason_.find("grant"), std::string::npos);
  }
}

TEST(MetaFailoverReconcilerPlannerTest,
     CandidateReconnectBeforeUncontrolledSetDoesNotPoisonNewAction) {
  Fixture fixture;
  BeginUncontrolled(fixture);
  fixture.ReportCandidate(fixture.candidate, fixture.candidate_assignment,
                          fixture.candidate_boot, 1'000, {10, 20});
  fixture.observations.InvalidateCandidateOnDisconnect(
      {fixture.candidate, fixture.candidate_boot, 1}, 1'001);
  fixture.ReportCandidate(fixture.candidate, fixture.candidate_assignment,
                          fixture.candidate_boot, 1'002, {10, 20}, std::nullopt,
                          std::nullopt, 2);

  IdSequence set_ids{0x9a, 0x9b};
  auto planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'003,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return set_ids.Next(); }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  ASSERT_NE(std::get_if<meta::SetUncontrolledCandidate>(&**planned), nullptr);
  fixture.Accept(**planned);

  fixture.CompleteRecovery(1003);
  IdSequence authorize_ids{0x9c};
  planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'004,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return authorize_ids.Next(); }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  EXPECT_NE(std::get_if<meta::AuthorizeFailoverPrepare>(&**planned), nullptr);
}

TEST(MetaFailoverReconcilerPlannerTest,
     BeginUncontrolledRetainsPreselectedCandidateUntilTargetTermHeartbeat) {
  Fixture fixture;
  const meta::MetaCandidateProgressObs progress =
      fixture.CandidateProgress(fixture.candidate, fixture.candidate_assignment,
                                fixture.candidate_boot, {20, 20});
  fixture.ReportCandidate(fixture.candidate, fixture.candidate_assignment,
                          fixture.candidate_boot, 1'000, {20, 20});
  const meta::MetaFailoverCandidateAction action{
      .action_id_ = Bytes<16>(0x9d),
      .candidate_ =
          {
              .node_id_ = fixture.candidate,
              .assignment_id_ = fixture.candidate_assignment,
              .boot_id_ = fixture.candidate_boot,
          },
      .domain_ = meta::CandidateCompatibilityDomain(progress),
  };

  BeginUncontrolled(fixture, action);
  fixture.observations.RevalidateAll(meta::MetaStoresFacts(fixture.stores),
                                     1'001);

  IdSequence premature_replacement_ids{0x9e};
  auto planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'001,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return premature_replacement_ids.Next(); }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  EXPECT_FALSE(planned->has_value());
  EXPECT_EQ(premature_replacement_ids.consumed(), 0);

  fixture.ReportCandidate(fixture.candidate, fixture.candidate_assignment,
                          fixture.candidate_boot, 1'002, {20, 20}, std::nullopt,
                          action.domain_, 1,
                          fixture.CurrentFailoverProjection());
  fixture.CompleteRecovery(1002);
  IdSequence authorize_ids{0x9f};
  planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'003,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return authorize_ids.Next(); }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  const auto* authorize =
      std::get_if<meta::AuthorizeFailoverPrepare>(&**planned);
  ASSERT_NE(authorize, nullptr);
  EXPECT_EQ(authorize->action_id_, action.action_id_);
}

TEST(MetaFailoverReconcilerPlannerTest,
     UncontrolledCandidateNewBootClearsActionWithoutWaitingForOldBoot) {
  Fixture fixture;
  const meta::MetaCandidateProgressObs progress = fixture.CandidateProgress(
      fixture.candidate, fixture.candidate_assignment, fixture.candidate_boot);
  const meta::MetaFailoverCandidateAction action{
      .action_id_ = Bytes<16>(0xa7),
      .candidate_ =
          {
              .node_id_ = fixture.candidate,
              .assignment_id_ = fixture.candidate_assignment,
              .boot_id_ = fixture.candidate_boot,
          },
      .domain_ = meta::CandidateCompatibilityDomain(progress),
  };
  BeginUncontrolled(fixture, action);
  const auto transition = fixture.Transition();
  const auto replacement_boot = Bytes<20>(0x36);
  const meta::MetaObservationIdentity replacement_identity{fixture.candidate,
                                                           replacement_boot, 2};
  ASSERT_TRUE(fixture.observations
                  .AdoptSession(replacement_identity, 1'001, Bytes<20>(0x43))
                  .ok());
  const auto replacement_heartbeat = fixture.observations.ReplaceHeartbeat(
      replacement_identity,
      {.storage_ready_ = true, .population_ready_ = true, .active_groups_ = 1},
      std::nullopt, meta::MetaStoresFacts(fixture.stores), 1'002);
  ASSERT_TRUE(replacement_heartbeat.boot_status_.ok())
      << replacement_heartbeat.boot_status_;
  ASSERT_TRUE(replacement_heartbeat.health_status_.ok())
      << replacement_heartbeat.health_status_;
  ASSERT_TRUE(replacement_heartbeat.candidate_status_.ok())
      << replacement_heartbeat.candidate_status_;

  IdSequence ids{0xa8};
  const auto planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'003,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return ids.Next(); }});

  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  const auto* replacement =
      std::get_if<meta::SetUncontrolledCandidate>(&**planned);
  ASSERT_NE(replacement, nullptr);
  EXPECT_EQ(replacement->expected_transition_,
            (meta::MetaFailoverTransitionRef{transition.transition_id_,
                                             transition.revision_}));
  EXPECT_FALSE(replacement->candidate_action_.has_value());
  EXPECT_EQ(ids.consumed(), 1);
}

TEST(MetaFailoverReconcilerPlannerTest,
     TargetTermHeartbeatOmissionMakesPreselectedCandidateReplaceable) {
  Fixture fixture;
  const meta::MetaCandidateProgressObs progress = fixture.CandidateProgress(
      fixture.candidate, fixture.candidate_assignment, fixture.candidate_boot);
  fixture.ReportCandidate(1'000);
  const meta::MetaFailoverCandidateAction action{
      .action_id_ = Bytes<16>(0xa7),
      .candidate_ =
          {
              .node_id_ = fixture.candidate,
              .assignment_id_ = fixture.candidate_assignment,
              .boot_id_ = fixture.candidate_boot,
          },
      .domain_ = meta::CandidateCompatibilityDomain(progress),
  };
  BeginUncontrolled(fixture, action);
  fixture.observations.RevalidateAll(meta::MetaStoresFacts(fixture.stores),
                                     1'001);
  fixture.ReportCandidate(fixture.candidate, fixture.candidate_assignment,
                          fixture.candidate_boot, 1'002, {10, 20}, std::nullopt,
                          action.domain_, 1,
                          fixture.CurrentFailoverProjection());

  const meta::MetaObservationIdentity identity{fixture.candidate,
                                               fixture.candidate_boot, 1};
  const auto omitted = fixture.observations.ReplaceHeartbeat(
      identity,
      {.storage_ready_ = true, .population_ready_ = false, .active_groups_ = 1},
      std::nullopt, std::nullopt, fixture.CurrentFailoverProjection(),
      meta::MetaStoresFacts(fixture.stores), 1'003);
  ASSERT_TRUE(omitted.boot_status_.ok()) << omitted.boot_status_;
  ASSERT_TRUE(omitted.health_status_.ok()) << omitted.health_status_;
  ASSERT_TRUE(omitted.candidate_status_.ok()) << omitted.candidate_status_;

  IdSequence replacement_ids{0xa8};
  const auto planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'004,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return replacement_ids.Next(); }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  const auto* replacement =
      std::get_if<meta::SetUncontrolledCandidate>(&**planned);
  ASSERT_NE(replacement, nullptr);
  EXPECT_FALSE(replacement->candidate_action_.has_value());
}

TEST(MetaFailoverReconcilerPlannerTest,
     UncontrolledReplacesFailedCandidateWithFencedHistoricalOwner) {
  Fixture fixture;
  const meta::MetaCandidateProgressObs initial_progress =
      fixture.CandidateProgress(fixture.candidate, fixture.candidate_assignment,
                                fixture.candidate_boot);
  const meta::MetaFailoverCandidateAction initial_action{
      .action_id_ = Bytes<16>(0xa9),
      .candidate_ =
          {
              .node_id_ = fixture.candidate,
              .assignment_id_ = fixture.candidate_assignment,
              .boot_id_ = fixture.candidate_boot,
          },
      .domain_ = meta::CandidateCompatibilityDomain(initial_progress),
  };
  BeginUncontrolled(fixture, initial_action);
  fixture.ReportCandidate(fixture.candidate, fixture.candidate_assignment,
                          fixture.candidate_boot, 1'001, {20, 20}, std::nullopt,
                          initial_action.domain_, 1,
                          fixture.CurrentFailoverProjection());
  fixture.observations.InvalidateCandidateOnDisconnect(
      {fixture.candidate, fixture.candidate_boot, 1}, 1'002);

  const meta::MetaFailoverCompatibilityDomain former_owner_domain{
      .source_group_term_ = 1,
      .source_node_id_ = fixture.owner,
      .source_assignment_id_ = fixture.owner_assignment,
      .source_boot_id_ = fixture.owner_boot,
      .source_history_id_ = Bytes<20>(0x43),
      .flow_count_ = 2,
  };
  fixture.ReportCandidate(fixture.owner, fixture.owner_assignment,
                          fixture.owner_boot, 1'003, {30, 30}, std::nullopt,
                          former_owner_domain);

  IdSequence replacement_ids{0xaa, 0xab};
  auto planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'004,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return replacement_ids.Next(); }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  const auto* replacement =
      std::get_if<meta::SetUncontrolledCandidate>(&**planned);
  ASSERT_NE(replacement, nullptr);
  ASSERT_TRUE(replacement->candidate_action_.has_value());
  EXPECT_EQ(replacement->candidate_action_->candidate_.node_id_, fixture.owner);
  EXPECT_EQ(replacement->candidate_action_->candidate_.assignment_id_,
            fixture.owner_assignment);
  EXPECT_EQ(replacement->candidate_action_->candidate_.boot_id_,
            fixture.owner_boot);
  EXPECT_EQ(replacement->candidate_action_->domain_, former_owner_domain);
  fixture.Accept(**planned);

  fixture.CompleteRecovery(1004);
  IdSequence authorize_ids{0xac};
  planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'005,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return authorize_ids.Next(); }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  const auto* authorize =
      std::get_if<meta::AuthorizeFailoverPrepare>(&**planned);
  ASSERT_NE(authorize, nullptr);
  EXPECT_EQ(authorize->action_id_,
            fixture.Transition().candidate_action_->action_id_);
  EXPECT_EQ(authorize->loss_if_cutover_, meta::MetaFailoverLoss::kUnknown);
}

TEST(MetaFailoverReconcilerPlannerTest,
     UncontrolledWaitsWithoutCandidateThenReplacesFailureAndCommits) {
  Fixture fixture;
  fixture.AddAlternate();
  BeginUncontrolled(fixture);

  IdSequence wait_ids{0xa0};
  auto planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'001,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return wait_ids.Next(); }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  EXPECT_FALSE(planned->has_value());
  EXPECT_EQ(wait_ids.consumed(), 0);

  fixture.ReportCandidate(fixture.candidate, fixture.candidate_assignment,
                          fixture.candidate_boot, 1'002, {20, 20});
  fixture.ReportCandidate(fixture.alternate, fixture.alternate_assignment,
                          fixture.alternate_boot, 1'002, {10, 10});
  IdSequence select_ids{0xa1, 0xa2};
  planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'003,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return select_ids.Next(); }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  const auto* select = std::get_if<meta::SetUncontrolledCandidate>(&**planned);
  ASSERT_NE(select, nullptr);
  ASSERT_TRUE(select->candidate_action_.has_value());
  EXPECT_EQ(select->candidate_action_->candidate_.node_id_, fixture.candidate);
  fixture.Accept(**planned);

  fixture.ReportActionFailed(1'004);
  IdSequence replacement_ids{0xa3, 0xa4};
  planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'005,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return replacement_ids.Next(); }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  const auto* replacement =
      std::get_if<meta::SetUncontrolledCandidate>(&**planned);
  ASSERT_NE(replacement, nullptr);
  ASSERT_TRUE(replacement->candidate_action_.has_value());
  EXPECT_EQ(replacement->candidate_action_->candidate_.node_id_,
            fixture.alternate);
  EXPECT_FALSE(replacement->candidate_action_->authorization_.has_value());
  fixture.Accept(**planned);

  fixture.CompleteRecovery(1005);
  IdSequence authorize_ids{0xa5};
  planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'006,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return authorize_ids.Next(); }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  const auto* authorize =
      std::get_if<meta::AuthorizeFailoverPrepare>(&**planned);
  ASSERT_NE(authorize, nullptr);
  EXPECT_EQ(authorize->loss_if_cutover_, meta::MetaFailoverLoss::kUnknown);
  fixture.Accept(**planned);

  const auto authorized = fixture.Transition();
  fixture.ReportPrepared(1'007, {10, 10});
  IdSequence commit_ids{0xa6};
  planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'008,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return commit_ids.Next(); }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  const auto* commit =
      std::get_if<meta::CommitUncontrolledFailover>(&**planned);
  ASSERT_NE(commit, nullptr);
  EXPECT_EQ(commit->loss_if_cutover_, meta::MetaFailoverLoss::kUnknown);
  EXPECT_EQ(commit->authorized_revision_,
            authorized.candidate_action_->authorization_->authorized_revision_);
  EXPECT_EQ(commit->new_topology_epoch_, 6);
  fixture.Accept(**planned);

  const auto group = fixture.stores.topology_.FindGroup("g1");
  ASSERT_TRUE(group.has_value());
  EXPECT_EQ(group->record_.owner_, fixture.alternate);
  EXPECT_EQ(group->record_.group_term_, 2);
  EXPECT_FALSE(group->failover_transition_.has_value());
}

TEST(MetaFailoverReconcilerPlannerTest,
     SecondFailureAfterDestructiveFullWaitsFencedWithCandidateNull) {
  Fixture fixture;
  fixture.AddAlternate();
  const meta::MetaCandidateProgressObs initial_progress =
      fixture.CandidateProgress(fixture.candidate, fixture.candidate_assignment,
                                fixture.candidate_boot);
  const meta::MetaFailoverCandidateAction initial_action{
      .action_id_ = Bytes<16>(0xb0),
      .candidate_ =
          {
              .node_id_ = fixture.candidate,
              .assignment_id_ = fixture.candidate_assignment,
              .boot_id_ = fixture.candidate_boot,
          },
      .domain_ = meta::CandidateCompatibilityDomain(initial_progress),
  };
  fixture.ReportCandidate(1'000);
  BeginUncontrolled(fixture, initial_action);
  fixture.observations.RevalidateAll(meta::MetaStoresFacts(fixture.stores),
                                     1'001);
  fixture.ReportCandidate(fixture.candidate, fixture.candidate_assignment,
                          fixture.candidate_boot, 1'002, {10, 20}, std::nullopt,
                          initial_action.domain_, 1,
                          fixture.CurrentFailoverProjection());

  fixture.CompleteRecovery(1002);
  IdSequence authorize_ids{0xb1};
  auto planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'003,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return authorize_ids.Next(); }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  ASSERT_NE(std::get_if<meta::AuthorizeFailoverPrepare>(&**planned), nullptr);
  fixture.Accept(**planned);

  fixture.ReportPrepared(1'004);
  IdSequence commit_ids{0xb2};
  planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'005,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return commit_ids.Next(); }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  ASSERT_NE(std::get_if<meta::CommitUncontrolledFailover>(&**planned), nullptr);
  fixture.Accept(**planned);

  auto group = fixture.stores.topology_.FindGroup("g1");
  ASSERT_TRUE(group.has_value());
  EXPECT_EQ(group->record_.owner_, fixture.candidate);
  EXPECT_EQ(group->record_.group_term_, 2);
  EXPECT_FALSE(group->failover_transition_.has_value());

  fixture.observations.RevalidateAll(meta::MetaStoresFacts(fixture.stores),
                                     1'006);
  const auto report_without_candidate =
      [&](const std::string& node_id, const meta::MetaBootIncarnation& boot,
          const meta::MetaReplicationHistoryId& history, std::int64_t now) {
        const meta::MetaObservationIdentity identity{node_id, boot, 1};
        ASSERT_TRUE(
            fixture.observations.AdoptSession(identity, now - 1, history).ok());
        const auto result = fixture.observations.ReplaceHeartbeat(
            identity,
            {.storage_ready_ = true,
             .population_ready_ = false,
             .active_groups_ = 1},
            std::nullopt, meta::MetaStoresFacts(fixture.stores), now);
        ASSERT_TRUE(result.boot_status_.ok()) << result.boot_status_;
        ASSERT_TRUE(result.health_status_.ok()) << result.health_status_;
        ASSERT_TRUE(result.candidate_status_.ok()) << result.candidate_status_;
      };

  // At the Data/Meta seam, a destructive FULL rebuild is represented by a
  // live non-Owner heartbeat with no candidate progress. Both followers have
  // crossed that point before the newly committed Owner fails.
  report_without_candidate(fixture.owner, fixture.owner_boot,
                           fixture.source_history, 1'007);
  report_without_candidate(fixture.alternate, fixture.alternate_boot,
                           Bytes<20>(0x44), 1'007);
  EXPECT_TRUE(fixture.observations
                  .LiveCandidateProgressFor(
                      "g1", meta::MetaStoresFacts(fixture.stores), 1'008)
                  .empty());
  fixture.observations.InvalidateCandidateOnDisconnect(
      {fixture.candidate, fixture.candidate_boot, 1}, 1'008);

  // Owner-failure detection is outside this executor's scope. Apply its
  // committed output explicitly so this test covers the recovery seam after
  // every follower has withdrawn its old recoverable population.
  BeginUncontrolled(fixture, std::nullopt, 0xd0);
  IdSequence wait_ids{0xd2};
  planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'009,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return wait_ids.Next(); }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  EXPECT_FALSE(planned->has_value());
  EXPECT_EQ(wait_ids.consumed(), 0);

  group = fixture.stores.topology_.FindGroup("g1");
  ASSERT_TRUE(group.has_value());
  ASSERT_TRUE(group->failover_transition_.has_value());
  EXPECT_EQ(group->record_.owner_, fixture.candidate);
  EXPECT_EQ(group->record_.group_term_, 3);
  const auto grant = fixture.stores.topology_.AuthorityFor("g1");
  ASSERT_TRUE(grant.has_value());
  EXPECT_FALSE(grant->grant_.has_value());
  EXPECT_FALSE(group->failover_transition_->candidate_action_.has_value());
}

TEST(MetaFailoverReconcilerPlannerTest,
     UncontrolledHealthyActionIsNotPreemptedByNewerDomain) {
  Fixture fixture;
  fixture.AddAlternate();
  BeginUncontrolled(fixture);
  fixture.ReportCandidate(fixture.candidate, fixture.candidate_assignment,
                          fixture.candidate_boot, 1'000, {10, 20});
  IdSequence select_ids{0xc0, 0xc1};
  auto planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'001,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return select_ids.Next(); }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  fixture.Accept(**planned);
  const auto current_action = *fixture.Transition().candidate_action_;

  const meta::MetaFailoverCompatibilityDomain newer{
      .source_group_term_ = 2,
      .source_node_id_ = fixture.owner,
      .source_assignment_id_ = fixture.owner_assignment,
      .source_boot_id_ = fixture.owner_boot,
      .source_history_id_ = Bytes<20>(0x7b),
      .flow_count_ = 2};
  fixture.ReportCandidate(fixture.alternate, fixture.alternate_assignment,
                          fixture.alternate_boot, 1'002, {100, 100},
                          std::nullopt, newer);

  fixture.CompleteRecovery(1003);
  IdSequence authorize_ids{0xc2};
  planned = meta::PlanFailoverStep(
      meta::MetaCommittedView(fixture.stores, fixture.next_index - 1),
      fixture.observations,
      {.now_unix_ms_ = 1'003,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return authorize_ids.Next(); }});
  ASSERT_TRUE(planned.ok()) << planned.status();
  ASSERT_TRUE(planned->has_value());
  const auto* authorize =
      std::get_if<meta::AuthorizeFailoverPrepare>(&**planned);
  ASSERT_NE(authorize, nullptr);
  EXPECT_EQ(authorize->action_id_, current_action.action_id_);
  EXPECT_EQ(authorize->loss_if_cutover_, meta::MetaFailoverLoss::kUnknown);
}

TEST(MetaFailoverReconcilerLifecycleTest,
     LostProposalReplyIsRecoveredFromCommittedTransitionAndCancellationJoins) {
  auto fixture_owner = std::make_unique<Fixture>();
  Fixture& fixture = *fixture_owner;
  fixture.SubmitControlled();

  const std::filesystem::path test_dir =
      lavik::test::TestDataDirectory() /
      ("lavik_failover_reconciler_lifecycle_" + std::to_string(::getpid()));
  std::error_code cleanup_error;
  std::filesystem::remove_all(test_dir, cleanup_error);
  auto machine_or = meta::MetaStateMachine::Open(test_dir.string());
  ASSERT_TRUE(machine_or.ok()) << machine_or.status();
  auto machine = std::move(*machine_or);
  std::uint64_t committed_index = 0;
  auto commit = [&](const meta::MetaCommand& command) {
    auto encoded = meta::MetaStateMachine::EncodeCommand(command);
    ASSERT_TRUE(encoded.ok()) << encoded.status();
    ASSERT_NE(machine->commit(++committed_index, **encoded), nullptr);
    const auto audit = machine->StoresSnapshot().audit_.Find(committed_index);
    ASSERT_TRUE(audit.has_value());
    ASSERT_EQ(audit->verdict_, meta::MetaAuditVerdict::kAccepted)
        << audit->verdict_detail_;
  };

  const meta::MetaOperationId root = Bytes<16>(0xe0);
  meta::ClusterCreateManifestV1 manifest;
  manifest.schema_version_ = 1;
  manifest.meta_members_ = {{1, "tcp://127.0.0.1:7101", "tcp://127.0.0.1:7301",
                             "tcp://127.0.0.1:7201"}};
  manifest.data_nodes_ = {{fixture.owner, "tcp://127.0.0.1:6379"},
                          {fixture.candidate, "tcp://127.0.0.1:6380"}};
  manifest.groups_ = {{"g1", fixture.owner, {fixture.candidate}}};
  manifest.slots_generated_ = true;
  manifest.slot_ranges_ = {{0, 16'383, "g1"}};
  auto creation_intent = meta::EncodeClusterCreateRequest(manifest, root);
  ASSERT_TRUE(creation_intent.ok()) << creation_intent.status();
  meta::SubmitOperation create;
  create.request_id_ = Bytes<16>(0xe1);
  create.operation_id_ = root;
  create.kind_ = std::string(meta::kMetaClusterCreateOperationKind);
  create.intent_ = *creation_intent;
  create.intent_hash_ = meta::MetaSha256(create.intent_);
  commit(meta::MetaCommand{create});
  meta::PutPolicy automatic;
  automatic.request_id_ = Bytes<16>(0x0f);
  automatic.policy_id_ =
      std::string(meta::kAutomaticUncontrolledFailoverPolicyId);
  automatic.version_ = 1;
  automatic.content_ =
      R"({"kind":"automatic-uncontrolled-failover-v1","suspect_after_ms":5000})";
  commit(meta::MetaCommand{automatic});
  meta::PutPolicy authority;
  authority.request_id_ = Bytes<16>(0x07);
  authority.policy_id_ = std::string(meta::kAuthorityLeasePolicyId);
  authority.version_ = 1;
  authority.content_ = R"({"kind":"authority-lease-v1","duration_ms":5000})";
  commit(meta::MetaCommand{authority});
  meta::PutPolicy recovery;
  recovery.request_id_ = Bytes<16>(0x0e);
  recovery.policy_id_ = std::string(meta::kCandidateRecoveryPolicyId);
  recovery.version_ = 1;
  recovery.content_ = R"({"kind":"candidate-recovery-v1","budget_ms":2000})";
  commit(meta::MetaCommand{recovery});
  meta::CompleteOperation complete;
  complete.request_id_ = Bytes<16>(0xe2);
  complete.operation_id_ = root;
  complete.expected_revision_ = 0;
  complete.result_ = "cluster-created";
  commit(meta::MetaCommand{complete});
  for (const auto& command : fixture.committed_commands) commit(command);

  meta::MetaObservationStore observations;
  bycorf::Runtime runtime;
  std::promise<absl::Status> initialized;
  auto initialized_result = initialized.get_future();
  runtime.Start(
      1,
      [&initialized](unsigned, bycorf::Worker& worker) {
        const absl::Status status = worker.Init();
        initialized.set_value(status);
        if (!status.ok()) return 1;
        worker.Run();
        worker.Shutdown();
        worker.DestroyDetachedTasks();
        return 0;
      },
      false);
  ASSERT_TRUE(initialized_result.get().ok());
  const bycorf::ForeignExecutor executor = runtime.GetForeignExecutor(0);
  meta::MetaCoordinatorOptions coordinator_options;
  coordinator_options.foreign_executor_ = executor;
  auto coordinator = std::make_unique<meta::MetaCoordinator>(
      std::shared_ptr<lavik::meta::MetaRaft>(nullptr), *machine, observations,
      coordinator_options);

  std::atomic<std::int64_t> clock{900};
  std::atomic<int> clock_calls{0};
  std::atomic<int> generated_ids{0};
  meta::MetaFailoverReconcilerOptions reconciler_options;
  reconciler_options.observation_grace_ms_ = 100;
  reconciler_options.poll_interval_ = std::chrono::milliseconds(500);
  reconciler_options.now_unix_ms_ = [&] {
    clock_calls.fetch_add(1, std::memory_order_relaxed);
    return clock.load(std::memory_order_acquire);
  };
  reconciler_options.next_id_ = [&]() -> absl::StatusOr<meta::MetaRequestId> {
    const int offset = generated_ids.fetch_add(1, std::memory_order_acq_rel);
    if (offset >= 3) {
      return absl::ResourceExhaustedError(
          "unexpected duplicate failover proposal");
    }
    return Bytes<16>(static_cast<std::uint8_t>(0xd0 + offset));
  };
  auto reconciler = std::make_shared<meta::MetaFailoverReconciler>(
      executor, std::move(reconciler_options));
  coordinator->RunAsLeader(reconciler);
  coordinator->BecomeLeader();
  ASSERT_TRUE(WaitUntil(
      [&] { return clock_calls.load(std::memory_order_acquire) >= 2; },
      std::chrono::seconds(2)));

  const auto current_owner = StoresSnapshotOnHeap(*machine);
  const meta::MetaStores& current = *current_owner;
  meta::MetaStoresFacts facts(current);
  const meta::MetaObservationIdentity owner_identity{fixture.owner,
                                                     fixture.owner_boot, 1};
  ASSERT_TRUE(
      observations.AdoptSession(owner_identity, 999, fixture.source_history)
          .ok());
  const auto owner_result = observations.ReplaceHeartbeat(
      owner_identity,
      {.storage_ready_ = true, .population_ready_ = true, .active_groups_ = 1},
      std::nullopt, facts, 1'000);
  ASSERT_TRUE(owner_result.boot_status_.ok()) << owner_result.boot_status_;
  const meta::MetaObservationIdentity candidate_identity{
      fixture.candidate, fixture.candidate_boot, 1};
  ASSERT_TRUE(
      observations.AdoptSession(candidate_identity, 999, Bytes<20>(0x43)).ok());
  const auto candidate_result = observations.ReplaceHeartbeat(
      candidate_identity,
      {.storage_ready_ = true, .population_ready_ = true, .active_groups_ = 1},
      fixture.CandidateProgress(fixture.candidate, fixture.candidate_assignment,
                                fixture.candidate_boot),
      facts, 1'000);
  ASSERT_TRUE(candidate_result.candidate_status_.ok())
      << candidate_result.candidate_status_;

  IdSequence expected_ids{0xd0, 0xd1, 0xd2};
  const auto expected = meta::PlanFailoverStep(
      meta::MetaCommittedView(current, committed_index), observations,
      {.now_unix_ms_ = 1'001,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return expected_ids.Next(); }});
  ASSERT_TRUE(expected.ok()) << expected.status();
  ASSERT_TRUE(expected->has_value());
  ASSERT_NE(std::get_if<meta::BeginControlledFailover>(&**expected), nullptr);

  clock.store(1'001, std::memory_order_release);
  ASSERT_TRUE(WaitUntil(
      [&] { return generated_ids.load(std::memory_order_acquire) == 3; },
      std::chrono::seconds(3)));
  // The no-server proposal returned failure, modeling an unusable reply. The
  // same command becoming committed through the stream must move the loop to
  // the transition-derived wait instead of generating another Begin.
  commit(**expected);
  std::this_thread::sleep_for(std::chrono::milliseconds(750));
  EXPECT_EQ(generated_ids.load(std::memory_order_acquire), 3);
  EXPECT_TRUE(machine->StoresSnapshot()
                  .topology_.FindGroup("g1")
                  ->failover_transition_.has_value());

  coordinator->BecomeFollower();
  ASSERT_TRUE(WaitUntil(
      [&] {
        return !observations.CurrentGeneration(fixture.owner).has_value();
      },
      std::chrono::seconds(3)));
  reconciler->Shutdown();
  coordinator.reset();
  reconciler.reset();
  executor.WaitUntilIdle();
  runtime.RequestStop();
  runtime.WaitUntilStopped();
  EXPECT_EQ(runtime.exit_code(), 0);
  machine.reset();
  std::filesystem::remove_all(test_dir, cleanup_error);
}

TEST(MetaFailoverReconcilerLifecycleTest,
     LostCutoverReplyConvergesFromCommittedControlledCommit) {
  auto fixture_owner = std::make_unique<Fixture>();
  Fixture& fixture = *fixture_owner;
  fixture.SubmitControlled();

  const std::filesystem::path test_dir =
      lavik::test::TestDataDirectory() /
      ("lavik_failover_reconciler_cutover_lifecycle_" +
       std::to_string(::getpid()));
  std::error_code cleanup_error;
  std::filesystem::remove_all(test_dir, cleanup_error);
  auto machine_or = meta::MetaStateMachine::Open(test_dir.string());
  ASSERT_TRUE(machine_or.ok()) << machine_or.status();
  auto machine = std::move(*machine_or);

  std::uint64_t committed_index = 0;
  auto commit = [&](const meta::MetaCommand& command) {
    auto encoded = meta::MetaStateMachine::EncodeCommand(command);
    ASSERT_TRUE(encoded.ok()) << encoded.status();
    ASSERT_NE(machine->commit(++committed_index, **encoded), nullptr);
    const auto audit = machine->StoresSnapshot().audit_.Find(committed_index);
    ASSERT_TRUE(audit.has_value());
    ASSERT_EQ(audit->verdict_, meta::MetaAuditVerdict::kAccepted)
        << audit->verdict_detail_;
  };

  const meta::MetaOperationId root = Bytes<16>(0xe4);
  meta::ClusterCreateManifestV1 manifest;
  manifest.schema_version_ = 1;
  manifest.meta_members_ = {{1, "tcp://127.0.0.1:7101", "tcp://127.0.0.1:7301",
                             "tcp://127.0.0.1:7201"}};
  manifest.data_nodes_ = {{fixture.owner, "tcp://127.0.0.1:6379"},
                          {fixture.candidate, "tcp://127.0.0.1:6380"}};
  manifest.groups_ = {{"g1", fixture.owner, {fixture.candidate}}};
  manifest.slots_generated_ = true;
  manifest.slot_ranges_ = {{0, 16'383, "g1"}};
  auto creation_intent = meta::EncodeClusterCreateRequest(manifest, root);
  ASSERT_TRUE(creation_intent.ok()) << creation_intent.status();
  meta::SubmitOperation create;
  create.request_id_ = Bytes<16>(0xe5);
  create.operation_id_ = root;
  create.kind_ = std::string(meta::kMetaClusterCreateOperationKind);
  create.intent_ = *creation_intent;
  create.intent_hash_ = meta::MetaSha256(create.intent_);
  commit(meta::MetaCommand{create});
  meta::PutPolicy automatic;
  automatic.request_id_ = Bytes<16>(0x0f);
  automatic.policy_id_ =
      std::string(meta::kAutomaticUncontrolledFailoverPolicyId);
  automatic.version_ = 1;
  automatic.content_ =
      R"({"kind":"automatic-uncontrolled-failover-v1","suspect_after_ms":5000})";
  commit(meta::MetaCommand{automatic});
  meta::PutPolicy authority;
  authority.request_id_ = Bytes<16>(0x07);
  authority.policy_id_ = std::string(meta::kAuthorityLeasePolicyId);
  authority.version_ = 1;
  authority.content_ = R"({"kind":"authority-lease-v1","duration_ms":5000})";
  commit(meta::MetaCommand{authority});
  meta::PutPolicy recovery;
  recovery.request_id_ = Bytes<16>(0x0e);
  recovery.policy_id_ = std::string(meta::kCandidateRecoveryPolicyId);
  recovery.version_ = 1;
  recovery.content_ = R"({"kind":"candidate-recovery-v1","budget_ms":2000})";
  commit(meta::MetaCommand{recovery});
  meta::CompleteOperation complete;
  complete.request_id_ = Bytes<16>(0xe6);
  complete.operation_id_ = root;
  complete.expected_revision_ = 0;
  complete.result_ = "cluster-created";
  commit(meta::MetaCommand{complete});
  for (const auto& command : fixture.committed_commands) commit(command);

  meta::MetaObservationStore observations;
  const meta::MetaObservationIdentity owner_identity{fixture.owner,
                                                     fixture.owner_boot, 1};
  ASSERT_TRUE(
      observations.AdoptSession(owner_identity, 999, fixture.source_history)
          .ok());
  const meta::MetaObservationIdentity candidate_identity{
      fixture.candidate, fixture.candidate_boot, 1};
  ASSERT_TRUE(
      observations.AdoptSession(candidate_identity, 999, Bytes<20>(0x43)).ok());

  const auto submitted_owner = StoresSnapshotOnHeap(*machine);
  const meta::MetaStores& submitted = *submitted_owner;
  const meta::MetaStoresFacts submitted_facts(submitted);
  auto owner_result = observations.ReplaceHeartbeat(
      owner_identity,
      {.storage_ready_ = true, .population_ready_ = true, .active_groups_ = 1},
      std::nullopt, submitted_facts, 1'000);
  ASSERT_TRUE(owner_result.boot_status_.ok()) << owner_result.boot_status_;
  auto candidate_result = observations.ReplaceHeartbeat(
      candidate_identity,
      {.storage_ready_ = true, .population_ready_ = true, .active_groups_ = 1},
      fixture.CandidateProgress(fixture.candidate, fixture.candidate_assignment,
                                fixture.candidate_boot),
      submitted_facts, 1'000);
  ASSERT_TRUE(candidate_result.candidate_status_.ok())
      << candidate_result.candidate_status_;

  IdSequence begin_ids{0xe7, 0xe8, 0xe9};
  auto begin = meta::PlanFailoverStep(
      meta::MetaCommittedView(submitted, committed_index), observations,
      {.now_unix_ms_ = 1'001,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return begin_ids.Next(); }});
  ASSERT_TRUE(begin.ok()) << begin.status();
  ASSERT_TRUE(begin->has_value());
  ASSERT_NE(std::get_if<meta::BeginControlledFailover>(&**begin), nullptr);
  commit(**begin);

  const auto begun_owner = StoresSnapshotOnHeap(*machine);
  const meta::MetaStores& begun = *begun_owner;
  const auto begun_group = begun.topology_.FindGroup("g1");
  ASSERT_TRUE(begun_group.has_value());
  ASSERT_TRUE(begun_group->failover_transition_.has_value());
  const auto& begun_transition = *begun_group->failover_transition_;
  ASSERT_TRUE(begun_transition.candidate_action_.has_value());
  const auto& domain = begun_transition.candidate_action_->domain_;
  meta::MetaSourcePausedObs paused{
      .group_id_ = "g1",
      .transition_id_ = begun_transition.transition_id_,
      .source_node_id_ = domain.source_node_id_,
      .source_assignment_id_ = domain.source_assignment_id_,
      .source_boot_id_ = domain.source_boot_id_,
      .source_history_id_ = domain.source_history_id_,
      .source_group_term_ = domain.source_group_term_,
      .stable_next_lsns_ = {10, 20}};
  owner_result = observations.ReplaceHeartbeat(
      owner_identity,
      {.storage_ready_ = true, .population_ready_ = true, .active_groups_ = 1},
      std::nullopt, meta::MetaFailoverObservationObs{.payload_ = paused},
      meta::MetaStoresFacts(begun), 1'010);
  ASSERT_TRUE(owner_result.failover_status_.ok())
      << owner_result.failover_status_;

  IdSequence authorize_ids{0xea};
  auto authorize = meta::PlanFailoverStep(
      meta::MetaCommittedView(begun, committed_index), observations,
      {.now_unix_ms_ = 1'011,
       .leadership_started_unix_ms_ = 900,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return authorize_ids.Next(); }});
  ASSERT_TRUE(authorize.ok()) << authorize.status();
  ASSERT_TRUE(authorize->has_value());
  ASSERT_NE(std::get_if<meta::AuthorizeFailoverPrepare>(&**authorize), nullptr);
  commit(**authorize);

  const auto authorized_owner = StoresSnapshotOnHeap(*machine);
  const meta::MetaStores& authorized = *authorized_owner;
  const auto authorized_group = authorized.topology_.FindGroup("g1");
  ASSERT_TRUE(authorized_group.has_value());
  ASSERT_TRUE(authorized_group->failover_transition_.has_value());
  const auto& authorized_transition = *authorized_group->failover_transition_;
  ASSERT_TRUE(authorized_transition.candidate_action_.has_value());
  const auto& action = *authorized_transition.candidate_action_;
  meta::MetaCandidatePreparedObs prepared{
      .group_id_ = "g1",
      .transition_id_ = authorized_transition.transition_id_,
      .action_id_ = action.action_id_,
      .candidate_node_id_ = action.candidate_.node_id_,
      .candidate_assignment_id_ = action.candidate_.assignment_id_,
      .candidate_boot_id_ = action.candidate_.boot_id_,
      .prepared_context_id_ = Bytes<16>(0xeb)};
  candidate_result = observations.ReplaceHeartbeat(
      candidate_identity,
      {.storage_ready_ = true, .population_ready_ = true, .active_groups_ = 1},
      fixture.CandidateProgress(fixture.candidate, fixture.candidate_assignment,
                                fixture.candidate_boot, {10, 20},
                                action.domain_),
      meta::MetaFailoverObservationObs{.payload_ = prepared},
      meta::MetaStoresFacts(authorized), 1'012);
  ASSERT_TRUE(candidate_result.candidate_status_.ok())
      << candidate_result.candidate_status_;
  ASSERT_TRUE(candidate_result.failover_status_.ok())
      << candidate_result.failover_status_;

  const auto before_cutover_owner = StoresSnapshotOnHeap(*machine);
  const meta::MetaStores& before_cutover = *before_cutover_owner;
  IdSequence expected_ids{0xf0};
  const auto expected = meta::PlanFailoverStep(
      meta::MetaCommittedView(before_cutover, committed_index), observations,
      {.now_unix_ms_ = 1'013,
       .leadership_started_unix_ms_ = 1'013,
       .observation_grace_ms_ = 100,
       .next_id_ = [&] { return expected_ids.Next(); }});
  ASSERT_TRUE(expected.ok()) << expected.status();
  ASSERT_TRUE(expected->has_value());
  ASSERT_NE(std::get_if<meta::CommitControlledFailover>(&**expected), nullptr);
  EXPECT_EQ(expected_ids.consumed(), 1);

  bycorf::Runtime runtime;
  std::promise<absl::Status> initialized;
  auto initialized_result = initialized.get_future();
  runtime.Start(
      1,
      [&initialized](unsigned, bycorf::Worker& worker) {
        const absl::Status status = worker.Init();
        initialized.set_value(status);
        if (!status.ok()) return 1;
        worker.Run();
        worker.Shutdown();
        worker.DestroyDetachedTasks();
        return 0;
      },
      false);
  ASSERT_TRUE(initialized_result.get().ok());
  const bycorf::ForeignExecutor executor = runtime.GetForeignExecutor(0);
  meta::MetaCoordinatorOptions coordinator_options;
  coordinator_options.foreign_executor_ = executor;
  auto coordinator = std::make_unique<meta::MetaCoordinator>(
      std::shared_ptr<lavik::meta::MetaRaft>(nullptr), *machine, observations,
      coordinator_options);

  std::atomic<std::int64_t> clock{900};
  std::atomic<int> clock_calls{0};
  std::atomic<int> generated_ids{0};
  meta::MetaFailoverReconcilerOptions reconciler_options;
  reconciler_options.observation_grace_ms_ = 100;
  reconciler_options.poll_interval_ = std::chrono::milliseconds(500);
  reconciler_options.now_unix_ms_ = [&] {
    clock_calls.fetch_add(1, std::memory_order_relaxed);
    return clock.load(std::memory_order_acquire);
  };
  reconciler_options.next_id_ = [&]() -> absl::StatusOr<meta::MetaRequestId> {
    const int offset = generated_ids.fetch_add(1, std::memory_order_acq_rel);
    if (offset != 0) {
      return absl::ResourceExhaustedError(
          "unexpected duplicate controlled cutover proposal");
    }
    return Bytes<16>(0xf0);
  };
  auto reconciler = std::make_shared<meta::MetaFailoverReconciler>(
      executor, std::move(reconciler_options));
  coordinator->RunAsLeader(reconciler);
  coordinator->BecomeLeader();
  ASSERT_TRUE(WaitUntil(
      [&] { return clock_calls.load(std::memory_order_acquire) >= 2; },
      std::chrono::seconds(2)));

  // Leadership changes intentionally discard soft observations. Re-report
  // the exact source and Prepared action against the already-authorized
  // committed transition before allowing the driver to plan cutover.
  ASSERT_TRUE(
      observations.AdoptSession(owner_identity, 1'009, fixture.source_history)
          .ok());
  ASSERT_TRUE(
      observations.AdoptSession(candidate_identity, 1'009, Bytes<20>(0x43))
          .ok());
  owner_result = observations.ReplaceHeartbeat(
      owner_identity,
      {.storage_ready_ = true, .population_ready_ = true, .active_groups_ = 1},
      std::nullopt, meta::MetaStoresFacts(before_cutover), 1'010);
  ASSERT_TRUE(owner_result.boot_status_.ok()) << owner_result.boot_status_;
  candidate_result = observations.ReplaceHeartbeat(
      candidate_identity,
      {.storage_ready_ = true, .population_ready_ = true, .active_groups_ = 1},
      fixture.CandidateProgress(fixture.candidate, fixture.candidate_assignment,
                                fixture.candidate_boot, {10, 20},
                                action.domain_),
      meta::MetaFailoverObservationObs{.payload_ = prepared},
      meta::MetaStoresFacts(before_cutover), 1'012);
  ASSERT_TRUE(candidate_result.candidate_status_.ok())
      << candidate_result.candidate_status_;
  ASSERT_TRUE(candidate_result.failover_status_.ok())
      << candidate_result.failover_status_;

  clock.store(1'013, std::memory_order_release);
  ASSERT_TRUE(WaitUntil(
      [&] { return generated_ids.load(std::memory_order_acquire) == 1; },
      std::chrono::seconds(3)));

  // A no-server proposal has no usable reply. Committing that exact cutover
  // through the state-machine stream must make the level-triggered driver
  // observe terminal committed state instead of proposing Commit again.
  commit(**expected);
  ASSERT_TRUE(WaitUntil(
      [&] {
        const auto stores = machine->StoresSnapshot();
        const auto group = stores.topology_.FindGroup("g1");
        const auto operation =
            stores.operation_.FindOperation(fixture.operation_id);
        return group.has_value() && !group->failover_transition_.has_value() &&
               group->record_.owner_ == fixture.candidate &&
               operation.has_value() &&
               operation->lifecycle_ ==
                   meta::MetaOperationLifecycle::kCompleted;
      },
      std::chrono::seconds(3)));
  std::this_thread::sleep_for(std::chrono::milliseconds(750));
  EXPECT_EQ(generated_ids.load(std::memory_order_acquire), 1);

  const auto after_cutover_owner = StoresSnapshotOnHeap(*machine);
  const meta::MetaStores& after_cutover = *after_cutover_owner;
  const auto group = after_cutover.topology_.FindGroup("g1");
  ASSERT_TRUE(group.has_value());
  EXPECT_EQ(group->record_.owner_, fixture.candidate);
  EXPECT_EQ(group->record_.group_term_, 2);
  EXPECT_FALSE(group->failover_transition_.has_value());
  const auto operation =
      after_cutover.operation_.FindOperation(fixture.operation_id);
  ASSERT_TRUE(operation.has_value());
  EXPECT_EQ(operation->lifecycle_, meta::MetaOperationLifecycle::kCompleted);

  coordinator->BecomeFollower();
  ASSERT_TRUE(WaitUntil(
      [&] {
        return !observations.CurrentGeneration(fixture.owner).has_value();
      },
      std::chrono::seconds(3)));
  reconciler->Shutdown();
  coordinator.reset();
  reconciler.reset();
  executor.WaitUntilIdle();
  runtime.RequestStop();
  runtime.WaitUntilStopped();
  EXPECT_EQ(runtime.exit_code(), 0);
  machine.reset();
  std::filesystem::remove_all(test_dir, cleanup_error);
}

}  // namespace
